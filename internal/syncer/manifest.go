package syncer

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"synct/internal/config"
)

const (
	// tombstoneRetention bounds how long a deletion record is kept. It is long
	// enough for a peer that is offline for a month to still learn about deletes,
	// while keeping data/state.json and memory bounded.
	tombstoneRetention = 30 * 24 * time.Hour
	// tombstoneLimit caps the number of deletion records kept in memory/on disk.
	tombstoneLimit = 10000
	// saveDebounce coalesces bursts of index mutations into one atomic state write.
	saveDebounce = 500 * time.Millisecond
)

// FileStatus describes the synchronization state of a single file in the UI.
type FileStatus string

const (
	StatusSynced      FileStatus = "已同步"
	StatusUploading   FileStatus = "上传中"
	StatusDownloading FileStatus = "下载中"
	StatusPending     FileStatus = "待同步"
	StatusConflict    FileStatus = "冲突已备份"
	StatusError       FileStatus = "错误"
)

// FileMeta holds metadata for a single relative file path inside a synced folder.
type FileMeta struct {
	FolderID string `json:"folder_id,omitempty"` // Logical folder mapping ID ("default" or custom ID)
	RelPath  string `json:"rel_path"`            // Virtual relative path ("file.txt" for default, "@<id>/file.txt" for mapped folders)
	Size     int64  `json:"size"`
	ModTime  int64  `json:"mod_time"` // Unix milliseconds (UTC)
	SHA256   string `json:"sha256"`
}

// DirMeta is one directory inside a synced folder. Directories are tracked so
// an empty folder created on one computer also appears on the other; files
// alone would only carry their parent directories implicitly.
type DirMeta struct {
	FolderID string `json:"folder_id,omitempty"`
	RelPath  string `json:"rel_path"`
	ModTime  int64  `json:"mod_time,omitempty"`
}

// Tombstone records a file deletion so peers can distinguish a deleted file from a new file.
type Tombstone struct {
	FolderID  string `json:"folder_id,omitempty"`
	RelPath   string `json:"rel_path"`
	DeletedAt int64  `json:"deleted_at"` // Unix milliseconds (UTC)
	LastHash  string `json:"last_hash,omitempty"`
}

// Manifest is the complete snapshot of all synced folders at a point in time.
type Manifest struct {
	NodeID      string                 `json:"node_id"`
	GeneratedAt int64                  `json:"generated_at"`
	Folders     []config.FolderMapping `json:"folders,omitempty"`
	Dirs        map[string]DirMeta     `json:"dirs,omitempty"`
	Files       map[string]FileMeta    `json:"files"`
	Tombstones  map[string]Tombstone   `json:"tombstones"`
}

// NewManifest creates an empty initialized Manifest.
func NewManifest(nodeID string) Manifest {
	return Manifest{
		NodeID:      nodeID,
		GeneratedAt: time.Now().UTC().UnixMilli(),
		Dirs:        make(map[string]DirMeta),
		Files:       make(map[string]FileMeta),
		Tombstones:  make(map[string]Tombstone),
	}
}

// Clone returns a deep copy of the Manifest.
func (m Manifest) Clone() Manifest {
	out := NewManifest(m.NodeID)
	out.GeneratedAt = m.GeneratedAt
	if len(m.Folders) > 0 {
		out.Folders = append([]config.FolderMapping(nil), m.Folders...)
	}
	for k, v := range m.Dirs {
		out.Dirs[k] = v
	}
	for k, v := range m.Files {
		out.Files[k] = v
	}
	for k, v := range m.Tombstones {
		out.Tombstones[k] = v
	}
	return out
}

// FileEntry is a view-friendly entry for displaying in the UI file table.
type FileEntry struct {
	FolderID    string
	FolderLabel string
	InnerPath   string
	RelPath     string
	Size        int64
	ModTime     time.Time
	SHA256      string
	Status      FileStatus
	Direction   string // "本地", "远端 -> 本地", "本地 -> 远端", "双向一致"
}

// JoinFolderRelPath builds a canonical virtual relative path from a folder mapping ID and inner path.
// For the default folder ("default"), it returns innerRel directly for backward compatibility.
// For additional mapped folders, it prefixes with "@<folderID>/".
func JoinFolderRelPath(folderID, innerRel string) string {
	folderID = config.NormalizeFolderID(folderID)
	innerRel = filepath.ToSlash(strings.TrimPrefix(strings.TrimSpace(innerRel), "/"))
	if folderID == "" || folderID == config.DefaultFolderID {
		return innerRel
	}
	return "@" + folderID + "/" + innerRel
}

// SplitFolderRelPath splits a virtual relative path into (folderID, innerRelPath).
func SplitFolderRelPath(virtualRel string) (string, string) {
	virtualRel = filepath.ToSlash(strings.TrimPrefix(strings.TrimSpace(virtualRel), "/"))
	if strings.HasPrefix(virtualRel, "@") {
		if idx := strings.IndexByte(virtualRel, '/'); idx > 1 {
			id := config.NormalizeFolderID(virtualRel[1:idx])
			inner := strings.TrimPrefix(virtualRel[idx+1:], "/")
			return id, inner
		}
	}
	return config.DefaultFolderID, virtualRel
}

// ResolveFolderFullPath resolves a virtual relative path against the active FolderMappings in cfg.
func ResolveFolderFullPath(cfg config.Config, virtualRel string) (config.FolderMapping, string, string, bool) {
	cleaned, ok := CleanRelPath(virtualRel)
	if !ok {
		return config.FolderMapping{}, "", "", false
	}
	folderID, inner := SplitFolderRelPath(cleaned)
	innerCleaned, ok := CleanRelPath(inner)
	if !ok {
		return config.FolderMapping{}, "", "", false
	}
	folder, found := cfg.FindEnabledFolder(folderID)
	if !found || strings.TrimSpace(folder.LocalPath) == "" {
		return config.FolderMapping{}, "", "", false
	}
	fullPath := filepath.Join(folder.LocalPath, filepath.FromSlash(innerCleaned))
	if !isPathWithinRoot(folder.LocalPath, fullPath) {
		return config.FolderMapping{}, "", "", false
	}
	return folder, innerCleaned, fullPath, true
}

// ignoreMatcher is a precompiled ignore-rule matcher. Compiling once per scan
// avoids splitting and normalizing every pattern for every directory entry.
type ignoreMatcher struct {
	plain map[string]struct{}
	globs []string
}

func newIgnoreMatcher(patterns []string) *ignoreMatcher {
	m := &ignoreMatcher{plain: make(map[string]struct{}, len(patterns))}
	for _, raw := range patterns {
		pat := strings.TrimSpace(filepath.ToSlash(raw))
		if pat == "" {
			continue
		}
		if runtime.GOOS == "windows" {
			pat = strings.ToLower(pat)
		}
		if strings.ContainsAny(pat, "*?[") {
			m.globs = append(m.globs, pat)
		} else {
			m.plain[pat] = struct{}{}
		}
	}
	return m
}

// Match reports whether a slash-normalized relative path matches any ignore rule.
// Plain names are matched by map lookup against the full path and every segment,
// which covers the common ".git", "node_modules" and "Thumbs.db" cases cheaply.
func (m *ignoreMatcher) Match(rel string, _ bool) bool {
	if m == nil {
		return false
	}
	rel = filepath.ToSlash(strings.TrimPrefix(strings.TrimSpace(rel), "/"))
	if rel == "" || rel == "." {
		return false
	}
	if runtime.GOOS == "windows" {
		rel = strings.ToLower(rel)
	}
	base := path.Base(rel)
	if strings.HasPrefix(base, ".synct") {
		return true
	}
	if len(m.plain) > 0 {
		if _, ok := m.plain[base]; ok {
			return true
		}
		if _, ok := m.plain[rel]; ok {
			return true
		}
		for _, seg := range strings.Split(rel, "/") {
			if _, ok := m.plain[seg]; ok {
				return true
			}
		}
	}
	if len(m.globs) > 0 {
		for _, pat := range m.globs {
			if matched, _ := path.Match(pat, base); matched {
				return true
			}
			if matched, _ := path.Match(pat, rel); matched {
				return true
			}
			for _, seg := range strings.Split(rel, "/") {
				if matched, _ := path.Match(pat, seg); matched {
					return true
				}
			}
		}
	}
	return false
}

// ShouldIgnore checks if a slash-normalized relative path matches any ignore pattern.
func ShouldIgnore(relPath string, isDir bool, patterns []string) bool {
	_, inner := SplitFolderRelPath(relPath)
	return newIgnoreMatcher(patterns).Match(inner, isDir)
}

// MatchVirtual matches a virtual relative path (with its optional @folderID
// prefix) against the compiled rules. It avoids recompiling patterns inside
// reconciliation loops that run once per file.
func (m *ignoreMatcher) MatchVirtual(virtualRel string, isDir bool) bool {
	_, inner := SplitFolderRelPath(virtualRel)
	return m.Match(inner, isDir)
}

// IndexStore caches file hashes by (RelPath, Size, ModTime) and persists state/tombstones.
type IndexStore struct {
	mu           sync.RWMutex
	nodeID       string
	statePath    string
	folders      []config.FolderMapping
	files        map[string]FileMeta
	dirs         map[string]DirMeta
	tombstones   map[string]Tombstone
	statuses     map[string]FileStatus
	statusCounts map[FileStatus]int
	directions   map[string]string
	addedDirs    []DirMeta
	// newFilesPending marks newly scanned files 待同步 until a transfer
	// confirms them. The host serves its files directly, so only a client
	// that is allowed to push treats new local files as pending.
	newFilesPending bool
	// hashCache is created lazily so a zero-value IndexStore (as some callers
	// still construct in tests) behaves exactly as it did before.
	hashCache *fileHashCache

	// folderStats and the global totals let Stats() run in O(1) instead of
	// hashing every tracked file on every UI frame.
	folderStats map[string]folderStat
	folderRoots map[string]string
	fileCount   int
	totalBytes  int64
	// pendingWrites holds paths whose transfer is in flight. Entries never
	// expire on a timer: a slow download can legally take up to an hour, and
	// expiring the guard would let a poll scan tombstone the file and propagate
	// a deletion for content that is still being written.
	pendingWrites     map[string]struct{}
	generation        uint64
	entries           []FileEntry
	entriesGeneration uint64

	// scanMu serializes directory walks inside one store so concurrent watcher,
	// manual and reconciliation scans cannot create duplicate tombstones.
	scanMu sync.Mutex

	saveMu     sync.Mutex
	saveExecMu sync.Mutex
	saveTimer  *time.Timer
	savedGen   uint64
	savedOnce  bool
	loadErr    error

	onSaveError func(error)
}

type folderStat struct {
	files int
	bytes int64
}

type persistedState struct {
	NodeID      string               `json:"node_id"`
	Files       map[string]FileMeta  `json:"files"`
	Dirs        map[string]DirMeta   `json:"dirs,omitempty"`
	Tombstones  map[string]Tombstone `json:"tombstones"`
	FolderRoots map[string]string    `json:"folder_roots,omitempty"`
	SavedAt     int64                `json:"saved_at,omitempty"`
}

// NewIndexStore creates or loads a persistent IndexStore.
func NewIndexStore(statePath string) *IndexStore {
	s := &IndexStore{
		nodeID:        newIndexNodeID(),
		statePath:     statePath,
		files:         make(map[string]FileMeta),
		dirs:          make(map[string]DirMeta),
		tombstones:    make(map[string]Tombstone),
		statuses:      make(map[string]FileStatus),
		statusCounts:  make(map[FileStatus]int),
		directions:    make(map[string]string),
		folderStats:   make(map[string]folderStat),
		folderRoots:   make(map[string]string),
		pendingWrites: make(map[string]struct{}),
		hashCache:     newFileHashCache(),
	}
	s.load()
	return s
}

// newIndexNodeID builds a node identity that is unique even when two instances
// start in the same millisecond; origin filtering depends on it being distinct.
func newIndexNodeID() string {
	base := "node-" + time.Now().UTC().Format("150405.000")
	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err == nil {
		return base + "-" + hex.EncodeToString(suffix[:])
	}
	return base
}

// LoadError reports a state file that could not be read or decoded.
func (s *IndexStore) LoadError() error { return s.loadErr }

// SetSaveErrorHandler installs a callback for background (debounced) save failures.
func (s *IndexStore) SetSaveErrorHandler(fn func(error)) { s.onSaveError = fn }

func (s *IndexStore) load() {
	if s.statePath == "" {
		return
	}
	data, err := os.ReadFile(s.statePath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			s.loadErr = fmt.Errorf("读取状态索引失败: %w", err)
		}
		return
	}
	var ps persistedState
	if err := json.Unmarshal(data, &ps); err != nil {
		// Preserve the damaged file instead of silently overwriting it on the next save.
		backup := s.statePath + ".corrupt-" + time.Now().Format("20060102-150405")
		_ = os.Rename(s.statePath, backup)
		s.loadErr = fmt.Errorf("状态索引损坏，已备份至 %s: %w", filepath.Base(backup), err)
		return
	}
	if ps.NodeID != "" {
		s.nodeID = ps.NodeID
	}
	if ps.Files != nil && len(ps.Files) > 0 {
		s.files = ps.Files
	}
	if ps.Dirs != nil && len(ps.Dirs) > 0 {
		s.dirs = ps.Dirs
	}
	if ps.Tombstones != nil && len(ps.Tombstones) > 0 {
		s.tombstones = ps.Tombstones
	}
	if ps.FolderRoots != nil {
		s.folderRoots = ps.FolderRoots
	}
	for rel, meta := range s.files {
		fid, _ := SplitFolderRelPath(rel)
		st := s.folderStats[fid]
		st.files++
		st.bytes += meta.Size
		s.folderStats[fid] = st
		s.fileCount++
		s.totalBytes += meta.Size
		// Entries restored from the last run were synced before the app was
		// closed; the first reconciliation re-checks them. Only files that the
		// scanner discovers or sees change afterwards start as pending.
		s.statuses[rel] = StatusSynced
		s.statusCounts[StatusSynced]++
	}
	s.mu.Lock()
	s.pruneTombstonesLocked(time.Now().UTC().UnixMilli())
	s.mu.Unlock()
	s.savedOnce = true
}

// Save persists the current index and tombstones to disk atomically.
func (s *IndexStore) Save() error {
	s.saveMu.Lock()
	if s.saveTimer != nil {
		s.saveTimer.Stop()
		s.saveTimer = nil
	}
	s.saveMu.Unlock()
	return s.saveNow()
}

// RequestSave schedules an atomic state write after a short debounce.
// A burst of file transfers therefore costs one state write, not one per file.
func (s *IndexStore) RequestSave() {
	if s.statePath == "" {
		return
	}
	s.saveMu.Lock()
	if s.saveTimer != nil {
		s.saveTimer.Reset(saveDebounce)
		s.saveMu.Unlock()
		return
	}
	s.saveTimer = time.AfterFunc(saveDebounce, func() {
		s.saveMu.Lock()
		s.saveTimer = nil
		s.saveMu.Unlock()
		if err := s.saveNow(); err != nil && s.onSaveError != nil {
			s.onSaveError(err)
		}
	})
	s.saveMu.Unlock()
}

// Flush writes any pending state immediately.
func (s *IndexStore) Flush() error { return s.Save() }

func (s *IndexStore) saveNow() error {
	if s.statePath == "" {
		return nil
	}
	s.saveExecMu.Lock()
	defer s.saveExecMu.Unlock()

	s.mu.RLock()
	gen := s.generation
	if s.savedOnce && gen == s.savedGen {
		s.mu.RUnlock()
		return nil
	}
	ps := persistedState{
		NodeID:      s.nodeID,
		Files:       make(map[string]FileMeta, len(s.files)),
		Dirs:        make(map[string]DirMeta, len(s.dirs)),
		Tombstones:  make(map[string]Tombstone, len(s.tombstones)),
		FolderRoots: make(map[string]string, len(s.folderRoots)),
		SavedAt:     time.Now().UTC().UnixMilli(),
	}
	for k, v := range s.files {
		ps.Files[k] = v
	}
	for k, v := range s.dirs {
		ps.Dirs[k] = v
	}
	for k, v := range s.tombstones {
		ps.Tombstones[k] = v
	}
	for k, v := range s.folderRoots {
		ps.FolderRoots[k] = v
	}
	s.mu.RUnlock()

	data, err := json.Marshal(ps)
	if err != nil {
		return fmt.Errorf("编码状态索引失败: %w", err)
	}
	tmp := s.statePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("写入状态索引失败: %w", err)
	}
	if err := os.Rename(tmp, s.statePath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("提交状态索引失败: %w", err)
	}
	s.mu.Lock()
	s.savedGen = gen
	s.savedOnce = true
	s.mu.Unlock()
	return nil
}

// NodeID returns this node's unique identifier.
func (s *IndexStore) NodeID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nodeID
}

// Generation returns a monotonically increasing version of the index contents.
func (s *IndexStore) Generation() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.generation
}

// Counts returns (file count, total bytes) in O(1).
func (s *IndexStore) Counts() (int, int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fileCount, s.totalBytes
}

// FolderFileCount returns how many indexed files belong to one folder mapping.
func (s *IndexStore) FolderFileCount(folderID string) int {
	folderID = config.NormalizeFolderID(folderID)
	s.mu.RLock()
	count := s.folderStats[folderID].files
	s.mu.RUnlock()
	return count
}

// FileMeta returns one indexed file without copying the whole manifest.
func (s *IndexStore) FileMeta(relPath string) (FileMeta, bool) {
	relPath = filepath.ToSlash(strings.TrimSpace(relPath))
	s.mu.RLock()
	meta, ok := s.files[relPath]
	s.mu.RUnlock()
	return meta, ok
}

// Tombstone returns the deletion record for a path, if one exists.
func (s *IndexStore) Tombstone(relPath string) (Tombstone, bool) {
	relPath = filepath.ToSlash(strings.TrimSpace(relPath))
	s.mu.RLock()
	tomb, ok := s.tombstones[relPath]
	s.mu.RUnlock()
	return tomb, ok
}

// BeginWrite marks a path as being written so a concurrent scan does not
// mistake the not-yet-renamed file for a deletion.
func (s *IndexStore) BeginWrite(relPath string) {
	relPath = filepath.ToSlash(strings.TrimSpace(relPath))
	if relPath == "" {
		return
	}
	s.mu.Lock()
	s.pendingWrites[relPath] = struct{}{}
	s.mu.Unlock()
}

// EndWrite clears the write guard installed by BeginWrite.
func (s *IndexStore) EndWrite(relPath string) {
	relPath = filepath.ToSlash(strings.TrimSpace(relPath))
	if relPath == "" {
		return
	}
	s.mu.Lock()
	delete(s.pendingWrites, relPath)
	s.mu.Unlock()
}

// ScanDirectory walks a single rootDir as the default folder mapping.
func (s *IndexStore) ScanDirectory(rootDir string, ignorePatterns []string) (Manifest, []FileMeta, []Tombstone, error) {
	return s.ScanFolders([]config.FolderMapping{
		{
			ID:        config.DefaultFolderID,
			Label:     "主同步目录",
			LocalPath: rootDir,
			Enabled:   true,
		},
	}, ignorePatterns)
}

// ScanFolders walks all enabled FolderMappings and returns the resulting Manifest.
// It is kept for callers that need the full snapshot; hot-path callers use
// ScanFoldersIncremental to avoid copying every map on every scan.
func (s *IndexStore) ScanFolders(folders []config.FolderMapping, ignorePatterns []string) (Manifest, []FileMeta, []Tombstone, error) {
	changed, deleted, err := s.ScanFoldersIncremental(folders, ignorePatterns)
	return s.Snapshot(), changed, deleted, err
}

// ScanFoldersIncremental walks every enabled folder, updates only changed
// entries, and returns the changed files and new deletion records. Files in a
// folder that cannot be read are left untouched instead of being tombstoned.
func (s *IndexStore) ScanFoldersIncremental(folders []config.FolderMapping, ignorePatterns []string) ([]FileMeta, []Tombstone, error) {
	s.scanMu.Lock()
	defer s.scanMu.Unlock()

	matcher := newIgnoreMatcher(ignorePatterns)
	active := make([]config.FolderMapping, 0, len(folders))
	activeIDs := make(map[string]struct{}, len(folders))
	for _, folder := range folders {
		folder = normalizeFolder(folder)
		if !folder.Enabled || strings.TrimSpace(folder.LocalPath) == "" {
			continue
		}
		active = append(active, folder)
		activeIDs[folder.ID] = struct{}{}
	}

	var changed []FileMeta
	var deleted []Tombstone
	var errs []error
	for _, folder := range active {
		c, d, err := s.scanFolderLocked(folder, matcher)
		changed = append(changed, c...)
		deleted = append(deleted, d...)
		if err != nil {
			errs = append(errs, &ScanError{FolderID: folder.ID, Path: folder.LocalPath, Err: err})
		}
	}

	s.mu.Lock()
	s.folders = active
	s.removeInactiveFoldersLocked(activeIDs)
	s.pruneTombstonesLocked(time.Now().UTC().UnixMilli())
	// Inactive-folder removal changes what Entries() would return even when no
	// folder was walked; bump the generation so caches are invalidated.
	s.generation++
	s.mu.Unlock()

	return changed, deleted, errors.Join(errs...)
}

// ScanFolderIncremental rescans one folder. It is used by watcher events so a
// change in one mapped folder does not re-walk every other folder.
func (s *IndexStore) ScanFolderIncremental(folder config.FolderMapping, ignorePatterns []string) ([]FileMeta, []Tombstone, error) {
	s.scanMu.Lock()
	defer s.scanMu.Unlock()

	folder = normalizeFolder(folder)
	if !folder.Enabled || strings.TrimSpace(folder.LocalPath) == "" {
		return nil, nil, nil
	}
	matcher := newIgnoreMatcher(ignorePatterns)
	changed, deleted, err := s.scanFolderLocked(folder, matcher)
	if err != nil {
		return changed, deleted, &ScanError{FolderID: folder.ID, Path: folder.LocalPath, Err: err}
	}
	s.mu.Lock()
	s.upsertFolderLocked(folder)
	s.pruneTombstonesLocked(time.Now().UTC().UnixMilli())
	s.mu.Unlock()
	return changed, deleted, nil
}

// ScanError describes one folder that could not be scanned completely.
// An unavailable drive or a permission error must never be mistaken for a
// deletion, so callers log it and leave that folder's index entries untouched.
type ScanError struct {
	FolderID string
	Path     string
	Err      error
}

func (e *ScanError) Error() string {
	return fmt.Sprintf("扫描文件夹 %s (%s) 失败: %v", e.FolderID, e.Path, e.Err)
}

func (e *ScanError) Unwrap() error { return e.Err }

func normalizeFolder(folder config.FolderMapping) config.FolderMapping {
	folder.ID = config.NormalizeFolderID(folder.ID)
	folder.LocalPath = strings.TrimSpace(folder.LocalPath)
	folder.RemotePath = strings.TrimSpace(folder.RemotePath)
	folder.Label = strings.TrimSpace(folder.Label)
	if folder.Label == "" {
		if folder.ID == config.DefaultFolderID {
			folder.Label = "主同步目录"
		} else if folder.LocalPath != "" {
			folder.Label = filepath.Base(folder.LocalPath)
		} else {
			folder.Label = folder.ID
		}
	}
	return folder
}

// scanFolderLocked walks one folder and merges the result into the index.
// The caller must hold scanMu.
func (s *IndexStore) scanFolderLocked(folder config.FolderMapping, matcher *ignoreMatcher) ([]FileMeta, []Tombstone, error) {
	rootDir := strings.TrimSpace(folder.LocalPath)
	if rootDir == "" {
		return nil, nil, errors.New("文件夹路径为空")
	}

	lookupRoot := cleanRootKey(rootDir)
	s.mu.RLock()
	previousRoot := s.folderRoots[folder.ID]
	indexedFiles := s.folderStats[folder.ID].files
	s.mu.RUnlock()
	samePhysicalRoot := previousRoot != "" && strings.EqualFold(previousRoot, lookupRoot)

	rootInfo, err := os.Stat(rootDir)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, nil, err
		}
		// A root that vanished while it holds indexed files is almost always a
		// renamed/moved folder, an unplugged drive or a temporarily missing
		// network share. Re-creating it here would make the empty walk below
		// tombstone every file and propagate mass deletion to the peer. Explicit
		// Start/config actions create folders; scans must not. The one exception
		// is a deliberate remap to a different path while files are still indexed
		// under the same mapping ID.
		explicitRemap := previousRoot != "" && !samePhysicalRoot
		if indexedFiles > 0 && !explicitRemap {
			return nil, nil, fmt.Errorf("文件夹不存在或暂不可用 (%s)，已保留索引条目以避免误删", rootDir)
		}
		if mkErr := os.MkdirAll(rootDir, 0o755); mkErr != nil {
			return nil, nil, mkErr
		}
	} else if !rootInfo.IsDir() {
		return nil, nil, fmt.Errorf("路径不是文件夹: %s", rootDir)
	}

	rootKey := lookupRoot
	if resolved, resolveErr := filepath.EvalSymlinks(rootDir); resolveErr == nil {
		rootKey = cleanRootKey(resolved)
	}
	// When a mapping starts pointing at a different physical folder, cached
	// hashes at the same relative paths are no longer trustworthy.
	forceRehash := previousRoot != "" && !strings.EqualFold(previousRoot, rootKey)

	seen := make(map[string]FileMeta, 128)
	changed := make([]FileMeta, 0, 8)
	seenDirs := make(map[string]DirMeta, 32)
	cachedHashes := s.hashCache
	hashJobs := make([]hashJob, 0, 8)
	var skipped int
	var scanErrs []string

	unreadableDirs, walkErr := walkFolderTree(rootDir, matcher, func(innerDir string) {
		virtualRel := JoinFolderRelPath(folder.ID, innerDir)
		seenDirs[virtualRel] = DirMeta{FolderID: folder.ID, RelPath: virtualRel}
	}, func(fullPath, innerRel string, d fs.DirEntry) {
		info, infoErr := d.Info()
		if infoErr != nil || !info.Mode().IsRegular() {
			if infoErr != nil {
				skipped++
				if len(scanErrs) < 5 {
					scanErrs = append(scanErrs, innerRel)
				}
			}
			return
		}

		size := info.Size()
		mtime := info.ModTime().UTC().UnixMilli()
		virtualRel := JoinFolderRelPath(folder.ID, innerRel)

		s.mu.RLock()
		prev, hadPrev := s.files[virtualRel]
		s.mu.RUnlock()

		// Reuse the cached SHA256 when size and millisecond mtime match. This
		// turns steady-state polling into a metadata walk instead of re-reading
		// every file on disk.
		if !forceRehash && hadPrev && prev.Size == size && abs64(prev.ModTime-mtime) <= 2 && prev.SHA256 != "" {
			prev.FolderID = folder.ID
			prev.RelPath = virtualRel
			seen[virtualRel] = prev
			return
		}

		hash, hashErr := cachedHashes.hashOrCompute(virtualRel, fullPath, size, mtime)
		if hashErr != nil {
			// The file may be locked by another program; keep its previous
			// metadata instead of treating it as deleted.
			skipped++
			if hadPrev {
				seen[virtualRel] = prev
			}
			if len(scanErrs) < 5 {
				scanErrs = append(scanErrs, innerRel)
			}
			return
		}
		hashJobs = append(hashJobs, hashJob{rel: virtualRel, fullPath: fullPath, size: size, mtime: mtime, hash: hash})
		meta := FileMeta{
			FolderID: folder.ID,
			RelPath:  virtualRel,
			Size:     size,
			ModTime:  mtime,
			SHA256:   hash,
		}
		seen[virtualRel] = meta
		if !hadPrev || prev.SHA256 != hash || prev.Size != size {
			changed = append(changed, meta)
		}
	})
	if walkErr != nil {
		return nil, nil, walkErr
	}
	if len(unreadableDirs) > 0 {
		skipped += len(unreadableDirs)
		if len(scanErrs) < 5 {
			scanErrs = append(scanErrs, unreadableDirs[:minInt(5-len(scanErrs), len(unreadableDirs))]...)
		}
	}

	// Retain cached entries under directories that could not be read.
	if len(unreadableDirs) > 0 {
		s.mu.RLock()
		for rel, meta := range s.files {
			fid, inner := SplitFolderRelPath(rel)
			if fid != folder.ID {
				continue
			}
			if _, ok := seen[rel]; ok {
				continue
			}
			for _, prefix := range unreadableDirs {
				if inner == prefix || strings.HasPrefix(inner, prefix+"/") {
					seen[rel] = meta
					break
				}
			}
		}
		s.mu.RUnlock()
	}

	now := time.Now().UTC().UnixMilli()
	deleted := make([]Tombstone, 0, 4)
	s.mu.Lock()

	// Protect in-flight writes (temporary file already registered in the index
	// but not yet renamed into place) from being tombstoned by this scan.
	for rel, prev := range s.files {
		fid, _ := SplitFolderRelPath(rel)
		if fid != folder.ID {
			continue
		}
		if _, ok := seen[rel]; ok {
			continue
		}
		if _, pending := s.pendingWrites[rel]; pending {
			seen[rel] = prev
		}
	}

	newStat := folderStat{}
	for _, meta := range seen {
		newStat.files++
		newStat.bytes += meta.Size
	}

	// Remove entries that disappeared from this folder, creating tombstones so
	// the deletion can propagate to the peer.
	for rel, prev := range s.files {
		fid, inner := SplitFolderRelPath(rel)
		if fid != folder.ID {
			continue
		}
		if _, ok := seen[rel]; ok {
			continue
		}
		delete(s.files, rel)
		s.deleteStatusLocked(rel)
		delete(s.directions, rel)
		if matcher.Match(inner, false) {
			// Newly ignored files are dropped without a deletion event.
			continue
		}
		tomb := Tombstone{
			FolderID:  fid,
			RelPath:   rel,
			DeletedAt: max64(now, prev.ModTime+1),
			LastHash:  prev.SHA256,
		}
		s.tombstones[rel] = tomb
		deleted = append(deleted, tomb)
	}

	// A file the walk discovered as new or changed has not reached the peer
	// yet; the next upload or reconciliation moves it to 已同步. The host
	// serves its own files directly and can never upload to a client, so its
	// local files count as synced; a client that may push does not.
	pendingSet := make(map[string]struct{}, len(changed))
	if s.newFilesPending {
		for _, meta := range changed {
			pendingSet[meta.RelPath] = struct{}{}
		}
	}
	for rel, meta := range seen {
		s.files[rel] = meta
		s.ensureStatusLocked(rel)
		if _, isNewOrChanged := pendingSet[rel]; isNewOrChanged {
			s.markPendingLocked(rel)
		}
		if _, ok := s.directions[rel]; !ok {
			s.directions[rel] = "本地"
		}
		if tomb, hasTomb := s.tombstones[rel]; hasTomb && meta.ModTime >= tomb.DeletedAt {
			delete(s.tombstones, rel)
		}
	}

	// Directories are only ever created on this side from an explicit scan of
	// a folder that exists, or from a peer's manifest. Dropping them here would
	// forget empty folders without telling the peer anything, so they persist.
	for rel, dir := range seenDirs {
		if _, ok := s.dirs[rel]; !ok {
			s.dirs[rel] = dir
			s.addedDirs = append(s.addedDirs, dir)
		}
	}

	s.setFolderStatLocked(folder.ID, newStat)
	s.folderRoots[folder.ID] = rootKey
	s.upsertFolderLocked(folder)
	s.generation++
	s.mu.Unlock()

	// Prepare digests for the files that just changed so the next scan stays a
	// metadata walk, and let the transfers reuse them.
	s.hashCache.retain(hashJobs)
	s.hashCache.prefetch(hashJobs)

	if skipped > 0 {
		detail := fmt.Sprintf("%d 个文件或目录暂时无法读取", skipped)
		if len(scanErrs) > 0 {
			detail += " (如 " + strings.Join(scanErrs, ", ") + ")"
		}
		return changed, deleted, errors.New(detail)
	}
	return changed, deleted, nil
}

// walkFolderTree enumerates regular files under root using os.ReadDir, which
// follows a configured root that is a symlink or Windows junction.
// filepath.WalkDir treats such a root as a non-directory and would report an
// empty tree, which must never be mistaken for a mass deletion. Nested
// symlinks/junctions are not followed. Unreadable subdirectories are returned
// so callers can preserve their cached entries; an unreadable root is fatal.
func walkFolderTree(root string, matcher *ignoreMatcher, onDir func(innerRel string), onFile func(fullPath, innerRel string, d fs.DirEntry)) ([]string, error) {
	var unreadable []string
	var walk func(dir, rel string) error
	walk = func(dir, rel string) error {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if rel == "" {
				return err
			}
			unreadable = append(unreadable, rel)
			return nil
		}
		for _, entry := range entries {
			name := entry.Name()
			innerRel := name
			if rel != "" {
				innerRel = rel + "/" + name
			}
			fullPath := filepath.Join(dir, name)
			if matcher.Match(innerRel, entry.IsDir()) {
				continue
			}
			if entry.IsDir() {
				if entry.Type()&fs.ModeSymlink != 0 {
					continue
				}
				if onDir != nil {
					onDir(innerRel)
				}
				if err := walk(fullPath, innerRel); err != nil {
					return err
				}
				continue
			}
			if entry.Type()&fs.ModeSymlink != 0 {
				continue
			}
			onFile(fullPath, innerRel, entry)
		}
		return nil
	}
	if err := walk(root, ""); err != nil {
		return unreadable, err
	}
	return unreadable, nil
}

// Snapshot returns a thread-safe copy of the current Manifest.
func (s *IndexStore) Snapshot() Manifest {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := NewManifest(s.nodeID)
	if len(s.folders) > 0 {
		m.Folders = append([]config.FolderMapping(nil), s.folders...)
	}
	for k, v := range s.dirs {
		m.Dirs[k] = v
	}
	for k, v := range s.files {
		m.Files[k] = v
	}
	for k, v := range s.tombstones {
		m.Tombstones[k] = v
	}
	return m
}

// MarkFileSynced records a file that was just written or synced so the watcher does not echo it.
func (s *IndexStore) MarkFileSynced(meta FileMeta, status FileStatus, direction string) {
	if meta.FolderID == "" {
		meta.FolderID, _ = SplitFolderRelPath(meta.RelPath)
	}
	meta.FolderID = config.NormalizeFolderID(meta.FolderID)
	s.mu.Lock()
	prev, existed := s.files[meta.RelPath]
	s.files[meta.RelPath] = meta
	delete(s.tombstones, meta.RelPath)
	s.setStatusLocked(meta.RelPath, status)
	if direction != "" {
		s.directions[meta.RelPath] = direction
	}
	s.adjustFolderStatLocked(meta.FolderID, prev, existed, meta)
	delete(s.pendingWrites, meta.RelPath)
	s.generation++
	s.mu.Unlock()
}

// InvalidateHash clears the cached digest for a path so the next scan re-reads
// the file even when size and mtime are unchanged. It is used after a peer
// rejects an upload's content hash, which can otherwise remain stale forever.
func (s *IndexStore) InvalidateHash(relPath string) {
	relPath = filepath.ToSlash(strings.TrimSpace(relPath))
	s.mu.Lock()
	if meta, ok := s.files[relPath]; ok {
		meta.SHA256 = ""
		s.files[relPath] = meta
		s.generation++
	}
	s.mu.Unlock()
}

// SetFileStatus updates the status and direction shown in the UI table.
func (s *IndexStore) SetFileStatus(relPath string, status FileStatus, direction string) {
	s.mu.Lock()
	if _, ok := s.files[relPath]; ok {
		changed := s.statuses[relPath] != status || (direction != "" && s.directions[relPath] != direction)
		s.setStatusLocked(relPath, status)
		if direction != "" {
			s.directions[relPath] = direction
		}
		if changed {
			s.generation++
		}
	}
	s.mu.Unlock()
}

// MarkFileDeleted records a deletion tombstone in the index.
func (s *IndexStore) MarkFileDeleted(relPath string, deletedAt int64) Tombstone {
	fid, _ := SplitFolderRelPath(relPath)
	s.mu.Lock()
	prev, existed := s.files[relPath]
	delete(s.files, relPath)
	s.deleteStatusLocked(relPath)
	delete(s.directions, relPath)
	delete(s.pendingWrites, relPath)
	if existed {
		st := s.folderStats[fid]
		st.files--
		st.bytes -= prev.Size
		s.setFolderStatLocked(fid, st)
	}
	if deletedAt <= 0 {
		deletedAt = time.Now().UTC().UnixMilli()
	}
	if prev.ModTime >= deletedAt {
		deletedAt = prev.ModTime + 1
	}
	tomb := Tombstone{
		FolderID:  fid,
		RelPath:   relPath,
		DeletedAt: deletedAt,
		LastHash:  prev.SHA256,
	}
	s.tombstones[relPath] = tomb
	s.generation++
	s.mu.Unlock()
	return tomb
}

// Entries returns a sorted, cached slice of FileEntry for the UI table.
// The returned slice is read-only and must not be mutated by callers.
func (s *IndexStore) Entries() []FileEntry {
	out, _ := s.EntriesWithVersion()
	return out
}

// EntriesWithVersion returns the entries together with the index generation
// they were read from, under one consistent lock. Callers that cache a derived
// view (the UI file table) must key it by this version; reading Entries() and
// Generation() separately can pair an old slice with a new generation and pin
// a stale table until the next unrelated change.
func (s *IndexStore) EntriesWithVersion() ([]FileEntry, uint64) {
	s.mu.RLock()
	if s.entries != nil && s.entriesGeneration == s.generation {
		out, gen := s.entries, s.generation
		s.mu.RUnlock()
		return out, gen
	}
	gen := s.generation
	labelMap := make(map[string]string, len(s.folders))
	for _, f := range s.folders {
		id := config.NormalizeFolderID(f.ID)
		if f.Label != "" {
			labelMap[id] = f.Label
		} else {
			labelMap[id] = id
		}
	}
	out := make([]FileEntry, 0, len(s.files))
	for rel, meta := range s.files {
		st := s.statuses[rel]
		if st == "" {
			st = StatusSynced
		}
		dir := s.directions[rel]
		if dir == "" {
			dir = "双向一致"
		}
		fid, inner := SplitFolderRelPath(rel)
		flabel := labelMap[fid]
		if flabel == "" {
			if fid == config.DefaultFolderID {
				flabel = "主同步目录"
			} else {
				flabel = fid
			}
		}
		out = append(out, FileEntry{
			FolderID:    fid,
			FolderLabel: flabel,
			InnerPath:   inner,
			RelPath:     rel,
			Size:        meta.Size,
			ModTime:     time.UnixMilli(meta.ModTime).Local(),
			SHA256:      meta.SHA256,
			Status:      st,
			Direction:   dir,
		})
	}
	s.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool {
		return out[i].RelPath < out[j].RelPath
	})

	s.mu.Lock()
	if s.generation == gen {
		s.entries = out
		s.entriesGeneration = gen
	}
	s.mu.Unlock()
	return out, gen
}

// HashFile computes the hex-encoded SHA-256 of a file on disk with a pooled buffer.
func HashFile(fullPath string) (string, error) {
	f, err := os.Open(fullPath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	buf := ioBufferPool.Get().(*[]byte)
	defer ioBufferPool.Put(buf)
	if _, err := io.CopyBuffer(h, f, *buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// ioBufferPool keeps transfer/hash buffers bounded and reused.
var ioBufferPool = sync.Pool{New: func() any {
	b := make([]byte, 64*1024)
	return &b
}}

// CleanRelPath validates and normalizes a relative path, rejecting path traversal (`..`).
func CleanRelPath(rel string) (string, bool) {
	rel = filepath.ToSlash(strings.TrimSpace(rel))
	rel = strings.TrimPrefix(rel, "/")
	if rel == "" || rel == "." {
		return "", false
	}
	for _, seg := range strings.Split(rel, "/") {
		if seg == ".." {
			return "", false
		}
	}
	cleaned := path.Clean(rel)
	if cleaned == "." || cleaned == "/" || strings.HasPrefix(cleaned, "../") || strings.Contains(cleaned, "/../") || cleaned == ".." {
		return "", false
	}
	if strings.Contains(cleaned, ":") {
		return "", false
	}
	return cleaned, true
}

// ConflictBackupPath generates a conflict backup filename next to fullPath.
// Milliseconds keep two conflicts in the same second from overwriting each
// other's only copy.
func ConflictBackupPath(fullPath string) string {
	ext := filepath.Ext(fullPath)
	base := strings.TrimSuffix(fullPath, ext)
	stamp := time.Now().Format("20060102-150405.000")
	return base + ".conflict-" + stamp + ext
}

func (s *IndexStore) upsertFolderLocked(folder config.FolderMapping) {
	for i := range s.folders {
		if config.NormalizeFolderID(s.folders[i].ID) == folder.ID {
			s.folders[i] = folder
			return
		}
	}
	s.folders = append(s.folders, folder)
}

// setStatusLocked is the single writer for per-file status. Keeping the count
// per status lets the UI show an unsynced total on every frame without
// walking the whole index.
func (s *IndexStore) setStatusLocked(rel string, st FileStatus) {
	if rel == "" {
		return
	}
	if st == "" {
		st = StatusSynced
	}
	if old, ok := s.statuses[rel]; ok {
		if s.statusCounts[old] > 0 {
			s.statusCounts[old]--
		}
	}
	s.statuses[rel] = st
	s.statusCounts[st]++
}

func (s *IndexStore) deleteStatusLocked(rel string) {
	if old, ok := s.statuses[rel]; ok {
		s.statusCounts[old]--
		delete(s.statuses, rel)
	}
}

// markPendingLocked moves a newly discovered or changed file to 待同步 unless
// a transfer already owns that path. A transfer finishing later sets its own
// status, so overwriting "上传中" here would make the table flicker.
func (s *IndexStore) markPendingLocked(rel string) {
	switch s.statuses[rel] {
	case StatusUploading, StatusDownloading:
		return
	}
	s.setStatusLocked(rel, StatusPending)
}

func (s *IndexStore) ensureStatusLocked(rel string) {
	if _, ok := s.statuses[rel]; !ok {
		s.setStatusLocked(rel, StatusSynced)
	}
}

// TakeAddedDirs returns directories discovered since the last call, so the
// engine can tell the peer about an empty folder as soon as it appears.
func (s *IndexStore) TakeAddedDirs() []DirMeta {
	s.mu.Lock()
	out := s.addedDirs
	s.addedDirs = nil
	s.mu.Unlock()
	return out
}

// DirsList returns a copy of the indexed directories for a manifest request.
func (s *IndexStore) DirsList() []DirMeta {
	s.mu.RLock()
	out := make([]DirMeta, 0, len(s.dirs))
	for _, d := range s.dirs {
		out = append(out, d)
	}
	s.mu.RUnlock()
	return out
}

// SetNewFilesPending controls whether files discovered by a later scan start
// as 待同步. It is set from the role and direction before scans run.
func (s *IndexStore) SetNewFilesPending(pending bool) {
	s.mu.Lock()
	s.newFilesPending = pending
	s.mu.Unlock()
}

// AddDir records a directory learned from the peer or created for it.
func (s *IndexStore) AddDir(dir DirMeta) {
	if dir.RelPath == "" {
		return
	}
	s.mu.Lock()
	if _, ok := s.dirs[dir.RelPath]; !ok {
		s.dirs[dir.RelPath] = dir
		s.generation++
	}
	s.mu.Unlock()
}

// DirCount reports how many directories the index knows, for the UI's
// folder totals.
func (s *IndexStore) DirCount() int {
	s.mu.RLock()
	n := len(s.dirs)
	s.mu.RUnlock()
	return n
}

// UnsyncedCount reports how many indexed files have not been confirmed on the
// peer yet (pending, transferring, failed or conflicted).
func (s *IndexStore) UnsyncedCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := s.fileCount - s.statusCounts[StatusSynced]
	if n < 0 {
		return 0
	}
	return n
}

func (s *IndexStore) setFolderStatLocked(fid string, st folderStat) {
	old := s.folderStats[fid]
	s.fileCount += st.files - old.files
	s.totalBytes += st.bytes - old.bytes
	if st.files <= 0 {
		delete(s.folderStats, fid)
	} else {
		s.folderStats[fid] = st
	}
}

func (s *IndexStore) adjustFolderStatLocked(fid string, prev FileMeta, existed bool, meta FileMeta) {
	if existed && prev.FolderID != "" && prev.FolderID != fid {
		// The virtual path moved between mappings; keep both folders' totals right.
		old := s.folderStats[prev.FolderID]
		old.files--
		old.bytes -= prev.Size
		s.setFolderStatLocked(prev.FolderID, old)
		existed = false
	}
	st := s.folderStats[fid]
	if existed {
		st.bytes += meta.Size - prev.Size
	} else {
		st.files++
		st.bytes += meta.Size
	}
	s.setFolderStatLocked(fid, st)
}

// removeInactiveFoldersLocked drops index entries belonging to folders that
// are no longer enabled/configured. Their tombstones are no longer useful.
func (s *IndexStore) removeInactiveFoldersLocked(active map[string]struct{}) {
	for rel := range s.files {
		fid, _ := SplitFolderRelPath(rel)
		if _, ok := active[fid]; ok {
			continue
		}
		prev := s.files[rel]
		delete(s.files, rel)
		s.deleteStatusLocked(rel)
		delete(s.directions, rel)
		st := s.folderStats[fid]
		st.files--
		st.bytes -= prev.Size
		s.setFolderStatLocked(fid, st)
		delete(s.pendingWrites, rel)
	}
	for rel, tomb := range s.tombstones {
		fid := tomb.FolderID
		if fid == "" {
			fid, _ = SplitFolderRelPath(rel)
		}
		if _, ok := active[fid]; !ok {
			delete(s.tombstones, rel)
		}
	}
	for rel := range s.dirs {
		fid, _ := SplitFolderRelPath(rel)
		if _, ok := active[fid]; !ok {
			delete(s.dirs, rel)
		}
	}
	for fid := range s.folderRoots {
		if _, ok := active[fid]; !ok {
			delete(s.folderRoots, fid)
		}
	}
}

func (s *IndexStore) pruneTombstonesLocked(now int64) {
	cutoff := now - int64(tombstoneRetention/time.Millisecond)
	for rel, tomb := range s.tombstones {
		if tomb.DeletedAt < cutoff {
			delete(s.tombstones, rel)
		}
	}
	if len(s.tombstones) <= tombstoneLimit {
		return
	}
	type agedTomb struct {
		rel string
		at  int64
	}
	aged := make([]agedTomb, 0, len(s.tombstones))
	for rel, tomb := range s.tombstones {
		aged = append(aged, agedTomb{rel: rel, at: tomb.DeletedAt})
	}
	sort.Slice(aged, func(i, j int) bool { return aged[i].at < aged[j].at })
	for i := 0; i < len(aged)-tombstoneLimit; i++ {
		delete(s.tombstones, aged[i].rel)
	}
}

func cleanRootKey(root string) string {
	if abs, err := filepath.Abs(root); err == nil {
		return filepath.Clean(abs)
	}
	return filepath.Clean(root)
}

// isPathWithinRoot reports whether p is root itself or inside it, using
// lexical containment with Windows-aware case folding via filepath.Rel.
func isPathWithinRoot(root, p string) bool {
	rel, err := filepath.Rel(root, p)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	if filepath.IsAbs(rel) {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

func abs64(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
