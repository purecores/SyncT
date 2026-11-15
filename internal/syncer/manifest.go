package syncer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"synct/internal/config"
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
	Files       map[string]FileMeta    `json:"files"`
	Tombstones  map[string]Tombstone   `json:"tombstones"`
}

// NewManifest creates an empty initialized Manifest.
func NewManifest(nodeID string) Manifest {
	return Manifest{
		NodeID:      nodeID,
		GeneratedAt: time.Now().UTC().UnixMilli(),
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
	folder, found := cfg.FindFolder(folderID)
	if !found || strings.TrimSpace(folder.LocalPath) == "" {
		return config.FolderMapping{}, "", "", false
	}
	fullPath := filepath.Join(folder.LocalPath, filepath.FromSlash(innerCleaned))
	return folder, innerCleaned, fullPath, true
}

// IndexStore caches file hashes by (RelPath, Size, ModTime) and persists state/tombstones.
type IndexStore struct {
	mu         sync.RWMutex
	nodeID     string
	statePath  string
	folders    []config.FolderMapping
	files      map[string]FileMeta
	tombstones map[string]Tombstone
	statuses   map[string]FileStatus
	directions map[string]string
}

type persistedState struct {
	NodeID     string               `json:"node_id"`
	Files      map[string]FileMeta  `json:"files"`
	Tombstones map[string]Tombstone `json:"tombstones"`
}

// NewIndexStore creates or loads a persistent IndexStore.
func NewIndexStore(statePath string) *IndexStore {
	s := &IndexStore{
		nodeID:     "node-" + time.Now().UTC().Format("150405.000"),
		statePath:  statePath,
		files:      make(map[string]FileMeta),
		tombstones: make(map[string]Tombstone),
		statuses:   make(map[string]FileStatus),
		directions: make(map[string]string),
	}
	s.load()
	return s
}

func (s *IndexStore) load() {
	if s.statePath == "" {
		return
	}
	data, err := os.ReadFile(s.statePath)
	if err != nil {
		return
	}
	var ps persistedState
	if err := json.Unmarshal(data, &ps); err != nil {
		return
	}
	if ps.NodeID != "" {
		s.nodeID = ps.NodeID
	}
	if ps.Files != nil {
		s.files = ps.Files
	}
	if ps.Tombstones != nil {
		s.tombstones = ps.Tombstones
	}
}

// Save persists the current index and tombstones to disk.
func (s *IndexStore) Save() {
	if s.statePath == "" {
		return
	}
	s.mu.RLock()
	ps := persistedState{
		NodeID:     s.nodeID,
		Files:      make(map[string]FileMeta, len(s.files)),
		Tombstones: make(map[string]Tombstone, len(s.tombstones)),
	}
	for k, v := range s.files {
		ps.Files[k] = v
	}
	for k, v := range s.tombstones {
		ps.Tombstones[k] = v
	}
	s.mu.RUnlock()

	data, err := json.MarshalIndent(ps, "", "  ")
	if err != nil {
		return
	}
	tmp := s.statePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err == nil {
		_ = os.Rename(tmp, s.statePath)
	}
}

// NodeID returns this node's unique identifier.
func (s *IndexStore) NodeID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nodeID
}

// ShouldIgnore checks if a slash-normalized relative path matches any ignore pattern.
func ShouldIgnore(relPath string, isDir bool, patterns []string) bool {
	_, inner := SplitFolderRelPath(relPath)
	relPath = filepath.ToSlash(strings.TrimPrefix(inner, "/"))
	if relPath == "" || relPath == "." {
		return false
	}
	base := path.Base(relPath)
	if strings.HasPrefix(base, ".synct") {
		return true
	}
	parts := strings.Split(relPath, "/")
	for _, rawPat := range patterns {
		pat := strings.TrimSpace(filepath.ToSlash(rawPat))
		if pat == "" {
			continue
		}
		// Match against base name
		if matched, _ := path.Match(pat, base); matched {
			return true
		}
		// Match against full relative path
		if matched, _ := path.Match(pat, relPath); matched {
			return true
		}
		// Match against any parent directory segment (e.g., ".git" or "node_modules")
		for _, seg := range parts {
			if seg == pat {
				return true
			}
			if matched, _ := path.Match(pat, seg); matched {
				return true
			}
		}
	}
	return false
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

// ScanFolders walks all enabled FolderMappings, computes or reuses cached SHA256 hashes,
// records tombstones for files that disappeared since the last scan, and returns the latest Manifest.
func (s *IndexStore) ScanFolders(folders []config.FolderMapping, ignorePatterns []string) (Manifest, []FileMeta, []Tombstone, error) {
	s.mu.RLock()
	prevFiles := make(map[string]FileMeta, len(s.files))
	for k, v := range s.files {
		prevFiles[k] = v
	}
	prevTombs := make(map[string]Tombstone, len(s.tombstones))
	for k, v := range s.tombstones {
		prevTombs[k] = v
	}
	nodeID := s.nodeID
	s.mu.RUnlock()

	nextFiles := make(map[string]FileMeta)
	var changedFiles []FileMeta
	var newTombs []Tombstone
	var activeFolders []config.FolderMapping
	scannedFolderIDs := make(map[string]bool, len(folders))

	for _, folder := range folders {
		if !folder.Enabled || strings.TrimSpace(folder.LocalPath) == "" {
			continue
		}
		folder.ID = config.NormalizeFolderID(folder.ID)
		if folder.Label == "" {
			if folder.ID == config.DefaultFolderID {
				folder.Label = "主同步目录"
			} else {
				folder.Label = filepath.Base(folder.LocalPath)
			}
		}
		rootDir := strings.TrimSpace(folder.LocalPath)
		if err := os.MkdirAll(rootDir, 0o755); err != nil {
			return Manifest{}, nil, nil, err
		}
		activeFolders = append(activeFolders, folder)
		scannedFolderIDs[folder.ID] = true

		err := filepath.WalkDir(rootDir, func(fullPath string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return nil // Skip unreadable entries gracefully
			}
			if fullPath == rootDir {
				return nil
			}
			innerRel, err := filepath.Rel(rootDir, fullPath)
			if err != nil {
				return nil
			}
			innerRel = filepath.ToSlash(innerRel)
			if ShouldIgnore(innerRel, d.IsDir(), ignorePatterns) {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if d.IsDir() {
				return nil
			}
			info, err := d.Info()
			if err != nil || !info.Mode().IsRegular() {
				return nil
			}

			size := info.Size()
			mtime := info.ModTime().UTC().UnixMilli()
			virtualRel := JoinFolderRelPath(folder.ID, innerRel)

			// Reuse cached SHA256 if size and mtime (within 2ms precision) match
			if prev, ok := prevFiles[virtualRel]; ok && prev.Size == size && abs64(prev.ModTime-mtime) <= 2 && prev.SHA256 != "" {
				prev.FolderID = folder.ID
				prev.RelPath = virtualRel
				nextFiles[virtualRel] = prev
				return nil
			}

			hash, err := HashFile(fullPath)
			if err != nil {
				return nil
			}
			meta := FileMeta{
				FolderID: folder.ID,
				RelPath:  virtualRel,
				Size:     size,
				ModTime:  mtime,
				SHA256:   hash,
			}
			nextFiles[virtualRel] = meta
			if prev, ok := prevFiles[virtualRel]; !ok || prev.SHA256 != meta.SHA256 {
				changedFiles = append(changedFiles, meta)
			}
			return nil
		})
		if err != nil {
			return Manifest{}, nil, nil, err
		}
	}

	now := time.Now().UTC().UnixMilli()
	// Check for files that existed previously in a scanned folder but are now gone on disk
	for rel, prev := range prevFiles {
		fid, _ := SplitFolderRelPath(rel)
		if !scannedFolderIDs[fid] {
			continue
		}
		if ShouldIgnore(rel, false, ignorePatterns) {
			delete(prevTombs, rel)
			continue
		}
		if _, exists := nextFiles[rel]; !exists {
			tomb := Tombstone{
				FolderID:  fid,
				RelPath:   rel,
				DeletedAt: max64(now, prev.ModTime+1),
				LastHash:  prev.SHA256,
			}
			prevTombs[rel] = tomb
			newTombs = append(newTombs, tomb)
		}
	}

	// If a file exists on disk, clear any older tombstone
	for rel, meta := range nextFiles {
		if t, hasTomb := prevTombs[rel]; hasTomb {
			if meta.ModTime >= t.DeletedAt {
				delete(prevTombs, rel)
			}
		}
	}

	s.mu.Lock()
	s.folders = activeFolders
	s.files = nextFiles
	s.tombstones = prevTombs
	for rel := range nextFiles {
		if _, ok := s.statuses[rel]; !ok {
			s.statuses[rel] = StatusSynced
		}
		if _, ok := s.directions[rel]; !ok {
			s.directions[rel] = "本地"
		}
	}
	for rel := range s.statuses {
		if _, ok := nextFiles[rel]; !ok {
			delete(s.statuses, rel)
			delete(s.directions, rel)
		}
	}
	s.mu.Unlock()

	manifest := Manifest{
		NodeID:      nodeID,
		GeneratedAt: now,
		Folders:     activeFolders,
		Files:       nextFiles,
		Tombstones:  prevTombs,
	}
	return manifest, changedFiles, newTombs, nil
}

// Snapshot returns a thread-safe copy of the current Manifest.
func (s *IndexStore) Snapshot() Manifest {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := NewManifest(s.nodeID)
	if len(s.folders) > 0 {
		m.Folders = append([]config.FolderMapping(nil), s.folders...)
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
	s.mu.Lock()
	s.files[meta.RelPath] = meta
	delete(s.tombstones, meta.RelPath)
	s.statuses[meta.RelPath] = status
	if direction != "" {
		s.directions[meta.RelPath] = direction
	}
	s.mu.Unlock()
}

// SetFileStatus updates the status and direction shown in the UI table.
func (s *IndexStore) SetFileStatus(relPath string, status FileStatus, direction string) {
	s.mu.Lock()
	if _, ok := s.files[relPath]; ok {
		s.statuses[relPath] = status
		if direction != "" {
			s.directions[relPath] = direction
		}
	}
	s.mu.Unlock()
}

// MarkFileDeleted records a deletion tombstone in the index.
func (s *IndexStore) MarkFileDeleted(relPath string, deletedAt int64) Tombstone {
	fid, _ := SplitFolderRelPath(relPath)
	s.mu.Lock()
	prev := s.files[relPath]
	delete(s.files, relPath)
	delete(s.statuses, relPath)
	delete(s.directions, relPath)
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
	s.mu.Unlock()
	return tomb
}

// Entries returns a sorted slice of FileEntry for the UI table.
func (s *IndexStore) Entries() []FileEntry {
	s.mu.RLock()
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
	return out
}

// HashFile computes the hex-encoded SHA-256 of a file on disk.
func HashFile(fullPath string) (string, error) {
	f, err := os.Open(fullPath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// CleanRelPath validates and normalizes a relative path, rejecting path traversal (`..`).
func CleanRelPath(rel string) (string, bool) {
	rel = filepath.ToSlash(strings.TrimSpace(rel))
	rel = strings.TrimPrefix(rel, "/")
	if rel == "" || rel == "." {
		return "", false
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
func ConflictBackupPath(fullPath string) string {
	ext := filepath.Ext(fullPath)
	base := strings.TrimSuffix(fullPath, ext)
	stamp := time.Now().Format("20060102-150405")
	return base + ".conflict-" + stamp + ext
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
