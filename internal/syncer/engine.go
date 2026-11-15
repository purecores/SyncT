package syncer

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/egoist/mygo"

	"synct/internal/config"
)

const (
	maxLogEntries       = 400
	scanDebounce        = 180 * time.Millisecond
	maxScanSettle       = time.Second
	minScanInterval     = 400 * time.Millisecond
	pollInterval        = 30 * time.Second
	remoteEventDebounce = 150 * time.Millisecond
	remoteEventBuffer   = 512
	reconcileInterval   = 2 * time.Minute
	sseIdleTimeout      = 60 * time.Second
	speedIdleWindow     = 3 * time.Second
	watchRetryMax       = 30 * time.Second
)

// EngineState represents the high-level runtime state of the synchronization engine.
type EngineState string

const (
	StateStopped    EngineState = "未启动"
	StateStarting   EngineState = "正在启动"
	StateRunning    EngineState = "实时同步中"
	StateSyncing    EngineState = "正在传输同步"
	StateConnecting EngineState = "正在连接远端"
	StateError      EngineState = "连接异常"
)

// LogEntry is a timestamped activity record displayed in the UI log list.
type LogEntry struct {
	ID      int64
	Time    time.Time
	Level   string // "INFO", "SYNC", "WARN", "ERROR"
	Message string
}

// Stats holds live synchronization telemetry for the UI dashboard.
type Stats struct {
	State           EngineState
	LastError       string
	LocalPort       int
	ConnectedPeers  int
	RemoteConnected bool
	RemoteNodeID    string
	FolderCount     int
	TotalFiles      int
	TotalBytes      int64
	BytesUploaded   int64
	BytesDownloaded int64
	UploadSpeed     int64
	DownloadSpeed   int64
	LastSyncFile    string
	LastSyncBytes   int64
	SyncedEvents    int64
	ConflictsSaved  int64
	LastSyncTime    time.Time
	LastScanTime    time.Time
	LastScanError   string
	ScanErrors      int64
}

// Engine coordinates local folder watching (`mygo.WatchFiles`), the Host HTTP/SSE server,
// and the Client HTTP/SSE synchronization loop.
type Engine struct {
	mu       sync.RWMutex
	cfg      config.Config
	index    *IndexStore
	server   *Server
	client   *http.Client // control plane (manifest, delete)
	transfer *http.Client // file transfers and SSE: no fixed overall timeout
	onUpdate func()

	state           EngineState
	lastError       string
	localPort       int
	remoteConnected bool
	remoteNodeID    string
	lastSyncTime    time.Time
	lastSyncFile    string
	lastSyncBytes   int64
	lastScanTime    time.Time
	lastScanError   string

	bytesUploaded   atomic.Int64
	bytesDownloaded atomic.Int64
	uploadSpeed     atomic.Int64
	downloadSpeed   atomic.Int64
	lastUploadAt    atomic.Int64
	lastDownloadAt  atomic.Int64
	syncedEvents    atomic.Int64
	conflictsSaved  atomic.Int64
	scanErrors      atomic.Int64
	logSeq          atomic.Int64

	logsMu sync.RWMutex
	logs   []LogEntry

	runCancel   context.CancelFunc
	reconcileMu sync.Mutex

	// Scan coalescing: watcher events mark one folder dirty and wake the scan loop,
	// which settles bursts before walking the filesystem.
	scanMu      sync.Mutex
	scanPending map[string]struct{}
	scanAll     bool
	scanReason  string
	scanWake    chan struct{}

	watchCfgCh    chan struct{}
	remoteEvents  chan SyncEvent
	reconcileWake chan struct{}

	// onConfigChange lets the UI adopt folder mappings learned from the peer.
	onConfigChange func(config.Config)

	logThrottleMu sync.Mutex
	logThrottle   map[string]time.Time
}

// NewEngine creates a new Synchronization Engine.
func NewEngine(cfg config.Config, statePath string, onUpdate func()) *Engine {
	cfg = cfg.Clone()
	cfg.Normalize()
	tr := newSyncHTTPTransport()
	if tr.ResponseHeaderTimeout == 0 {
		tr.ResponseHeaderTimeout = 45 * time.Second
	}
	e := &Engine{
		cfg:           cfg,
		index:         NewIndexStore(statePath),
		onUpdate:      onUpdate,
		state:         StateStopped,
		scanPending:   make(map[string]struct{}),
		scanWake:      make(chan struct{}, 1),
		watchCfgCh:    make(chan struct{}, 1),
		remoteEvents:  make(chan SyncEvent, remoteEventBuffer),
		reconcileWake: make(chan struct{}, 1),
		logThrottle:   make(map[string]time.Time),
		client: &http.Client{
			Transport: tr,
			Timeout:   2 * time.Minute,
		},
		transfer: &http.Client{Transport: tr},
	}
	e.index.SetSaveErrorHandler(func(err error) {
		e.LogThrottled("state-save:"+err.Error(), "WARN", fmt.Sprintf("持久化状态索引失败: %v", err), 30*time.Second)
	})
	e.server = NewServer(e)

	if loadErr := e.index.LoadError(); loadErr != nil {
		e.LogLevel("WARN", loadErr.Error())
	}

	// Perform initial scan of all enabled folders. Failure to read one folder is
	// reported but does not prevent the engine from starting.
	folders := cfg.EnabledFolders()
	if _, _, err := e.index.ScanFoldersIncremental(folders, cfg.IgnorePatterns); err != nil {
		e.setScanFailure(err)
		e.LogLevel("WARN", err.Error())
	} else {
		e.setScanSuccess()
	}
	if err := e.index.Save(); err != nil {
		e.LogLevel("WARN", fmt.Sprintf("保存初始状态索引失败: %v", err))
	}
	e.Logf("同步引擎就绪，当前监控 %d 个文件夹 (主目录: %s)", len(folders), cfg.SyncDir)
	return e
}

// SetConfigChangeHandler installs a callback invoked after peer folder mappings
// change the effective configuration. The UI uses it to adopt auto-mappings
// instead of later overwriting them with a stale local copy.
func (e *Engine) SetConfigChangeHandler(fn func(config.Config)) {
	e.mu.Lock()
	e.onConfigChange = fn
	e.mu.Unlock()
}

// Config returns a deep copy of the engine's current configuration.
func (e *Engine) Config() config.Config {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.cfg.Clone()
}

// UpdateConfig updates the configuration and schedules a rescan when the sync
// folders or ignore rules changed. The scan runs in the background so the UI
// thread never blocks on large directories.
func (e *Engine) UpdateConfig(cfg config.Config) {
	cfg = cfg.Clone()
	cfg.Normalize()

	e.mu.Lock()
	foldersChanged := e.cfg.SyncDir != cfg.SyncDir || !sameFolderMappings(e.cfg.SyncFolders, cfg.SyncFolders)
	ignoreChanged := !sameStrings(e.cfg.IgnorePatterns, cfg.IgnorePatterns)
	e.cfg = cfg
	e.mu.Unlock()

	if foldersChanged || ignoreChanged {
		e.signalWatchers()
	}
	if e.IsRunning() {
		e.requestScan("", true, "配置更新")
	} else if foldersChanged || ignoreChanged {
		// The scan loop is not running while stopped, but the UI should still
		// reflect the new folders/ignore rules immediately.
		e.TriggerManualSync()
	}
	e.Logf("已更新同步配置 (共 %d 个启用目录，主目录: %s)", len(cfg.EnabledFolders()), cfg.SyncDir)
	e.notifyUI()
}

func sameFolderMappings(a, b []config.FolderMapping) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if config.NormalizeFolderID(a[i].ID) != config.NormalizeFolderID(b[i].ID) ||
			a[i].LocalPath != b[i].LocalPath ||
			a[i].RemotePath != b[i].RemotePath ||
			a[i].Label != b[i].Label ||
			a[i].Enabled != b[i].Enabled {
			return false
		}
	}
	return true
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ApplyPeerFolderMappings merges folder mappings announced by the remote peer.
// When the peer specifies a RemotePath (target folder on this computer) or adds a new folder ID,
// this node automatically creates/updates the corresponding local folder mapping.
func (e *Engine) ApplyPeerFolderMappings(peerFolders []config.FolderMapping) bool {
	if len(peerFolders) == 0 {
		return false
	}
	e.mu.Lock()
	cfg := e.cfg.Clone()
	changed := false
	var dirsToCreate []string

	for _, pf := range peerFolders {
		if !pf.Enabled {
			continue
		}
		id := config.NormalizeFolderID(pf.ID)
		peerTargetForUs := strings.TrimSpace(pf.RemotePath)
		peerLocal := strings.TrimSpace(pf.LocalPath)

		foundIdx := -1
		for i := range cfg.SyncFolders {
			if config.NormalizeFolderID(cfg.SyncFolders[i].ID) == id {
				foundIdx = i
				break
			}
		}

		if foundIdx >= 0 {
			existing := &cfg.SyncFolders[foundIdx]
			if peerTargetForUs != "" && existing.LocalPath != peerTargetForUs {
				existing.LocalPath = peerTargetForUs
				if foundIdx == 0 {
					cfg.SyncDir = peerTargetForUs
				}
				dirsToCreate = append(dirsToCreate, peerTargetForUs)
				changed = true
			}
			if pf.Label != "" && existing.Label == "" {
				existing.Label = pf.Label
				changed = true
			}
		} else {
			localDir := peerTargetForUs
			if localDir == "" {
				if id == config.DefaultFolderID {
					localDir = cfg.SyncDir
				} else {
					localDir = filepath.Join(cfg.SyncDir, id)
				}
			}
			label := strings.TrimSpace(pf.Label)
			if label == "" {
				if peerLocal != "" {
					label = filepath.Base(peerLocal)
				} else {
					label = id
				}
			}
			cfg.UpsertFolder(config.FolderMapping{
				ID:        id,
				Label:     label,
				LocalPath: localDir,
				Enabled:   true,
			})
			dirsToCreate = append(dirsToCreate, localDir)
			changed = true
		}
	}

	if !changed {
		e.mu.Unlock()
		return false
	}

	cfg.Normalize()
	e.cfg = cfg
	e.mu.Unlock()

	// Only create directories for mappings this call added or changed. An
	// unrelated vanished folder must not be re-created just because the peer
	// sent a manifest, or its indexed files would be tombstoned as deleted.
	for _, dir := range dirsToCreate {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			e.LogThrottled("mkdir:"+dir, "WARN", fmt.Sprintf("无法创建同步目录 (%s): %v", dir, err), 30*time.Second)
		}
	}
	if err := config.Save(cfg); err != nil {
		e.LogLevel("WARN", fmt.Sprintf("保存远端文件夹映射失败: %v", err))
	}
	e.signalWatchers()
	e.requestScan("", true, "远端文件夹映射更新")
	e.Logf("已自动应用远端文件夹映射配置 (共 %d 个同步文件夹)", len(cfg.EnabledFolders()))
	e.mu.RLock()
	handler := e.onConfigChange
	e.mu.RUnlock()
	if handler != nil {
		handler(cfg.Clone())
	}
	e.notifyUI()
	return true
}

// IsRunning reports whether the engine is actively syncing or starting.
func (e *Engine) IsRunning() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.state != StateStopped
}

// Start launches either the Host server + file watcher or Client connector + file watcher.
func (e *Engine) Start() error {
	e.mu.Lock()
	if e.runCancel != nil {
		e.mu.Unlock()
		return nil
	}
	cfg := e.cfg
	ctx, cancel := context.WithCancel(context.Background())
	e.runCancel = cancel
	e.state = StateStarting
	e.lastError = ""
	e.lastScanError = ""
	e.mu.Unlock()
	e.notifyUI()

	folders := cfg.EnabledFolders()
	for _, f := range folders {
		indexed := e.index.FolderFileCount(f.ID)
		if info, statErr := os.Stat(f.LocalPath); statErr != nil {
			if indexed > 0 {
				// The folder was renamed, unplugged or is on a temporarily
				// unavailable share. Re-creating an empty directory here would
				// tombstone all indexed files on the next scan.
				e.LogThrottled("start-missing:"+f.ID, "WARN", fmt.Sprintf("同步目录暂不可用，已保留 %d 个索引条目且不自动创建: %s (%v)", indexed, f.LocalPath, statErr), 30*time.Second)
				continue
			}
		} else if !info.IsDir() && indexed > 0 {
			e.LogThrottled("start-notdir:"+f.ID, "WARN", fmt.Sprintf("同步目录路径不是文件夹，已保留 %d 个索引条目: %s", indexed, f.LocalPath), 30*time.Second)
			continue
		}
		if err := os.MkdirAll(f.LocalPath, 0o755); err != nil {
			if indexed > 0 {
				e.LogThrottled("start-mkdir:"+f.ID, "WARN", fmt.Sprintf("无法准备同步目录 (%s): %v；已保留现有索引", f.LocalPath, err), 30*time.Second)
				continue
			}
			e.Stop()
			e.setError(fmt.Sprintf("无法创建同步目录 (%s): %v", f.LocalPath, err))
			return err
		}
	}

	// Initial directory scan across all enabled folders. A folder that is
	// temporarily unavailable is reported and retried, but does not abort Start.
	if _, _, err := e.index.ScanFoldersIncremental(folders, cfg.IgnorePatterns); err != nil {
		e.setScanFailure(err)
	} else {
		e.setScanSuccess()
	}
	e.index.RequestSave()

	if cfg.Role == config.RoleHost {
		port, err := e.server.Start(cfg.LocalPort)
		if err != nil {
			e.Stop()
			e.setError(fmt.Sprintf("启动本地同步服务失败: %v", err))
			return err
		}
		e.mu.Lock()
		e.localPort = port
		e.state = StateRunning
		e.mu.Unlock()
		e.Logf("本地同步服务已启动: https://localhost:%d (共监控 %d 个文件夹，兼容 HTTPS/HTTP)", port, len(folders))
		e.notifyUI()

		go e.watchLocalLoop(ctx)
		return nil
	}

	// Client mode
	remoteURL := config.NormalizeRemoteURL(cfg.RemoteURL)
	if remoteURL == "" {
		e.Stop()
		err := errors.New("请先填写远端 Cloudflared 隧道地址或连接码")
		e.setError(err.Error())
		return err
	}

	e.mu.Lock()
	e.state = StateConnecting
	e.mu.Unlock()
	e.notifyUI()

	go e.watchLocalLoop(ctx)
	go e.clientLoop(ctx)

	return nil
}

// Stop halts all watchers, local servers, and remote connections.
func (e *Engine) Stop() {
	e.mu.Lock()
	cancel := e.runCancel
	e.runCancel = nil
	e.state = StateStopped
	e.remoteConnected = false
	e.localPort = 0
	e.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	e.server.Stop()
	if err := e.index.Flush(); err != nil {
		e.LogLevel("WARN", fmt.Sprintf("停止时保存状态索引失败: %v", err))
	}
	e.Logf("实时同步已停止")
	e.notifyUI()
}

// TriggerManualSync forces an immediate directory scan and remote reconciliation.
func (e *Engine) TriggerManualSync() {
	if !e.IsRunning() {
		// The scan loop only runs while started; still allow a manual index refresh
		// from the UI when sync is stopped, without touching the network.
		go func() {
			cfg := e.Config()
			if _, _, err := e.index.ScanFoldersIncremental(cfg.EnabledFolders(), cfg.IgnorePatterns); err != nil {
				e.setScanFailure(err)
				e.LogThrottled("manual-scan:"+err.Error(), "WARN", err.Error(), 30*time.Second)
			} else {
				e.setScanSuccess()
				e.Logf("已完成手动目录扫描 (%d 个文件夹)", len(cfg.EnabledFolders()))
			}
			e.index.RequestSave()
			e.notifyUI()
		}()
		return
	}
	e.requestScan("", true, "手动扫描")
	if e.Config().Role == config.RoleClient {
		e.requestReconcile()
	}
}

// ImportFiles copies external files dropped onto the window into the primary SyncDir.
func (e *Engine) ImportFiles(paths []string) (int, error) {
	return e.ImportFilesToFolder(config.DefaultFolderID, paths)
}

// ImportFilesToFolder copies external files dropped onto the window into the specified mapped folder.
func (e *Engine) ImportFilesToFolder(folderID string, paths []string) (int, error) {
	cfg := e.Config()
	folder, ok := cfg.FindEnabledFolder(folderID)
	targetDir := folder.LocalPath
	scanID := config.NormalizeFolderID(folder.ID)
	if !ok || strings.TrimSpace(targetDir) == "" {
		targetDir = cfg.SyncDir
		scanID = config.DefaultFolderID
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return 0, fmt.Errorf("无法创建目标同步目录 (%s): %w", targetDir, err)
	}
	imported := 0
	failures := make([]error, 0, len(paths))
	for _, src := range paths {
		info, err := os.Stat(src)
		if err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", filepath.Base(src), err))
			continue
		}
		if info.IsDir() {
			failures = append(failures, fmt.Errorf("%s: 暂不支持导入文件夹", filepath.Base(src)))
			continue
		}
		if !info.Mode().IsRegular() {
			failures = append(failures, fmt.Errorf("%s: 不是常规文件", filepath.Base(src)))
			continue
		}
		dst := filepath.Join(targetDir, filepath.Base(src))
		if filepath.Clean(src) == filepath.Clean(dst) {
			continue
		}
		if err := copyFileContents(src, dst); err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", filepath.Base(src), err))
			continue
		}
		imported++
	}
	if imported > 0 {
		e.Logf("已导入 %d 个外部拖入文件至同步目录 (%s)", imported, targetDir)
		if e.IsRunning() {
			e.requestScan(scanID, false, "导入文件")
		} else {
			// No scan loop while stopped; refresh the index immediately anyway.
			e.TriggerManualSync()
		}
	}
	if len(failures) > 0 {
		if imported == 0 {
			return 0, errors.Join(failures...)
		}
		e.LogLevel("WARN", fmt.Sprintf("有 %d 个文件导入失败 (首个错误: %v)", len(failures), failures[0]))
	}
	return imported, nil
}

// requestScan marks a folder (or all folders) dirty and wakes the coalescing scan loop.
func (e *Engine) requestScan(folderID string, all bool, reason string) {
	e.scanMu.Lock()
	if all {
		e.scanAll = true
	} else if folderID != "" {
		e.scanPending[folderID] = struct{}{}
	}
	if reason != "" {
		e.scanReason = reason
	}
	e.scanMu.Unlock()
	select {
	case e.scanWake <- struct{}{}:
	default:
	}
}

func (e *Engine) takeScanRequests() (map[string]struct{}, bool, string) {
	e.scanMu.Lock()
	defer e.scanMu.Unlock()
	pending := e.scanPending
	all := e.scanAll
	reason := e.scanReason
	e.scanPending = make(map[string]struct{})
	e.scanAll = false
	e.scanReason = ""
	return pending, all, reason
}

func (e *Engine) requestReconcile() {
	select {
	case e.reconcileWake <- struct{}{}:
	default:
	}
}

func (e *Engine) signalWatchers() {
	select {
	case e.watchCfgCh <- struct{}{}:
	default:
	}
}

// watchLocalLoop keeps one cheap "hints only" watcher per enabled folder and
// feeds changes into the coalesced scan loop. HintsOnly avoids a per-file
// handle/snapshot and its entry limit on large trees; the scanner supplies the
// exact diff, so directory-level hints are sufficient.
func (e *Engine) watchLocalLoop(ctx context.Context) {
	watchers := make(map[string]context.CancelFunc)
	watcherKey := ""
	defer func() {
		for _, cancel := range watchers {
			cancel()
		}
	}()

	go e.scanLoop(ctx)

	startOrReplace := func(force bool) {
		cfg := e.Config()
		key := watcherKeyFor(cfg)
		if !force && key == watcherKey {
			return
		}
		for id, cancel := range watchers {
			cancel()
			delete(watchers, id)
		}
		folders := cfg.EnabledFolders()
		patterns := cfg.IgnorePatterns
		for _, folder := range folders {
			folderID := folder.ID
			dir := folder.LocalPath
			wctx, cancel := context.WithCancel(ctx)
			watchers[folderID] = cancel
			go e.watchFolderLoop(wctx, folderID, dir, patterns)
		}
		watcherKey = key
	}

	startOrReplace(false)
	e.requestScan("", true, "启动扫描")

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.watchCfgCh:
			startOrReplace(true)
		case <-ticker.C:
			e.requestScan("", true, "周期兜底扫描")
		}
	}
}

func watcherKeyFor(cfg config.Config) string {
	var sb strings.Builder
	for _, f := range cfg.EnabledFolders() {
		sb.WriteString(f.ID)
		sb.WriteByte('=')
		sb.WriteString(strings.ToLower(filepath.Clean(f.LocalPath)))
		sb.WriteByte('\x00')
	}
	sb.WriteString("|")
	for _, p := range cfg.IgnorePatterns {
		sb.WriteString(strings.ToLower(p))
		sb.WriteByte('\x00')
	}
	return sb.String()
}

func (e *Engine) watchFolderLoop(ctx context.Context, folderID, dir string, ignorePatterns []string) {
	matcher := newIgnoreMatcher(ignorePatterns)
	backoff := 2 * time.Second
	for ctx.Err() == nil {
		watcher, err := mygo.WatchFiles(ctx, dir, mygo.FileWatchOptions{
			Recursive:  true,
			HintsOnly:  true,
			Debounce:   80 * time.Millisecond,
			MaxDelay:   400 * time.Millisecond,
			MaxEntries: 65536,
			Ignore: func(p string, isDir bool) bool {
				return matcher.Match(p, isDir)
			},
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			e.LogThrottled("watch-fail:"+folderID, "WARN", fmt.Sprintf("目录实时监控暂不可用 (%s): %v；已启用轮询兜底", dir, err), watchRetryMax)
			e.requestScan(folderID, false, "监控兜底扫描")
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < watchRetryMax {
				backoff *= 2
			}
			continue
		}
		backoff = 2 * time.Second
		e.LogThrottled("watch-ok:"+folderID, "INFO", fmt.Sprintf("已启用目录实时监控: %s", dir), time.Hour)

		for ctx.Err() == nil {
			events, nextErr := watcher.Next(ctx)
			if nextErr != nil {
				_ = watcher.Close()
				if ctx.Err() != nil {
					return
				}
				e.LogThrottled("watch-break:"+folderID, "WARN", fmt.Sprintf("目录监控中断 (%s): %v；正在自动重建", dir, nextErr), watchRetryMax)
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				break
			}
			if len(events) > 0 {
				e.requestScan(folderID, false, "目录变更")
			}
		}
	}
}

// scanLoop coalesces bursty watcher events, then runs one incremental scan.
// A minimum interval between scan starts keeps a large, continuously written
// folder from consuming a full CPU core.
func (e *Engine) scanLoop(ctx context.Context) {
	var lastScan time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.scanWake:
		}
		if wait := minScanInterval - time.Since(lastScan); wait > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
		}

		// Settle bursts, but never postpone a scan indefinitely while a large
		// copy keeps producing hints.
		settleDeadline := time.Now().Add(maxScanSettle)
		timer := time.NewTimer(scanDebounce)
	settle:
		for {
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-e.scanWake:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				delay := scanDebounce
				if remaining := time.Until(settleDeadline); remaining < delay {
					delay = remaining
					if delay < 0 {
						delay = 0
					}
				}
				timer.Reset(delay)
			case <-timer.C:
				break settle
			}
		}
		if ctx.Err() != nil {
			return
		}
		pending, all, reason := e.takeScanRequests()
		e.runLocalScan(ctx, pending, all, reason)
		lastScan = time.Now()
	}
}

func (e *Engine) runLocalScan(ctx context.Context, pending map[string]struct{}, all bool, reason string) {
	started := time.Now()
	cfg := e.Config()
	folders := cfg.EnabledFolders()

	var changed []FileMeta
	var deleted []Tombstone
	var errs []error

	if all || len(pending) == 0 {
		c, d, err := e.index.ScanFoldersIncremental(folders, cfg.IgnorePatterns)
		changed, deleted = c, d
		if err != nil {
			errs = append(errs, err)
		}
	} else {
		matched := 0
		for _, folder := range folders {
			if _, ok := pending[folder.ID]; !ok {
				continue
			}
			matched++
			c, d, err := e.index.ScanFolderIncremental(folder, cfg.IgnorePatterns)
			changed = append(changed, c...)
			deleted = append(deleted, d...)
			if err != nil {
				errs = append(errs, err)
			}
		}
		if matched == 0 {
			// Config changed under us: fall back to a full scan.
			c, d, err := e.index.ScanFoldersIncremental(folders, cfg.IgnorePatterns)
			changed, deleted = c, d
			if err != nil {
				errs = append(errs, err)
			}
		}
	}

	if ctx.Err() != nil {
		return
	}
	if len(errs) > 0 {
		scanErr := errors.Join(errs...)
		e.setScanFailure(scanErr)
		e.LogThrottled("scan:"+scanErr.Error(), "WARN", scanErr.Error(), 30*time.Second)
	} else {
		e.setScanSuccess()
	}

	if len(changed) == 0 && len(deleted) == 0 {
		if elapsed := time.Since(started); elapsed > 2*time.Second {
			e.Logf("目录扫描完成，无变更 (耗时 %s，来源: %s)", elapsed.Round(time.Millisecond), reason)
		}
		return
	}

	totalEvents := len(changed) + len(deleted)
	if totalEvents > 20 {
		e.LogLevel("SYNC", fmt.Sprintf("目录扫描完成: %d 项新增/更新，%d 项删除 (来源: %s)", len(changed), len(deleted), reason))
	} else {
		for i := range changed {
			f := changed[i]
			e.LogLevel("SYNC", fmt.Sprintf("检测到本地文件更新: %s (%s)", f.RelPath, FormatBytes(f.Size)))
		}
	}

	if cfg.Role == config.RoleHost {
		for i := range changed {
			f := changed[i]
			e.server.Broadcast(SyncEvent{
				Type:       EventUpsert,
				OriginNode: e.index.NodeID(),
				File:       &f,
			})
		}
		if cfg.SyncDeletions {
			for i := range deleted {
				t := deleted[i]
				if totalEvents <= 20 {
					e.LogLevel("SYNC", fmt.Sprintf("检测到本地文件删除: %s", t.RelPath))
				}
				e.server.Broadcast(SyncEvent{
					Type:       EventDelete,
					OriginNode: e.index.NodeID(),
					Tombstone:  &t,
				})
			}
		}
	} else {
		// Client mode: push local changes if direction allows. Serialize with
		// reconciliation so two goroutines never write the same peer file at once.
		if ctx.Err() == nil {
			e.reconcileMu.Lock()
			if cfg.SyncDirection != config.SyncPullOnly {
				for _, f := range changed {
					if ctx.Err() != nil {
						break
					}
					if err := e.pushFileToRemote(ctx, cfg, f, ""); err != nil {
						e.LogThrottled("push:"+f.RelPath+":"+err.Error(), "WARN", fmt.Sprintf("上传 %s 失败: %v", f.RelPath, err), 30*time.Second)
					}
				}
				if cfg.SyncDeletions {
					for _, t := range deleted {
						if ctx.Err() != nil {
							break
						}
						if err := e.pushDeleteToRemote(ctx, cfg, t); err != nil {
							e.LogThrottled("del:"+t.RelPath+":"+err.Error(), "WARN", fmt.Sprintf("同步删除 %s 失败: %v", t.RelPath, err), 30*time.Second)
						}
					}
				}
			}
			e.reconcileMu.Unlock()
		}
	}

	e.index.RequestSave()
	e.notifyUI()
}

func (e *Engine) clientLoop(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		if err := e.ReconcileWithRemote(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			e.mu.Lock()
			e.remoteConnected = false
			e.state = StateError
			e.lastError = err.Error()
			e.mu.Unlock()
			e.LogThrottled("client:"+err.Error(), "WARN", fmt.Sprintf("连接远端同步服务异常: %v", err), 15*time.Second)
			e.notifyUI()

			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 15*time.Second {
				backoff *= 2
			}
			continue
		}

		backoff = time.Second
		e.mu.Lock()
		e.remoteConnected = true
		e.state = StateRunning
		e.lastError = ""
		e.mu.Unlock()
		e.Logf("已连接远端隧道并完成初始同步对账")
		e.notifyUI()

		sseCtx, cancelSSE := context.WithCancel(ctx)
		eventsCtx, cancelEvents := context.WithCancel(ctx)
		go e.remoteEventLoop(eventsCtx)
		sseDone := make(chan error, 1)
		go func() { sseDone <- e.listenRemoteEvents(sseCtx) }()

		connectedAt := time.Now()
		ticker := time.NewTicker(reconcileInterval)

	inner:
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				cancelSSE()
				cancelEvents()
				return
			case err := <-sseDone:
				ticker.Stop()
				cancelSSE()
				cancelEvents()
				if ctx.Err() != nil {
					return
				}
				// Only reset the backoff after a connection that actually lived.
				if time.Since(connectedAt) > 20*time.Second {
					backoff = time.Second
				}
				e.mu.Lock()
				e.remoteConnected = false
				e.state = StateError
				e.lastError = fmt.Sprintf("实时事件流断开: %v", err)
				e.mu.Unlock()
				e.LogThrottled("sse:"+err.Error(), "WARN", fmt.Sprintf("实时事件流断开，将自动重连: %v", err), 15*time.Second)
				e.notifyUI()
				if backoff < 15*time.Second {
					backoff *= 2
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				break inner
			case <-e.reconcileWake:
				if err := e.ReconcileWithRemote(ctx); err != nil && ctx.Err() == nil {
					e.LogThrottled("reconcile-wake:"+err.Error(), "WARN", fmt.Sprintf("即时对账失败: %v", err), 15*time.Second)
				}
			case <-ticker.C:
				if err := e.ReconcileWithRemote(ctx); err != nil && ctx.Err() == nil {
					e.LogThrottled("reconcile-poll:"+err.Error(), "WARN", fmt.Sprintf("周期对账失败: %v", err), 30*time.Second)
				}
			}
		}
	}
}

// ReconcileWithRemote compares the local manifest with the remote Host's manifest
// and performs bidirectional, push-only, or pull-only synchronization across all mapped folders.
func (e *Engine) ReconcileWithRemote(ctx context.Context) error {
	e.reconcileMu.Lock()
	defer e.reconcileMu.Unlock()

	cfg := e.Config()
	baseURL := config.NormalizeRemoteURL(cfg.RemoteURL)
	if baseURL == "" {
		return errors.New("远端地址为空")
	}

	if _, _, err := e.index.ScanFoldersIncremental(cfg.EnabledFolders(), cfg.IgnorePatterns); err != nil {
		e.LogThrottled("reconcile-scan:"+err.Error(), "WARN", fmt.Sprintf("对账前本地扫描存在异常: %v", err), 30*time.Second)
	}
	localManifest := e.index.Snapshot()

	remoteManifest, err := e.fetchRemoteManifest(ctx, baseURL, cfg.AuthToken, cfg.EnabledFolders())
	if err != nil {
		return err
	}

	if len(remoteManifest.Folders) > 0 && e.ApplyPeerFolderMappings(remoteManifest.Folders) {
		cfg = e.Config()
		if _, _, err := e.index.ScanFoldersIncremental(cfg.EnabledFolders(), cfg.IgnorePatterns); err != nil {
			e.LogThrottled("reconcile-rescan:"+err.Error(), "WARN", fmt.Sprintf("应用远端目录映射后扫描异常: %v", err), 30*time.Second)
		}
		localManifest = e.index.Snapshot()
	}

	e.mu.Lock()
	e.remoteNodeID = remoteManifest.NodeID
	e.remoteConnected = true
	e.mu.Unlock()

	canPull := cfg.SyncDirection == config.SyncBidirectional || cfg.SyncDirection == config.SyncPullOnly
	canPush := cfg.SyncDirection == config.SyncBidirectional || cfg.SyncDirection == config.SyncPushOnly
	matcher := newIgnoreMatcher(cfg.IgnorePatterns)

	// 1. Handle files present on Remote
	for rel, remFile := range remoteManifest.Files {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if matcher.MatchVirtual(rel, false) {
			continue
		}
		locFile, hasLocal := localManifest.Files[rel]
		locTomb, hasLocTomb := localManifest.Tombstones[rel]

		if !hasLocal {
			// If deleted locally more recently than remote file's mtime, push deletion to remote
			if hasLocTomb && locTomb.DeletedAt > remFile.ModTime {
				if canPush && cfg.SyncDeletions {
					if err := e.pushDeleteToRemote(ctx, cfg, locTomb); err != nil {
						e.LogThrottled("del:"+rel+":"+err.Error(), "WARN", fmt.Sprintf("同步删除 %s 失败: %v", rel, err), 30*time.Second)
					}
				}
				continue
			}
			if canPull {
				if err := e.pullFileFromRemote(ctx, cfg, remFile, false); err != nil {
					e.LogThrottled("pull:"+rel+":"+err.Error(), "WARN", fmt.Sprintf("下载文件 %s 失败: %v", rel, err), 30*time.Second)
				}
			}
			continue
		}

		// Both have the file: check if identical
		if locFile.SHA256 == remFile.SHA256 {
			e.index.SetFileStatus(rel, StatusSynced, "双向一致")
			continue
		}

		// Hashes differ! Decide direction based on SyncDirection and ModTime
		if cfg.SyncDirection == config.SyncPullOnly {
			if err := e.pullFileFromRemote(ctx, cfg, remFile, cfg.ConflictPolicy == config.ConflictKeepBoth); err != nil {
				e.LogThrottled("pull:"+rel+":"+err.Error(), "WARN", fmt.Sprintf("下载文件 %s 失败: %v", rel, err), 30*time.Second)
			}
		} else if cfg.SyncDirection == config.SyncPushOnly {
			if err := e.pushFileToRemote(ctx, cfg, locFile, remFile.SHA256); err != nil {
				e.LogThrottled("push:"+rel+":"+err.Error(), "WARN", fmt.Sprintf("上传文件 %s 失败: %v", rel, err), 30*time.Second)
			}
		} else {
			// Bidirectional: compare ModTime
			if remFile.ModTime >= locFile.ModTime {
				if err := e.pullFileFromRemote(ctx, cfg, remFile, cfg.ConflictPolicy == config.ConflictKeepBoth); err != nil {
					e.LogThrottled("pull:"+rel+":"+err.Error(), "WARN", fmt.Sprintf("下载文件 %s 失败: %v", rel, err), 30*time.Second)
				}
			} else {
				if err := e.pushFileToRemote(ctx, cfg, locFile, remFile.SHA256); err != nil {
					e.LogThrottled("push:"+rel+":"+err.Error(), "WARN", fmt.Sprintf("上传文件 %s 失败: %v", rel, err), 30*time.Second)
				}
			}
		}
	}

	// 2. Handle files present Locally but missing on Remote
	for rel, locFile := range localManifest.Files {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if matcher.MatchVirtual(rel, false) {
			continue
		}
		if _, onRemote := remoteManifest.Files[rel]; onRemote {
			continue
		}
		remTomb, hasRemTomb := remoteManifest.Tombstones[rel]
		if hasRemTomb && remTomb.DeletedAt >= locFile.ModTime {
			if canPull && cfg.SyncDeletions {
				if err := e.deleteLocalFile(cfg, rel, remTomb.DeletedAt, "同步远端删除"); err != nil {
					e.LogThrottled("del-local:"+rel+":"+err.Error(), "WARN", fmt.Sprintf("删除本地文件 %s 失败: %v", rel, err), 30*time.Second)
				}
			}
			continue
		}
		if canPush {
			if err := e.pushFileToRemote(ctx, cfg, locFile, ""); err != nil {
				e.LogThrottled("push:"+rel+":"+err.Error(), "WARN", fmt.Sprintf("上传文件 %s 失败: %v", rel, err), 30*time.Second)
			}
		}
	}

	e.index.RequestSave()
	e.notifyUI()
	return nil
}

// listenRemoteEvents reads the Host's SSE stream and queues events for
// coalesced processing. An idle watchdog turns a silently dead tunnel into a
// visible reconnect instead of hanging forever.
func (e *Engine) listenRemoteEvents(ctx context.Context) error {
	cfg := e.Config()
	baseURL := config.NormalizeRemoteURL(cfg.RemoteURL)
	if baseURL == "" {
		return errors.New("远端地址为空")
	}

	sseCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	idle := time.AfterFunc(sseIdleTimeout, cancel)
	defer idle.Stop()

	req, err := http.NewRequestWithContext(sseCtx, http.MethodGet, baseURL+"/api/events", nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-SyncT-Token", cfg.AuthToken)
	req.Header.Set("X-SyncT-Node", e.index.NodeID())
	req.Header.Set("Accept", "text/event-stream")

	resp, err := e.transfer.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if sseCtx.Err() != nil {
			return fmt.Errorf("SSE 连接空闲超时 (%s)", sseIdleTimeout)
		}
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("SSE 状态码 %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)
	for scanner.Scan() {
		idle.Reset(sseIdleTimeout)
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		rawJSON := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if rawJSON == "" {
			continue
		}
		var ev SyncEvent
		if err := json.Unmarshal([]byte(rawJSON), &ev); err != nil || ev.Type == "" {
			continue
		}
		// Ignore events that originated from our own node
		if ev.OriginNode != "" && ev.OriginNode == e.index.NodeID() {
			continue
		}
		if e.isPushOnly() {
			continue
		}
		e.enqueueRemoteEvent(ev)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if sseCtx.Err() != nil {
		return fmt.Errorf("SSE 连接空闲超时 (%s)", sseIdleTimeout)
	}
	return errors.New("SSE 连接已断开")
}

// isPushOnly reads the direction without cloning the whole config (called per SSE line).
func (e *Engine) isPushOnly() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.cfg.SyncDirection == config.SyncPushOnly
}

func (e *Engine) enqueueRemoteEvent(ev SyncEvent) {
	select {
	case e.remoteEvents <- ev:
	default:
		// Slow consumer: fall back to a full reconciliation instead of blocking
		// the SSE reader or growing memory without bound.
		e.requestReconcile()
	}
}

// remoteEventLoop merges duplicate/stale events per path and applies a batch,
// so one editor save storm causes one download, not ten.
func (e *Engine) remoteEventLoop(ctx context.Context) {
	for {
		var first SyncEvent
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-e.remoteEvents:
			if !ok {
				return
			}
			first = ev
		}

		pending := make(map[string]SyncEvent)
		needRescan := false
		if first.Type == EventRescan {
			needRescan = true
		} else {
			mergeRemoteEvent(pending, first)
		}

		timer := time.NewTimer(remoteEventDebounce)
	collect:
		for {
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case ev, ok := <-e.remoteEvents:
				if !ok {
					timer.Stop()
					return
				}
				if ev.Type == EventRescan {
					needRescan = true
				} else {
					mergeRemoteEvent(pending, ev)
				}
			case <-timer.C:
				break collect
			}
		}

		if len(pending) > 0 {
			e.reconcileMu.Lock()
			if e.processRemoteEventsLocked(ctx, pending) {
				needRescan = true
			}
			e.reconcileMu.Unlock()
		}
		if needRescan {
			e.requestReconcile()
		}
	}
}

func mergeRemoteEvent(pending map[string]SyncEvent, ev SyncEvent) {
	var rel string
	switch {
	case ev.File != nil:
		rel = ev.File.RelPath
	case ev.Tombstone != nil:
		rel = ev.Tombstone.RelPath
	default:
		return
	}
	if rel == "" {
		return
	}
	if prev, ok := pending[rel]; ok && prev.Timestamp > ev.Timestamp {
		return
	}
	pending[rel] = ev
}

// processRemoteEventsLocked applies a coalesced batch. The caller holds reconcileMu.
// It returns true when a full reconciliation is needed (e.g. unknown mapping).
func (e *Engine) processRemoteEventsLocked(ctx context.Context, pending map[string]SyncEvent) bool {
	cfg := e.Config()
	if cfg.SyncDirection == config.SyncPushOnly {
		return false
	}
	rels := make([]string, 0, len(pending))
	for rel := range pending {
		rels = append(rels, rel)
	}
	sort.Strings(rels)

	matcher := newIgnoreMatcher(cfg.IgnorePatterns)
	needReconcile := false
	applied := 0
	for _, rel := range rels {
		if ctx.Err() != nil {
			return false
		}
		ev := pending[rel]
		switch ev.Type {
		case EventUpsert:
			if ev.File == nil || matcher.MatchVirtual(ev.File.RelPath, false) {
				continue
			}
			if _, _, _, ok := ResolveFolderFullPath(cfg, ev.File.RelPath); !ok {
				needReconcile = true
				continue
			}
			loc, hasLocal := e.index.FileMeta(ev.File.RelPath)
			if hasLocal && loc.SHA256 == ev.File.SHA256 {
				continue
			}
			if tomb, hasTomb := e.index.Tombstone(ev.File.RelPath); hasTomb &&
				cfg.SyncDirection != config.SyncPullOnly && tomb.DeletedAt > ev.File.ModTime {
				// A newer local deletion must not be undone by a stale upsert.
				needReconcile = true
				continue
			}
			if hasLocal && cfg.SyncDirection != config.SyncPullOnly && loc.ModTime > ev.File.ModTime {
				// Our copy is newer than the event's snapshot. Let the regular
				// reconciliation decide the direction instead of overwriting it.
				needReconcile = true
				continue
			}
			saveConflict := cfg.ConflictPolicy == config.ConflictKeepBoth && hasLocal
			if err := e.pullFileFromRemote(ctx, cfg, *ev.File, saveConflict); err != nil {
				e.LogThrottled("pull:"+rel+":"+err.Error(), "WARN", fmt.Sprintf("实时下载 %s 失败: %v", rel, err), 30*time.Second)
				continue
			}
			applied++
		case EventDelete:
			if ev.Tombstone == nil || !cfg.SyncDeletions || matcher.MatchVirtual(ev.Tombstone.RelPath, false) {
				continue
			}
			if _, _, _, ok := ResolveFolderFullPath(cfg, ev.Tombstone.RelPath); !ok {
				needReconcile = true
				continue
			}
			if loc, hasLocal := e.index.FileMeta(ev.Tombstone.RelPath); hasLocal &&
				cfg.SyncDirection != config.SyncPullOnly && loc.ModTime > ev.Tombstone.DeletedAt {
				// A local edit newer than the remote deletion wins; reconcile
				// decides whether to resurrect the file or apply the delete.
				needReconcile = true
				continue
			}
			if err := e.deleteLocalFile(cfg, ev.Tombstone.RelPath, ev.Tombstone.DeletedAt, "实时同步删除"); err != nil {
				e.LogThrottled("del-local:"+rel+":"+err.Error(), "WARN", fmt.Sprintf("实时删除 %s 失败: %v", rel, err), 30*time.Second)
				continue
			}
			applied++
		}
	}
	if applied > 0 {
		e.index.RequestSave()
		e.notifyUI()
	}
	return needReconcile
}

func (e *Engine) fetchRemoteManifest(ctx context.Context, baseURL, token string, localFolders []config.FolderMapping) (Manifest, error) {
	payload, err := json.Marshal(struct {
		Folders []config.FolderMapping `json:"folders"`
	}{
		Folders: localFolders,
	})
	if err != nil {
		return Manifest{}, fmt.Errorf("编码目录映射失败: %w", err)
	}
	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, baseURL+"/api/manifest", bytes.NewReader(payload))
	if err != nil {
		return Manifest{}, err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("X-SyncT-Token", token)
	req.Header.Set("X-SyncT-Node", e.index.NodeID())

	resp, err := e.client.Do(req)
	if err != nil {
		return Manifest{}, fmt.Errorf("获取远端清单失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return Manifest{}, errors.New("访问密钥 (Auth Token) 校验失败 (401)")
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return Manifest{}, fmt.Errorf("远端返回状态码 %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var m Manifest
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return Manifest{}, fmt.Errorf("解析远端清单失败: %w", err)
	}
	if m.Files == nil {
		m.Files = make(map[string]FileMeta)
	}
	if m.Tombstones == nil {
		m.Tombstones = make(map[string]Tombstone)
	}
	return m, nil
}

func transferTimeout(size int64) time.Duration {
	const minTimeout = 2 * time.Minute
	const maxTimeout = 60 * time.Minute
	// Assume a pessimistic 200 KiB/s link so slow tunnels do not spuriously fail.
	d := time.Duration(float64(size)/(200*1024)) * time.Second
	if d < minTimeout {
		d = minTimeout
	}
	if d > maxTimeout {
		d = maxTimeout
	}
	return d
}

func (e *Engine) pullFileFromRemote(ctx context.Context, cfg config.Config, rem FileMeta, saveConflict bool) error {
	rel, ok := CleanRelPath(rem.RelPath)
	if !ok {
		return errors.New("invalid remote path")
	}
	folder, _, fullPath, ok := ResolveFolderFullPath(cfg, rel)
	if !ok {
		return fmt.Errorf("未找到文件夹映射: %s", rel)
	}

	e.index.BeginWrite(rel)
	defer e.index.EndWrite(rel)

	start := time.Now()
	e.index.SetFileStatus(rel, StatusDownloading, "远端 -> 本地")
	e.notifyUI()

	baseURL := config.NormalizeRemoteURL(cfg.RemoteURL)
	u := fmt.Sprintf("%s/api/file?path=%s", baseURL, url.QueryEscape(rel))
	transferCtx, cancel := context.WithTimeout(ctx, transferTimeout(rem.Size))
	defer cancel()
	req, err := http.NewRequestWithContext(transferCtx, http.MethodGet, u, nil)
	if err != nil {
		e.index.SetFileStatus(rel, StatusError, "下载失败")
		return err
	}
	req.Header.Set("X-SyncT-Token", cfg.AuthToken)
	req.Header.Set("X-SyncT-Node", e.index.NodeID())

	resp, err := e.transfer.Do(req)
	if err != nil {
		e.index.SetFileStatus(rel, StatusError, "下载失败")
		return fmt.Errorf("请求远端文件失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		e.index.SetFileStatus(rel, StatusError, "下载失败")
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	parentDir := filepath.Dir(fullPath)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		e.index.SetFileStatus(rel, StatusError, "下载失败")
		return fmt.Errorf("创建目标目录失败: %w", err)
	}

	var randBuf [6]byte
	_, _ = rand.Read(randBuf[:])
	tmpPath := filepath.Join(parentDir, fmt.Sprintf(".synct.tmp.%s", hex.EncodeToString(randBuf[:])))

	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		e.index.SetFileStatus(rel, StatusError, "下载失败")
		return fmt.Errorf("创建临时文件失败: %w", err)
	}

	hasher := sha256.New()
	buf := ioBufferPool.Get().(*[]byte)
	written, copyErr := io.CopyBuffer(io.MultiWriter(tmpFile, hasher), resp.Body, *buf)
	ioBufferPool.Put(buf)
	closeErr := tmpFile.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(tmpPath)
		e.index.SetFileStatus(rel, StatusError, "下载失败")
		if copyErr != nil {
			return fmt.Errorf("写入下载文件失败: %w", copyErr)
		}
		return fmt.Errorf("关闭下载文件失败: %w", closeErr)
	}

	actualHash := hex.EncodeToString(hasher.Sum(nil))
	if rem.SHA256 != "" && !strings.EqualFold(rem.SHA256, actualHash) {
		_ = os.Remove(tmpPath)
		e.index.SetFileStatus(rel, StatusError, "下载失败")
		return fmt.Errorf("下载文件哈希校验失败 (%s)", rel)
	}

	conflictCreated := false
	if saveConflict {
		if existingInfo, statErr := os.Stat(fullPath); statErr == nil && existingInfo.Mode().IsRegular() {
			existingHash, hashErr := HashFile(fullPath)
			if hashErr == nil && existingHash != "" && existingHash != actualHash {
				backupPath := ConflictBackupPath(fullPath)
				if err := copyFileContents(fullPath, backupPath); err == nil {
					conflictCreated = true
					e.incrementConflict()
					e.Logf("冲突保护: 已保存本地冲突副本 %s", filepath.Base(backupPath))
				} else {
					e.LogThrottled("conflict:"+backupPath+":"+err.Error(), "WARN", fmt.Sprintf("保存冲突副本失败 (%s): %v", filepath.Base(backupPath), err), 30*time.Second)
				}
			}
		}
	}

	modTime := time.UnixMilli(rem.ModTime)
	if err := os.Chtimes(tmpPath, modTime, modTime); err != nil {
		e.LogThrottled("chtimes:"+rel, "WARN", fmt.Sprintf("设置文件时间失败 (%s): %v", rel, err), time.Minute)
	}

	st := StatusSynced
	if conflictCreated {
		st = StatusConflict
	}
	meta := FileMeta{
		FolderID: folder.ID,
		RelPath:  rel,
		Size:     written,
		ModTime:  rem.ModTime,
		SHA256:   actualHash,
	}

	// Commit first, index second, so a failed rename cannot leave a metadata
	// entry for content that never reached the final path.
	if err := replaceFile(tmpPath, fullPath); err != nil {
		_ = os.Remove(tmpPath)
		e.index.SetFileStatus(rel, StatusError, "下载失败")
		return fmt.Errorf("提交下载文件失败: %w", err)
	}
	if err := os.Chtimes(fullPath, modTime, modTime); err != nil {
		e.LogThrottled("chtimes:"+rel, "WARN", fmt.Sprintf("设置文件时间失败 (%s): %v", rel, err), time.Minute)
	}
	if info, err := os.Stat(fullPath); err == nil {
		meta.ModTime = info.ModTime().UTC().UnixMilli()
	}
	e.index.MarkFileSynced(meta, st, "远端 -> 本地")
	e.index.RequestSave()

	e.recordDownload(rel, written, time.Since(start))
	e.markSyncedActivity(fmt.Sprintf("已下载同步: %s (%s)", rel, FormatBytes(written)))
	return nil
}

// replaceFile atomically renames tmpPath over fullPath. It never removes the
// existing destination first: on a transient sharing violation the previous
// file stays intact and the caller reports the error.
func replaceFile(tmpPath, fullPath string) error {
	err := os.Rename(tmpPath, fullPath)
	if err == nil {
		return nil
	}
	// Windows antivirus/indexers can briefly hold the destination; one retry
	// resolves most cases without risking data loss.
	time.Sleep(25 * time.Millisecond)
	return os.Rename(tmpPath, fullPath)
}

func (e *Engine) pushFileToRemote(ctx context.Context, cfg config.Config, loc FileMeta, baseHash string) error {
	rel, ok := CleanRelPath(loc.RelPath)
	if !ok {
		return errors.New("invalid local path")
	}
	_, _, fullPath, ok := ResolveFolderFullPath(cfg, rel)
	if !ok {
		return fmt.Errorf("未找到文件夹映射: %s", rel)
	}

	f, err := os.Open(fullPath)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("不是常规文件: %s", rel)
	}
	size := info.Size()
	mtime := info.ModTime().UTC().UnixMilli()
	hash := loc.SHA256
	// The file may have changed since the last scan (or just be new): refresh
	// its hash without loading the whole file into memory.
	if hash == "" || loc.Size != size || abs64(loc.ModTime-mtime) > 2 {
		h := sha256.New()
		buf := ioBufferPool.Get().(*[]byte)
		_, hashErr := io.CopyBuffer(h, f, *buf)
		ioBufferPool.Put(buf)
		if hashErr != nil {
			return fmt.Errorf("读取文件失败: %w", hashErr)
		}
		hash = hex.EncodeToString(h.Sum(nil))
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("重置文件读取位置失败: %w", err)
		}
	}
	loc.Size = size
	loc.ModTime = mtime
	loc.SHA256 = hash
	if loc.FolderID == "" {
		loc.FolderID, _ = SplitFolderRelPath(rel)
	}

	start := time.Now()
	e.index.SetFileStatus(rel, StatusUploading, "本地 -> 远端")
	e.notifyUI()

	baseURL := config.NormalizeRemoteURL(cfg.RemoteURL)
	u := fmt.Sprintf(
		"%s/api/file?path=%s&mtime=%d&sha256=%s&base_hash=%s",
		baseURL,
		url.QueryEscape(rel),
		mtime,
		url.QueryEscape(hash),
		url.QueryEscape(baseHash),
	)
	transferCtx, cancel := context.WithTimeout(ctx, transferTimeout(size))
	defer cancel()
	req, err := http.NewRequestWithContext(transferCtx, http.MethodPut, u, f)
	if err != nil {
		e.index.SetFileStatus(rel, StatusError, "上传失败")
		return err
	}
	req.ContentLength = size
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-SyncT-Token", cfg.AuthToken)
	req.Header.Set("X-SyncT-Node", e.index.NodeID())

	resp, err := e.transfer.Do(req)
	if err != nil {
		e.index.SetFileStatus(rel, StatusError, "上传失败")
		return fmt.Errorf("上传请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		e.index.SetFileStatus(rel, StatusError, "上传失败")
		// A hash rejection with unchanged size/mtime would otherwise be cached
		// forever; force a real re-read and rescan.
		if resp.StatusCode == http.StatusBadRequest && strings.Contains(strings.ToLower(string(body)), "sha256") {
			e.index.InvalidateHash(rel)
			scanID := loc.FolderID
			if scanID == "" {
				scanID, _ = SplitFolderRelPath(rel)
			}
			e.requestScan(scanID, false, "哈希校验失败重扫")
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	e.index.MarkFileSynced(loc, StatusSynced, "本地 -> 远端")
	e.index.RequestSave()
	e.recordUpload(rel, size, time.Since(start))
	e.markSyncedActivity(fmt.Sprintf("已上传同步: %s (%s)", rel, FormatBytes(size)))
	return nil
}

func (e *Engine) pushDeleteToRemote(ctx context.Context, cfg config.Config, tomb Tombstone) error {
	rel, ok := CleanRelPath(tomb.RelPath)
	if !ok {
		return nil
	}
	baseURL := config.NormalizeRemoteURL(cfg.RemoteURL)
	u := fmt.Sprintf("%s/api/file?path=%s&deleted_at=%d", baseURL, url.QueryEscape(rel), tomb.DeletedAt)
	reqCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodDelete, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-SyncT-Token", cfg.AuthToken)
	req.Header.Set("X-SyncT-Node", e.index.NodeID())

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("删除请求失败: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	e.markSyncedActivity(fmt.Sprintf("已向远端同步删除: %s", rel))
	return nil
}

// deleteLocalFile removes a local file and records a tombstone. The index is
// only changed when the filesystem operation succeeds (or the file is already
// gone), so a locked file is retried on the next reconciliation.
func (e *Engine) deleteLocalFile(cfg config.Config, rel string, deletedAt int64, activity string) error {
	cleanRel, ok := CleanRelPath(rel)
	if !ok {
		return errors.New("invalid relative path")
	}
	folder, _, fullPath, ok := ResolveFolderFullPath(cfg, cleanRel)
	if !ok {
		return fmt.Errorf("未找到文件夹映射: %s", cleanRel)
	}
	e.index.BeginWrite(cleanRel)
	defer e.index.EndWrite(cleanRel)
	if err := os.Remove(fullPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	e.index.MarkFileDeleted(cleanRel, deletedAt)
	cleanEmptyParents(folder.LocalPath, filepath.Dir(fullPath))
	e.markSyncedActivity(fmt.Sprintf("%s: %s", activity, cleanRel))
	return nil
}

// Files returns the current list of tracked files for display in the UI.
func (e *Engine) Files() []FileEntry {
	return e.index.Entries()
}

// FilesWithVersion returns the cached file list plus the index generation,
// letting the UI reuse a filtered/sorted view across frames.
func (e *Engine) FilesWithVersion() ([]FileEntry, uint64) {
	return e.index.EntriesWithVersion()
}

// Stats returns a snapshot of the engine's live metrics in O(1).
func (e *Engine) Stats() Stats {
	now := time.Now()
	e.mu.RLock()
	st := Stats{
		State:           e.state,
		LastError:       e.lastError,
		LocalPort:       e.localPort,
		ConnectedPeers:  e.server.ConnectedPeers(),
		RemoteConnected: e.remoteConnected,
		RemoteNodeID:    e.remoteNodeID,
		FolderCount:     e.cfg.EnabledFolderCount(),
		BytesUploaded:   e.bytesUploaded.Load(),
		BytesDownloaded: e.bytesDownloaded.Load(),
		UploadSpeed:     e.uploadSpeed.Load(),
		DownloadSpeed:   e.downloadSpeed.Load(),
		LastSyncFile:    e.lastSyncFile,
		LastSyncBytes:   e.lastSyncBytes,
		SyncedEvents:    e.syncedEvents.Load(),
		ConflictsSaved:  e.conflictsSaved.Load(),
		LastSyncTime:    e.lastSyncTime,
		LastScanTime:    e.lastScanTime,
		LastScanError:   e.lastScanError,
		ScanErrors:      e.scanErrors.Load(),
	}
	e.mu.RUnlock()

	// Speeds decay to zero when nothing has moved recently instead of showing
	// the last burst rate forever.
	if last := e.lastUploadAt.Load(); last == 0 || now.Sub(time.Unix(0, last)) > speedIdleWindow {
		st.UploadSpeed = 0
	}
	if last := e.lastDownloadAt.Load(); last == 0 || now.Sub(time.Unix(0, last)) > speedIdleWindow {
		st.DownloadSpeed = 0
	}

	st.TotalFiles, st.TotalBytes = e.index.Counts()
	return st
}

// Logs returns a copy of the activity log entries.
func (e *Engine) Logs() []LogEntry {
	e.logsMu.RLock()
	defer e.logsMu.RUnlock()
	out := make([]LogEntry, len(e.logs))
	copy(out, e.logs)
	return out
}

// LogCount returns the number of buffered log entries without copying them.
func (e *Engine) LogCount() int {
	e.logsMu.RLock()
	defer e.logsMu.RUnlock()
	return len(e.logs)
}

// ClearLogs empties the activity log buffer.
func (e *Engine) ClearLogs() {
	e.logsMu.Lock()
	e.logs = nil
	e.logsMu.Unlock()
	e.notifyUI()
}

// Logf appends an INFO log entry.
func (e *Engine) Logf(format string, args ...any) {
	e.LogLevel("INFO", fmt.Sprintf(format, args...))
}

// LogLevel appends a log entry with the given level.
func (e *Engine) LogLevel(level, msg string) {
	id := e.logSeq.Add(1)
	entry := LogEntry{
		ID:      id,
		Time:    time.Now(),
		Level:   level,
		Message: msg,
	}
	e.logsMu.Lock()
	e.logs = append(e.logs, entry)
	if len(e.logs) > maxLogEntries {
		// Trim in place to avoid allocating a new slice on every line once full.
		copy(e.logs, e.logs[len(e.logs)-maxLogEntries:])
		e.logs = e.logs[:maxLogEntries]
	}
	e.logsMu.Unlock()
	e.notifyUI()
}

// LogThrottled logs a recurring error at most once per interval. Keys are
// bounded so a changing error string cannot grow memory without limit.
func (e *Engine) LogThrottled(key, level, msg string, interval time.Duration) {
	now := time.Now()
	e.logThrottleMu.Lock()
	if last, ok := e.logThrottle[key]; ok && now.Sub(last) < interval {
		e.logThrottleMu.Unlock()
		return
	}
	if len(e.logThrottle) > 128 {
		e.logThrottle = make(map[string]time.Time)
	}
	e.logThrottle[key] = now
	e.logThrottleMu.Unlock()
	e.LogLevel(level, msg)
}

func (e *Engine) setError(msg string) {
	e.mu.Lock()
	e.state = StateError
	e.lastError = msg
	e.mu.Unlock()
	e.LogLevel("ERROR", msg)
	e.notifyUI()
}

func (e *Engine) setScanFailure(err error) {
	e.mu.Lock()
	e.lastScanTime = time.Now()
	e.lastScanError = err.Error()
	e.mu.Unlock()
	e.scanErrors.Add(1)
	e.notifyUI()
}

func (e *Engine) setScanSuccess() {
	e.mu.Lock()
	e.lastScanTime = time.Now()
	changed := e.lastScanError != ""
	e.lastScanError = ""
	e.mu.Unlock()
	if changed {
		e.notifyUI()
	}
}

func (e *Engine) recordUpload(relPath string, n int64, dur time.Duration) {
	if n <= 0 {
		return
	}
	e.bytesUploaded.Add(n)
	if dur <= 0 {
		dur = time.Millisecond
	}
	bps := int64(float64(n) / dur.Seconds())
	if bps <= 0 {
		bps = n
	}
	e.uploadSpeed.Store(bps)
	e.lastUploadAt.Store(time.Now().UnixNano())
	e.mu.Lock()
	e.lastSyncFile = relPath
	e.lastSyncBytes = n
	e.mu.Unlock()
}

func (e *Engine) recordDownload(relPath string, n int64, dur time.Duration) {
	if n <= 0 {
		return
	}
	e.bytesDownloaded.Add(n)
	if dur <= 0 {
		dur = time.Millisecond
	}
	bps := int64(float64(n) / dur.Seconds())
	if bps <= 0 {
		bps = n
	}
	e.downloadSpeed.Store(bps)
	e.lastDownloadAt.Store(time.Now().UnixNano())
	e.mu.Lock()
	e.lastSyncFile = relPath
	e.lastSyncBytes = n
	e.mu.Unlock()
}

func (e *Engine) incrementConflict() {
	e.conflictsSaved.Add(1)
}

func (e *Engine) markSyncedActivity(msg string) {
	e.syncedEvents.Add(1)
	e.mu.Lock()
	e.lastSyncTime = time.Now()
	e.mu.Unlock()
	e.LogLevel("SYNC", msg)
}

func (e *Engine) notifyUI() {
	if e.onUpdate != nil {
		e.onUpdate()
	}
}
