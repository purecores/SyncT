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
	"html"
	"io"
	mathrand "math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/egoist/mygo"

	"synct/internal/applog"
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

// Host-side health check cadence. Variables so tests can shorten them.
var (
	hostHealthInterval  = 15 * time.Second
	publicProbeInterval = 60 * time.Second
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

type activityBucket struct {
	count int
	bytes int64
	last  string
	first time.Time
}

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
	HealthWarning   string
	ConnectedPeers  int
	RemoteConnected bool
	RemoteNodeID    string
	FolderCount     int
	TotalFiles      int
	UnsyncedFiles   int
	TotalDirs       int
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
	// LargeUploadBlocked reports that the peer refused a file for its size:
	// its SyncT build predates chunked uploads and cannot cross the tunnel's
	// per-request limit.
	LargeUploadBlocked bool
	// TLSRejected reports that cloudflared refused the local self-signed
	// certificate, which makes the public tunnel answer 502 until the cert is
	// trusted (or the ingress sets noTLSVerify).
	TLSRejected bool
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
	healthWarning   string
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
	uploadMeter     rateMeter
	downloadMeter   rateMeter
	lastUploadAt    atomic.Int64
	lastDownloadAt  atomic.Int64
	lastNotifyAt    atomic.Int64
	syncedEvents    atomic.Int64
	conflictsSaved  atomic.Int64
	scanErrors      atomic.Int64
	logSeq          atomic.Int64
	tlsRejected     atomic.Bool
	largeBlocked    atomic.Bool

	logsMu sync.RWMutex
	logs   []LogEntry

	// Health cadences are per engine so a test can shorten them without
	// racing a loop left by a previous engine.
	hostHealthEvery  time.Duration
	publicProbeEvery time.Duration

	// Activity is aggregated: a bulk sync produces thousands of successful
	// transfers, and one log line each buries the warnings the user needs to
	// see. noteSynced counts them and flushActivities writes one summary.
	activityMu sync.Mutex
	activities map[string]*activityBucket

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
	// A caller passing 0 explicitly asks for an OS-assigned port (tests and
	// embedded use). Normalize fills production defaults, so preserve that
	// intent instead of pinning every test host to the production port.
	autoPort := cfg.LocalPort == 0
	cfg = cfg.Clone()
	cfg.Normalize()
	if autoPort {
		cfg.LocalPort = 0
	}
	tr := newSyncHTTPTransport()
	// A transfer's response headers arrive only after its body is accepted.
	// A fixed 45-second header timeout made every slow upload fail with
	// "http2: timeout awaiting response headers" even though the tunnel was
	// still moving bytes. Per-request contexts carry the real deadline.
	tr.ResponseHeaderTimeout = 0
	e := &Engine{
		cfg:              cfg,
		index:            NewIndexStore(statePath),
		onUpdate:         onUpdate,
		state:            StateStopped,
		scanPending:      make(map[string]struct{}),
		scanWake:         make(chan struct{}, 1),
		watchCfgCh:       make(chan struct{}, 1),
		remoteEvents:     make(chan SyncEvent, remoteEventBuffer),
		reconcileWake:    make(chan struct{}, 1),
		hostHealthEvery:  hostHealthInterval,
		publicProbeEvery: publicProbeInterval,
		activities:       make(map[string]*activityBucket),
		logThrottle:      make(map[string]time.Time),
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
	e.index.SetNewFilesPending(cfg.Role == config.RoleClient && cfg.SyncDirection != config.SyncPullOnly)

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
	autoPort := cfg.LocalPort == 0
	cfg = cfg.Clone()
	cfg.Normalize()
	if autoPort {
		cfg.LocalPort = 0
	}

	e.mu.Lock()
	foldersChanged := e.cfg.SyncDir != cfg.SyncDir || !sameFolderMappings(e.cfg.SyncFolders, cfg.SyncFolders)
	ignoreChanged := !sameStrings(e.cfg.IgnorePatterns, cfg.IgnorePatterns)
	e.cfg = cfg
	e.mu.Unlock()
	e.index.SetNewFilesPending(cfg.Role == config.RoleClient && cfg.SyncDirection != config.SyncPullOnly)

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

// validatePeerRemotePath checks whether a RemotePath proposed by a peer is safe
// to materialize on this machine. By default only paths strictly inside syncDir
// are accepted; paths outside syncDir require explicit local opt-in via
// AllowPeerRemotePath and still reject relative paths, traversal segments, and
// filesystem roots.
func validatePeerRemotePath(syncDir, candidate string, allowExternal bool) (string, bool) {
	candidate = strings.TrimSpace(candidate)
	if candidate == "" || strings.ContainsRune(candidate, '\x00') {
		return "", false
	}
	for _, seg := range strings.Split(strings.ReplaceAll(candidate, "\\", "/"), "/") {
		if seg == ".." {
			return "", false
		}
	}
	cleaned := filepath.Clean(candidate)
	if !filepath.IsAbs(cleaned) {
		return "", false
	}
	// Reject filesystem roots such as "/" or "C:\".
	if filepath.Dir(cleaned) == cleaned {
		return "", false
	}
	if syncDir != "" {
		cleanSyncDir := filepath.Clean(syncDir)
		if isPathWithinRoot(cleanSyncDir, cleaned) {
			return cleaned, true
		}
	}
	if allowExternal {
		return cleaned, true
	}
	return "", false
}

// ApplyPeerFolderMappings merges folder mappings announced by the remote peer.
// Existing local folder paths are never overwritten by a peer. Unknown folder IDs
// are mapped under this machine's SyncDir unless the peer's RemotePath is inside
// SyncDir or the user explicitly enabled AllowPeerRemotePath.
func (e *Engine) ApplyPeerFolderMappings(peerFolders []config.FolderMapping) bool {
	if len(peerFolders) == 0 {
		return false
	}
	e.mu.Lock()
	cfg := e.cfg.Clone()
	if cfg.SyncDirection == config.SyncPushOnly {
		// A push-only node accepts nothing from the peer, including folder mappings.
		e.mu.Unlock()
		return false
	}
	changed := false
	var dirsToCreate []string
	var rejectedWarnings []string

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
			if peerTargetForUs != "" && !strings.EqualFold(filepath.Clean(existing.LocalPath), filepath.Clean(peerTargetForUs)) {
				rejectedWarnings = append(rejectedWarnings, fmt.Sprintf("已忽略远端对现有目录 %s 的路径覆盖请求 (%s)", id, peerTargetForUs))
			}
			if pf.Label != "" && existing.Label == "" {
				existing.Label = pf.Label
				changed = true
			}
		} else {
			var localDir string
			if peerTargetForUs != "" {
				if safeDir, ok := validatePeerRemotePath(cfg.SyncDir, peerTargetForUs, cfg.AllowPeerRemotePath); ok {
					localDir = safeDir
				} else {
					rejectedWarnings = append(rejectedWarnings, fmt.Sprintf("已拒绝远端指定的同步根目录外路径 (%s -> %s)，已回退至主同步目录内", id, peerTargetForUs))
				}
			}
			if localDir == "" {
				if id == config.DefaultFolderID {
					localDir = cfg.SyncDir
				} else {
					candidate := filepath.Join(cfg.SyncDir, id)
					if !isPathWithinRoot(cfg.SyncDir, candidate) {
						rejectedWarnings = append(rejectedWarnings, fmt.Sprintf("已拒绝非法远端目录 ID (%s)", pf.ID))
						continue
					}
					localDir = candidate
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
		for _, w := range rejectedWarnings {
			e.LogThrottled("peer-map-reject:"+w, "WARN", w, 5*time.Minute)
		}
		return false
	}

	cfg.Normalize()
	e.cfg = cfg
	e.mu.Unlock()

	for _, w := range rejectedWarnings {
		e.LogThrottled("peer-map-reject:"+w, "WARN", w, 5*time.Minute)
	}

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

	// A short frame cadence keeps the live rate smooth while a transfer runs
	// and lets it decay visibly afterwards; event-driven redraws alone would
	// leave the last number on screen until the next file finished.
	if e.onUpdate != nil {
		go e.telemetryLoop(ctx)
	}

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

	if ctx.Err() != nil {
		// Stop was pressed while the initial scan ran; do not bind listeners
		// for an engine the user already stopped.
		e.Stop()
		return ctx.Err()
	}

	if cfg.Role == config.RoleHost {
		// LAN Direct is the one mode that genuinely needs non-loopback binds;
		// every mode keeps loopback-only otherwise so no firewall prompt fires.
		port, err := e.server.Start(cfg.LocalPort, cfg.TunnelMode == config.TunnelDirect)
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
		go e.hostHealthLoop(ctx, port)
		return nil
	}

	// Client mode
	if ctx.Err() != nil {
		e.Stop()
		return ctx.Err()
	}
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
	e.flushActivities()
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

	addedDirs := e.index.TakeAddedDirs()
	deletedDirs := e.index.TakeDeletedDirs()
	if len(changed) == 0 && len(deleted) == 0 && len(addedDirs) == 0 && len(deletedDirs) == 0 {
		if elapsed := time.Since(started); elapsed > 2*time.Second {
			e.Logf("目录扫描完成，无变更 (耗时 %s，来源: %s)", elapsed.Round(time.Millisecond), reason)
		}
		return
	}

	totalEvents := len(changed) + len(deleted) + len(addedDirs) + len(deletedDirs)
	if totalEvents > 20 {
		e.LogLevel("SYNC", fmt.Sprintf("目录扫描完成: %d 项新增/更新，%d 项删除，%d 个新文件夹，%d 个删除文件夹 (来源: %s)", len(changed), len(deleted), len(addedDirs), len(deletedDirs), reason))
	} else {
		for i := range changed {
			f := changed[i]
			e.LogLevel("SYNC", fmt.Sprintf("检测到本地文件更新: %s (%s)", f.RelPath, FormatBytes(f.Size)))
		}
		for i := range addedDirs {
			e.LogLevel("SYNC", fmt.Sprintf("检测到新文件夹: %s", addedDirs[i].RelPath))
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
		for i := range addedDirs {
			d := addedDirs[i]
			e.server.Broadcast(SyncEvent{
				Type:       EventDir,
				OriginNode: e.index.NodeID(),
				Dir:        &d,
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
			for i := range deletedDirs {
				dt := deletedDirs[i]
				if totalEvents <= 20 {
					e.LogLevel("SYNC", fmt.Sprintf("检测到本地文件夹删除: %s", dt.RelPath))
				}
				e.server.Broadcast(SyncEvent{
					Type:       EventDirDelete,
					OriginNode: e.index.NodeID(),
					Tombstone:  &dt,
				})
			}
		}
	} else {
		// Client mode: push local changes if direction allows. Serialize with
		// reconciliation so two goroutines never write the same peer file at once.
		needDirDeleteSync := false
		if ctx.Err() == nil {
			e.reconcileMu.Lock()
			if cfg.SyncDirection != config.SyncPullOnly {
				if len(addedDirs) > 0 {
					if err := e.pushDirsToRemote(ctx, cfg, addedDirs); err != nil && !transferCanceled(ctx, err) {
						e.LogThrottled("dir-push:"+err.Error(), "WARN", fmt.Sprintf("同步空文件夹失败: %v", err), 30*time.Second)
					}
				}
				runTransferTasks(ctx, changed, func(f FileMeta) int64 { return f.Size }, func(f FileMeta) {
					baseHash := e.index.SyncedHash(f.RelPath)
					if err := e.pushFileToRemote(ctx, cfg, f, baseHash); err != nil && !transferCanceled(ctx, err) {
						e.LogThrottled("push:"+f.RelPath+":"+err.Error(), "WARN", fmt.Sprintf("上传 %s 失败: %v", f.RelPath, err), 30*time.Second)
					}
				})
				if cfg.SyncDeletions {
					for _, t := range deleted {
						if ctx.Err() != nil {
							break
						}
						if err := e.pushDeleteToRemote(ctx, cfg, t); err != nil {
							e.LogThrottled("del:"+t.RelPath+":"+err.Error(), "WARN", fmt.Sprintf("同步删除 %s 失败: %v", t.RelPath, err), 30*time.Second)
						}
					}
					if len(deletedDirs) > 0 {
						needDirDeleteSync = true
					}
				}
			}
			e.reconcileMu.Unlock()
		}
		if needDirDeleteSync {
			e.requestReconcile()
		}
	}

	e.index.RequestSave()
	e.notifyUI()
}

// reconnectDelay adds jitter to a reconnect backoff. Without it, both peers
// reconnecting after a shared outage retry in lockstep and keep colliding;
// with the origin rebinding on the Host side, synchronized retries also hide
// how long the work PC actually stays down.
func reconnectDelay(backoff time.Duration) time.Duration {
	if backoff <= 0 {
		return 0
	}
	jitter := time.Duration(mathrand.Int63n(int64(backoff) / 2))
	return backoff + jitter
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
			case <-time.After(reconnectDelay(backoff)):
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
				case <-time.After(reconnectDelay(backoff)):
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

	// Empty folders have no files to carry them across, so the manifest lists
	// directories separately. Pulling them first means a peer's file writes
	// land in a folder that already exists with the right name and casing.
	if canPull {
		e.ensureRemoteDirs(cfg, remoteManifest.Dirs)
	}

	// Gather the work first, then run independent file transfers concurrently.
	// A tunnel carries many requests over one HTTP/2 connection, so sequential
	// one-file-at-a-time sync leaves most of the link idle on small trees.
	type pullTask struct {
		rel      string
		rem      FileMeta
		conflict bool
	}
	type pushTask struct {
		rel      string
		loc      FileMeta
		baseHash string
	}
	var pulls []pullTask
	var pushes []pushTask

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
			// If deleted locally more recently than the remote file was last
			// changed or seen, push the deletion; otherwise the remote copy was
			// restored after our delete and wins.
			if hasLocTomb && locTomb.DeletedAt > effectiveTime(remFile) {
				if canPush && cfg.SyncDeletions {
					if err := e.pushDeleteToRemote(ctx, cfg, locTomb); err != nil {
						e.LogThrottled("del:"+rel+":"+err.Error(), "WARN", fmt.Sprintf("同步删除 %s 失败: %v", rel, err), 30*time.Second)
					}
				}
				continue
			}
			if canPull {
				pulls = append(pulls, pullTask{rel: rel, rem: remFile})
			}
			continue
		}

		// Both have the file: check if identical
		if locFile.SHA256 == remFile.SHA256 {
			e.index.SetFileStatus(rel, StatusSynced, "双向一致")
			e.index.ClearDirty(rel)
			continue
		}

		// Hashes differ! Decide direction based on SyncDirection and ModTime.
		switch {
		case cfg.SyncDirection == config.SyncPullOnly:
			pulls = append(pulls, pullTask{rel: rel, rem: remFile, conflict: cfg.ConflictPolicy == config.ConflictKeepBoth})
		case cfg.SyncDirection == config.SyncPushOnly:
			pushes = append(pushes, pushTask{rel: rel, loc: locFile, baseHash: e.index.SyncedHash(rel)})
		case remFile.ModTime >= locFile.ModTime:
			pulls = append(pulls, pullTask{rel: rel, rem: remFile, conflict: cfg.ConflictPolicy == config.ConflictKeepBoth})
		default:
			pushes = append(pushes, pushTask{rel: rel, loc: locFile, baseHash: e.index.SyncedHash(rel)})
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
		if hasRemTomb && remTomb.DeletedAt >= effectiveTime(locFile) {
			if canPull && cfg.SyncDeletions {
				if err := e.deleteLocalFile(cfg, rel, remTomb.DeletedAt, "同步远端删除"); err != nil {
					e.LogThrottled("del-local:"+rel+":"+err.Error(), "WARN", fmt.Sprintf("删除本地文件 %s 失败: %v", rel, err), 30*time.Second)
				}
			}
			continue
		}
		if canPush {
			pushes = append(pushes, pushTask{rel: rel, loc: locFile, baseHash: e.index.SyncedHash(rel)})
		}
	}

	runTransferTasks(ctx, pulls, func(t pullTask) int64 { return t.rem.Size }, func(t pullTask) {
		if err := e.pullFileFromRemote(ctx, cfg, t.rem, t.conflict); err != nil && !transferCanceled(ctx, err) {
			e.LogThrottled("pull:"+t.rel+":"+err.Error(), "WARN", fmt.Sprintf("下载文件 %s 失败: %v", t.rel, err), 30*time.Second)
		}
	})
	runTransferTasks(ctx, pushes, func(t pushTask) int64 { return t.loc.Size }, func(t pushTask) {
		if err := e.pushFileToRemote(ctx, cfg, t.loc, t.baseHash); err != nil && !transferCanceled(ctx, err) {
			e.LogThrottled("push:"+t.rel+":"+err.Error(), "WARN", fmt.Sprintf("上传文件 %s 失败: %v", t.rel, err), 30*time.Second)
		}
	})

	if canPull && cfg.SyncDeletions && len(remoteManifest.DirTombstones) > 0 {
		dirTombs := make([]Tombstone, 0, len(remoteManifest.DirTombstones))
		for _, dt := range remoteManifest.DirTombstones {
			dirTombs = append(dirTombs, dt)
		}
		e.applyPeerDirTombstones(cfg, dirTombs, remoteManifest.NodeID)
	}

	e.index.RequestSave()
	e.notifyUI()
	return nil
}

// Transfer concurrency is split by file size. Small files are latency-bound:
// a request carries one round trip, so concurrency is what fills the link. A
// latency proxy with 40 ms per request measured 94 files/s at 4 workers,
// 189 at 8, 368 at 16, 710 at 32 and 1390 at 64 (the raw request floor at
// 64 connections is 1524), so small files get 64 slots. Large files are
// bandwidth-bound and few slots avoid saturating the disk and uplink.
// The small-file count is a variable so transfer tests can compare settings.
var maxParallelTransfers = 64

const (
	maxParallelLargeTransfers = 4
	transferSmallFileLimit    = 1 << 20 // 1 MiB
	// Files above this use the chunked endpoint. A 1 MiB file can still take
	// over 100 seconds on a very slow uplink, and the tunnel edge answers
	// that with 524; chunking keeps each request inside both limits.
	chunkedUploadThreshold = 1 << 20
	uploadChunkSize        = 4 << 20
	minUploadChunkSize     = 512 << 10
)

// runTransferTasks splits work by size and runs both groups at the same time,
// so a large file never waits behind a queue of small ones and vice versa.
func runTransferTasks[T any](ctx context.Context, items []T, sizeOf func(T) int64, fn func(T)) {
	if len(items) == 0 {
		return
	}
	small := make([]T, 0, len(items))
	large := make([]T, 0, 4)
	for _, item := range items {
		if sizeOf(item) < transferSmallFileLimit {
			small = append(small, item)
		} else {
			large = append(large, item)
		}
	}
	var wg sync.WaitGroup
	if len(small) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			parallelDoN(ctx, small, maxParallelTransfers, fn)
		}()
	}
	if len(large) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			parallelDoN(ctx, large, maxParallelLargeTransfers, fn)
		}()
	}
	wg.Wait()
}

// parallelDoN runs fn over items with a bounded worker pool and returns when
// every item finished. The caller holds reconcileMu and owns ctx.
func parallelDoN[T any](ctx context.Context, items []T, workers int, fn func(T)) {
	if len(items) == 0 {
		return
	}
	if len(items) == 1 || workers <= 1 {
		for _, item := range items {
			if ctx.Err() != nil {
				return
			}
			fn(item)
		}
		return
	}
	if workers > len(items) {
		workers = len(items)
	}
	jobs := make(chan T)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for item := range jobs {
				if ctx.Err() != nil {
					continue
				}
				fn(item)
			}
		}()
	}
	for _, item := range items {
		if ctx.Err() != nil {
			break
		}
		jobs <- item
	}
	close(jobs)
	wg.Wait()
}

// ApplyPeerDirs creates the directories another node reports and records them.
// origin is ignored for duplicate filtering: a repeated creation is a no-op
// because the directory already exists.
func (e *Engine) ApplyPeerDirs(dirs []DirMeta, _ string) int {
	if len(dirs) == 0 {
		return 0
	}
	cfg := e.Config()
	if cfg.SyncDirection == config.SyncPushOnly {
		return 0
	}
	before := e.index.DirCount()
	byRel := make(map[string]DirMeta, len(dirs))
	for _, d := range dirs {
		if d.RelPath != "" {
			byRel[d.RelPath] = d
		}
	}
	e.ensureRemoteDirs(cfg, byRel)
	created := e.index.DirCount() - before
	if created > 0 {
		e.index.RequestSave()
		e.notifyUI()
	}
	return created
}

// ApplyPeerDirTombstones removes empty directories deleted on the peer and
// records directory tombstones so they do not reappear on future reconciliations.
func (e *Engine) ApplyPeerDirTombstones(tombstones []Tombstone, origin string) int {
	if len(tombstones) == 0 {
		return 0
	}
	cfg := e.Config()
	if cfg.SyncDirection == config.SyncPushOnly || !cfg.SyncDeletions {
		return 0
	}
	removed := e.applyPeerDirTombstones(cfg, tombstones, origin)
	if removed > 0 {
		e.index.RequestSave()
		e.notifyUI()
	}
	return removed
}

func (e *Engine) applyPeerDirTombstones(cfg config.Config, tombstones []Tombstone, origin string) int {
	if len(tombstones) == 0 || cfg.SyncDirection == config.SyncPushOnly || !cfg.SyncDeletions {
		return 0
	}
	sorted := append([]Tombstone(nil), tombstones...)
	// Remove deepest nested directories first so parents become empty before os.Remove.
	sort.Slice(sorted, func(i, j int) bool {
		di := strings.Count(sorted[i].RelPath, "/")
		dj := strings.Count(sorted[j].RelPath, "/")
		if di != dj {
			return di > dj
		}
		return sorted[i].RelPath > sorted[j].RelPath
	})

	matcher := newIgnoreMatcher(cfg.IgnorePatterns)
	removed := 0
	for _, tomb := range sorted {
		cleanRel, ok := CleanRelPath(tomb.RelPath)
		if !ok {
			continue
		}
		if matcher.MatchVirtual(cleanRel, true) {
			continue
		}
		_, inner, fullPath, ok := ResolveFolderFullPath(cfg, cleanRel)
		if !ok || inner == "" {
			continue
		}
		if d, hasDir := e.index.DirMeta(cleanRel); hasDir &&
			cfg.SyncDirection != config.SyncPullOnly && d.ModTime > tomb.DeletedAt {
			// Locally recreated after the peer's deletion; keep the directory.
			continue
		}
		info, statErr := os.Stat(fullPath)
		if statErr == nil {
			if !info.IsDir() {
				continue
			}
			if err := os.Remove(fullPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				// Non-empty directory or locked: never force-delete files inside.
				continue
			}
			rec, _ := e.index.MarkDirDeleted(cleanRel, tomb.DeletedAt)
			removed++
			if cfg.Role == config.RoleHost && e.server != nil {
				e.server.Broadcast(SyncEvent{
					Type:       EventDirDelete,
					OriginNode: origin,
					Tombstone:  &rec,
				})
			}
		} else if errors.Is(statErr, os.ErrNotExist) {
			rec, existed := e.index.MarkDirDeleted(cleanRel, tomb.DeletedAt)
			if existed {
				removed++
				if cfg.Role == config.RoleHost && e.server != nil {
					e.server.Broadcast(SyncEvent{
						Type:       EventDirDelete,
						OriginNode: origin,
						Tombstone:  &rec,
					})
				}
			}
		}
	}
	if removed > 0 {
		e.markSyncedActivity(fmt.Sprintf("已同步删除空文件夹: %d 个", removed))
	}
	return removed
}

// pushDirsToRemote tells the peer about folders created here. The host also
// sees them through the periodic manifest exchange, but this arrives while the
// user is still looking at the folder they just made.
func (e *Engine) pushDirsToRemote(ctx context.Context, cfg config.Config, dirs []DirMeta) error {
	if len(dirs) == 0 {
		return nil
	}
	baseURL := config.NormalizeRemoteURL(cfg.RemoteURL)
	if baseURL == "" {
		return errors.New("远端地址为空")
	}
	payload, err := json.Marshal(struct {
		OriginNode string    `json:"origin_node"`
		Dirs       []DirMeta `json:"dirs"`
	}{OriginNode: e.index.NodeID(), Dirs: dirs})
	if err != nil {
		return err
	}
	reqCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, baseURL+"/api/dirs", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("X-SyncT-Token", cfg.AuthToken)
	req.Header.Set("X-SyncT-Node", e.index.NodeID())
	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 512))
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

// ensureRemoteDirs creates the directories the peer reports. A path that no
// enabled mapping owns, or one the ignore rules cover, is skipped: an empty
// folder must never be created outside the folders the user chose to sync.
func (e *Engine) ensureRemoteDirs(cfg config.Config, dirs map[string]DirMeta) {
	created := 0
	matcher := newIgnoreMatcher(cfg.IgnorePatterns)
	for rel, dirMeta := range dirs {
		cleanRel, ok := CleanRelPath(rel)
		if !ok {
			continue
		}
		if matcher.MatchVirtual(cleanRel, true) {
			continue
		}
		if tomb, hasTomb := e.index.DirTombstone(cleanRel); hasTomb &&
			cfg.SyncDirection != config.SyncPullOnly && tomb.DeletedAt >= dirMeta.ModTime {
			// A local directory deletion newer than the peer's directory must not
			// be resurrected as a zombie folder.
			continue
		}
		folder, inner, fullPath, ok := ResolveFolderFullPath(cfg, cleanRel)
		if !ok || inner == "" {
			continue
		}
		if info, statErr := os.Stat(fullPath); statErr == nil {
			if info.IsDir() {
				e.index.AddDir(DirMeta{FolderID: folder.ID, RelPath: cleanRel, ModTime: dirMeta.ModTime})
				continue
			}
			// A file occupies the path; writing through it would fail anyway.
			e.LogThrottled("dir-conflict:"+rel, "WARN", fmt.Sprintf("远端文件夹与本地同名文件冲突，已跳过: %s", rel), time.Minute)
			continue
		}
		if err := os.MkdirAll(fullPath, 0o755); err != nil {
			e.LogThrottled("dir-mkdir:"+rel+":"+err.Error(), "WARN", fmt.Sprintf("创建远端空文件夹失败 (%s): %v", rel, err), 30*time.Second)
			continue
		}
		e.index.AddDir(DirMeta{FolderID: folder.ID, RelPath: cleanRel, ModTime: dirMeta.ModTime})
		created++
	}
	if created > 0 {
		e.markSyncedActivity(fmt.Sprintf("已创建远端空文件夹: %d 个", created))
	}
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
		return remoteStatusError("实时事件流", resp.StatusCode, string(body))
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
	case ev.Dir != nil:
		rel = ev.Dir.RelPath
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
	var dirDeletes []Tombstone
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
				cfg.SyncDirection != config.SyncPullOnly && tomb.DeletedAt > effectiveTime(*ev.File) {
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
				if transferCanceled(ctx, err) {
					return false
				}
				e.LogThrottled("pull:"+rel+":"+err.Error(), "WARN", fmt.Sprintf("实时下载 %s 失败: %v", rel, err), 30*time.Second)
				continue
			}
			applied++
		case EventDir:
			if ev.Dir == nil || ev.Dir.RelPath == "" || matcher.MatchVirtual(ev.Dir.RelPath, true) {
				continue
			}
			if tomb, hasTomb := e.index.DirTombstone(ev.Dir.RelPath); hasTomb &&
				cfg.SyncDirection != config.SyncPullOnly && tomb.DeletedAt >= ev.Dir.ModTime {
				needReconcile = true
				continue
			}
			e.ensureRemoteDirs(cfg, map[string]DirMeta{ev.Dir.RelPath: *ev.Dir})
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
				cfg.SyncDirection != config.SyncPullOnly && effectiveTime(loc) > ev.Tombstone.DeletedAt {
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
		case EventDirDelete:
			if ev.Tombstone == nil || !cfg.SyncDeletions || matcher.MatchVirtual(ev.Tombstone.RelPath, true) {
				continue
			}
			if _, _, _, ok := ResolveFolderFullPath(cfg, ev.Tombstone.RelPath); !ok {
				needReconcile = true
				continue
			}
			dirDeletes = append(dirDeletes, *ev.Tombstone)
		}
	}
	if len(dirDeletes) > 0 {
		applied += e.applyPeerDirTombstones(cfg, dirDeletes, "")
	}
	if applied > 0 {
		e.index.RequestSave()
		e.notifyUI()
	}
	return needReconcile
}

// remoteStatusError turns a non-200 reply from the tunnel into an actionable
// message. 502 from Cloudflare means the work PC reached the edge but the
// host's origin was unreachable; retrying forever without saying why leaves
// the user staring at "远端返回状态码 502" with no hint of what to fix.
var htmlTagRE = regexp.MustCompile(`<[^>]*>`)

// cleanHTTPBody turns an edge error page into one line. Cloudflare's 413/524
// bodies carry a full HTML page and an analytics script; writing that into the
// activity log hid the actual failure and made the UI unreadable.
func cleanHTTPBody(body string) string {
	s := htmlTagRE.ReplaceAllString(body, " ")
	s = html.UnescapeString(s)
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 180 {
		s = s[:180] + "…"
	}
	return s
}

// errTooLarge marks an upload the edge refused for its size (Cloudflare's
// free-plan body cap is 100 MB, and its edge returns 413 before the origin
// ever sees the request).
var errTooLarge = errors.New("文件超过隧道允许的单次上传大小")

// errGatewayTimeout marks a 524: the tunnel edge gave up waiting for the
// origin before the request finished.
var errGatewayTimeout = errors.New("隧道边缘等待源站超时")

func remoteStatusError(action string, status int, body string) error {
	detail := cleanHTTPBody(body)
	switch status {
	case http.StatusRequestEntityTooLarge:
		return fmt.Errorf("%s失败: %w (HTTP 413)。Cloudflare 隧道对单个请求体有 100MB 上限，SyncT 会改为分块上传，此提示通常表示补丁请求本身异常", action, errTooLarge)
	case 524:
		return fmt.Errorf("%s失败: %w (HTTP 524)。请检查主机端上行与 Cloudflared 源站；SyncT 会用分块续传重试", action, errGatewayTimeout)
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		msg := fmt.Sprintf("%s失败: 公网隧道返回 %d (源站不可达)。请确认主机端 SyncT 正在运行且本地监听端口 (18920) 未被占用，并重启主机端同步以恢复 Cloudflared 回源。", action, status)
		if detail != "" {
			msg += " 响应: " + detail
		}
		return errors.New(msg)
	}
	if detail == "" {
		return fmt.Errorf("%s失败: 远端返回状态码 %d", action, status)
	}
	return fmt.Errorf("%s失败: 远端返回状态码 %d: %s", action, status, detail)
}

func (e *Engine) fetchRemoteManifest(ctx context.Context, baseURL, token string, localFolders []config.FolderMapping) (Manifest, error) {
	cfg := e.Config()
	pushDirs := cfg.SyncDirection != config.SyncPullOnly
	var dirs []DirMeta
	var dirTombstones []Tombstone
	if pushDirs {
		dirs = e.index.DirsList()
		if cfg.SyncDeletions {
			dirTombstones = e.index.DirTombstonesList()
		}
	}
	payload, err := json.Marshal(struct {
		Folders       []config.FolderMapping `json:"folders"`
		Dirs          []DirMeta              `json:"dirs,omitempty"`
		DirTombstones []Tombstone            `json:"dir_tombstones,omitempty"`
		PushDirs      bool                   `json:"push_dirs,omitempty"`
	}{
		Folders:       localFolders,
		Dirs:          dirs,
		DirTombstones: dirTombstones,
		PushDirs:      pushDirs,
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
		return Manifest{}, remoteStatusError("获取远端清单", resp.StatusCode, string(body))
	}

	var m Manifest
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return Manifest{}, fmt.Errorf("解析远端清单失败: %w", err)
	}
	if m.Files == nil {
		m.Files = make(map[string]FileMeta)
	}
	if m.Dirs == nil {
		m.Dirs = make(map[string]DirMeta)
	}
	if m.DirTombstones == nil {
		m.DirTombstones = make(map[string]Tombstone)
	}
	if m.Tombstones == nil {
		m.Tombstones = make(map[string]Tombstone)
	}
	return m, nil
}

// transferCanceled reports a transfer that stopped because the engine is
// shutting down or switching configuration, not one that failed. Those are
// expected: logging one warning per in-flight file fills the log with noise
// the user cannot act on, and marking them errors hides the real state.
func transferCanceled(ctx context.Context, err error) bool {
	return ctx.Err() != nil || errors.Is(err, context.Canceled)
}

// transferTimeout gives every transfer a deadline. The 60-minute cap was
// shorter than several legitimate multi-gigabyte uploads over a home uplink
// and made them fail with a bare timeout. The estimate now assumes a slow
// 100 KiB/s link without a hard ceiling beyond 24 hours.
func transferTimeout(size int64) time.Duration {
	const minTimeout = 2 * time.Minute
	const maxTimeout = 24 * time.Hour
	d := time.Duration(float64(size)/(100*1024)) * time.Second
	if d < minTimeout {
		d = minTimeout
	}
	if d > maxTimeout {
		d = maxTimeout
	}
	return d
}

// transferStallTimeout aborts a transfer that stops making progress. A byte
// counter is watched while the body moves, so a dead connection is noticed in
// minutes instead of waiting for the whole deadline; a large file on a slow
// link keeps moving and is never cut off.
const transferStallTimeout = 3 * time.Minute

// stallOverride lets tests shorten the watchdog without waiting minutes. It
// holds a time.Duration in nanoseconds; product code reads it through
// stallTimeout.
var stallOverride atomic.Int64

func stallTimeout() time.Duration {
	if v := stallOverride.Load(); v > 0 {
		return time.Duration(v)
	}
	return transferStallTimeout
}

// fetchRemoteFile opens a download and retries a failed request a few times.
// The work-PC log showed three downloads failing with "哈希校验失败" in a row:
// one transient bad response turned into a visible error, and the mirror image
// (a truncated 200 response) would silently fail the digest check after the
// whole body arrived. A retry costs one request and removes both.
var (
	// Files at or above this threshold download in HTTP Range chunks into a
	// deterministic .synct.part file so an interrupted transfer resumes instead
	// of starting over from byte 0. Vars allow tests to exercise chunking with
	// small fixtures.
	resumableDownloadThreshold int64 = 1 << 20 // 1 MiB
	downloadChunkSize          int64 = 4 << 20 // 4 MiB per Range request
)

func partFilePath(parentDir, rel, sha string) string {
	sum := sha256.Sum256([]byte(rel + "\x00" + strings.ToLower(sha)))
	return filepath.Join(parentDir, ".synct.part."+hex.EncodeToString(sum[:8]))
}

func (e *Engine) fetchRemoteFile(transferCtx, ctx context.Context, cfg config.Config, u string) (*http.Response, error) {
	return e.fetchRemoteRange(transferCtx, ctx, cfg, u, -1, -1)
}

func (e *Engine) fetchRemoteRange(transferCtx, ctx context.Context, cfg config.Config, u string, start, end int64) (*http.Response, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		req, err := http.NewRequestWithContext(transferCtx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("X-SyncT-Token", cfg.AuthToken)
		req.Header.Set("X-SyncT-Node", e.index.NodeID())
		if start >= 0 && end >= start {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
		}
		resp, err := e.transfer.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("请求远端文件失败: %w", err)
			if transferCanceled(ctx, err) {
				return nil, lastErr
			}
			time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
			continue
		}
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent || resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
			return resp, nil
		}
		lastErr = remoteStatusError("下载文件", resp.StatusCode, "")
		resp.Body.Close()
		// Only a gateway/edge failure is worth retrying; a 404 or 403 is the
		// peer's answer and will not change on the next attempt.
		if resp.StatusCode != http.StatusBadGateway && resp.StatusCode != http.StatusServiceUnavailable &&
			resp.StatusCode != http.StatusGatewayTimeout && resp.StatusCode != 524 && resp.StatusCode != 520 {
			return nil, lastErr
		}
		time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
	}
	return nil, lastErr
}

func (e *Engine) pullFileFromRemote(ctx context.Context, cfg config.Config, rem FileMeta, saveConflict bool) error {
	return e.pullFile(ctx, cfg, rem, saveConflict, false)
}

// pullFileFromRemoteRetry is the one retry after a digest mismatch, so the
// failure path can call back in without recursing forever.
func (e *Engine) pullFileFromRemoteRetry(ctx context.Context, cfg config.Config, rem FileMeta, saveConflict bool) error {
	return e.pullFile(ctx, cfg, rem, saveConflict, true)
}

func (e *Engine) downloadFileSingle(ctx context.Context, cfg config.Config, u, parentDir, rel string, rem FileMeta) (string, int64, string, error) {
	transferCtx, cancel := context.WithTimeout(ctx, transferTimeout(rem.Size))
	defer cancel()
	stall := newStallWatch()
	stopStall := watchStall(stall, stallTimeout(), cancel)
	defer stopStall()
	resp, err := e.fetchRemoteFile(transferCtx, ctx, cfg, u)
	if err != nil {
		if transferCanceled(ctx, err) {
			return "", 0, "", context.Canceled
		}
		return "", 0, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", 0, "", remoteStatusError("下载文件", resp.StatusCode, "")
	}

	var randBuf [6]byte
	_, _ = rand.Read(randBuf[:])
	tmpPath := filepath.Join(parentDir, fmt.Sprintf(".synct.tmp.%s", hex.EncodeToString(randBuf[:])))

	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return "", 0, "", fmt.Errorf("创建临时文件失败: %w", err)
	}

	hasher := sha256.New()
	buf := ioBufferPool.Get().(*[]byte)
	written, copyErr := io.CopyBuffer(io.MultiWriter(&progressWriter{w: tmpFile, onMove: e.countDownloadBytes, stall: stall}, hasher), resp.Body, *buf)
	ioBufferPool.Put(buf)
	closeErr := tmpFile.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(tmpPath)
		if copyErr != nil && transferCanceled(ctx, copyErr) {
			return "", 0, "", context.Canceled
		}
		if copyErr != nil && stall.stalledFor(time.Now()) >= stallTimeout() {
			e.index.SetFileStatus(rel, StatusError, "传输停滞")
			return "", 0, "", fmt.Errorf("下载停滞超过 %s，已放弃本次传输 (%s)", stallTimeout(), rel)
		}
		if copyErr != nil {
			return "", 0, "", fmt.Errorf("写入下载文件失败: %w", copyErr)
		}
		return "", 0, "", fmt.Errorf("关闭下载文件失败: %w", closeErr)
	}
	return tmpPath, written, hex.EncodeToString(hasher.Sum(nil)), nil
}

// downloadFileResumable downloads rem in HTTP Range chunks into a deterministic
// .synct.part file so an interrupted large download resumes where it left off.
// Falls back cleanly to a full 200 OK body if the peer does not support Range.
func (e *Engine) downloadFileResumable(ctx context.Context, cfg config.Config, u, parentDir, rel string, rem FileMeta) (string, int64, string, error) {
	tmpPath := partFilePath(parentDir, rel, rem.SHA256)
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return "", 0, "", fmt.Errorf("创建断点续传临时文件失败: %w", err)
	}
	fileOpen := true
	closeFile := func() error {
		if !fileOpen {
			return nil
		}
		fileOpen = false
		return tmpFile.Close()
	}
	defer closeFile()

	info, err := tmpFile.Stat()
	if err != nil {
		return "", 0, "", fmt.Errorf("读取临时文件信息失败: %w", err)
	}
	offset := info.Size()
	if offset > rem.Size {
		if err := tmpFile.Truncate(0); err != nil {
			return "", 0, "", err
		}
		offset = 0
	} else if offset == rem.Size {
		// Previous attempt already fetched every byte; verify before re-fetching.
		if err := closeFile(); err != nil {
			return "", 0, "", err
		}
		if existingHash, hashErr := HashFile(tmpPath); hashErr == nil && strings.EqualFold(existingHash, rem.SHA256) {
			return tmpPath, rem.Size, existingHash, nil
		}
		tmpFile, err = os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
		if err != nil {
			return "", 0, "", fmt.Errorf("重置断点续传临时文件失败: %w", err)
		}
		fileOpen = true
		offset = 0
	}

	chunkSize := downloadChunkSize
	if chunkSize <= 0 {
		chunkSize = 4 << 20
	}

	zeroProgressFailures := 0
	for offset < rem.Size {
		if ctx.Err() != nil {
			return "", 0, "", context.Canceled
		}
		chunkEnd := offset + chunkSize - 1
		if chunkEnd >= rem.Size {
			chunkEnd = rem.Size - 1
		}
		expectedLen := chunkEnd - offset + 1

		transferCtx, cancel := context.WithTimeout(ctx, transferTimeout(expectedLen))
		stall := newStallWatch()
		stopStall := watchStall(stall, stallTimeout(), cancel)

		resp, reqErr := e.fetchRemoteRange(transferCtx, ctx, cfg, u, offset, chunkEnd)
		if reqErr != nil {
			stopStall()
			cancel()
			if transferCanceled(ctx, reqErr) {
				return "", 0, "", context.Canceled
			}
			// Keep the partial file on disk so the next reconciliation resumes.
			return "", 0, "", reqErr
		}

		switch resp.StatusCode {
		case http.StatusRequestedRangeNotSatisfiable:
			resp.Body.Close()
			stopStall()
			cancel()
			_ = closeFile()
			_ = os.Remove(tmpPath)
			return "", 0, "", fmt.Errorf("远端文件大小已变更 (HTTP 416): %s", rel)

		case http.StatusOK:
			// Peer returned the full file (no Range support): write from byte 0.
			if err := tmpFile.Truncate(0); err != nil {
				resp.Body.Close()
				stopStall()
				cancel()
				return "", 0, "", err
			}
			if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
				resp.Body.Close()
				stopStall()
				cancel()
				return "", 0, "", err
			}
			hasher := sha256.New()
			buf := ioBufferPool.Get().(*[]byte)
			written, copyErr := io.CopyBuffer(io.MultiWriter(&progressWriter{w: tmpFile, onMove: e.countDownloadBytes, stall: stall}, hasher), resp.Body, *buf)
			ioBufferPool.Put(buf)
			resp.Body.Close()
			stopStall()
			cancel()
			closeErr := closeFile()
			if copyErr != nil || closeErr != nil {
				_ = os.Remove(tmpPath)
				if copyErr != nil && transferCanceled(ctx, copyErr) {
					return "", 0, "", context.Canceled
				}
				if copyErr != nil && stall.stalledFor(time.Now()) >= stallTimeout() {
					e.index.SetFileStatus(rel, StatusError, "传输停滞")
					return "", 0, "", fmt.Errorf("下载停滞超过 %s，已放弃本次传输 (%s)", stallTimeout(), rel)
				}
				if copyErr != nil {
					return "", 0, "", fmt.Errorf("写入下载文件失败: %w", copyErr)
				}
				return "", 0, "", fmt.Errorf("关闭下载文件失败: %w", closeErr)
			}
			return tmpPath, written, hex.EncodeToString(hasher.Sum(nil)), nil

		case http.StatusPartialContent:
			serverHash := strings.TrimSpace(resp.Header.Get("X-SyncT-SHA256"))
			if serverHash != "" && !strings.EqualFold(serverHash, rem.SHA256) {
				resp.Body.Close()
				stopStall()
				cancel()
				_ = closeFile()
				_ = os.Remove(tmpPath)
				return "", 0, "", fmt.Errorf("远端文件在下载期间已变更 (%s)", rel)
			}
			contentRange := strings.TrimSpace(resp.Header.Get("Content-Range"))
			expectedPrefix := fmt.Sprintf("bytes %d-", offset)
			if !strings.HasPrefix(contentRange, expectedPrefix) {
				resp.Body.Close()
				stopStall()
				cancel()
				_ = closeFile()
				_ = os.Remove(tmpPath)
				return "", 0, "", fmt.Errorf("远端返回异常分段范围 (%s): %s", contentRange, rel)
			}
			if _, err := tmpFile.Seek(offset, io.SeekStart); err != nil {
				resp.Body.Close()
				stopStall()
				cancel()
				return "", 0, "", err
			}
			if err := tmpFile.Truncate(offset); err != nil {
				resp.Body.Close()
				stopStall()
				cancel()
				return "", 0, "", err
			}
			buf := ioBufferPool.Get().(*[]byte)
			n, copyErr := io.CopyBuffer(&progressWriter{w: tmpFile, onMove: e.countDownloadBytes, stall: stall}, io.LimitReader(resp.Body, expectedLen), *buf)
			ioBufferPool.Put(buf)
			resp.Body.Close()
			stalled := stall.stalledFor(time.Now()) >= stallTimeout()
			stopStall()
			cancel()

			if n > 0 {
				offset += n
				_ = tmpFile.Truncate(offset)
				zeroProgressFailures = 0
			}
			if copyErr != nil {
				if transferCanceled(ctx, copyErr) {
					return "", 0, "", context.Canceled
				}
				if stalled {
					e.index.SetFileStatus(rel, StatusError, "传输停滞")
					return "", 0, "", fmt.Errorf("下载停滞超过 %s，已放弃本次传输 (%s)", stallTimeout(), rel)
				}
				if n > 0 {
					// Partial progress persisted on disk; resume immediately from new offset.
					continue
				}
				zeroProgressFailures++
				if zeroProgressFailures >= 3 {
					return "", 0, "", fmt.Errorf("分块下载文件失败: %w", copyErr)
				}
				time.Sleep(time.Duration(zeroProgressFailures) * 200 * time.Millisecond)
				continue
			}
			if n == 0 {
				zeroProgressFailures++
				if zeroProgressFailures >= 3 {
					return "", 0, "", fmt.Errorf("远端分段响应提前结束 (%s)", rel)
				}
				time.Sleep(time.Duration(zeroProgressFailures) * 200 * time.Millisecond)
				continue
			}

		default:
			status := resp.StatusCode
			resp.Body.Close()
			stopStall()
			cancel()
			if status == http.StatusNotFound || status == http.StatusForbidden || status == http.StatusUnauthorized {
				_ = closeFile()
				_ = os.Remove(tmpPath)
			}
			return "", 0, "", remoteStatusError("下载文件", status, "")
		}
	}

	if err := closeFile(); err != nil {
		return "", 0, "", fmt.Errorf("关闭下载文件失败: %w", err)
	}
	actualHash, err := HashFile(tmpPath)
	if err != nil {
		return "", 0, "", fmt.Errorf("校验下载文件失败: %w", err)
	}
	return tmpPath, offset, actualHash, nil
}

func (e *Engine) pullFile(ctx context.Context, cfg config.Config, rem FileMeta, saveConflict, retried bool) error {
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

	parentDir := filepath.Dir(fullPath)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		e.index.SetFileStatus(rel, StatusError, "下载失败")
		return fmt.Errorf("创建目标目录失败: %w", err)
	}

	baseURL := config.NormalizeRemoteURL(cfg.RemoteURL)
	u := fmt.Sprintf("%s/api/file?path=%s", baseURL, url.QueryEscape(rel))

	var (
		tmpPath    string
		written    int64
		actualHash string
		err        error
	)
	if rem.Size > 0 && rem.Size >= resumableDownloadThreshold && rem.SHA256 != "" {
		tmpPath, written, actualHash, err = e.downloadFileResumable(ctx, cfg, u, parentDir, rel, rem)
	} else {
		tmpPath, written, actualHash, err = e.downloadFileSingle(ctx, cfg, u, parentDir, rel, rem)
	}
	if err != nil {
		if transferCanceled(ctx, err) {
			e.index.SetFileStatus(rel, StatusPending, "等待重试")
			return context.Canceled
		}
		e.index.SetFileStatus(rel, StatusError, "下载失败")
		return err
	}

	if rem.SHA256 != "" && !strings.EqualFold(rem.SHA256, actualHash) {
		_ = os.Remove(tmpPath)
		// A digest mismatch usually means the body was truncated or corrupted
		// in transit. That is worth one clean retry before surfacing an error;
		// the log showed the same file failing repeatedly otherwise, because
		// the next attempt re-read the same edge response.
		if !retried {
			retried = true
			e.index.SetFileStatus(rel, StatusPending, "校验失败重试")
			return e.pullFileFromRemoteRetry(ctx, cfg, rem, saveConflict)
		}
		e.index.SetFileStatus(rel, StatusError, "下载失败")
		return fmt.Errorf("下载文件哈希校验失败 (%s)", rel)
	}

	conflictCreated := false
	if saveConflict {
		if existingInfo, statErr := os.Stat(fullPath); statErr == nil && existingInfo.Mode().IsRegular() {
			existingHash, hashErr := HashFile(fullPath)
			if hashErr == nil && existingHash != "" && existingHash != actualHash {
				// Back the local file up only when the peer does not already
				// hold this exact content. An interrupted download leaves the
				// old copy in place; retrying it used to back that copy up on
				// every attempt, filling folders with identical .conflict-
				// files that carry no information the peer lacks.
				if e.index.IsDirty(rel) {
					conflictCreated = e.saveConflictCopy(rel, fullPath)
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
	e.noteSynced("已下载同步", rel, written)
	return nil
}

// saveConflictCopy preserves a differing local file before it is overwritten.
func (e *Engine) saveConflictCopy(rel, fullPath string) bool {
	backupPath := ConflictBackupPath(fullPath)
	if err := copyFileContents(fullPath, backupPath); err != nil {
		e.LogThrottled("conflict:"+backupPath+":"+err.Error(), "WARN", fmt.Sprintf("保存冲突副本失败 (%s): %v", filepath.Base(backupPath), err), 30*time.Second)
		return false
	}
	e.incrementConflict()
	e.Logf("冲突保护: 已保存本地冲突副本 %s", filepath.Base(backupPath))
	return true
}

// replaceFile atomically renames tmpPath over fullPath. It never removes the
// existing destination first: on a transient sharing violation the previous
// file stays intact and the caller reports the error.
func replaceFile(tmpPath, fullPath string) error {
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		err = os.Rename(tmpPath, fullPath)
		if err == nil {
			return nil
		}
		// Windows antivirus and the search indexer briefly hold freshly
		// written files as well as the destination. Back off a few times
		// before treating it as a real failure.
		time.Sleep(time.Duration(attempt+1) * 25 * time.Millisecond)
	}
	return err
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
	var resp *http.Response
	if size > chunkedUploadThreshold {
		// A single request this large can be rejected by the tunnel edge (413)
		// or time out there (524), so send it in parts that also allow a resume.
		resp, err = e.uploadChunked(ctx, cfg, baseURL, f, rel, size, mtime, hash, baseHash)
	} else {
		resp, err = e.uploadSingle(ctx, cfg, baseURL, f, rel, size, mtime, hash, baseHash)
	}
	if err != nil {
		if transferCanceled(ctx, err) {
			e.index.SetFileStatus(rel, StatusPending, "等待重试")
			return context.Canceled
		}
		e.index.SetFileStatus(rel, StatusError, "上传失败")
		return err
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
		if resp.StatusCode == http.StatusRequestEntityTooLarge {
			e.noteLargeUploadBlocked()
			return remoteStatusError("上传文件", resp.StatusCode, string(body))
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, cleanHTTPBody(string(body)))
	}

	e.index.MarkFileSynced(loc, StatusSynced, "本地 -> 远端")
	e.index.RequestSave()
	e.recordUpload(rel, size, time.Since(start))
	e.noteSynced("已上传同步", rel, size)
	return nil
}

// uploadSingle sends one file as a single PUT. Small files stay one request:
// paying chunk setup for a 40 KB file would cost more than it saves.
func (e *Engine) uploadSingle(ctx context.Context, cfg config.Config, baseURL string, f *os.File, rel string, size, mtime int64, hash, baseHash string) (*http.Response, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("重置文件读取位置失败: %w", err)
	}
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
	stall := newStallWatch()
	stopStall := watchStall(stall, stallTimeout(), cancel)
	defer stopStall()
	body := &progressReader{r: f, onMove: e.countUploadBytes, stall: stall}
	req, err := http.NewRequestWithContext(transferCtx, http.MethodPut, u, body)
	if err != nil {
		return nil, err
	}
	req.ContentLength = size
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-SyncT-Token", cfg.AuthToken)
	req.Header.Set("X-SyncT-Node", e.index.NodeID())
	resp, err := e.transfer.Do(req)
	if err != nil {
		if stall.stalledFor(time.Now()) >= stallTimeout() {
			return nil, fmt.Errorf("上传停滞超过 %s，已放弃本次传输: %w", stallTimeout(), err)
		}
		return nil, fmt.Errorf("上传请求失败: %w", err)
	}
	return resp, nil
}

// uploadChunked posts the file in ≤8 MB parts. Each part finishes well inside
// the edge's body limit and its 100-second origin timeout; a retry asks the
// origin how much it already has and resumes there instead of starting over.
func (e *Engine) uploadChunked(ctx context.Context, cfg config.Config, baseURL string, f *os.File, rel string, size, mtime int64, hash, baseHash string) (*http.Response, error) {
	session, err := e.beginChunkedUpload(ctx, cfg, baseURL, rel, size, mtime, hash, baseHash, "")
	if err != nil {
		return nil, err
	}
	buf := make([]byte, uploadChunkSize)
	chunkSize := int64(uploadChunkSize)
	var lastChunkErr error
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			_ = e.abortChunkedUpload(cfg, baseURL, session.UploadID)
			return nil, ctx.Err()
		}
		offset := session.Received
		for offset < size {
			n := chunkSize
			if size-offset < n {
				n = size - offset
			}
			// Seek for every attempt: a rejected part is retried at the same
			// offset with a smaller size, and the previous read left the file
			// position at its end.
			if _, err := f.Seek(offset, io.SeekStart); err != nil {
				_ = e.abortChunkedUpload(cfg, baseURL, session.UploadID)
				return nil, fmt.Errorf("重置文件读取位置失败: %w", err)
			}
			if _, err := io.ReadFull(f, buf[:n]); err != nil {
				_ = e.abortChunkedUpload(cfg, baseURL, session.UploadID)
				return nil, fmt.Errorf("读取文件失败: %w", err)
			}
			next, err := e.sendChunk(ctx, cfg, baseURL, session.UploadID, offset, buf[:n])
			if err != nil {
				lastChunkErr = err
				// The edge's body cap can be lower than ours (a zone or proxy
				// setting, or a build using a different limit). Halve the part
				// and retry the same offset until it fits.
				if (errors.Is(err, errTooLarge) || errors.Is(err, errGatewayTimeout)) && chunkSize > minUploadChunkSize {
					chunkSize /= 2
					if chunkSize < minUploadChunkSize {
						chunkSize = minUploadChunkSize
					}
					continue
				}
				break
			}
			offset = next.Received
		}
		session.Received = offset
		if offset >= size {
			break
		}
		if attempt >= 2 {
			_ = e.abortChunkedUpload(cfg, baseURL, session.UploadID)
			if lastChunkErr != nil {
				return nil, fmt.Errorf("分块上传多次中断，已放弃本次上传 (%s): %w", rel, lastChunkErr)
			}
			return nil, fmt.Errorf("分块上传多次中断，已放弃本次上传 (%s)", rel)
		}
		// Ask the origin for its durable offset; a lost connection may have
		// delivered a chunk whose acknowledgment never arrived.
		resumed, err := e.beginChunkedUpload(ctx, cfg, baseURL, rel, size, mtime, hash, baseHash, session.UploadID)
		if err != nil {
			return nil, err
		}
		if resumed.Received == 0 && resumed.UploadID == "" {
			resumed, err = e.beginChunkedUpload(ctx, cfg, baseURL, rel, size, mtime, hash, baseHash, "")
			if err != nil {
				return nil, err
			}
		}
		session = resumed
	}

	body, _ := json.Marshal(map[string]string{"upload_id": session.UploadID})
	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, baseURL+"/api/upload/commit?id="+url.QueryEscape(session.UploadID), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-SyncT-Token", cfg.AuthToken)
	req.Header.Set("X-SyncT-Node", e.index.NodeID())
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("提交分块上传失败: %w", err)
	}
	return resp, nil
}

func (e *Engine) beginChunkedUpload(ctx context.Context, cfg config.Config, baseURL, rel string, size, mtime int64, hash, baseHash, uploadID string) (uploadBeginResponse, error) {
	var out uploadBeginResponse
	payload, err := json.Marshal(uploadBeginRequest{
		Path: rel, Size: size, ModTime: mtime, SHA256: hash, BaseHash: baseHash, UploadID: uploadID,
	})
	if err != nil {
		return out, err
	}
	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, baseURL+"/api/upload/begin", bytes.NewReader(payload))
	if err != nil {
		return out, err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("X-SyncT-Token", cfg.AuthToken)
	req.Header.Set("X-SyncT-Node", e.index.NodeID())
	resp, err := e.client.Do(req)
	if err != nil {
		return out, fmt.Errorf("开始分块上传失败: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed {
		// The peer still runs a build without the chunked endpoints, so any
		// file over the tunnel's cap is impossible to send.
		e.noteLargeUploadBlocked()
		return out, fmt.Errorf("对端版本过旧，不支持分块上传: HTTP %d", resp.StatusCode)
	}
	if resp.StatusCode == http.StatusRequestEntityTooLarge {
		e.noteLargeUploadBlocked()
		return out, fmt.Errorf("开始分块上传失败: %w", errTooLarge)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return out, fmt.Errorf("开始分块上传失败: HTTP %d %s", resp.StatusCode, cleanHTTPBody(string(body)))
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return out, fmt.Errorf("解析分块上传会话失败: %w", err)
	}
	if out.UploadID == "" {
		return out, errors.New("分块上传会话为空")
	}
	e.ClearLargeUploadBlocked()
	return out, nil
}

func (e *Engine) sendChunk(ctx context.Context, cfg config.Config, baseURL, id string, offset int64, data []byte) (uploadBeginResponse, error) {
	var out uploadBeginResponse
	u := fmt.Sprintf("%s/api/upload/chunk?id=%s&offset=%d", baseURL, url.QueryEscape(id), offset)
	reqCtx, cancel := context.WithTimeout(ctx, transferTimeout(int64(len(data))))
	defer cancel()
	stall := newStallWatch()
	stopStall := watchStall(stall, stallTimeout(), cancel)
	defer stopStall()
	body := &progressReader{r: bytes.NewReader(data), onMove: e.countUploadBytes, stall: stall}
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, u, body)
	if err != nil {
		return out, err
	}
	req.ContentLength = int64(len(data))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-SyncT-Token", cfg.AuthToken)
	req.Header.Set("X-SyncT-Node", e.index.NodeID())
	resp, err := e.transfer.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
		return out, errors.New("分块上传会话已失效")
	}
	if resp.StatusCode == http.StatusRequestEntityTooLarge {
		return out, fmt.Errorf("分块被拒绝: %w", errTooLarge)
	}
	if resp.StatusCode == 524 {
		return out, fmt.Errorf("分块被边缘超时: %w", errGatewayTimeout)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return out, fmt.Errorf("发送分块失败: HTTP %d %s", resp.StatusCode, cleanHTTPBody(string(body)))
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return out, fmt.Errorf("解析分块响应失败: %w", err)
	}
	return out, nil
}

func (e *Engine) abortChunkedUpload(cfg config.Config, baseURL, id string) error {
	if id == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/api/upload/abort?id="+url.QueryEscape(id), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-SyncT-Token", cfg.AuthToken)
	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
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
		State:              e.state,
		LastError:          e.lastError,
		LocalPort:          e.localPort,
		HealthWarning:      e.healthWarning,
		ConnectedPeers:     e.server.ConnectedPeers(),
		RemoteConnected:    e.remoteConnected,
		RemoteNodeID:       e.remoteNodeID,
		FolderCount:        e.cfg.EnabledFolderCount(),
		BytesUploaded:      e.bytesUploaded.Load(),
		BytesDownloaded:    e.bytesDownloaded.Load(),
		UploadSpeed:        e.uploadMeter.rate(now),
		DownloadSpeed:      e.downloadMeter.rate(now),
		LastSyncFile:       e.lastSyncFile,
		LastSyncBytes:      e.lastSyncBytes,
		SyncedEvents:       e.syncedEvents.Load(),
		ConflictsSaved:     e.conflictsSaved.Load(),
		LastSyncTime:       e.lastSyncTime,
		LastScanTime:       e.lastScanTime,
		LastScanError:      e.lastScanError,
		ScanErrors:         e.scanErrors.Load(),
		TLSRejected:        e.tlsRejected.Load(),
		LargeUploadBlocked: e.largeBlocked.Load(),
	}
	e.mu.RUnlock()

	// Speeds decay to zero when nothing has moved recently instead of showing
	// the last burst rate forever. The meter's own window already excludes the
	// bytes, so this only needs to stop the UI from polling a stale sample.
	if last := e.lastUploadAt.Load(); last == 0 || now.Sub(time.Unix(0, last)) > speedIdleWindow {
		st.UploadSpeed = 0
		e.uploadMeter.reset()
	}
	if last := e.lastDownloadAt.Load(); last == 0 || now.Sub(time.Unix(0, last)) > speedIdleWindow {
		st.DownloadSpeed = 0
		e.downloadMeter.reset()
	}

	st.TotalFiles, st.TotalBytes = e.index.Counts()
	st.UnsyncedFiles = e.index.UnsyncedCount()
	st.TotalDirs = e.index.DirCount()
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
	applog.Write(level, msg)
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

// hostHealthLoop keeps the published origin honest on the Host machine:
//   - it probes the local listener so a dead origin is rebound long before a
//     peer notices, and
//   - when a public URL is configured it periodically asks that URL, which is
//     the cloudflared -> origin path the work PC actually uses. A 502 there
//     while the local listener answers means the tunnel and the origin
//     disagree (stale ingress, wrong port, recreated tunnel); a 502 while the
//     local listener is dead is repaired by rebinding the origin.
func (e *Engine) hostHealthLoop(ctx context.Context, port int) {
	e.mu.RLock()
	localEvery, publicEvery := e.hostHealthEvery, e.publicProbeEvery
	e.mu.RUnlock()
	if localEvery <= 0 {
		localEvery = hostHealthInterval
	}
	if publicEvery <= 0 {
		publicEvery = publicProbeInterval
	}
	localTicker := time.NewTicker(localEvery)
	publicTicker := time.NewTicker(publicEvery)
	defer localTicker.Stop()
	defer publicTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-localTicker.C:
			if !e.server.IsServing() || !e.probeLocal(port) {
				e.LogThrottled("origin-health", "WARN", "本地同步源站无响应，正在自动重建监听以恢复 Cloudflared 隧道", 30*time.Second)
				restartErr := e.server.Restart()
				if restartErr != nil {
					e.setHealthWarning(fmt.Sprintf("本地同步源站重建失败: %v", restartErr))
					continue
				}
				// Only clear a warning this loop owns; a 502 reported by the
				// public probe describes the tunnel and must survive.
				e.clearOriginWarning()
			}
		case <-publicTicker.C:
			baseURL := config.NormalizeRemoteURL(e.Config().CustomPublicURL)
			if baseURL == "" {
				continue
			}
			probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			status, err := e.probeURL(probeCtx, baseURL+"/api/status")
			cancel()
			switch {
			case err != nil:
				e.LogThrottled("public-probe:"+err.Error(), "WARN",
					fmt.Sprintf("公网隧道健康检查失败 (%s): %v", baseURL, err), 2*time.Minute)
			case status == http.StatusBadGateway || status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout:
				if e.probeLocal(port) {
					// The origin answers on loopback, so cloudflared's failure to
					// reach it is about TLS trust, not the listener.
					e.noteTLSRejection()
					e.LogThrottled(fmt.Sprintf("public-502:%d", status), "ERROR",
						fmt.Sprintf("公网地址 %s 返回 %d：本地源站正常，问题在 Cloudflared 回源 TLS 校验。请开启隧道源的 No TLS Verify，或安装本地证书到受信任根。", baseURL, status),
						time.Minute)
				} else {
					msg := fmt.Sprintf("公网地址 %s 返回 %d (Bad Gateway)：Cloudflared 已在线，但其回源目标 https://localhost:%d 不可达。请确认主机端 SyncT 已启动同步且端口未被占用。", baseURL, status, port)
					e.LogThrottled(fmt.Sprintf("public-502:%d", status), "ERROR", msg, time.Minute)
					e.setHealthWarning(msg)
				}
			case status == http.StatusUnauthorized, status >= 200 && status < 400:
				// 401 means cloudflared reached the origin and only this
				// unauthenticated probe was rejected; both ends are healthy.
				e.clearTLSRejection()
				e.clearHealthWarning()
			}
		}
	}
}

// probeLocal asks the local loopback origin for /api/status without auth.
// Any HTTP response, including 401, proves the listener and TLS path work.
func (e *Engine) probeLocal(port int) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	status, err := e.probeURL(ctx, fmt.Sprintf("http://127.0.0.1:%d/api/status", port))
	return err == nil && status > 0 && status < 500
}

func (e *Engine) probeURL(ctx context.Context, u string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("X-SyncT-Node", e.index.NodeID())
	resp, err := e.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return resp.StatusCode, nil
}

// noteTLSRejection records that the tunnel's origin client refused SyncT's
// self-signed certificate. Nothing on the SyncT side can complete that
// handshake: cloudflared must either skip verification (noTLSVerify) or trust
// the certificate installed on this machine.
func (e *Engine) noteTLSRejection() {
	if e.tlsRejected.Swap(true) {
		return
	}
	e.setHealthWarning("Cloudflared 拒绝了 SyncT 的本地自签名证书 (x509: certificate signed by unknown authority)，因此公网地址返回 502。可点击「修复 502」把本地证书安装到本机受信任根，或在 Cloudflare 控制台开启 No TLS Verify / 将源站改为 http://localhost:18920。")
	e.LogThrottled("tls-trust-hint", "WARN", "TLS 排查：cloudflared 日志 C:\\ProgramData\\cloudflared\\cloudflared.log 中的 x509 unknown authority 表示自签名证书未被信任", 5*time.Minute)
	e.notifyUI()
}

func (e *Engine) clearTLSRejection() {
	if e.tlsRejected.CompareAndSwap(true, false) {
		e.notifyUI()
	}
}

func (e *Engine) setHealthWarning(msg string) {
	e.mu.Lock()
	changed := e.healthWarning != msg
	e.healthWarning = msg
	e.mu.Unlock()
	if changed {
		e.notifyUI()
	}
}

// clearOriginWarning drops only the origin-recovery warning; a tunnel warning
// is cleared by the public probe that raised it.
func (e *Engine) clearOriginWarning() {
	e.mu.Lock()
	changed := e.healthWarning != "" && !strings.HasPrefix(e.healthWarning, "公网地址")
	if changed {
		e.healthWarning = ""
	}
	e.mu.Unlock()
	if changed {
		e.notifyUI()
	}
}

func (e *Engine) clearHealthWarning() {
	e.mu.Lock()
	changed := e.healthWarning != ""
	e.healthWarning = ""
	e.mu.Unlock()
	if changed {
		e.notifyUI()
	}
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

// countUploadBytes feeds the live upload meter as bytes are read from disk.
// It is called once per streamed buffer, so the rate moves while a large file
// transfers instead of jumping when the request finishes.
func (e *Engine) countUploadBytes(n int64) {
	if n <= 0 {
		return
	}
	sampleRateMeter(&e.uploadMeter, n)
	e.lastUploadAt.Store(time.Now().UnixNano())
	e.throttledNotifyUI()
}

// countDownloadBytes feeds the live download meter as bytes reach disk.
func (e *Engine) countDownloadBytes(n int64) {
	if n <= 0 {
		return
	}
	sampleRateMeter(&e.downloadMeter, n)
	e.lastDownloadAt.Store(time.Now().UnixNano())
	e.throttledNotifyUI()
}

// sampleRateMeter records a chunk at the current time. Kept free-standing so
// the copy loops can call it without allocating a closure per buffer.
func sampleRateMeter(m *rateMeter, n int64) {
	m.add(n, time.Now())
}

func (e *Engine) recordUpload(relPath string, n int64, _ time.Duration) {
	if n <= 0 {
		return
	}
	e.bytesUploaded.Add(n)
	e.lastUploadAt.Store(time.Now().UnixNano())
	e.mu.Lock()
	e.lastSyncFile = relPath
	e.lastSyncBytes = n
	e.mu.Unlock()
}

func (e *Engine) recordDownload(relPath string, n int64, _ time.Duration) {
	if n <= 0 {
		return
	}
	e.bytesDownloaded.Add(n)
	e.lastDownloadAt.Store(time.Now().UnixNano())
	e.mu.Lock()
	e.lastSyncFile = relPath
	e.lastSyncBytes = n
	e.mu.Unlock()
}

func (e *Engine) incrementConflict() {
	e.conflictsSaved.Add(1)
}

func (e *Engine) noteSynced(verb, relPath string, n int64) {
	now := time.Now()
	e.syncedEvents.Add(1)
	e.mu.Lock()
	e.lastSyncTime = now
	e.lastSyncFile = relPath
	e.lastSyncBytes = n
	e.mu.Unlock()
	e.activityMu.Lock()
	b := e.activities[verb]
	if b == nil {
		b = &activityBucket{first: now}
		e.activities[verb] = b
	}
	b.count++
	b.bytes += n
	b.last = relPath
	e.activityMu.Unlock()
	e.throttledNotifyUI()
}

// flushActivities writes one line per direction for every transfer that
// completed since the last call, with the batch size, duration and average.
func (e *Engine) flushActivities() {
	e.activityMu.Lock()
	if len(e.activities) == 0 {
		e.activityMu.Unlock()
		return
	}
	buckets := e.activities
	e.activities = make(map[string]*activityBucket)
	e.activityMu.Unlock()

	now := time.Now()
	verbs := make([]string, 0, len(buckets))
	for verb := range buckets {
		verbs = append(verbs, verb)
	}
	sort.Strings(verbs)
	for _, verb := range verbs {
		b := buckets[verb]
		if b.count <= 1 {
			e.LogLevel("SYNC", fmt.Sprintf("%s: %s (%s)", verb, b.last, FormatBytes(b.bytes)))
			continue
		}
		dur := now.Sub(b.first)
		if dur <= 0 {
			dur = time.Millisecond
		}
		rate := int64(float64(b.bytes) / dur.Seconds())
		e.LogLevel("SYNC", fmt.Sprintf("%s: %d 个文件 (%s)，用时 %s，平均 %s，最近 %s",
			verb, b.count, FormatBytes(b.bytes), dur.Round(100*time.Millisecond), FormatSpeed(rate), b.last))
	}
}

func (e *Engine) markSyncedActivity(msg string) {
	e.syncedEvents.Add(1)
	e.mu.Lock()
	e.lastSyncTime = time.Now()
	e.mu.Unlock()
	e.LogLevel("SYNC", msg)
}

func (e *Engine) noteLargeUploadBlocked() {
	if e.largeBlocked.Swap(true) {
		return
	}
	e.setHealthWarning("有文件因大小被隧道拒绝：对端 SyncT 版本较旧，不支持分块上传。请把两端都升级到 v1.3.0 或更新版本。")
	e.notifyUI()
}

// ClearLargeUploadBlocked drops the warning once a large upload succeeds.
func (e *Engine) ClearLargeUploadBlocked() {
	if e.largeBlocked.CompareAndSwap(true, false) {
		e.clearHealthWarning()
	}
}

// NoteResyncFailure records a transfer that failed and will be retried. It is
// an error the index shows as 待同步, but the user needs it once, not once per
// scan: the throttle key is the message, so a 413 storm collapses to a line.
func (e *Engine) NoteResyncFailure(relPath string, err error) {
	e.LogThrottled("resync:"+relPath+":"+err.Error(), "WARN",
		fmt.Sprintf("同步失败，将自动重试 %s: %v", relPath, err), 5*time.Minute)
}

func (e *Engine) notifyUI() {
	if e.onUpdate != nil {
		e.onUpdate()
	}
}

// telemetryLoop refreshes the dashboard while the engine runs. It is a
// display clock, not a poller: all measurements are atomic counters read by
// Stats, so a frame costs no locks beyond the O(1) snapshot.
func (e *Engine) telemetryLoop(ctx context.Context) {
	ticker := time.NewTicker(notifyUIInterval)
	defer ticker.Stop()
	lastFlush := time.Now()
	for {
		select {
		case <-ctx.Done():
			e.flushActivities()
			return
		case <-ticker.C:
			if time.Since(lastFlush) >= time.Second {
				e.flushActivities()
				lastFlush = time.Now()
			}
			e.notifyUI()
		}
	}
}

// notifyUIInterval bounds how often byte-level progress wakes the UI. Without
// it every 64 KB buffer would request a frame (~15,000 per gigabyte), and the
// view is rebuilt whole per frame; four updates a second keep the speed
// readout live at a negligible cost.
const notifyUIInterval = 250 * time.Millisecond

func (e *Engine) throttledNotifyUI() {
	now := time.Now().UnixNano()
	last := e.lastNotifyAt.Load()
	if now-last < int64(notifyUIInterval) {
		return
	}
	if e.lastNotifyAt.CompareAndSwap(last, now) {
		e.notifyUI()
	}
}
