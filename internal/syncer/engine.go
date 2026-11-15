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
	"strings"
	"sync"
	"sync/atomic"
	"time"

		"github.com/egoist/mygo"

	"synct/internal/config"
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
}

// Engine coordinates local folder watching (`mygo.WatchFiles`), the Host HTTP/SSE server,
// and the Client HTTP/SSE synchronization loop.
type Engine struct {
	mu       sync.RWMutex
	cfg      config.Config
	index    *IndexStore
	server   *Server
	client   *http.Client
	onUpdate func()

	state           EngineState
	lastError       string
	localPort       int
	remoteConnected bool
	remoteNodeID    string
	lastSyncTime    time.Time
	lastSyncFile    string
	lastSyncBytes   int64

	bytesUploaded   atomic.Int64
	bytesDownloaded atomic.Int64
	uploadSpeed     atomic.Int64
	downloadSpeed   atomic.Int64
	syncedEvents    atomic.Int64
	conflictsSaved  atomic.Int64
	logSeq          atomic.Int64

	logsMu sync.RWMutex
	logs   []LogEntry

	runCancel   context.CancelFunc
	reconcileMu sync.Mutex
}

// NewEngine creates a new Synchronization Engine.
func NewEngine(cfg config.Config, statePath string, onUpdate func()) *Engine {
	cfg.Normalize()
	tr := newSyncHTTPTransport()
	e := &Engine{
		cfg:      cfg,
		index:    NewIndexStore(statePath),
		onUpdate: onUpdate,
		state:    StateStopped,
		client: &http.Client{
			Transport: tr,
			Timeout:   60 * time.Second,
		},
	}
	e.server = NewServer(e)
	// Perform initial scan of all enabled folders
	folders := cfg.EnabledFolders()
	_, _, _, _ = e.index.ScanFolders(folders, cfg.IgnorePatterns)
	e.Logf("同步引擎就绪，当前监控 %d 个文件夹 (主目录: %s)", len(folders), cfg.SyncDir)
	return e
}

// Config returns a copy of the engine's current configuration.
func (e *Engine) Config() config.Config {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.cfg
}

// UpdateConfig updates the configuration and rescans if the sync folders or ignore rules changed.
func (e *Engine) UpdateConfig(cfg config.Config) {
	cfg.Normalize()
	e.mu.Lock()
	changed := e.cfg.SyncDir != cfg.SyncDir || !sameFolderMappings(e.cfg.SyncFolders, cfg.SyncFolders)
	e.cfg = cfg
	e.mu.Unlock()

	if changed {
		_, _, _, _ = e.index.ScanFolders(cfg.EnabledFolders(), cfg.IgnorePatterns)
		e.index.Save()
		e.Logf("已更新同步文件夹配置 (共 %d 个启用目录，主目录: %s)", len(cfg.EnabledFolders()), cfg.SyncDir)
	}
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

// ApplyPeerFolderMappings merges folder mappings announced by the remote peer.
// When the peer specifies a RemotePath (target folder on this computer) or adds a new folder ID,
// this node automatically creates/updates the corresponding local folder mapping.
func (e *Engine) ApplyPeerFolderMappings(peerFolders []config.FolderMapping) bool {
	if len(peerFolders) == 0 {
		return false
	}
	e.mu.Lock()
	cfg := e.cfg
	changed := false

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

	for _, f := range cfg.EnabledFolders() {
		_ = os.MkdirAll(f.LocalPath, 0o755)
	}
	_ = config.Save(cfg)
	_, _, _, _ = e.index.ScanFolders(cfg.EnabledFolders(), cfg.IgnorePatterns)
	e.index.Save()
	e.Logf("已自动应用远端文件夹映射配置 (共 %d 个同步文件夹)", len(cfg.EnabledFolders()))
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
	e.mu.Unlock()
	e.notifyUI()

	folders := cfg.EnabledFolders()
	for _, f := range folders {
		if err := os.MkdirAll(f.LocalPath, 0o755); err != nil {
			e.Stop()
			e.setError(fmt.Sprintf("无法创建同步目录 (%s): %v", f.LocalPath, err))
			return err
		}
	}

	// Initial directory scan across all enabled folders
	if _, _, _, err := e.index.ScanFolders(folders, cfg.IgnorePatterns); err != nil {
		e.Stop()
		e.setError(fmt.Sprintf("扫描目录失败: %v", err))
		return err
	}
	e.index.Save()

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
	e.index.Save()
	e.Logf("实时同步已停止")
	e.notifyUI()
}

// TriggerManualSync forces an immediate directory scan and remote reconciliation.
func (e *Engine) TriggerManualSync() {
	go func() {
		cfg := e.Config()
		_, changed, deleted, err := e.index.ScanFolders(cfg.EnabledFolders(), cfg.IgnorePatterns)
		if err != nil {
			e.LogLevel("ERROR", fmt.Sprintf("手动扫描失败: %v", err))
			return
		}
		e.index.Save()
		e.Logf("完成多目录扫描: 发现 %d 项变更, %d 项删除", len(changed), len(deleted))

		if cfg.Role == config.RoleHost && e.IsRunning() {
			for i := range changed {
				f := changed[i]
				e.server.Broadcast(SyncEvent{
					Type:       EventUpsert,
					OriginNode: e.index.NodeID(),
					File:       &f,
				})
			}
			for i := range deleted {
				t := deleted[i]
				e.server.Broadcast(SyncEvent{
					Type:       EventDelete,
					OriginNode: e.index.NodeID(),
					Tombstone:  &t,
				})
			}
		} else if cfg.Role == config.RoleClient && e.IsRunning() {
			_ = e.ReconcileWithRemote(context.Background())
		}
		e.notifyUI()
	}()
}

// ImportFiles copies external files dropped onto the window into the primary SyncDir.
func (e *Engine) ImportFiles(paths []string) (int, error) {
	return e.ImportFilesToFolder(config.DefaultFolderID, paths)
}

// ImportFilesToFolder copies external files dropped onto the window into the specified mapped folder.
func (e *Engine) ImportFilesToFolder(folderID string, paths []string) (int, error) {
	cfg := e.Config()
	folder, ok := cfg.FindFolder(folderID)
	targetDir := folder.LocalPath
	if !ok || strings.TrimSpace(targetDir) == "" {
		targetDir = cfg.SyncDir
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return 0, err
	}
	imported := 0
	for _, src := range paths {
		info, err := os.Stat(src)
		if err != nil || info.IsDir() {
			continue
		}
		dst := filepath.Join(targetDir, filepath.Base(src))
		if filepath.Clean(src) == filepath.Clean(dst) {
			continue
		}
		if err := copyFileContents(src, dst); err == nil {
			imported++
		}
	}
	if imported > 0 {
		e.Logf("已导入 %d 个外部拖入文件至同步目录 (%s)", imported, targetDir)
		e.TriggerManualSync()
	}
	return imported, nil
}

// watchLocalLoop watches all enabled folders using `mygo.WatchFiles` + a fallback ticker.
func (e *Engine) watchLocalLoop(ctx context.Context) {
	cfg := e.Config()
	folders := cfg.EnabledFolders()
	for _, folder := range folders {
		dir := folder.LocalPath
		go e.watchSingleDir(ctx, dir)
	}
	// Run periodic poll to catch any missed events or newly added folder mappings
	e.pollLocalLoop(ctx)
}

func (e *Engine) watchSingleDir(ctx context.Context, dir string) {
	watcher, err := mygo.WatchFiles(ctx, dir, mygo.FileWatchOptions{
		Recursive: true,
		Ignore: func(p string, isDir bool) bool {
			return ShouldIgnore(p, isDir, e.Config().IgnorePatterns)
		},
	})
	if err != nil {
		e.LogLevel("WARN", fmt.Sprintf("目录监控 (%s) 回退至轮询模式: %v", dir, err))
		return
	}
	defer watcher.Close()

	for {
		events, err := watcher.Next(ctx)
		if err != nil {
			return
		}
		if len(events) == 0 {
			continue
		}
		e.handleLocalChanges(ctx)
	}
}

func (e *Engine) pollLocalLoop(ctx context.Context) {
	ticker := time.NewTicker(4 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.handleLocalChanges(ctx)
		}
	}
}

func (e *Engine) handleLocalChanges(ctx context.Context) {
	cfg := e.Config()
	_, changed, deleted, err := e.index.ScanFolders(cfg.EnabledFolders(), cfg.IgnorePatterns)
	if err != nil {
		return
	}
	if len(changed) == 0 && len(deleted) == 0 {
		return
	}
	e.index.Save()

	if cfg.Role == config.RoleHost {
		for i := range changed {
			f := changed[i]
			e.LogLevel("SYNC", fmt.Sprintf("检测到本地文件更新: %s (%s)", f.RelPath, FormatBytes(f.Size)))
			e.server.Broadcast(SyncEvent{
				Type:       EventUpsert,
				OriginNode: e.index.NodeID(),
				File:       &f,
			})
		}
		if cfg.SyncDeletions {
			for i := range deleted {
				t := deleted[i]
				e.LogLevel("SYNC", fmt.Sprintf("检测到本地文件删除: %s", t.RelPath))
				e.server.Broadcast(SyncEvent{
					Type:       EventDelete,
					OriginNode: e.index.NodeID(),
					Tombstone:  &t,
				})
			}
		}
		e.notifyUI()
		return
	}

	// Client mode: push local changes if direction allows
	if cfg.SyncDirection != config.SyncPullOnly {
		for _, f := range changed {
			if err := e.pushFileToRemote(ctx, cfg, f, ""); err != nil {
				e.LogLevel("WARN", fmt.Sprintf("上传 %s 失败: %v", f.RelPath, err))
			}
		}
		if cfg.SyncDeletions {
			for _, t := range deleted {
				if err := e.pushDeleteToRemote(ctx, cfg, t); err != nil {
					e.LogLevel("WARN", fmt.Sprintf("同步删除 %s 失败: %v", t.RelPath, err))
				}
			}
		}
	}
	e.notifyUI()
}

func (e *Engine) clientLoop(ctx context.Context) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		if err := e.ReconcileWithRemote(ctx); err != nil {
			e.mu.Lock()
			e.remoteConnected = false
			e.state = StateError
			e.lastError = err.Error()
			e.mu.Unlock()
			e.LogLevel("WARN", fmt.Sprintf("连接远端同步服务异常: %v", err))
			e.notifyUI()

			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				if backoff < 10*time.Second {
					backoff *= 2
				}
				continue
			}
		}

		backoff = time.Second
		e.mu.Lock()
		e.remoteConnected = true
		e.state = StateRunning
		e.lastError = ""
		e.mu.Unlock()
		e.Logf("已连接远端隧道并完成初始同步对账")
		e.notifyUI()

		// Listen to real-time SSE stream (`/api/events`) while simultaneously running periodic reconciliation
		_ = e.listenRemoteEvents(ctx)
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

	localManifest, _, _, err := e.index.ScanFolders(cfg.EnabledFolders(), cfg.IgnorePatterns)
	if err != nil {
		return err
	}

	remoteManifest, err := e.fetchRemoteManifest(ctx, baseURL, cfg.AuthToken, cfg.EnabledFolders())
	if err != nil {
		return err
	}

	if len(remoteManifest.Folders) > 0 && e.ApplyPeerFolderMappings(remoteManifest.Folders) {
		cfg = e.Config()
		localManifest, _, _, err = e.index.ScanFolders(cfg.EnabledFolders(), cfg.IgnorePatterns)
		if err != nil {
			return err
		}
	}

	e.mu.Lock()
	e.remoteNodeID = remoteManifest.NodeID
	e.remoteConnected = true
	e.mu.Unlock()

	canPull := cfg.SyncDirection == config.SyncBidirectional || cfg.SyncDirection == config.SyncPullOnly
	canPush := cfg.SyncDirection == config.SyncBidirectional || cfg.SyncDirection == config.SyncPushOnly

	// 1. Handle files present on Remote
	for rel, remFile := range remoteManifest.Files {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if ShouldIgnore(rel, false, cfg.IgnorePatterns) {
			continue
		}
		locFile, hasLocal := localManifest.Files[rel]
		locTomb, hasLocTomb := localManifest.Tombstones[rel]

		if !hasLocal {
			// If deleted locally more recently than remote file's mtime, push deletion to remote
			if hasLocTomb && locTomb.DeletedAt > remFile.ModTime {
				if canPush && cfg.SyncDeletions {
					_ = e.pushDeleteToRemote(ctx, cfg, locTomb)
				}
				continue
			}
			if canPull {
				if err := e.pullFileFromRemote(ctx, cfg, remFile, false); err != nil {
					e.LogLevel("WARN", fmt.Sprintf("下载文件 %s 失败: %v", rel, err))
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
			_ = e.pullFileFromRemote(ctx, cfg, remFile, cfg.ConflictPolicy == config.ConflictKeepBoth)
		} else if cfg.SyncDirection == config.SyncPushOnly {
			_ = e.pushFileToRemote(ctx, cfg, locFile, remFile.SHA256)
		} else {
			// Bidirectional: compare ModTime
			if remFile.ModTime >= locFile.ModTime {
				_ = e.pullFileFromRemote(ctx, cfg, remFile, cfg.ConflictPolicy == config.ConflictKeepBoth)
			} else {
				_ = e.pushFileToRemote(ctx, cfg, locFile, remFile.SHA256)
			}
		}
	}

	// 2. Handle files present Locally but missing on Remote
	for rel, locFile := range localManifest.Files {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if ShouldIgnore(rel, false, cfg.IgnorePatterns) {
			continue
		}
		if _, onRemote := remoteManifest.Files[rel]; onRemote {
			continue
		}
		remTomb, hasRemTomb := remoteManifest.Tombstones[rel]
		if hasRemTomb && remTomb.DeletedAt >= locFile.ModTime {
			if canPull && cfg.SyncDeletions {
				if folder, _, fullPath, ok := ResolveFolderFullPath(cfg, rel); ok {
					e.index.MarkFileDeleted(rel, remTomb.DeletedAt)
					_ = os.Remove(fullPath)
					cleanEmptyParents(folder.LocalPath, filepath.Dir(fullPath))
					e.markSyncedActivity(fmt.Sprintf("同步远端删除: %s", rel))
				}
			}
			continue
		}
		if canPush {
			if err := e.pushFileToRemote(ctx, cfg, locFile, ""); err != nil {
				e.LogLevel("WARN", fmt.Sprintf("上传文件 %s 失败: %v", rel, err))
			}
		}
	}

	e.index.Save()
	e.notifyUI()
	return nil
}

func (e *Engine) listenRemoteEvents(ctx context.Context) error {
	cfg := e.Config()
	baseURL := config.NormalizeRemoteURL(cfg.RemoteURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/api/events", nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-SyncT-Token", cfg.AuthToken)
	req.Header.Set("X-SyncT-Node", e.index.NodeID())
	req.Header.Set("Accept", "text/event-stream")

	// Use a client without a fixed request timeout for the SSE stream
	sseClient := &http.Client{Transport: e.client.Transport}
	resp, err := sseClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("SSE 状态码 %d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
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

		curCfg := e.Config()
		if curCfg.SyncDirection == config.SyncPushOnly {
			continue
		}

		switch ev.Type {
		case EventUpsert:
			if ev.File != nil && !ShouldIgnore(ev.File.RelPath, false, curCfg.IgnorePatterns) {
				if _, _, _, ok := ResolveFolderFullPath(curCfg, ev.File.RelPath); !ok {
					_ = e.ReconcileWithRemote(ctx)
					continue
				}
				snap := e.index.Snapshot()
				if loc, ok := snap.Files[ev.File.RelPath]; !ok || loc.SHA256 != ev.File.SHA256 {
					_ = e.pullFileFromRemote(ctx, curCfg, *ev.File, curCfg.ConflictPolicy == config.ConflictKeepBoth && ok)
				}
			}
		case EventDelete:
			if ev.Tombstone != nil && curCfg.SyncDeletions && !ShouldIgnore(ev.Tombstone.RelPath, false, curCfg.IgnorePatterns) {
				rel := ev.Tombstone.RelPath
				if folder, _, fullPath, ok := ResolveFolderFullPath(curCfg, rel); ok {
					e.index.MarkFileDeleted(rel, ev.Tombstone.DeletedAt)
					_ = os.Remove(fullPath)
					cleanEmptyParents(folder.LocalPath, filepath.Dir(fullPath))
					e.index.Save()
					e.markSyncedActivity(fmt.Sprintf("实时同步删除: %s", rel))
				}
			}
		case EventRescan:
			_ = e.ReconcileWithRemote(ctx)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return errors.New("SSE 连接已断开")
}

func (e *Engine) fetchRemoteManifest(ctx context.Context, baseURL, token string, localFolders []config.FolderMapping) (Manifest, error) {
	payload, _ := json.Marshal(struct {
		Folders []config.FolderMapping `json:"folders"`
	}{
		Folders: localFolders,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/api/manifest", bytes.NewReader(payload))
	if err != nil {
		return Manifest{}, err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("X-SyncT-Token", token)
	req.Header.Set("X-SyncT-Node", e.index.NodeID())

	resp, err := e.client.Do(req)
	if err != nil {
		return Manifest{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return Manifest{}, errors.New("访问密钥 (Auth Token) 校验失败 (401)")
	}
	if resp.StatusCode != http.StatusOK {
		return Manifest{}, fmt.Errorf("远端返回状态码 %d", resp.StatusCode)
	}

	var m Manifest
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return Manifest{}, err
	}
	if m.Files == nil {
		m.Files = make(map[string]FileMeta)
	}
	if m.Tombstones == nil {
		m.Tombstones = make(map[string]Tombstone)
	}
	return m, nil
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

	start := time.Now()
	e.index.SetFileStatus(rel, StatusDownloading, "远端 -> 本地")
	e.notifyUI()

	baseURL := config.NormalizeRemoteURL(cfg.RemoteURL)
	u := fmt.Sprintf("%s/api/file?path=%s", baseURL, url.QueryEscape(rel))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-SyncT-Token", cfg.AuthToken)
	req.Header.Set("X-SyncT-Node", e.index.NodeID())

	resp, err := e.client.Do(req)
	if err != nil {
		e.index.SetFileStatus(rel, StatusError, "下载失败")
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		e.index.SetFileStatus(rel, StatusError, "下载失败")
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	parentDir := filepath.Dir(fullPath)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		return err
	}

	var randBuf [6]byte
	_, _ = rand.Read(randBuf[:])
	tmpPath := filepath.Join(parentDir, fmt.Sprintf(".synct.tmp.%s", hex.EncodeToString(randBuf[:])))

	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	hasher := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(tmpFile, hasher), resp.Body)
	closeErr := tmpFile.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(tmpPath)
		return errors.New("failed to write temp file")
	}

	actualHash := hex.EncodeToString(hasher.Sum(nil))
	if rem.SHA256 != "" && !strings.EqualFold(rem.SHA256, actualHash) {
		_ = os.Remove(tmpPath)
		return errors.New("downloaded file hash mismatch")
	}

	conflictCreated := false
	if saveConflict {
		if existingInfo, statErr := os.Stat(fullPath); statErr == nil && existingInfo.Mode().IsRegular() {
			existingHash, _ := HashFile(fullPath)
			if existingHash != "" && existingHash != actualHash {
				backupPath := ConflictBackupPath(fullPath)
				if err := copyFileContents(fullPath, backupPath); err == nil {
					conflictCreated = true
					e.incrementConflict()
					e.Logf("冲突保护: 已保存本地冲突副本 %s", filepath.Base(backupPath))
				}
			}
		}
	}

	modTime := time.UnixMilli(rem.ModTime)
	_ = os.Chtimes(tmpPath, modTime, modTime)

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
	e.index.MarkFileSynced(meta, st, "远端 -> 本地")

	if err := os.Rename(tmpPath, fullPath); err != nil {
		_ = os.Remove(fullPath)
		if err2 := os.Rename(tmpPath, fullPath); err2 != nil {
			_ = os.Remove(tmpPath)
			return err2
		}
	}
	_ = os.Chtimes(fullPath, modTime, modTime)
	if info, err := os.Stat(fullPath); err == nil {
		meta.ModTime = info.ModTime().UTC().UnixMilli()
		e.index.MarkFileSynced(meta, st, "远端 -> 本地")
	}
	e.index.Save()

	e.recordDownload(rel, written, time.Since(start))
	e.markSyncedActivity(fmt.Sprintf("已下载同步: %s (%s)", rel, FormatBytes(written)))
	return nil
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
	data, err := os.ReadFile(fullPath)
	if err != nil {
		return err
	}

	start := time.Now()
	e.index.SetFileStatus(rel, StatusUploading, "本地 -> 远端")
	e.notifyUI()

	baseURL := config.NormalizeRemoteURL(cfg.RemoteURL)
	u := fmt.Sprintf(
		"%s/api/file?path=%s&mtime=%d&sha256=%s&base_hash=%s",
		baseURL,
		url.QueryEscape(rel),
		loc.ModTime,
		url.QueryEscape(loc.SHA256),
		url.QueryEscape(baseHash),
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-SyncT-Token", cfg.AuthToken)
	req.Header.Set("X-SyncT-Node", e.index.NodeID())

	resp, err := e.client.Do(req)
	if err != nil {
		e.index.SetFileStatus(rel, StatusError, "上传失败")
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		e.index.SetFileStatus(rel, StatusError, "上传失败")
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	e.index.MarkFileSynced(loc, StatusSynced, "本地 -> 远端")
	e.index.Save()
	e.recordUpload(rel, int64(len(data)), time.Since(start))
	e.markSyncedActivity(fmt.Sprintf("已上传同步: %s (%s)", rel, FormatBytes(int64(len(data)))))
	return nil
}

func (e *Engine) pushDeleteToRemote(ctx context.Context, cfg config.Config, tomb Tombstone) error {
	rel, ok := CleanRelPath(tomb.RelPath)
	if !ok {
		return nil
	}
	baseURL := config.NormalizeRemoteURL(cfg.RemoteURL)
	u := fmt.Sprintf("%s/api/file?path=%s&deleted_at=%d", baseURL, url.QueryEscape(rel), tomb.DeletedAt)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-SyncT-Token", cfg.AuthToken)
	req.Header.Set("X-SyncT-Node", e.index.NodeID())

	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	e.markSyncedActivity(fmt.Sprintf("已向远端同步删除: %s", rel))
	return nil
}

// Files returns the current list of tracked files for display in the UI.
func (e *Engine) Files() []FileEntry {
	return e.index.Entries()
}

// Stats returns a snapshot of the engine's live metrics.
func (e *Engine) Stats() Stats {
	e.mu.RLock()
	folderCount := len(e.cfg.EnabledFolders())
	st := Stats{
		State:           e.state,
		LastError:       e.lastError,
		LocalPort:       e.localPort,
		ConnectedPeers:  e.server.ConnectedPeers(),
		RemoteConnected: e.remoteConnected,
		RemoteNodeID:    e.remoteNodeID,
		FolderCount:     folderCount,
		BytesUploaded:   e.bytesUploaded.Load(),
		BytesDownloaded: e.bytesDownloaded.Load(),
		UploadSpeed:     e.uploadSpeed.Load(),
		DownloadSpeed:   e.downloadSpeed.Load(),
		LastSyncFile:    e.lastSyncFile,
		LastSyncBytes:   e.lastSyncBytes,
		SyncedEvents:    e.syncedEvents.Load(),
		ConflictsSaved:  e.conflictsSaved.Load(),
		LastSyncTime:    e.lastSyncTime,
	}
	e.mu.RUnlock()

	snap := e.index.Snapshot()
	st.TotalFiles = len(snap.Files)
	for _, f := range snap.Files {
		st.TotalBytes += f.Size
	}
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
	if len(e.logs) > 400 {
		e.logs = append([]LogEntry(nil), e.logs[len(e.logs)-400:]...)
	}
	e.logsMu.Unlock()
	e.notifyUI()
}

func (e *Engine) setError(msg string) {
	e.mu.Lock()
	e.state = StateError
	e.lastError = msg
	e.mu.Unlock()
	e.LogLevel("ERROR", msg)
	e.notifyUI()
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
