package syncer

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"synct/internal/config"
)

// SyncTProtocolVersion is reported by /api/status for compatibility checks.
const SyncTProtocolVersion = "1.3.1"

// SyncEventType identifies the kind of real-time change broadcast over SSE.
type SyncEventType string

const (
	EventUpsert SyncEventType = "upsert"
	EventDelete SyncEventType = "delete"
	EventDir    SyncEventType = "dir"
	EventRescan SyncEventType = "rescan"
)

// SyncEvent is sent over the `/api/events` SSE stream when a file changes.
type SyncEvent struct {
	Type       SyncEventType `json:"type"`
	OriginNode string        `json:"origin_node"`
	File       *FileMeta     `json:"file,omitempty"`
	Tombstone  *Tombstone    `json:"tombstone,omitempty"`
	Dir        *DirMeta      `json:"dir,omitempty"`
	Timestamp  int64         `json:"timestamp"`
}

// subscriber is one connected SSE client. rescan is a dedicated one-slot
// signal so a full event buffer can still trigger a catch-up reconciliation.
type subscriber struct {
	events chan SyncEvent
	rescan chan struct{}
	label  string
}

// Server exposes a local folder over HTTP + SSE so Cloudflared can tunnel it.
type Server struct {
	engine   *Engine
	listener net.Listener
	httpSrv  *http.Server

	// lifeMu serializes Start/Stop/Restart and the automatic origin recovery
	// below; gen identifies the current listener so a serve goroutine that
	// returns late cannot revive a listener the engine already replaced.
	lifeMu     sync.Mutex
	port       int
	allowLAN   bool
	gen        uint64
	restartGen uint64
	stopped    atomic.Bool
	serving    atomic.Bool

	subsMu sync.Mutex
	subs   map[chan SyncEvent]*subscriber

	uploadsMu sync.Mutex
	uploads   map[string]*chunkUpload
}

// chunkUpload is one resumable upload assembled from several requests. The
// temporary file lives under the folder's .synct-uploads directory, so a
// final rename stays on the same volume and scans ignore it.
type chunkUpload struct {
	mu         sync.Mutex
	id         string
	key        string
	relPath    string
	fullPath   string
	tmpPath    string
	size       int64
	mtime      int64
	expected   string
	baseHash   string
	originNode string
	received   int64
	hasher     hash.Hash
	createdAt  time.Time
	lastActive time.Time
	done       bool
}

const (
	// maxUploadChunkBytes stays well inside what a Cloudflare-proxied request
	// can carry and what finishes comfortably under its ~100s origin timeout.
	maxUploadChunkBytes = 8 << 20
	uploadSessionTTL    = 45 * time.Minute
)

// NewServer constructs a sync HTTP server attached to the Engine.
func NewServer(engine *Engine) *Server {
	return &Server{
		engine:  engine,
		subs:    make(map[chan SyncEvent]*subscriber),
		uploads: make(map[string]*chunkUpload),
	}
}

// Start binds loopback (`127.0.0.1:<port>` and `[::1]:<port>`) and serves both
// HTTPS (`https://localhost:18920`) and HTTP requests without triggering Windows
// Firewall UAC prompts. allowLAN additionally binds all interfaces for the
// explicit LAN Direct mode, whose advertised address is a LAN IP.
func (s *Server) Start(port int, allowLAN bool) (int, error) {
	s.lifeMu.Lock()
	defer s.lifeMu.Unlock()
	s.stopped.Store(false)
	// Staging directories left by an earlier crash belong to no live session.
	go s.cleanupUploadDirs()
	return s.startLocked(port, allowLAN)
}

// startLocked binds the listeners and starts serving. The caller holds lifeMu.
// A fixed port is never silently replaced: cloudflared's ingress forwards to
// exactly that port, and listening elsewhere while the tunnel stays pointed at
// the configured port is what makes the work PC see a bare 502.
func (s *Server) startLocked(port int, allowLAN bool) (int, error) {
	// Bind loopback first and strictly: on Windows a wildcard bind can
	// succeed next to another process's specific bind, which would silently
	// leave loopback traffic (and cloudflared) talking to the other process.
	ln4, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return 0, fmt.Errorf("本地同步端口 %d 无法监听（可能被其他程序或旧的 SyncT 实例占用），公网 Cloudflared 源站将返回 502: %w", port, err)
	}
	actualPort := ln4.Addr().(*net.TCPAddr).Port
	listeners := []net.Listener{ln4}

	// LAN Direct additionally accepts non-loopback peers. Both listeners stay
	// alive: Windows routes 127.0.0.1 to the specific bind and LAN addresses
	// to the wildcard one.
	if allowLAN && port > 0 {
		if lanLn, lanErr := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", actualPort)); lanErr == nil {
			listeners = append(listeners, lanLn)
		} else {
			s.engine.LogThrottled("lan-bind:"+lanErr.Error(), "WARN", fmt.Sprintf("局域网监听 0.0.0.0:%d 失败（仍可通过本机回环同步）: %v", actualPort, lanErr), time.Minute)
		}
	}

	// Also bind IPv6 so `https://localhost:18920` works whether Windows resolves
	// `localhost` to 127.0.0.1 or ::1. cloudflared logs show it often dials
	// [::1], so an IPv4-only origin would still answer 502 to the tunnel.
	if ln6, err6 := net.Listen("tcp6", fmt.Sprintf("[::1]:%d", actualPort)); err6 == nil {
		listeners = append(listeners, ln6)
		if allowLAN && port > 0 {
			if lan6, lanErr := net.Listen("tcp6", fmt.Sprintf("[::]:%d", actualPort)); lanErr == nil {
				listeners = append(listeners, lan6)
			}
		}
	} else {
		s.engine.LogThrottled("ipv6-bind:"+err6.Error(), "WARN", fmt.Sprintf("IPv6 监听 [::1]:%d 失败: %v；若 cloudflared 优先访问 ::1 将返回 502", actualPort, err6), time.Minute)
	}

	tlsCfg, _, tlsErr := EnsureLocalTLSConfig()
	if tlsErr != nil {
		s.engine.LogThrottled("tls-init:"+tlsErr.Error(), "WARN", fmt.Sprintf("本地 TLS 证书初始化失败，将以 HTTP 回环提供服务: %v", tlsErr), time.Minute)
	}
	s.gen++
	gen := s.gen
	dualLn := newDualProtocolListener(listeners, tlsCfg, func(err error) {
		if s.stopped.Load() {
			return
		}
		// One stable throttle key: transient accept errors can differ by
		// client and would otherwise bypass the interval and flood the log.
		s.engine.LogThrottled("accept-loop", "WARN", fmt.Sprintf("本地同步监听器接收异常（将自动重试）: %v", err), 30*time.Second)
	})
	s.listener = dualLn
	s.port = actualPort
	s.allowLAN = allowLAN

	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", s.withAuth(s.handleStatus))
	mux.HandleFunc("/api/manifest", s.withAuth(s.handleManifest))
	mux.HandleFunc("/api/file", s.withAuth(s.handleFile))
	mux.HandleFunc("/api/upload/begin", s.withAuth(s.handleUploadBegin))
	mux.HandleFunc("/api/upload/chunk", s.withAuth(s.handleUploadChunk))
	mux.HandleFunc("/api/upload/commit", s.withAuth(s.handleUploadCommit))
	mux.HandleFunc("/api/upload/abort", s.withAuth(s.handleUploadAbort))
	mux.HandleFunc("/api/dirs", s.withAuth(s.handleDirs))
	mux.HandleFunc("/api/events", s.withAuth(s.handleEvents))

	httpSrv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 15 * time.Second,
		IdleTimeout:       120 * time.Second,
		// Chunked uploads are many small requests; a generous per-request
		// timeout keeps a slow 8 MB part alive without letting a stalled one
		// hold a connection forever.
		ReadTimeout:    10 * time.Minute,
		WriteTimeout:   10 * time.Minute,
		MaxHeaderBytes: 1 << 20,
		// Cloudflared's origin TLS errors used to go to a stderr nobody reads
		// in a windowsgui build. Route them into the visible log; a rejected
		// self-signed certificate is the direct cause of a public 502.
		ErrorLog: log.New(engineServerLogger{s.engine}, "", 0),
	}
	s.httpSrv = httpSrv
	s.serving.Store(true)

	go func() {
		err := httpSrv.Serve(dualLn)
		if err == nil || errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
			return
		}
		if s.stopped.Load() {
			return
		}
		s.engine.LogThrottled("serve-exit", "ERROR", fmt.Sprintf("本地同步服务异常退出: %v；正在自动重建监听", err), 30*time.Second)
		// Without this the cloudflared ingress keeps answering 502 until the
		// user restarts the app. Rebind the same port so the tunnel recovers.
		s.scheduleRestart(gen)
	}()

	return actualPort, nil
}

// scheduleRestart rebinds the origin after a listener failed on its own.
// Only the generation that actually died is restarted, so a delayed failure
// from a listener that Stop already replaced cannot create a duplicate.
func (s *Server) scheduleRestart(gen uint64) {
	s.lifeMu.Lock()
	if s.gen != gen || s.restartGen == gen {
		s.lifeMu.Unlock()
		return
	}
	s.restartGen = gen
	port := s.port
	s.lifeMu.Unlock()

	go func() {
		backoff := time.Second
		for {
			time.Sleep(backoff)
			if s.stopped.Load() {
				return
			}
			s.lifeMu.Lock()
			if s.gen != gen || s.stopped.Load() {
				s.lifeMu.Unlock()
				return
			}
			s.serving.Store(false)
			if s.listener != nil {
				_ = s.listener.Close()
			}
			// The Serve goroutine has already returned; close any keep-alive
			// connections it left behind before binding the replacement.
			oldSrv := s.httpSrv
			s.httpSrv = nil
			if oldSrv != nil {
				_ = oldSrv.Close()
			}
			actual, err := s.startLocked(port, s.allowLAN)
			s.lifeMu.Unlock()
			if err == nil {
				s.engine.LogThrottled("serve-restart", "INFO", fmt.Sprintf("本地同步服务已自动恢复监听 (端口 %d)", actual), 30*time.Second)
				return
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			s.engine.LogThrottled("serve-restart-fail", "ERROR", fmt.Sprintf("本地同步服务自动重启失败，将重试: %v", err), 30*time.Second)
		}
	}()
}

// Restart rebinds the origin on its current port. The host health loop uses it
// when the local origin stops answering, which otherwise leaves cloudflared
// with nothing to forward to and the work PC with a 502.
func (s *Server) Restart() error {
	s.lifeMu.Lock()
	defer s.lifeMu.Unlock()
	if s.stopped.Load() {
		return errors.New("server stopped")
	}
	if s.listener != nil {
		_ = s.listener.Close()
	}
	if s.httpSrv != nil {
		_ = s.httpSrv.Close()
	}
	_, err := s.startLocked(s.port, s.allowLAN)
	return err
}

// IsServing reports whether the origin currently accepts connections.
func (s *Server) IsServing() bool {
	return !s.stopped.Load() && s.serving.Load()
}

// Stop closes all active SSE streams and shuts down the HTTP listener.
func (s *Server) Stop() {
	s.stopped.Store(true)
	s.serving.Store(false)
	s.abortAllUploads()

	s.subsMu.Lock()
	for ch, sub := range s.subs {
		close(sub.events)
		delete(s.subs, ch)
	}
	s.subsMu.Unlock()

	s.lifeMu.Lock()
	httpSrv := s.httpSrv
	listener := s.listener
	s.httpSrv = nil
	s.listener = nil
	s.gen++
	s.lifeMu.Unlock()

	if httpSrv != nil {
		_ = httpSrv.Close()
	}
	if listener != nil {
		_ = listener.Close()
	}
}

// engineServerLogger forwards net/http server errors into the engine log.
// TLS handshake failures are how a rejected self-signed certificate becomes
// visible: cloudflared aborts the origin connection and Cloudflare answers
// the public hostname with 502.
type engineServerLogger struct{ engine *Engine }

func (l engineServerLogger) Write(p []byte) (int, error) {
	line := strings.TrimSpace(string(p))
	if line != "" {
		key := "http-server"
		if strings.Contains(line, "TLS handshake error") {
			key = "http-tls-handshake"
			lower := strings.ToLower(line)
			if strings.Contains(lower, "certificate") || strings.Contains(lower, "unknown authority") {
				l.engine.noteTLSRejection()
			}
		}
		l.engine.LogThrottled(key, "WARN", line, 5*time.Minute)
	}
	return len(p), nil
}

// ConnectedPeers returns the count of active SSE peer connections.
func (s *Server) ConnectedPeers() int {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	return len(s.subs)
}

// Broadcast sends a SyncEvent to all connected SSE clients. If a subscriber's
// bounded event buffer is full, it raises a rescan signal instead of blocking
// the watcher; the client then reconciles and catches up on every missed change.
func (s *Server) Broadcast(ev SyncEvent) {
	if ev.Timestamp == 0 {
		ev.Timestamp = time.Now().UTC().UnixMilli()
	}
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	for _, sub := range s.subs {
		select {
		case sub.events <- ev:
		default:
			select {
			case sub.rescan <- struct{}{}:
			default:
			}
		}
	}
}

func (s *Server) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		expected := strings.TrimSpace(s.engine.Config().AuthToken)
		if expected != "" {
			got := strings.TrimSpace(r.Header.Get("X-SyncT-Token"))
			if got == "" {
				got = strings.TrimSpace(r.URL.Query().Get("token"))
			}
			if subtle.ConstantTimeCompare([]byte(got), []byte(expected)) != 1 {
				http.Error(w, "unauthorized: invalid X-SyncT-Token", http.StatusUnauthorized)
				return
			}
		}
		next(w, r)
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	fileCount, totalBytes := s.engine.index.Counts()
	resp := map[string]any{
		"node_id":     s.engine.index.NodeID(),
		"version":     SyncTProtocolVersion,
		"file_count":  fileCount,
		"total_bytes": totalBytes,
		"peers":       s.ConnectedPeers(),
		"timestamp":   time.Now().UTC().UnixMilli(),
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleManifest(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost && r.Body != nil {
		defer r.Body.Close()
		var reqPayload struct {
			Folders  []config.FolderMapping `json:"folders"`
			Dirs     []DirMeta              `json:"dirs,omitempty"`
			PushDirs bool                   `json:"push_dirs,omitempty"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 2*1024*1024)).Decode(&reqPayload); err == nil {
			if len(reqPayload.Folders) > 0 {
				s.engine.ApplyPeerFolderMappings(reqPayload.Folders)
			}
			if reqPayload.PushDirs && len(reqPayload.Dirs) > 0 {
				s.engine.ApplyPeerDirs(reqPayload.Dirs, strings.TrimSpace(r.Header.Get("X-SyncT-Node")))
			}
		}
	}
	snap := s.engine.index.Snapshot()
	if len(snap.Folders) == 0 {
		snap.Folders = s.engine.Config().EnabledFolders()
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(snap)
}

// handleDirs accepts directories pushed between reconciliations, which is how
// a newly created empty folder appears on the other computer in real time
// instead of waiting for the next periodic manifest exchange.
func (s *Server) handleDirs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var payload struct {
		OriginNode string    `json:"origin_node"`
		Dirs       []DirMeta `json:"dirs"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4*1024*1024)).Decode(&payload); err != nil {
		http.Error(w, "invalid directory payload", http.StatusBadRequest)
		return
	}
	origin := payload.OriginNode
	if origin == "" {
		origin = strings.TrimSpace(r.Header.Get("X-SyncT-Node"))
	}
	created := s.engine.ApplyPeerDirs(payload.Dirs, origin)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(map[string]int{"created": created})
}

func (s *Server) handleFile(w http.ResponseWriter, r *http.Request) {
	rawPath := r.URL.Query().Get("path")
	relPath, ok := CleanRelPath(rawPath)
	if !ok {
		http.Error(w, "invalid relative path", http.StatusBadRequest)
		return
	}

	cfg := s.engine.Config()
	if ShouldIgnore(relPath, false, cfg.IgnorePatterns) {
		http.Error(w, "path is ignored", http.StatusForbidden)
		return
	}

	folder, _, fullPath, ok := ResolveFolderFullPath(cfg, relPath)
	if !ok {
		http.Error(w, "mapped folder not found for path", http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.serveFileDownload(w, r, relPath, fullPath)
	case http.MethodPut, http.MethodPost:
		s.serveFileUpload(w, r, cfg, relPath, fullPath)
	case http.MethodDelete:
		s.serveFileDelete(w, r, cfg, folder.LocalPath, relPath, fullPath)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) serveFileDownload(w http.ResponseWriter, _ *http.Request, relPath, fullPath string) {
	f, err := os.Open(fullPath)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			s.engine.LogThrottled("dl-open:"+relPath+":"+err.Error(), "WARN", fmt.Sprintf("打开待下载文件失败 (%s): %v", relPath, err), 30*time.Second)
		}
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		http.Error(w, "not a regular file", http.StatusNotFound)
		return
	}

	meta, hasMeta := s.engine.index.FileMeta(relPath)
	if !hasMeta || meta.SHA256 == "" {
		hash, hashErr := HashFile(fullPath)
		if hashErr != nil {
			s.engine.LogThrottled("dl-hash:"+relPath+":"+hashErr.Error(), "WARN", fmt.Sprintf("计算文件哈希失败 (%s): %v", relPath, hashErr), 30*time.Second)
		}
		fid, _ := SplitFolderRelPath(relPath)
		meta = FileMeta{
			FolderID: fid,
			RelPath:  relPath,
			Size:     info.Size(),
			ModTime:  info.ModTime().UTC().UnixMilli(),
			SHA256:   hash,
		}
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	w.Header().Set("X-SyncT-ModTime", strconv.FormatInt(meta.ModTime, 10))
	w.Header().Set("X-SyncT-SHA256", meta.SHA256)

	start := time.Now()
	buf := ioBufferPool.Get().(*[]byte)
	n, err := io.CopyBuffer(&progressWriter{w: w, onMove: s.engine.countUploadBytes}, f, *buf)
	ioBufferPool.Put(buf)
	if err != nil {
		// Client went away mid-transfer or the disk stalled; nothing to retry here.
		s.engine.LogThrottled("dl-copy:"+relPath, "WARN", fmt.Sprintf("发送文件中断 (%s): %v", relPath, err), 30*time.Second)
		return
	}
	if n > 0 {
		s.engine.recordUpload(relPath, n, time.Since(start))
	}
}

func (s *Server) serveFileUpload(w http.ResponseWriter, r *http.Request, cfg config.Config, relPath, fullPath string) {
	defer r.Body.Close()
	start := time.Now()

	mtimeMs, _ := strconv.ParseInt(r.URL.Query().Get("mtime"), 10, 64)
	if mtimeMs <= 0 {
		mtimeMs = time.Now().UTC().UnixMilli()
	}
	expectedHash := strings.TrimSpace(r.URL.Query().Get("sha256"))
	baseHash := strings.TrimSpace(r.URL.Query().Get("base_hash"))
	originNode := strings.TrimSpace(r.Header.Get("X-SyncT-Node"))

	s.engine.index.BeginWrite(relPath)
	defer s.engine.index.EndWrite(relPath)

	parentDir := filepath.Dir(fullPath)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		s.engine.LogThrottled("up-mkdir:"+relPath+":"+err.Error(), "WARN", fmt.Sprintf("创建上传目标目录失败 (%s): %v", relPath, err), 30*time.Second)
		http.Error(w, "failed to create parent directory: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Write incoming stream to a temporary file in the same directory
	var randBuf [6]byte
	_, _ = rand.Read(randBuf[:])
	tmpPath := filepath.Join(parentDir, fmt.Sprintf(".synct.tmp.%s", hex.EncodeToString(randBuf[:])))

	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		s.engine.LogThrottled("up-tmp:"+relPath+":"+err.Error(), "WARN", fmt.Sprintf("创建上传临时文件失败 (%s): %v", relPath, err), 30*time.Second)
		http.Error(w, "failed to create temp file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	hasher := sha256.New()
	buf := ioBufferPool.Get().(*[]byte)
	written, copyErr := io.CopyBuffer(io.MultiWriter(tmpFile, &progressWriter{w: hasher, onMove: s.engine.countDownloadBytes}), r.Body, *buf)
	ioBufferPool.Put(buf)
	closeErr := tmpFile.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(tmpPath)
		s.engine.LogThrottled("up-write:"+relPath+":"+fmt.Sprint(copyErr, closeErr), "WARN", fmt.Sprintf("写入上传文件失败 (%s): %v", relPath, errors.Join(copyErr, closeErr)), 30*time.Second)
		http.Error(w, "failed to write uploaded file", http.StatusInternalServerError)
		return
	}

	actualHash := hex.EncodeToString(hasher.Sum(nil))
	if expectedHash != "" && !strings.EqualFold(expectedHash, actualHash) {
		_ = os.Remove(tmpPath)
		http.Error(w, "sha256 mismatch", http.StatusBadRequest)
		return
	}

	// Check for conflict if existing file differs from both actualHash and baseHash
	conflictSaved := false
	if existingInfo, statErr := os.Stat(fullPath); statErr == nil && existingInfo.Mode().IsRegular() {
		existingHash, hashErr := HashFile(fullPath)
		if hashErr == nil && existingHash != "" && existingHash != actualHash {
			if cfg.ConflictPolicy == config.ConflictKeepBoth && baseHash != "" && baseHash != existingHash {
				backupPath := ConflictBackupPath(fullPath)
				if err := copyFileContents(fullPath, backupPath); err == nil {
					conflictSaved = true
					s.engine.incrementConflict()
					s.engine.Logf("冲突保护: 已保存本地冲突副本 %s", filepath.Base(backupPath))
				} else {
					s.engine.LogThrottled("up-conflict:"+relPath+":"+err.Error(), "WARN", fmt.Sprintf("保存上传冲突副本失败 (%s): %v", filepath.Base(backupPath), err), 30*time.Second)
				}
			}
		}
	}

	modTime := time.UnixMilli(mtimeMs)
	if err := os.Chtimes(tmpPath, modTime, modTime); err != nil {
		s.engine.LogThrottled("up-chtimes:"+relPath, "WARN", fmt.Sprintf("设置上传文件时间失败 (%s): %v", relPath, err), time.Minute)
	}

	fid, _ := SplitFolderRelPath(relPath)
	meta := FileMeta{
		FolderID: fid,
		RelPath:  relPath,
		Size:     written,
		ModTime:  mtimeMs,
		SHA256:   actualHash,
	}
	st := StatusSynced
	if conflictSaved {
		st = StatusConflict
	}

	// Commit first, index second: a failed rename leaves the index describing
	// the file that is actually on disk, instead of advertising content that
	// was never committed.
	if err := replaceFile(tmpPath, fullPath); err != nil {
		_ = os.Remove(tmpPath)
		s.engine.LogThrottled("up-commit:"+relPath+":"+err.Error(), "WARN", fmt.Sprintf("提交上传文件失败 (%s): %v", relPath, err), 30*time.Second)
		http.Error(w, "failed to commit file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := os.Chtimes(fullPath, modTime, modTime); err != nil {
		s.engine.LogThrottled("up-chtimes:"+relPath, "WARN", fmt.Sprintf("设置上传文件时间失败 (%s): %v", relPath, err), time.Minute)
	}

	// Refresh exact mtime from filesystem in case the OS rounded it.
	if info, err := os.Stat(fullPath); err == nil {
		meta.ModTime = info.ModTime().UTC().UnixMilli()
	}
	s.engine.index.MarkFileSynced(meta, st, "远端 -> 本地")
	s.engine.index.RequestSave()

	s.engine.recordDownload(relPath, written, time.Since(start))
	s.engine.noteSynced("接收文件", relPath, written)

	s.Broadcast(SyncEvent{
		Type:       EventUpsert,
		OriginNode: originNode,
		File:       &meta,
		Timestamp:  time.Now().UTC().UnixMilli(),
	})

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(meta)
}

func (s *Server) serveFileDelete(w http.ResponseWriter, r *http.Request, cfg config.Config, rootDir, relPath, fullPath string) {
	if !cfg.SyncDeletions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	deletedAt, _ := strconv.ParseInt(r.URL.Query().Get("deleted_at"), 10, 64)
	originNode := strings.TrimSpace(r.Header.Get("X-SyncT-Node"))

	if err := os.Remove(fullPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		// Do not record a tombstone for a file that is still present; the peer
		// will retry on its next reconciliation instead of flapping.
		s.engine.LogThrottled("del-fail:"+relPath+":"+err.Error(), "WARN", fmt.Sprintf("删除本地文件失败 (%s): %v", relPath, err), 30*time.Second)
		http.Error(w, "failed to delete file: "+err.Error(), http.StatusConflict)
		return
	}
	tomb := s.engine.index.MarkFileDeleted(relPath, deletedAt)
	cleanEmptyParents(rootDir, filepath.Dir(fullPath))
	s.engine.index.RequestSave()

	s.engine.markSyncedActivity(fmt.Sprintf("远端同步删除: %s", relPath))

	s.Broadcast(SyncEvent{
		Type:       EventDelete,
		OriginNode: originNode,
		Tombstone:  &tomb,
		Timestamp:  time.Now().UTC().UnixMilli(),
	})

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(tomb)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	peerLabel := r.RemoteAddr
	if node := r.Header.Get("X-SyncT-Node"); node != "" {
		peerLabel = node
	}
	sub := &subscriber{
		events: make(chan SyncEvent, 32),
		rescan: make(chan struct{}, 1),
		label:  peerLabel,
	}

	s.subsMu.Lock()
	s.subs[sub.events] = sub
	s.subsMu.Unlock()
	s.engine.notifyUI()

	defer func() {
		s.subsMu.Lock()
		delete(s.subs, sub.events)
		s.subsMu.Unlock()
		s.engine.notifyUI()
	}()

	// Send initial handshake event
	if _, err := fmt.Fprintf(w, "event: connected\ndata: {\"node_id\":%q}\n\n", s.engine.index.NodeID()); err != nil {
		return
	}
	flusher.Flush()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, open := <-sub.events:
			if !open {
				return
			}
			payload, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "event: sync\ndata: %s\n\n", payload); err != nil {
				return
			}
			flusher.Flush()
		case <-sub.rescan:
			payload, _ := json.Marshal(SyncEvent{Type: EventRescan, Timestamp: time.Now().UTC().UnixMilli()})
			if _, err := fmt.Fprintf(w, "event: sync\ndata: %s\n\n", payload); err != nil {
				return
			}
			flusher.Flush()
		case <-ticker.C:
			if _, err := fmt.Fprintf(w, ": heartbeat %d\n\n", time.Now().Unix()); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// copyFileContents copies src to dst through a same-directory temporary file
// and an atomic rename. The temporary name starts with ".synct" so watchers
// ignore it, and readers never observe a half-written destination.
func copyFileContents(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	parent := filepath.Dir(dst)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return err
	}
	var randBuf [6]byte
	_, _ = rand.Read(randBuf[:])
	tmpPath := filepath.Join(parent, ".synct.tmp."+hex.EncodeToString(randBuf[:]))

	out, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	buf := ioBufferPool.Get().(*[]byte)
	_, copyErr := io.CopyBuffer(out, in, *buf)
	ioBufferPool.Put(buf)
	closeErr := out.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(tmpPath)
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	}
	if err := replaceFile(tmpPath, dst); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

func cleanEmptyParents(rootDir, dir string) {
	rootClean := filepath.Clean(rootDir)
	for {
		dirClean := filepath.Clean(dir)
		if dirClean == rootClean || !isPathWithinRoot(rootClean, dirClean) {
			return
		}
		entries, err := os.ReadDir(dirClean)
		if err != nil || len(entries) > 0 {
			return
		}
		if err := os.Remove(dirClean); err != nil {
			return
		}
		dir = filepath.Dir(dirClean)
	}
}

// FormatBytes formats a byte count into human-readable units (B, KB, MB, GB).
func FormatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// FormatSpeed formats a byte-per-second transfer rate into human-readable units (B/s, KB/s, MB/s, GB/s).
func FormatSpeed(bytesPerSec int64) string {
	if bytesPerSec <= 0 {
		return "0 B/s"
	}
	return FormatBytes(bytesPerSec) + "/s"
}
