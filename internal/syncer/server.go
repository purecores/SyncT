package syncer

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"synct/internal/config"
)

// SyncEventType identifies the kind of real-time change broadcast over SSE.
type SyncEventType string

const (
	EventUpsert SyncEventType = "upsert"
	EventDelete SyncEventType = "delete"
	EventRescan SyncEventType = "rescan"
)

// SyncEvent is sent over the `/api/events` SSE stream when a file changes.
type SyncEvent struct {
	Type       SyncEventType `json:"type"`
	OriginNode string        `json:"origin_node"`
	File       *FileMeta     `json:"file,omitempty"`
	Tombstone  *Tombstone    `json:"tombstone,omitempty"`
	Timestamp  int64         `json:"timestamp"`
}

// Server exposes a local folder over HTTP + SSE so Cloudflared can tunnel it.
type Server struct {
	engine   *Engine
	listener net.Listener
	httpSrv  *http.Server

	subsMu sync.Mutex
	subs   map[chan SyncEvent]string // subscriber channel -> remote peer label
}

// NewServer constructs a sync HTTP server attached to the Engine.
func NewServer(engine *Engine) *Server {
	return &Server{
		engine: engine,
		subs:   make(map[chan SyncEvent]string),
	}
}

// Start binds to loopback `127.0.0.1:<port>` and `[::1]:<port>` (`localhost:<port>`)
// and serves both HTTPS (`https://localhost:18920`) and HTTP requests without triggering Windows Firewall UAC prompts.
func (s *Server) Start(port int) (int, error) {
	addr4 := fmt.Sprintf("127.0.0.1:%d", port)
	ln4, err := net.Listen("tcp4", addr4)
	if err != nil {
		// Fall back to any free localhost port if the configured port is busy
		ln4, err = net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			return 0, err
		}
	}
	actualPort := ln4.Addr().(*net.TCPAddr).Port
	listeners := []net.Listener{ln4}

	// Also bind IPv6 loopback `[::1]:<actualPort>` so `https://localhost:18920` works
	// whether Windows resolves `localhost` to 127.0.0.1 or ::1.
	if ln6, err6 := net.Listen("tcp6", fmt.Sprintf("[::1]:%d", actualPort)); err6 == nil {
		listeners = append(listeners, ln6)
	}

	tlsCfg, _, _ := EnsureLocalTLSConfig()
	dualLn := newDualProtocolListener(listeners, tlsCfg)
	s.listener = dualLn

	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", s.withAuth(s.handleStatus))
	mux.HandleFunc("/api/manifest", s.withAuth(s.handleManifest))
	mux.HandleFunc("/api/file", s.withAuth(s.handleFile))
	mux.HandleFunc("/api/events", s.withAuth(s.handleEvents))

	s.httpSrv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 15 * time.Second,
	}

	go func() {
		_ = s.httpSrv.Serve(dualLn)
	}()

	return actualPort, nil
}

// Stop closes all active SSE streams and shuts down the HTTP listener.
func (s *Server) Stop() {
	s.subsMu.Lock()
	for ch := range s.subs {
		close(ch)
		delete(s.subs, ch)
	}
	s.subsMu.Unlock()

	if s.httpSrv != nil {
		_ = s.httpSrv.Close()
	}
	if s.listener != nil {
		_ = s.listener.Close()
	}
}

// ConnectedPeers returns the count of active SSE peer connections.
func (s *Server) ConnectedPeers() int {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	return len(s.subs)
}

// Broadcast sends a SyncEvent to all connected SSE clients.
func (s *Server) Broadcast(ev SyncEvent) {
	if ev.Timestamp == 0 {
		ev.Timestamp = time.Now().UTC().UnixMilli()
	}
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	for ch := range s.subs {
		select {
		case ch <- ev:
		default:
			// Drop if client buffer is full; client will rescan
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
	snap := s.engine.index.Snapshot()
	var totalBytes int64
	for _, f := range snap.Files {
		totalBytes += f.Size
	}
	resp := map[string]any{
		"node_id":     snap.NodeID,
		"version":     "1.0.0",
		"file_count":  len(snap.Files),
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
			Folders []config.FolderMapping `json:"folders"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 256*1024)).Decode(&reqPayload); err == nil && len(reqPayload.Folders) > 0 {
			s.engine.ApplyPeerFolderMappings(reqPayload.Folders)
		}
	}
	snap := s.engine.index.Snapshot()
	if len(snap.Folders) == 0 {
		snap.Folders = s.engine.Config().EnabledFolders()
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(snap)
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
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		http.Error(w, "not a regular file", http.StatusNotFound)
		return
	}

	snap := s.engine.index.Snapshot()
	meta, hasMeta := snap.Files[relPath]
	if !hasMeta {
		hash, _ := HashFile(fullPath)
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
	n, err := io.Copy(w, f)
	if err == nil && n > 0 {
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

	parentDir := filepath.Dir(fullPath)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		http.Error(w, "failed to create parent directory: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Write incoming stream to a temporary file in the same directory
	var randBuf [6]byte
	_, _ = rand.Read(randBuf[:])
	tmpPath := filepath.Join(parentDir, fmt.Sprintf(".synct.tmp.%s", hex.EncodeToString(randBuf[:])))

	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		http.Error(w, "failed to create temp file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	hasher := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(tmpFile, hasher), r.Body)
	closeErr := tmpFile.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(tmpPath)
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
		existingHash, _ := HashFile(fullPath)
		if existingHash != "" && existingHash != actualHash {
			if cfg.ConflictPolicy == config.ConflictKeepBoth && baseHash != "" && baseHash != existingHash {
				backupPath := ConflictBackupPath(fullPath)
				if err := copyFileContents(fullPath, backupPath); err == nil {
					conflictSaved = true
					s.engine.incrementConflict()
					s.engine.Logf("冲突保护: 已保存本地冲突副本 %s", filepath.Base(backupPath))
				}
			}
		}
	}

	modTime := time.UnixMilli(mtimeMs)
	_ = os.Chtimes(tmpPath, modTime, modTime)

	fid, _ := SplitFolderRelPath(relPath)
	// Record in index BEFORE rename so local file watcher sees it as already synced
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
	s.engine.index.MarkFileSynced(meta, st, "远端 -> 本地")

	if err := os.Rename(tmpPath, fullPath); err != nil {
		_ = os.Remove(fullPath)
		if err2 := os.Rename(tmpPath, fullPath); err2 != nil {
			_ = os.Remove(tmpPath)
			http.Error(w, "failed to commit file: "+err2.Error(), http.StatusInternalServerError)
			return
		}
	}
	_ = os.Chtimes(fullPath, modTime, modTime)

	// Refresh exact mtime from filesystem in case OS rounded it
	if info, err := os.Stat(fullPath); err == nil {
		meta.ModTime = info.ModTime().UTC().UnixMilli()
		s.engine.index.MarkFileSynced(meta, st, "远端 -> 本地")
	}
	s.engine.index.Save()

	s.engine.recordDownload(relPath, written, time.Since(start))
	s.engine.markSyncedActivity(fmt.Sprintf("接收文件更新: %s (%s)", relPath, FormatBytes(written)))

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

	tomb := s.engine.index.MarkFileDeleted(relPath, deletedAt)
	_ = os.Remove(fullPath)
	cleanEmptyParents(rootDir, filepath.Dir(fullPath))
	s.engine.index.Save()

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

	ch := make(chan SyncEvent, 32)
	peerLabel := r.RemoteAddr
	if node := r.Header.Get("X-SyncT-Node"); node != "" {
		peerLabel = node
	}

	s.subsMu.Lock()
	s.subs[ch] = peerLabel
	s.subsMu.Unlock()
	s.engine.notifyUI()

	defer func() {
		s.subsMu.Lock()
		delete(s.subs, ch)
		s.subsMu.Unlock()
		s.engine.notifyUI()
	}()

	// Send initial handshake event
	_, _ = fmt.Fprintf(w, "event: connected\ndata: {\"node_id\":%q}\n\n", s.engine.index.NodeID())
	flusher.Flush()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, open := <-ch:
			if !open {
				return
			}
			payload, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			_, _ = fmt.Fprintf(w, "event: sync\ndata: %s\n\n", payload)
			flusher.Flush()
		case <-ticker.C:
			_, _ = fmt.Fprintf(w, ": heartbeat %d\n\n", time.Now().Unix())
			flusher.Flush()
		}
	}
}

func copyFileContents(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}

func cleanEmptyParents(rootDir, dir string) {
	rootClean := filepath.Clean(rootDir)
	for {
		dirClean := filepath.Clean(dir)
		if dirClean == rootClean || !strings.HasPrefix(dirClean, rootClean) {
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
