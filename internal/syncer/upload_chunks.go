package syncer

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"synct/internal/config"
)

// Cloudflare's edge caps a proxied request body (100 MB on the free plan) and
// times out a request that stays open for about 100 seconds with 524. A single
// PUT of a large file therefore cannot work through the tunnel no matter how
// fast the link is. Large uploads are split into small requests that finish
// well inside both bounds, and the parts are assembled on the origin, so the
// same protocol also gives a resume point after a dropped connection.

// uploadDirName is the per-folder staging directory. Its leading ".synct"
// keeps scans from indexing it, and it sits on the destination volume so the
// final rename stays atomic.
const uploadDirName = ".synct-uploads"

type uploadBeginRequest struct {
	Path     string `json:"path"`
	Size     int64  `json:"size"`
	ModTime  int64  `json:"mtime"`
	SHA256   string `json:"sha256"`
	BaseHash string `json:"base_hash"`
	UploadID string `json:"upload_id,omitempty"`
}

type uploadBeginResponse struct {
	UploadID string `json:"upload_id"`
	Received int64  `json:"received"`
	Done     bool   `json:"done"`
	Chunk    int64  `json:"chunk"`
}

func (s *Server) uploadDirFor(fullPath string) string {
	base := filepath.Base(fullPath)
	return filepath.Join(filepath.Dir(fullPath), "."+uploadDirName+"-"+hexSafe(base))
}

func hexSafe(name string) string {
	sum := sha256.Sum256([]byte(name))
	return hex.EncodeToString(sum[:4])
}

func (s *Server) handleUploadBegin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var req uploadBeginRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		http.Error(w, "invalid upload request", http.StatusBadRequest)
		return
	}
	cfg := s.engine.Config()
	relPath, ok := CleanRelPath(req.Path)
	if !ok {
		http.Error(w, "invalid relative path", http.StatusBadRequest)
		return
	}
	if ShouldIgnore(relPath, false, cfg.IgnorePatterns) {
		http.Error(w, "path is ignored", http.StatusForbidden)
		return
	}
	_, _, fullPath, ok := ResolveFolderFullPath(cfg, relPath)
	if !ok {
		http.Error(w, "mapped folder not found for path", http.StatusNotFound)
		return
	}
	if req.Size <= 0 {
		http.Error(w, "invalid size", http.StatusBadRequest)
		return
	}

	key := relPath + "|" + strconv.FormatInt(req.Size, 10) + "|" + req.SHA256
	s.uploadsMu.Lock()
	if req.UploadID != "" {
		if up := s.uploads[req.UploadID]; up != nil && up.key == key {
			up.mu.Lock()
			resp := uploadBeginResponse{UploadID: up.id, Received: up.received, Chunk: maxUploadChunkBytes}
			up.lastActive = time.Now()
			up.mu.Unlock()
			s.uploadsMu.Unlock()
			writeJSON(w, resp)
			return
		}
	}
	s.uploadsMu.Unlock()

	id := newUploadID()
	dir := s.uploadDirFor(fullPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		s.engine.LogThrottled("upbegin-mkdir:"+relPath+":"+err.Error(), "WARN", fmt.Sprintf("创建分块上传目录失败 (%s): %v", relPath, err), 30*time.Second)
		http.Error(w, "failed to create upload directory", http.StatusInternalServerError)
		return
	}
	tmpPath := filepath.Join(dir, id+".part")
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		http.Error(w, "failed to create temp file", http.StatusInternalServerError)
		return
	}
	_ = f.Close()

	now := time.Now()
	up := &chunkUpload{
		id:         id,
		key:        key,
		relPath:    relPath,
		fullPath:   fullPath,
		tmpPath:    tmpPath,
		size:       req.Size,
		mtime:      req.ModTime,
		expected:   strings.TrimSpace(req.SHA256),
		baseHash:   strings.TrimSpace(req.BaseHash),
		originNode: strings.TrimSpace(r.Header.Get("X-SyncT-Node")),
		hasher:     sha256.New(),
		createdAt:  now,
		lastActive: now,
	}
	s.uploadsMu.Lock()
	s.uploads[id] = up
	s.uploadsMu.Unlock()

	s.engine.index.BeginWrite(relPath)
	writeJSON(w, uploadBeginResponse{UploadID: id, Chunk: maxUploadChunkBytes})
}

func (s *Server) lookupUpload(id string) *chunkUpload {
	s.expireUploads()
	s.uploadsMu.Lock()
	up := s.uploads[id]
	s.uploadsMu.Unlock()
	return up
}

func (s *Server) handleUploadChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	id := r.URL.Query().Get("id")
	offset, _ := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	up := s.lookupUpload(id)
	if up == nil {
		http.Error(w, "unknown upload", http.StatusNotFound)
		return
	}
	up.mu.Lock()
	defer up.mu.Unlock()
	if offset != up.received {
		writeJSON(w, uploadBeginResponse{UploadID: up.id, Received: up.received, Chunk: maxUploadChunkBytes})
		return
	}
	if r.ContentLength > maxUploadChunkBytes {
		http.Error(w, "chunk too large", http.StatusRequestEntityTooLarge)
		return
	}
	f, err := os.OpenFile(up.tmpPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		http.Error(w, "upload session lost", http.StatusGone)
		return
	}
	buf := ioBufferPool.Get().(*[]byte)
	n, copyErr := io.CopyBuffer(io.MultiWriter(f, up.hasher), io.LimitReader(r.Body, maxUploadChunkBytes+1), *buf)
	ioBufferPool.Put(buf)
	closeErr := f.Close()
	if copyErr != nil || closeErr != nil {
		http.Error(w, "failed to write chunk", http.StatusInternalServerError)
		return
	}
	if n > maxUploadChunkBytes {
		http.Error(w, "chunk too large", http.StatusRequestEntityTooLarge)
		return
	}
	up.received += n
	up.lastActive = time.Now()
	s.engine.countDownloadBytes(n)
	writeJSON(w, uploadBeginResponse{UploadID: up.id, Received: up.received, Chunk: maxUploadChunkBytes})
}

func (s *Server) handleUploadCommit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := r.URL.Query().Get("id")
	up := s.lookupUpload(id)
	if up == nil {
		http.Error(w, "unknown upload", http.StatusNotFound)
		return
	}
	up.mu.Lock()
	relPath := up.relPath
	received, size := up.received, up.size
	actual := hex.EncodeToString(up.hasher.Sum(nil))
	expected := up.expected
	tmpPath, fullPath := up.tmpPath, up.fullPath
	mtimeMs, baseHash, originNode := up.mtime, up.baseHash, up.originNode
	up.mu.Unlock()

	fail := func(status int, msg string) {
		s.discardUpload(up)
		http.Error(w, msg, status)
	}
	if received != size {
		fail(http.StatusConflict, fmt.Sprintf("incomplete upload: %d/%d bytes", received, size))
		return
	}
	if expected != "" && !strings.EqualFold(expected, actual) {
		fail(http.StatusBadRequest, "sha256 mismatch")
		return
	}

	cfg := s.engine.Config()
	conflictSaved := false
	if existingInfo, statErr := os.Stat(fullPath); statErr == nil && existingInfo.Mode().IsRegular() {
		existingHash, hashErr := HashFile(fullPath)
		if hashErr == nil && existingHash != "" && existingHash != actual {
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
	_ = os.Chtimes(tmpPath, modTime, modTime)
	fid, _ := SplitFolderRelPath(relPath)
	meta := FileMeta{
		FolderID: fid,
		RelPath:  relPath,
		Size:     received,
		ModTime:  mtimeMs,
		SHA256:   actual,
	}
	st := StatusSynced
	if conflictSaved {
		st = StatusConflict
	}
	if err := replaceFile(tmpPath, fullPath); err != nil {
		fail(http.StatusInternalServerError, "failed to commit file: "+err.Error())
		return
	}
	_ = os.Chtimes(fullPath, modTime, modTime)
	if info, err := os.Stat(fullPath); err == nil {
		meta.ModTime = info.ModTime().UTC().UnixMilli()
	}
	s.engine.index.MarkFileSynced(meta, st, "远端 -> 本地")
	s.engine.index.RequestSave()
	s.engine.recordDownload(relPath, received, 0)
	s.engine.noteSynced("接收文件", relPath, received)

	s.Broadcast(SyncEvent{
		Type:       EventUpsert,
		OriginNode: originNode,
		File:       &meta,
		Timestamp:  time.Now().UTC().UnixMilli(),
	})
	s.finishUpload(up)
	writeJSON(w, meta)
}

func (s *Server) handleUploadAbort(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if up := s.lookupUpload(id); up != nil {
		s.discardUpload(up)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) discardUpload(up *chunkUpload) {
	up.mu.Lock()
	if up.done {
		up.mu.Unlock()
		return
	}
	up.done = true
	tmp, rel := up.tmpPath, up.relPath
	up.mu.Unlock()
	_ = os.Remove(tmp)
	_ = os.RemoveAll(filepath.Dir(tmp))
	s.engine.index.EndWrite(rel)
	s.uploadsMu.Lock()
	delete(s.uploads, up.id)
	s.uploadsMu.Unlock()
}

func (s *Server) finishUpload(up *chunkUpload) {
	up.mu.Lock()
	if up.done {
		up.mu.Unlock()
		return
	}
	up.done = true
	rel := up.relPath
	tmp := up.tmpPath
	up.mu.Unlock()
	// The part file was renamed away; the per-name staging directory is now
	// empty and must not linger next to the user's files.
	_ = os.RemoveAll(filepath.Dir(tmp))
	s.engine.index.EndWrite(rel)
	s.uploadsMu.Lock()
	delete(s.uploads, up.id)
	s.uploadsMu.Unlock()
}

func (s *Server) abortAllUploads() {
	s.uploadsMu.Lock()
	list := make([]*chunkUpload, 0, len(s.uploads))
	for _, up := range s.uploads {
		list = append(list, up)
	}
	s.uploadsMu.Unlock()
	for _, up := range list {
		s.discardUpload(up)
	}
}

// expireUploads drops sessions that stopped sending. A resumed upload keeps
// its temporary file, so the client's next retry starts where it stopped.
func (s *Server) expireUploads() {
	cutoff := time.Now().Add(-uploadSessionTTL)
	s.uploadsMu.Lock()
	var stale []*chunkUpload
	for _, up := range s.uploads {
		up.mu.Lock()
		last := up.lastActive
		up.mu.Unlock()
		if last.Before(cutoff) {
			stale = append(stale, up)
		}
	}
	s.uploadsMu.Unlock()
	for _, up := range stale {
		s.discardUpload(up)
	}
}

// cleanupUploadDirs removes staging directories left by a crash. It runs at
// start, before any upload could belong to this run.
func (s *Server) cleanupUploadDirs() {
	cfg := s.engine.Config()
	for _, folder := range cfg.EnabledFolders() {
		root := strings.TrimSpace(folder.LocalPath)
		if root == "" {
			continue
		}
		var dirs []string
		_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if d.IsDir() && strings.HasPrefix(d.Name(), "."+uploadDirName+"-") {
				dirs = append(dirs, path)
				return filepath.SkipDir
			}
			return nil
		})
		for _, dir := range dirs {
			_ = os.RemoveAll(dir)
		}
	}
}

func newUploadID() string {
	var buf [12]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(buf[:])
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}
