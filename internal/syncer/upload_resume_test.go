package syncer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"synct/internal/config"
)

func newChunkTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	dir := filepath.Join(tmp, "host")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Role = config.RoleHost
	cfg.TunnelMode = config.TunnelDirect
	cfg.SyncDir = dir
	cfg.LocalPort = 0
	cfg.AuthToken = "st_chunk_test"
	cfg.AutoStartSync = false
	engine := NewEngine(cfg, filepath.Join(tmp, "host_state.json"), nil)
	if err := engine.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Stop)
	return engine.server, dir
}

func chunkPost(t *testing.T, url, token string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-SyncT-Token", token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// An upload larger than the edge's body cap must complete as several small
// requests, and a retry after a lost chunk must resume, not restart.
func TestChunkedUploadResumesAndCommits(t *testing.T) {
	srv, hostDir := newChunkTestServer(t)
	ts := httptest.NewTLSServer(srv.httpSrv.Handler)
	defer ts.Close()
	_ = ts // handler is already served by the engine's listener; use it directly

	base := fmt.Sprintf("http://127.0.0.1:%d", srv.port)
	token := "st_chunk_test"

	payload := make([]byte, 3*maxUploadChunkBytes+12345)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	sum := sha256.Sum256(payload)
	wantHash := hex.EncodeToString(sum[:])
	rel := "big/archive.bin"

	beginBody, _ := json.Marshal(uploadBeginRequest{
		Path: rel, Size: int64(len(payload)), ModTime: time.Now().UTC().UnixMilli(), SHA256: wantHash,
	})
	resp := chunkPost(t, base+"/api/upload/begin", token, beginBody)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("begin: HTTP %d: %s", resp.StatusCode, body)
	}
	var session uploadBeginResponse
	if err := json.NewDecoder(resp.Body).Decode(&session); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if session.UploadID == "" {
		t.Fatal("begin returned no upload id")
	}

	sendChunk := func(offset int64) (uploadBeginResponse, int) {
		end := offset + maxUploadChunkBytes
		if end > int64(len(payload)) {
			end = int64(len(payload))
		}
		req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/upload/chunk?id=%s&offset=%d", base, session.UploadID, offset), bytes.NewReader(payload[offset:end]))
		req.Header.Set("X-SyncT-Token", token)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		var out uploadBeginResponse
		_ = json.NewDecoder(r.Body).Decode(&out)
		return out, r.StatusCode
	}

	// First two chunks, then "lose" the connection and resume from the
	// server-reported offset with a new begin call carrying the same id.
	first, status := sendChunk(0)
	if status != http.StatusOK || first.Received != maxUploadChunkBytes {
		t.Fatalf("chunk 1: status %d received %d", status, first.Received)
	}
	second, status := sendChunk(first.Received)
	if status != http.StatusOK {
		t.Fatalf("chunk 2: status %d", status)
	}

	resumeBody, _ := json.Marshal(uploadBeginRequest{
		Path: rel, Size: int64(len(payload)), ModTime: time.Now().UTC().UnixMilli(), SHA256: wantHash, UploadID: session.UploadID,
	})
	resp = chunkPost(t, base+"/api/upload/begin", token, resumeBody)
	var resumed uploadBeginResponse
	_ = json.NewDecoder(resp.Body).Decode(&resumed)
	resp.Body.Close()
	if resumed.UploadID != session.UploadID || resumed.Received != second.Received {
		t.Fatalf("resume: id %q received %d; want %q / %d", resumed.UploadID, resumed.Received, session.UploadID, second.Received)
	}

	for offset := resumed.Received; offset < int64(len(payload)); {
		out, status := sendChunk(offset)
		if status != http.StatusOK {
			t.Fatalf("chunk at %d: status %d", offset, status)
		}
		if out.Received <= offset {
			t.Fatalf("chunk at %d made no progress: %d", offset, out.Received)
		}
		offset = out.Received
	}

	resp = chunkPost(t, base+"/api/upload/commit?id="+session.UploadID, token, nil)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("commit: HTTP %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	got, err := os.ReadFile(filepath.Join(hostDir, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("assembled file differs from the source")
	}
	if srv.engine.index == nil {
		t.Fatal("missing index")
	}
	if _, ok := srv.engine.index.FileMeta(rel); !ok {
		t.Fatal("committed file is not indexed")
	}
	// The staging directory must not leave files behind.
	entries, _ := os.ReadDir(filepath.Dir(filepath.Join(hostDir, filepath.FromSlash(rel), "."+uploadDirName)))
	for _, e := range entries {
		t.Fatalf("staging dir still holds %s", e.Name())
	}
}

func TestChunkedUploadRejectsHashMismatch(t *testing.T) {
	srv, hostDir := newChunkTestServer(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", srv.port)
	token := "st_chunk_test"
	payload := []byte(strings.Repeat("x", 1000))
	rel := "bad/hash.bin"

	beginBody, _ := json.Marshal(uploadBeginRequest{Path: rel, Size: int64(len(payload)), ModTime: time.Now().UTC().UnixMilli(), SHA256: "deadbeef"})
	resp := chunkPost(t, base+"/api/upload/begin", token, beginBody)
	var session uploadBeginResponse
	_ = json.NewDecoder(resp.Body).Decode(&session)
	resp.Body.Close()

	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/upload/chunk?id=%s&offset=0", base, session.UploadID), bytes.NewReader(payload))
	req.Header.Set("X-SyncT-Token", token)
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()

	resp = chunkPost(t, base+"/api/upload/commit?id="+session.UploadID, token, nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("commit with bad hash: HTTP %d; want 400", resp.StatusCode)
	}
	resp.Body.Close()
	if _, err := os.Stat(filepath.Join(hostDir, filepath.FromSlash(rel))); !os.IsNotExist(err) {
		t.Fatal("a rejected upload must not be committed")
	}
}

func TestUploadPathsAreIgnoredByScan(t *testing.T) {
	matcher := newIgnoreMatcher(config.DefaultIgnorePatterns())
	if !matcher.Match(".synct-uploads-abc123/part.part", false) {
		t.Fatal("staging directories must not be indexed")
	}
	_ = context.Background()
}
