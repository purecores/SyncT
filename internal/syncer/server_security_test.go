package syncer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"synct/internal/config"
)

func TestServerRejectsEmptyAuthToken(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		t.Fatal(err)
	}

	host := startTestHost(t, hostDir, "")
	base := hostBaseURL(t, host)

	resp, body := syncRequest(t, http.MethodGet, base+"/api/status", "", nil, nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /api/status with empty server token = %d (%s); want 401", resp.StatusCode, body)
	}

	resp, body = syncRequest(t, http.MethodGet, base+"/api/status", "any-token", nil, nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /api/status with header against empty server token = %d (%s); want 401", resp.StatusCode, body)
	}
}

func TestServerRejectsQueryParamToken(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		t.Fatal(err)
	}

	const token = "st_header_only_token"
	host := startTestHost(t, hostDir, token)
	base := hostBaseURL(t, host)

	resp, body := syncRequest(t, http.MethodGet, base+"/api/status?token="+token, "", nil, nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /api/status?token=<valid> = %d (%s); want 401", resp.StatusCode, body)
	}

	resp, body = syncRequest(t, http.MethodGet, base+"/api/status", token, nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/status with X-SyncT-Token = %d (%s); want 200", resp.StatusCode, body)
	}
}

func TestServerAuthRateLimiting(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		t.Fatal(err)
	}

	const token = "st_ratelimit_token"
	host := startTestHost(t, hostDir, token)
	host.server.authMu.Lock()
	host.server.authMaxFailures = 5
	host.server.authWindow = time.Minute
	host.server.authLockout = 5 * time.Second
	host.server.authMu.Unlock()

	base := hostBaseURL(t, host)

	// 4 bad attempts (below max=5) return 401.
	for i := 1; i <= 4; i++ {
		resp, _ := syncRequest(t, http.MethodGet, base+"/api/status", "wrong", nil, nil)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("attempt %d: status = %d; want 401", i, resp.StatusCode)
		}
	}

	// Valid auth before lockout clears the failure counter.
	resp, _ := syncRequest(t, http.MethodGet, base+"/api/status", token, nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("valid auth before lockout: status = %d; want 200", resp.StatusCode)
	}

	// Now 4 more bad attempts should again return 401 (counter was cleared).
	for i := 1; i <= 4; i++ {
		resp, _ := syncRequest(t, http.MethodGet, base+"/api/status", "wrong", nil, nil)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("post-reset attempt %d: status = %d; want 401", i, resp.StatusCode)
		}
	}

	// 5th bad attempt triggers lockout with 429 + Retry-After.
	resp, _ = syncRequest(t, http.MethodGet, base+"/api/status", "wrong", nil, nil)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("lockout-triggering attempt: status = %d; want 429", resp.StatusCode)
	}
	ra, err := strconv.Atoi(resp.Header.Get("Retry-After"))
	if err != nil || ra < 1 {
		t.Fatalf("Retry-After = %q (err=%v); want integer >= 1", resp.Header.Get("Retry-After"), err)
	}

	// Even a valid token is blocked while locked out.
	resp, _ = syncRequest(t, http.MethodGet, base+"/api/status", token, nil, nil)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("valid auth during lockout: status = %d; want 429", resp.StatusCode)
	}
	if ra2, err := strconv.Atoi(resp.Header.Get("Retry-After")); err != nil || ra2 < 1 {
		t.Fatalf("Retry-After during lockout = %q (err=%v); want integer >= 1", resp.Header.Get("Retry-After"), err)
	}

	// Distinct CF-Connecting-IP over loopback is tracked independently.
	resp, _ = syncRequest(t, http.MethodGet, base+"/api/status", token, nil, map[string]string{
		"CF-Connecting-IP": "203.0.113.42",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("distinct CF-Connecting-IP status = %d; want 200", resp.StatusCode)
	}
}

func TestClientIPTrustModel(t *testing.T) {
	reqLoopback := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	reqLoopback.RemoteAddr = "127.0.0.1:12345"
	reqLoopback.Header.Set("CF-Connecting-IP", "198.51.100.7")
	if got := clientIP(reqLoopback); got != "198.51.100.7" {
		t.Fatalf("loopback + valid CF-Connecting-IP = %q; want 198.51.100.7", got)
	}

	reqBadCF := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	reqBadCF.RemoteAddr = "127.0.0.1:12345"
	reqBadCF.Header.Set("CF-Connecting-IP", "not-an-ip")
	if got := clientIP(reqBadCF); got != "127.0.0.1" {
		t.Fatalf("loopback + invalid CF-Connecting-IP = %q; want 127.0.0.1", got)
	}

	reqLAN := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	reqLAN.RemoteAddr = "192.168.1.50:54321"
	reqLAN.Header.Set("CF-Connecting-IP", "198.51.100.7")
	if got := clientIP(reqLAN); got != "192.168.1.50" {
		t.Fatalf("non-loopback + spoofed CF-Connecting-IP = %q; want 192.168.1.50", got)
	}
}

func TestServerSyncDirectionPushOnly(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		t.Fatal(err)
	}
	content := []byte("push-only readable file")
	mustWriteFile(t, filepath.Join(hostDir, "read.txt"), content)

	const token = "st_push_only_token"
	host := startTestHost(t, hostDir, token)
	cfg := host.Config()
	cfg.SyncDirection = config.SyncPushOnly
	host.UpdateConfig(cfg)
	base := hostBaseURL(t, host)

	// GET is allowed on push-only host (clients pull from host).
	resp, body := syncRequest(t, http.MethodGet, base+"/api/file?path=read.txt", token, nil, nil)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, content) {
		t.Fatalf("GET on push-only host = %d (%q); want 200", resp.StatusCode, body)
	}

	// PUT is forbidden.
	upload := []byte("nope")
	putURL := fmt.Sprintf("%s/api/file?path=write.txt&mtime=%d&sha256=%s", base, time.Now().UTC().UnixMilli(), sha256Hex(upload))
	if resp, _ := syncRequest(t, http.MethodPut, putURL, token, upload, nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("PUT on push-only host = %d; want 403", resp.StatusCode)
	}

	// DELETE is forbidden.
	delURL := fmt.Sprintf("%s/api/file?path=read.txt&deleted_at=%d", base, time.Now().UTC().UnixMilli())
	if resp, _ := syncRequest(t, http.MethodDelete, delURL, token, nil, nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("DELETE on push-only host = %d; want 403", resp.StatusCode)
	}
	if _, err := os.Stat(filepath.Join(hostDir, "read.txt")); err != nil {
		t.Fatalf("file must remain after forbidden DELETE: %v", err)
	}

	// POST /api/dirs is forbidden.
	dirsBody, _ := json.Marshal(map[string]any{
		"dirs": []DirMeta{{RelPath: "ShouldNotExist"}},
	})
	if resp, _ := syncRequest(t, http.MethodPost, base+"/api/dirs", token, dirsBody, nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST /api/dirs on push-only host = %d; want 403", resp.StatusCode)
	}

	// POST /api/upload/begin is forbidden.
	beginBody, _ := json.Marshal(uploadBeginRequest{
		Path: "chunked.bin", Size: 1024, ModTime: time.Now().UTC().UnixMilli(), SHA256: sha256Hex(make([]byte, 1024)),
	})
	if resp, _ := syncRequest(t, http.MethodPost, base+"/api/upload/begin", token, beginBody, nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST /api/upload/begin on push-only host = %d; want 403", resp.StatusCode)
	}
}

func TestServerSyncDirectionPullOnly(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		t.Fatal(err)
	}
	mustWriteFile(t, filepath.Join(hostDir, "secret.txt"), []byte("do not serve"))

	const token = "st_pull_only_token"
	host := startTestHost(t, hostDir, token)
	cfg := host.Config()
	cfg.SyncDirection = config.SyncPullOnly
	host.UpdateConfig(cfg)
	base := hostBaseURL(t, host)

	// GET is forbidden on pull-only host.
	if resp, _ := syncRequest(t, http.MethodGet, base+"/api/file?path=secret.txt", token, nil, nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("GET on pull-only host = %d; want 403", resp.StatusCode)
	}

	// PUT is allowed on pull-only host.
	upload := []byte("incoming data")
	putURL := fmt.Sprintf("%s/api/file?path=incoming.txt&mtime=%d&sha256=%s", base, time.Now().UTC().UnixMilli(), sha256Hex(upload))
	resp, body := syncRequest(t, http.MethodPut, putURL, token, upload, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT on pull-only host = %d (%s); want 200", resp.StatusCode, body)
	}
	got, err := os.ReadFile(filepath.Join(hostDir, "incoming.txt"))
	if err != nil || !bytes.Equal(got, upload) {
		t.Fatalf("incoming.txt = %q (err=%v); want %q", got, err, upload)
	}
}

func TestServerFileDownloadRange(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		t.Fatal(err)
	}
	content := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	mustWriteFile(t, filepath.Join(hostDir, "alpha.txt"), content)
	mustWriteFile(t, filepath.Join(hostDir, "empty.txt"), nil)

	const token = "st_range_token"
	host := startTestHost(t, hostDir, token)
	base := hostBaseURL(t, host)

	// Full download advertises Accept-Ranges and X-SyncT-Size.
	resp, body := syncRequest(t, http.MethodGet, base+"/api/file?path=alpha.txt", token, nil, nil)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, content) {
		t.Fatalf("full download = %d (%q)", resp.StatusCode, body)
	}
	if resp.Header.Get("Accept-Ranges") != "bytes" {
		t.Fatalf("Accept-Ranges = %q; want bytes", resp.Header.Get("Accept-Ranges"))
	}
	if resp.Header.Get("X-SyncT-Size") != strconv.Itoa(len(content)) {
		t.Fatalf("X-SyncT-Size = %q; want %d", resp.Header.Get("X-SyncT-Size"), len(content))
	}

	// Prefix range: bytes=0-9
	resp, body = syncRequest(t, http.MethodGet, base+"/api/file?path=alpha.txt", token, nil, map[string]string{
		"Range": "bytes=0-9",
	})
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("bytes=0-9 status = %d; want 206", resp.StatusCode)
	}
	if string(body) != "0123456789" {
		t.Fatalf("bytes=0-9 body = %q; want 0123456789", body)
	}
	wantCR := fmt.Sprintf("bytes 0-9/%d", len(content))
	if resp.Header.Get("Content-Range") != wantCR {
		t.Fatalf("Content-Range = %q; want %q", resp.Header.Get("Content-Range"), wantCR)
	}

	// Middle range: bytes=10-15
	resp, body = syncRequest(t, http.MethodGet, base+"/api/file?path=alpha.txt", token, nil, map[string]string{
		"Range": "bytes=10-15",
	})
	if resp.StatusCode != http.StatusPartialContent || string(body) != "ABCDEF" {
		t.Fatalf("bytes=10-15 = %d (%q); want 206 ABCDEF", resp.StatusCode, body)
	}
	wantCR = fmt.Sprintf("bytes 10-15/%d", len(content))
	if resp.Header.Get("Content-Range") != wantCR {
		t.Fatalf("Content-Range = %q; want %q", resp.Header.Get("Content-Range"), wantCR)
	}

	// Open-ended range: bytes=10-
	resp, body = syncRequest(t, http.MethodGet, base+"/api/file?path=alpha.txt", token, nil, map[string]string{
		"Range": "bytes=10-",
	})
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, content[10:]) {
		t.Fatalf("bytes=10- = %d (%q); want 206 %q", resp.StatusCode, body, content[10:])
	}
	wantCR = fmt.Sprintf("bytes 10-%d/%d", len(content)-1, len(content))
	if resp.Header.Get("Content-Range") != wantCR {
		t.Fatalf("Content-Range = %q; want %q", resp.Header.Get("Content-Range"), wantCR)
	}

	// Invalid ranges -> 416 with Content-Range: bytes */<size>
	invalidRanges := []string{
		"bytes=999-1000",
		"bytes=20-10",
		"bytes=abc-def",
		"items=0-5",
		"bytes=0-1,2-3",
	}
	for _, rng := range invalidRanges {
		resp, _ = syncRequest(t, http.MethodGet, base+"/api/file?path=alpha.txt", token, nil, map[string]string{
			"Range": rng,
		})
		if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
			t.Fatalf("Range %q status = %d; want 416", rng, resp.StatusCode)
		}
		if got := resp.Header.Get("Content-Range"); got != fmt.Sprintf("bytes */%d", len(content)) {
			t.Fatalf("Range %q Content-Range = %q; want bytes */%d", rng, got, len(content))
		}
	}

	// Empty file with Range header -> 416
	resp, _ = syncRequest(t, http.MethodGet, base+"/api/file?path=empty.txt", token, nil, map[string]string{
		"Range": "bytes=0-0",
	})
	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("empty file Range status = %d; want 416", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Range"); got != "bytes */0" {
		t.Fatalf("empty file Content-Range = %q; want bytes */0", got)
	}
}

type failingReader struct {
	prefix []byte
	sent   bool
}

func (f *failingReader) Read(p []byte) (int, error) {
	if !f.sent {
		f.sent = true
		n := copy(p, f.prefix)
		return n, nil
	}
	return 0, errors.New("simulated mid-chunk connection reset")
}

func TestChunkedUploadRollbackAndCleanRetry(t *testing.T) {
	srv, hostDir := newChunkTestServer(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", srv.port)
	const token = "st_chunk_test"

	part1 := bytes.Repeat([]byte("A"), 1024)
	part2 := bytes.Repeat([]byte("B"), 2048)
	fullPayload := append(append([]byte(nil), part1...), part2...)
	wantHash := sha256Hex(fullPayload)

	beginBody, _ := json.Marshal(uploadBeginRequest{
		Path:    "rollback.bin",
		Size:    int64(len(fullPayload)),
		ModTime: time.Now().UTC().UnixMilli(),
		SHA256:  wantHash,
	})
	resp := chunkPost(t, base+"/api/upload/begin", token, beginBody)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("begin = %d: %s", resp.StatusCode, body)
	}
	var session uploadBeginResponse
	if err := json.NewDecoder(resp.Body).Decode(&session); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// Upload chunk 1 cleanly.
	req1, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/upload/chunk?id=%s&offset=0", base, session.UploadID), bytes.NewReader(part1))
	req1.Header.Set("X-SyncT-Token", token)
	r1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	r1.Body.Close()
	if r1.StatusCode != http.StatusOK {
		t.Fatalf("chunk 1 status = %d; want 200", r1.StatusCode)
	}

	// Simulate mid-chunk read error after writing garbage bytes: must roll back temp file to 1024.
	failReq := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/upload/chunk?id=%s&offset=%d", session.UploadID, len(part1)), &failingReader{
		prefix: []byte("CORRUPTED_PARTIAL_BYTES"),
	})
	failReq.ContentLength = -1
	failRec := httptest.NewRecorder()
	srv.handleUploadChunk(failRec, failReq)
	if failRec.Code != http.StatusInternalServerError {
		t.Fatalf("mid-chunk error status = %d; want 500", failRec.Code)
	}

	// Verify the staging .part file was truncated back to len(part1).
	up := srv.lookupUpload(session.UploadID)
	if up == nil {
		t.Fatal("upload session disappeared after chunk error")
	}
	if info, err := os.Stat(up.tmpPath); err != nil || info.Size() != int64(len(part1)) {
		t.Fatalf("staging file size after mid-chunk rollback = %v (err=%v); want %d", info, err, len(part1))
	}

	// Send an oversized chunk (exceeds declared file size): must fail and roll back to len(part1).
	oversized := bytes.Repeat([]byte("Z"), len(part2)+512)
	reqOver, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/upload/chunk?id=%s&offset=%d", base, session.UploadID, len(part1)), bytes.NewReader(oversized))
	reqOver.Header.Set("X-SyncT-Token", token)
	rOver, err := http.DefaultClient.Do(reqOver)
	if err != nil {
		t.Fatal(err)
	}
	rOver.Body.Close()
	if rOver.StatusCode != http.StatusBadRequest {
		t.Fatalf("oversized chunk status = %d; want 400", rOver.StatusCode)
	}
	if info, err := os.Stat(up.tmpPath); err != nil || info.Size() != int64(len(part1)) {
		t.Fatalf("staging file size after oversized rollback = %v (err=%v); want %d", info, err, len(part1))
	}

	// Retry chunk 2 cleanly at offset=len(part1).
	req2, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/upload/chunk?id=%s&offset=%d", base, session.UploadID, len(part1)), bytes.NewReader(part2))
	req2.Header.Set("X-SyncT-Token", token)
	r2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	var chunk2Resp uploadBeginResponse
	_ = json.NewDecoder(r2.Body).Decode(&chunk2Resp)
	r2.Body.Close()
	if r2.StatusCode != http.StatusOK || chunk2Resp.Received != int64(len(fullPayload)) {
		t.Fatalf("clean retry chunk 2 = %d received=%d; want 200 received=%d", r2.StatusCode, chunk2Resp.Received, len(fullPayload))
	}

	// Commit must verify SHA-256 over the assembled file and succeed.
	commitResp := chunkPost(t, fmt.Sprintf("%s/api/upload/commit?id=%s", base, session.UploadID), token, nil)
	commitBody, _ := io.ReadAll(commitResp.Body)
	commitResp.Body.Close()
	if commitResp.StatusCode != http.StatusOK {
		t.Fatalf("commit = %d: %s", commitResp.StatusCode, commitBody)
	}

	disk, err := os.ReadFile(filepath.Join(hostDir, "rollback.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(disk, fullPayload) || sha256Hex(disk) != wantHash {
		t.Fatalf("committed file hash = %s; want %s", sha256Hex(disk), wantHash)
	}
}

func TestChunkedUploadConcurrentCommitRace(t *testing.T) {
	srv, hostDir := newChunkTestServer(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", srv.port)
	const token = "st_chunk_test"

	payload := make([]byte, 256*1024)
	for i := range payload {
		payload[i] = byte((i*31 + 7) & 0xff)
	}
	wantHash := sha256Hex(payload)

	beginBody, _ := json.Marshal(uploadBeginRequest{
		Path:    "race.bin",
		Size:    int64(len(payload)),
		ModTime: time.Now().UTC().UnixMilli(),
		SHA256:  wantHash,
	})
	resp := chunkPost(t, base+"/api/upload/begin", token, beginBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("begin = %d", resp.StatusCode)
	}
	var session uploadBeginResponse
	if err := json.NewDecoder(resp.Body).Decode(&session); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	reqChunk, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/upload/chunk?id=%s&offset=0", base, session.UploadID), bytes.NewReader(payload))
	reqChunk.Header.Set("X-SyncT-Token", token)
	rc, err := http.DefaultClient.Do(reqChunk)
	if err != nil {
		t.Fatal(err)
	}
	rc.Body.Close()
	if rc.StatusCode != http.StatusOK {
		t.Fatalf("chunk = %d", rc.StatusCode)
	}

	const workers = 12
	start := make(chan struct{})
	var okCount atomic.Int32
	var rejectCount atomic.Int32
	var unexpected atomic.Int32
	var wg sync.WaitGroup

	commitURL := fmt.Sprintf("%s/api/upload/commit?id=%s", base, session.UploadID)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r := chunkPost(t, commitURL, token, nil)
			_, _ = io.Copy(io.Discard, r.Body)
			r.Body.Close()
			switch r.StatusCode {
			case http.StatusOK:
				okCount.Add(1)
			case http.StatusConflict, http.StatusNotFound:
				rejectCount.Add(1)
			default:
				unexpected.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if okCount.Load() != 1 {
		t.Fatalf("concurrent commit 200 count = %d; want exactly 1 (rejected=%d unexpected=%d)",
			okCount.Load(), rejectCount.Load(), unexpected.Load())
	}
	if rejectCount.Load() != workers-1 || unexpected.Load() != 0 {
		t.Fatalf("concurrent commit outcomes: ok=%d reject=%d unexpected=%d",
			okCount.Load(), rejectCount.Load(), unexpected.Load())
	}

	disk, err := os.ReadFile(filepath.Join(hostDir, "race.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(disk, payload) || sha256Hex(disk) != wantHash {
		t.Fatalf("final file corrupted after concurrent commits")
	}
}

func TestFirstTimeConflictPreservesHostFile(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		t.Fatal(err)
	}
	mustWriteFile(t, filepath.Join(hostDir, "both.txt"), []byte("host independent version"))

	const token = "st_first_conflict"
	host := startTestHost(t, hostDir, token)
	base := hostBaseURL(t, host)

	// Upload with empty base_hash over an existing different host file.
	incoming := []byte("peer independent version")
	putURL := fmt.Sprintf("%s/api/file?path=both.txt&mtime=%d&sha256=%s", base, time.Now().UTC().UnixMilli(), sha256Hex(incoming))
	resp, body := syncRequest(t, http.MethodPut, putURL, token, incoming, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT = %d (%s)", resp.StatusCode, body)
	}

	entries, err := os.ReadDir(hostDir)
	if err != nil {
		t.Fatal(err)
	}
	foundConflict := false
	for _, e := range entries {
		if strings.Contains(e.Name(), ".conflict-") {
			foundConflict = true
			data, err := os.ReadFile(filepath.Join(hostDir, e.Name()))
			if err != nil || string(data) != "host independent version" {
				t.Fatalf("conflict backup content = %q (err=%v)", data, err)
			}
		}
	}
	if !foundConflict {
		t.Fatal("expected a conflict backup when overwriting an existing host file with empty base_hash")
	}
}
