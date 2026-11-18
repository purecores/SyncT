package syncer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"synct/internal/config"
)

func syncRequest(t *testing.T, method, rawURL, token string, body []byte, headers map[string]string) (*http.Response, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, rawURL, reader)
	if err != nil {
		t.Fatalf("new request %s %s: %v", method, rawURL, err)
	}
	if token != "" {
		req.Header.Set("X-SyncT-Token", token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, rawURL, err)
	}
	data, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return resp, data
}

func TestFormatBytesSpeedAndTransferTimeout(t *testing.T) {
	bytesCases := map[int64]string{
		0:                      "0 B",
		512:                    "512 B",
		1024:                   "1.0 KB",
		1536:                   "1.5 KB",
		1024 * 1024:            "1.0 MB",
		3 * 1024 * 1024 * 1024: "3.0 GB",
	}
	for in, want := range bytesCases {
		if got := FormatBytes(in); got != want {
			t.Errorf("FormatBytes(%d) = %q; want %q", in, got, want)
		}
	}
	if got := FormatSpeed(0); got != "0 B/s" {
		t.Errorf("FormatSpeed(0) = %q", got)
	}
	if got := FormatSpeed(2048); got != "2.0 KB/s" {
		t.Errorf("FormatSpeed(2048) = %q", got)
	}

	if got := transferTimeout(0); got != 2*time.Minute {
		t.Errorf("transferTimeout(0) = %s; want 2m", got)
	}
	// A 100 GiB file over a slow uplink is legitimate and must not be cut off
	// by a fixed ceiling; the stall watchdog catches dead connections instead.
	huge := transferTimeout(100 * 1024 * 1024 * 1024)
	if huge != 24*time.Hour {
		t.Errorf("transferTimeout(100GiB) = %s; want the 24h cap", huge)
	}
	small := transferTimeout(10 * 1024 * 1024)
	large := transferTimeout(100 * 1024 * 1024)
	if small < 2*time.Minute || small > large || large > 24*time.Hour {
		t.Errorf("transfer timeouts are not bounded/monotonic: small=%s large=%s", small, large)
	}
	// A 300 MB file gets well over ten minutes at the conservative estimate.
	if got := transferTimeout(300 * 1024 * 1024); got < 10*time.Minute {
		t.Errorf("transferTimeout(300MiB) = %s; too short for a slow uplink", got)
	}
}

func TestServerAuthManifestAndFileAPI(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		t.Fatal(err)
	}
	content := []byte("hello server")
	mustWriteFile(t, filepath.Join(hostDir, "hello.txt"), content)

	const token = "st_server_test_token"
	host := startTestHost(t, hostDir, token)
	base := hostBaseURL(t, host)

	// --- Auth ---
	if resp, _ := syncRequest(t, http.MethodGet, base+"/api/status", "", nil, nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status without token = %d; want 401", resp.StatusCode)
	}
	resp, body := syncRequest(t, http.MethodGet, base+"/api/status", token, nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status with token = %d: %s", resp.StatusCode, body)
	}
	var status struct {
		NodeID    string `json:"node_id"`
		FileCount int    `json:"file_count"`
	}
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatalf("status json: %v (%s)", err, body)
	}
	if status.NodeID == "" || status.FileCount != 1 {
		t.Fatalf("status = %+v; want node id and 1 file", status)
	}

	// --- Manifest ---
	if resp, _ := syncRequest(t, http.MethodPost, base+"/api/manifest", "", []byte("{}"), nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("manifest without token = %d; want 401", resp.StatusCode)
	}
	resp, body = syncRequest(t, http.MethodPost, base+"/api/manifest", token, []byte("{}"), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("manifest = %d: %s", resp.StatusCode, body)
	}
	var manifest Manifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		t.Fatalf("manifest json: %v", err)
	}
	meta, ok := manifest.Files["hello.txt"]
	if !ok || meta.SHA256 != sha256Hex(content) {
		t.Fatalf("manifest file = %+v, ok=%v", meta, ok)
	}
	if len(manifest.Folders) == 0 {
		t.Fatal("manifest should advertise folders")
	}

	// --- Download and path safety ---
	resp, body = syncRequest(t, http.MethodGet, base+"/api/file?path=hello.txt", token, nil, nil)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, content) {
		t.Fatalf("download = %d body=%q", resp.StatusCode, body)
	}
	if got := resp.Header.Get("X-SyncT-SHA256"); got != sha256Hex(content) {
		t.Fatalf("download sha header = %q", got)
	}
	badPath := base + "/api/file?path=" + url.QueryEscape("../secret.txt")
	if resp, _ := syncRequest(t, http.MethodGet, badPath, token, nil, nil); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("traversal download = %d; want 400", resp.StatusCode)
	}
	ignoredPath := base + "/api/file?path=" + url.QueryEscape("node_modules/pkg.js")
	if resp, _ := syncRequest(t, http.MethodGet, ignoredPath, token, nil, nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("ignored path = %d; want 403", resp.StatusCode)
	}
	unknownFolder := base + "/api/file?path=" + url.QueryEscape("@nope/x.txt")
	if resp, _ := syncRequest(t, http.MethodGet, unknownFolder, token, nil, nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown folder = %d; want 404", resp.StatusCode)
	}

	// --- Upload: hash mismatch is rejected without writing ---
	upload := []byte("uploaded data")
	mtime := time.Now().UTC().UnixMilli()
	badHashURL := fmt.Sprintf("%s/api/file?path=uploaded.txt&mtime=%d&sha256=%s",
		base, mtime, sha256Hex([]byte("wrong")))
	if resp, _ := syncRequest(t, http.MethodPut, badHashURL, token, upload, nil); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("upload with wrong hash = %d; want 400", resp.StatusCode)
	}
	if _, err := os.Stat(filepath.Join(hostDir, "uploaded.txt")); !os.IsNotExist(err) {
		t.Fatalf("hash-mismatched upload must not create a file, stat err=%v", err)
	}

	goodUploadURL := fmt.Sprintf("%s/api/file?path=uploaded.txt&mtime=%d&sha256=%s",
		base, mtime, sha256Hex(upload))
	resp, body = syncRequest(t, http.MethodPut, goodUploadURL, token, upload, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("upload = %d: %s", resp.StatusCode, body)
	}
	got, err := os.ReadFile(filepath.Join(hostDir, "uploaded.txt"))
	if err != nil || !bytes.Equal(got, upload) {
		t.Fatalf("uploaded file = %q, err=%v", got, err)
	}
	if meta, ok := host.index.FileMeta("uploaded.txt"); !ok || meta.SHA256 != sha256Hex(upload) {
		t.Fatalf("indexed upload = %+v, ok=%v", meta, ok)
	}

	// --- Delete ---
	deleteURL := fmt.Sprintf("%s/api/file?path=uploaded.txt&deleted_at=%d", base, time.Now().UTC().UnixMilli())
	resp, body = syncRequest(t, http.MethodDelete, deleteURL, token, nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete = %d: %s", resp.StatusCode, body)
	}
	if _, err := os.Stat(filepath.Join(hostDir, "uploaded.txt")); !os.IsNotExist(err) {
		t.Fatalf("delete should remove the file, stat err=%v", err)
	}
	if _, ok := host.index.Snapshot().Tombstones["uploaded.txt"]; !ok {
		t.Fatal("delete should record a tombstone")
	}

	// Deleting an already-missing file is idempotent.
	if resp, _ := syncRequest(t, http.MethodDelete, deleteURL, token, nil, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("second delete = %d; want 200", resp.StatusCode)
	}
}

func TestServerDeletionToggle(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		t.Fatal(err)
	}
	mustWriteFile(t, filepath.Join(hostDir, "keep.txt"), []byte("keep"))
	const token = "st_no_delete"
	host := startTestHost(t, hostDir, token)

	cfg := host.Config()
	cfg.SyncDeletions = false
	host.UpdateConfig(cfg)

	base := hostBaseURL(t, host)
	deleteURL := fmt.Sprintf("%s/api/file?path=keep.txt&deleted_at=%d", base, time.Now().UTC().UnixMilli())
	resp, _ := syncRequest(t, http.MethodDelete, deleteURL, token, nil, nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete with SyncDeletions=false = %d; want 204", resp.StatusCode)
	}
	if _, err := os.Stat(filepath.Join(hostDir, "keep.txt")); err != nil {
		t.Fatalf("file must remain when deletions are disabled: %v", err)
	}
	if _, ok := host.index.Snapshot().Tombstones["keep.txt"]; ok {
		t.Fatal("no tombstone should be recorded when deletions are disabled")
	}
}

func TestBroadcastRescanWhenSubscriberFull(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	engine := NewEngine(config.Default(), filepath.Join(tmp, "state.json"), nil)
	defer engine.Stop()
	srv := NewServer(engine)

	sub := &subscriber{
		events: make(chan SyncEvent, 1),
		rescan: make(chan struct{}, 1),
		label:  "test-peer",
	}
	sub.events <- SyncEvent{Type: EventUpsert}
	srv.subsMu.Lock()
	srv.subs[sub.events] = sub
	srv.subsMu.Unlock()
	if got := srv.ConnectedPeers(); got != 1 {
		t.Fatalf("connected peers = %d; want 1", got)
	}

	srv.Broadcast(SyncEvent{Type: EventUpsert, File: &FileMeta{RelPath: "x.txt"}})
	select {
	case <-sub.rescan:
	case <-time.After(time.Second):
		t.Fatal("a full subscriber should receive a rescan signal")
	}
}

func TestSSEStreamAuthAndDelivery(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		t.Fatal(err)
	}
	const token = "st_sse_token"
	host := startTestHost(t, hostDir, token)
	base := hostBaseURL(t, host)

	if resp, _ := syncRequest(t, http.MethodGet, base+"/api/events", "", nil, nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("SSE without token = %d; want 401", resp.StatusCode)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-SyncT-Token", token)
	req.Header.Set("X-SyncT-Node", "test-sse-node")
	req.Header.Set("Accept", "text/event-stream")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("SSE connect: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("SSE status = %d", resp.StatusCode)
	}

	lines := make(chan string, 32)
	go func() {
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			select {
			case lines <- scanner.Text():
			case <-ctx.Done():
				return
			}
		}
		close(lines)
	}()

	waitFor(t, 5*time.Second, "SSE handshake", func() bool {
		select {
		case line := <-lines:
			if strings.Contains(line, "connected") {
				return true
			}
			return false
		case <-time.After(50 * time.Millisecond):
			return false
		}
	})
	waitFor(t, 5*time.Second, "host registers SSE subscriber", func() bool {
		return host.Stats().ConnectedPeers > 0
	})

	host.server.Broadcast(SyncEvent{
		Type:       EventUpsert,
		OriginNode: "other-node",
		File:       &FileMeta{RelPath: "sse-event.txt", Size: 5, SHA256: "abc"},
	})
	deadline := time.After(5 * time.Second)
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatal("SSE stream closed unexpectedly")
			}
			if strings.Contains(line, "sse-event.txt") {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for broadcast SSE event")
		}
	}
}
