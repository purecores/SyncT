package syncer

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"synct/internal/config"
)

// The first body is corrupted on the wire; the retry must succeed without the
// user ever seeing a failed sync for that file.
func TestDownloadRetriesOnceAfterDigestMismatch(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	dir := filepath.Join(tmp, "sync")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	payload := []byte("content that must arrive intact")
	sum := sha256Hex(payload)

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/manifest":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"node_id":"peer","files":{"doc.txt":{"rel_path":"doc.txt","size":%d,"mod_time":1,"sha256":"%s"}},"tombstones":{}}`, len(payload), sum)
		case "/api/file":
			n := hits.Add(1)
			w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
			if n == 1 {
				// First attempt: same length, different bytes.
				corrupt := append([]byte(nil), payload...)
				corrupt[0] ^= 0xFF
				w.Write(corrupt)
				return
			}
			w.Write(payload)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	cfg := config.Default()
	cfg.Role = config.RoleClient
	cfg.SyncDir = dir
	cfg.RemoteURL = srv.URL
	cfg.AuthToken = "st_retry_hash"
	engine := NewEngine(cfg, filepath.Join(tmp, "state.json"), nil)
	defer engine.Stop()

	if err := engine.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "doc.txt"))
	if err != nil || string(got) != string(payload) {
		t.Fatalf("file = %q, %v", got, err)
	}
	if hits.Load() != 2 {
		t.Fatalf("download attempts = %d; want exactly one retry", hits.Load())
	}
	for _, entry := range engine.Logs() {
		if entry.Level == "WARN" && strings.Contains(entry.Message, "哈希校验失败") {
			t.Fatalf("a successful retry still logged a failure: %s", entry.Message)
		}
	}
	if f, ok := engine.index.FileMeta("doc.txt"); !ok || f.SHA256 != sum {
		t.Fatalf("indexed digest = %+v", f)
	}
}
