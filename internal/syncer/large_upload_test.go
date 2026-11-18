package syncer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"synct/internal/config"
)

// A file above the tunnel's per-request cap must reach the host through the
// chunked path and be byte-identical, indexed, and free of staging leftovers.
func TestLargeFileUploadsThroughChunks(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		t.Fatal(err)
	}
	token := "st_large_chunk_token"
	host := startTestHost(t, hostDir, token)
	port := host.Stats().LocalPort

	clientDir := filepath.Join(tmp, "client")
	if err := os.MkdirAll(filepath.Join(clientDir, "big"), 0o755); err != nil {
		t.Fatal(err)
	}
	size := int64(2*maxUploadChunkBytes + 777)
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	rel := "big/archive.bin"
	if err := os.WriteFile(filepath.Join(clientDir, filepath.FromSlash(rel)), payload, 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.Role = config.RoleClient
	cfg.SyncDir = clientDir
	cfg.RemoteURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	cfg.AuthToken = token
	client := NewEngine(cfg, filepath.Join(tmp, "client_state.json"), nil)
	defer client.Stop()

	if err := client.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(hostDir, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("host did not receive the file: %v", err)
	}
	if len(got) != len(payload) {
		t.Fatalf("host size = %d; want %d", len(got), len(payload))
	}
	wantSum := sha256.Sum256(payload)
	gotSum := sha256.Sum256(got)
	if hex.EncodeToString(wantSum[:]) != hex.EncodeToString(gotSum[:]) {
		t.Fatal("host content differs from the source")
	}
	if _, ok := host.index.FileMeta(rel); !ok {
		t.Fatal("uploaded file is not indexed on the host")
	}
	entries, _ := os.ReadDir(filepath.Join(hostDir, "big"))
	for _, e := range entries {
		if e.Name() != "archive.bin" {
			t.Fatalf("leftover entry in destination: %s", e.Name())
		}
	}
	waitFor(t, 5*time.Second, "client to mark the upload synced", func() bool {
		files, _ := client.FilesWithVersion()
		for _, f := range files {
			if f.RelPath == rel {
				return f.Status == StatusSynced
			}
		}
		return false
	})
}
