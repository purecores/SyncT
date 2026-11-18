package syncer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"synct/internal/config"
)

// A file restored from a backup or copied in from an archive keeps its old
// modification time. A peer deletion recorded after that mtime must not erase
// it on the next reconciliation: the local restore is newer than the delete.
func TestRestoredFileResurrectsInsteadOfBeingDeleted(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	clientDir := filepath.Join(tmp, "client")
	for _, d := range []string{hostDir, clientDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	rel := "restore.txt"
	mustWriteFile(t, filepath.Join(hostDir, rel), []byte("original content"))

	token := "st_restore_token"
	host := startTestHost(t, hostDir, token)
	port := host.Stats().LocalPort

	cfg := config.Default()
	cfg.Role = config.RoleClient
	cfg.SyncDir = clientDir
	cfg.RemoteURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	cfg.AuthToken = token
	client := NewEngine(cfg, filepath.Join(tmp, "client_state.json"), nil)
	defer client.Stop()

	if err := client.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(clientDir, rel)); err != nil {
		t.Fatalf("initial pull failed: %v", err)
	}

	// Delete on the client; the host removes its copy and keeps a tombstone.
	if err := os.Remove(filepath.Join(clientDir, rel)); err != nil {
		t.Fatal(err)
	}
	if err := client.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(hostDir, rel)); !os.IsNotExist(err) {
		t.Fatalf("host copy should be deleted, stat err=%v", err)
	}

	// Restore the file with a deliberately old timestamp, as a backup tool would.
	old := time.Now().Add(-48 * time.Hour)
	path := filepath.Join(clientDir, rel)
	mustWriteFile(t, path, []byte("restored from backup"))
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}

	if err := client.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "restored from backup" {
		t.Fatalf("restored file was lost: %q, %v", got, err)
	}
	hostGot, err := os.ReadFile(filepath.Join(hostDir, rel))
	if err != nil || string(hostGot) != "restored from backup" {
		t.Fatalf("restore did not propagate to host: %q, %v", hostGot, err)
	}
}
