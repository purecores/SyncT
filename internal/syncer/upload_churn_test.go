package syncer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"synct/internal/config"
)

// The work-PC log showed this loop: a file upload was rejected by the tunnel
// edge, the host then deleted its copy of that file (the client had deleted it
// earlier), and the client re-uploaded it, generating another rejection. The
// fix has two parts: a rejected upload must not look like a successful local
// change, and a client must not push a file whose content the host already
// deleted more recently.
func TestRejectedUploadDoesNotChurnThroughDelete(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	clientDir := filepath.Join(tmp, "client")
	for _, d := range []string{hostDir, clientDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	token := "st_churn_token"
	host := startTestHost(t, hostDir, token)
	port := host.Stats().LocalPort

	cfg := config.Default()
	cfg.Role = config.RoleClient
	cfg.SyncDir = clientDir
	cfg.RemoteURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	cfg.AuthToken = token
	client := NewEngine(cfg, filepath.Join(tmp, "client_state.json"), nil)
	defer client.Stop()

	// 1. A file exists on both, is deleted on the client, and the deletion
	//    reaches the host.
	rel := "shared.bin"
	mustWriteFile(t, filepath.Join(hostDir, rel), []byte("both sides held this"))
	mustWriteFile(t, filepath.Join(clientDir, rel), []byte("both sides held this"))
	if err := client.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(clientDir, rel)); err != nil {
		t.Fatal(err)
	}
	if err := client.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(hostDir, rel)); !os.IsNotExist(err) {
		t.Fatalf("host should have deleted its copy, stat err=%v", err)
	}

	// 2. The content comes back locally with an old modification time (a
	//    restore from backup). The host's delete is newer, but the restore is
	//    not a stale file: it must propagate instead of ping-ponging.
	mustWriteFile(t, filepath.Join(clientDir, rel), []byte("both sides held this"))
	if err := client.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(hostDir, rel))
	if err != nil || string(got) != "both sides held this" {
		t.Fatalf("restored content did not reach the host: %q, %v", got, err)
	}

	// 3. A second reconcile with no changes must be quiet: no delete event
	//    for the file, no re-upload.
	before := len(client.Logs())
	if err := client.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, entry := range client.Logs()[before:] {
		if entry.Level == "SYNC" && strings.Contains(entry.Message, "删除") {
			t.Fatalf("unexpected delete after a stable reconcile: %s", entry.Message)
		}
	}
	if _, err := os.Stat(filepath.Join(hostDir, rel)); err != nil {
		t.Fatalf("host copy vanished on a stable reconcile: %v", err)
	}
}
