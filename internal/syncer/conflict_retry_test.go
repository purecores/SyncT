package syncer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"synct/internal/config"
)

// Retrying a download whose local copy is already indexed and unchanged must
// not produce a second conflict file. Only genuinely new local content is
// backed up, once.
func TestConflictCopyOnlyForNewLocalContent(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	clientDir := filepath.Join(tmp, "client")
	for _, d := range []string{hostDir, clientDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	mustWriteFile(t, filepath.Join(hostDir, "doc.txt"), []byte("host version, much newer"))
	mustWriteFile(t, filepath.Join(clientDir, "doc.txt"), []byte("client version"))

	token := "st_conflict_retry"
	host := startTestHost(t, hostDir, token)
	port := host.Stats().LocalPort

	cfg := config.Default()
	cfg.Role = config.RoleClient
	cfg.SyncDir = clientDir
	cfg.RemoteURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	cfg.AuthToken = token
	client := NewEngine(cfg, filepath.Join(tmp, "client_state.json"), nil)
	defer client.Stop()

	// Make the host copy newer so the pull overwrites ours.
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(filepath.Join(hostDir, "doc.txt"), future, future); err != nil {
		t.Fatal(err)
	}
	host.index.ScanFoldersIncremental(host.Config().EnabledFolders(), host.Config().IgnorePatterns)
	if err := client.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatal(err)
	}
	conflicts := countConflictFiles(t, clientDir)
	if conflicts != 1 {
		t.Fatalf("conflict files after first pull = %d; want 1", conflicts)
	}
	// Simulate an interrupted retry: the local copy is unchanged and indexed.
	got, err := os.ReadFile(filepath.Join(clientDir, "doc.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), "host version") {
		t.Fatalf("client copy = %q", got)
	}
	// Force another pull by changing the host file while keeping our index.
	mustWriteFile(t, filepath.Join(hostDir, "doc.txt"), []byte("host version, even newer"))
	host.index.ScanFoldersIncremental(host.Config().EnabledFolders(), host.Config().IgnorePatterns)
	if err := client.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Our local copy is already indexed and unchanged, so the retry must not
	// back it up again.
	if got := countConflictFiles(t, clientDir); got != 1 {
		t.Fatalf("conflict files after retry = %d; want still 1", got)
	}
}

func countConflictFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if strings.Contains(e.Name(), ".conflict-") {
			n++
		}
	}
	return n
}
