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

// Empty folders carry no files, so they only travel as explicit directory
// records; without this a folder made on one PC would never appear on the other.
func TestEmptyFoldersSyncBetweenPeers(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	clientDir := filepath.Join(tmp, "client")
	if err := os.MkdirAll(filepath.Join(hostDir, "Shared", "Empty", "Nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(clientDir, 0o755); err != nil {
		t.Fatal(err)
	}
	mustWriteFile(t, filepath.Join(hostDir, "hello.txt"), []byte("hello"))

	token := "st_dirs_token"
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
		t.Fatalf("reconcile: %v", err)
	}
	for _, rel := range []string{"Shared", "Shared/Empty", "Shared/Empty/Nested"} {
		info, err := os.Stat(filepath.Join(clientDir, filepath.FromSlash(rel)))
		if err != nil || !info.IsDir() {
			t.Fatalf("empty folder %q missing on client: %v", rel, err)
		}
	}
	if _, err := os.ReadFile(filepath.Join(clientDir, "hello.txt")); err != nil {
		t.Fatalf("hello.txt missing: %v", err)
	}
}

// A folder created on the pushing client reaches the host through the same
// manifest dirs list, independent of the host's own scan.
func TestClientEmptyFolderAppearsOnHost(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	clientDir := filepath.Join(tmp, "client")
	for _, dir := range []string{hostDir, filepath.Join(clientDir, "MadeOnWorkPC", "Deep")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	token := "st_dirs_token_2"
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
		t.Fatalf("reconcile: %v", err)
	}
	waitFor(t, 5*time.Second, "host to create the client's empty folder", func() bool {
		info, err := os.Stat(filepath.Join(hostDir, "MadeOnWorkPC", "Deep"))
		return err == nil && info.IsDir()
	})
	if got := host.index.DirCount(); got == 0 {
		t.Fatal("host index should record directories")
	}
}

// applyDirs is refused in push-only mode: a node that never accepts the peer's
// files must not create the peer's folders either.
func TestPushOnlyNodeRefusesPeerDirs(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	dir := filepath.Join(tmp, "push")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.SyncDir = dir
	cfg.SyncDirection = config.SyncPushOnly
	engine := NewEngine(cfg, filepath.Join(tmp, "state.json"), nil)
	defer engine.Stop()

	if created := engine.ApplyPeerDirs([]DirMeta{{RelPath: "OnlyRemote"}}, "node-x"); created != 0 {
		t.Fatalf("created = %d in push-only mode", created)
	}
	if _, err := os.Stat(filepath.Join(dir, "OnlyRemote")); !os.IsNotExist(err) {
		t.Fatal("push-only node created a peer directory")
	}
}

func TestRateMeterReportsLiveThroughput(t *testing.T) {
	var m rateMeter
	base := time.Unix(1700000000, 0)
	// 1 MB per 100 ms for one second is 10 MB/s across the window.
	for i := 0; i < 10; i++ {
		m.add(100*1024, base.Add(time.Duration(i)*100*time.Millisecond))
	}
	got := m.rate(base.Add(950 * time.Millisecond))
	if got < 900*1024 || got > 1100*1024 {
		t.Fatalf("rate = %d B/s; want about 1 MB/s", got)
	}
	// After the window passes with no new bytes the rate is zero.
	if got := m.rate(base.Add(5 * time.Second)); got != 0 {
		t.Fatalf("stale rate = %d; want 0", got)
	}
}

func TestUnsyncedCountAndPendingStatus(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	dir := filepath.Join(tmp, "sync")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	mustWriteFile(t, filepath.Join(dir, "a.txt"), []byte("a"))

	store := NewIndexStore(filepath.Join(tmp, "state.json"))
	store.SetNewFilesPending(true)
	if _, _, err := store.ScanFoldersIncremental([]config.FolderMapping{{
		ID: config.DefaultFolderID, LocalPath: dir, Enabled: true,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if got := store.UnsyncedCount(); got != 1 {
		t.Fatalf("unsynced = %d; want 1 pending file", got)
	}
	meta, ok := store.FileMeta("a.txt")
	if !ok {
		t.Fatal("a.txt not indexed")
	}
	store.MarkFileSynced(meta, StatusSynced, "本地 -> 远端")
	if got := store.UnsyncedCount(); got != 0 {
		t.Fatalf("unsynced after confirm = %d; want 0", got)
	}
}

func TestManifestCarriesDirsRoundTrip(t *testing.T) {
	m := NewManifest("n1")
	m.Dirs["@work/docs"] = DirMeta{FolderID: "work", RelPath: "@work/docs"}
	clone := m.Clone()
	if _, ok := clone.Dirs["@work/docs"]; !ok {
		t.Fatal("clone lost its directories")
	}
}

// Live propagation exercises the watcher -> scan -> SSE/push path, not only a
// manual reconciliation.
func TestEmptyFolderRealtimePropagation(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	clientDir := filepath.Join(tmp, "client")
	for _, dir := range []string{hostDir, clientDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	token := "st_realtime_dirs"
	host := startTestHost(t, hostDir, token)
	port := host.Stats().LocalPort

	cfg := config.Default()
	cfg.Role = config.RoleClient
	cfg.SyncDir = clientDir
	cfg.RemoteURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	cfg.AuthToken = token
	cfg.AutoStartSync = false
	client := NewEngine(cfg, filepath.Join(tmp, "client_state.json"), nil)
	if err := client.Start(); err != nil {
		t.Fatalf("client start: %v", err)
	}
	defer client.Stop()
	waitFor(t, 15*time.Second, "client connection", func() bool { return client.Stats().RemoteConnected })

	if err := os.MkdirAll(filepath.Join(hostDir, "FromHost", "Empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 15*time.Second, "host folder on client", func() bool {
		info, err := os.Stat(filepath.Join(clientDir, "FromHost", "Empty"))
		return err == nil && info.IsDir()
	})

	if err := os.MkdirAll(filepath.Join(clientDir, "FromClient", "Empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 15*time.Second, "client folder on host", func() bool {
		info, err := os.Stat(filepath.Join(hostDir, "FromClient", "Empty"))
		return err == nil && info.IsDir()
	})
}

func TestParallelScanHashesCoversEveryIndex(t *testing.T) {
	const n = 257
	hits := make([]int, n)
	parallelScanHashes(n, func(i int) { hits[i]++ })
	for i, h := range hits {
		if h != 1 {
			t.Fatalf("index %d ran %d times; want exactly once", i, h)
		}
	}
}
