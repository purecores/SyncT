package syncer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"synct/internal/config"
)

func TestRealTimeSyncWatcher(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	clientDir := filepath.Join(tmp, "client")
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(clientDir, 0o755); err != nil {
		t.Fatal(err)
	}

	const token = "st_realtime_token"
	host := startTestHost(t, hostDir, token)

	clientCfg := config.Default()
	clientCfg.Role = config.RoleClient
	clientCfg.SyncDir = clientDir
	clientCfg.RemoteURL = hostBaseURL(t, host)
	clientCfg.AuthToken = token
	clientCfg.SyncDirection = config.SyncBidirectional
	clientCfg.LocalPort = 0
	client := NewEngine(clientCfg, filepath.Join(tmp, "client_state.json"), nil)
	if err := client.Start(); err != nil {
		t.Fatalf("client Start: %v", err)
	}
	defer client.Stop()

	waitFor(t, 15*time.Second, "client connected with an active SSE stream", func() bool {
		return client.Stats().RemoteConnected && host.Stats().ConnectedPeers > 0
	})

	// Host -> Client, through watcher, incremental scan, SSE and download.
	hostFile := filepath.Join(hostDir, "realtime.txt")
	mustWriteFile(t, hostFile, []byte("hello realtime"))
	propagationStart := time.Now()
	waitFor(t, 15*time.Second, "realtime file on client", func() bool {
		data, err := os.ReadFile(filepath.Join(clientDir, "realtime.txt"))
		return err == nil && string(data) == "hello realtime"
	})
	t.Logf("host -> client propagation latency: %s", time.Since(propagationStart).Round(time.Millisecond))

	// Client -> Host, through watcher, scan and streamed upload.
	mustWriteFile(t, filepath.Join(clientDir, "from_client.txt"), []byte("client wrote this"))
	propagationStart = time.Now()
	waitFor(t, 15*time.Second, "client file on host", func() bool {
		data, err := os.ReadFile(filepath.Join(hostDir, "from_client.txt"))
		return err == nil && string(data) == "client wrote this"
	})
	t.Logf("client -> host propagation latency: %s", time.Since(propagationStart).Round(time.Millisecond))

	// Deletion propagates through a tombstone over SSE.
	if err := os.Remove(hostFile); err != nil {
		t.Fatal(err)
	}
	propagationStart = time.Now()
	waitFor(t, 15*time.Second, "realtime file removed on client", func() bool {
		_, err := os.Stat(filepath.Join(clientDir, "realtime.txt"))
		return os.IsNotExist(err)
	})
	t.Logf("deletion propagation latency: %s", time.Since(propagationStart).Round(time.Millisecond))
}

func TestDynamicFolderWatcherAfterConfigUpdate(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	mainDir := filepath.Join(tmp, "main")
	secondDir := filepath.Join(tmp, "second")
	if err := os.MkdirAll(mainDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(secondDir, 0o755); err != nil {
		t.Fatal(err)
	}

	host := startTestHost(t, mainDir, "st_dynamic_watch")
	cfg := host.Config()
	cfg.UpsertFolder(config.FolderMapping{
		ID:        "second",
		Label:     "第二目录",
		LocalPath: secondDir,
		Enabled:   true,
	})
	host.UpdateConfig(cfg)

	waitFor(t, 10*time.Second, "watcher attached to newly added folder", func() bool {
		for _, entry := range host.Logs() {
			if strings.Contains(entry.Message, "已启用目录实时监控: "+secondDir) {
				return true
			}
		}
		return false
	})

	mustWriteFile(t, filepath.Join(secondDir, "late.txt"), []byte("added later"))
	waitFor(t, 10*time.Second, "file in newly watched folder indexed", func() bool {
		meta, ok := host.index.FileMeta("@second/late.txt")
		return ok && meta.SHA256 != ""
	})
}

func TestStreamingLargeFileTransfer(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	clientDir := filepath.Join(tmp, "client")
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(clientDir, 0o755); err != nil {
		t.Fatal(err)
	}

	hostPayload := bytes.Repeat([]byte("0123456789abcdef"), (6*1024*1024)/16)
	clientPayload := bytes.Repeat([]byte("fedcba9876543210"), (2*1024*1024)/16)
	mustWriteFile(t, filepath.Join(hostDir, "large_host.bin"), hostPayload)
	mustWriteFile(t, filepath.Join(clientDir, "large_client.bin"), clientPayload)

	const token = "st_stream_token"
	host := startTestHost(t, hostDir, token)

	clientCfg := config.Default()
	clientCfg.Role = config.RoleClient
	clientCfg.SyncDir = clientDir
	clientCfg.RemoteURL = hostBaseURL(t, host)
	clientCfg.AuthToken = token
	client := NewEngine(clientCfg, filepath.Join(tmp, "client_state.json"), nil)
	defer client.Stop()
	if err := client.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatalf("ReconcileWithRemote: %v", err)
	}

	gotHost, err := os.ReadFile(filepath.Join(clientDir, "large_host.bin"))
	if err != nil {
		t.Fatalf("read downloaded large file: %v", err)
	}
	if sha256Hex(gotHost) != sha256Hex(hostPayload) {
		t.Fatal("downloaded large file hash mismatch")
	}
	gotClient, err := os.ReadFile(filepath.Join(hostDir, "large_client.bin"))
	if err != nil {
		t.Fatalf("read uploaded large file: %v", err)
	}
	if sha256Hex(gotClient) != sha256Hex(clientPayload) {
		t.Fatal("uploaded large file hash mismatch")
	}
	st := client.Stats()
	if st.BytesDownloaded < int64(len(hostPayload)) || st.BytesUploaded < int64(len(clientPayload)) {
		t.Fatalf("transfer counters = up %d / down %d", st.BytesUploaded, st.BytesDownloaded)
	}
}

func TestAuthMismatchAndUnreachableRemote(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	clientDir := filepath.Join(tmp, "client")
	for _, dir := range []string{hostDir, clientDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	host := startTestHost(t, hostDir, "st_correct_token")

	badTokenCfg := config.Default()
	badTokenCfg.Role = config.RoleClient
	badTokenCfg.SyncDir = clientDir
	badTokenCfg.RemoteURL = hostBaseURL(t, host)
	badTokenCfg.AuthToken = "st_wrong_token"
	badTokenEngine := NewEngine(badTokenCfg, filepath.Join(tmp, "bad_state.json"), nil)
	defer badTokenEngine.Stop()
	err := badTokenEngine.ReconcileWithRemote(context.Background())
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("reconcile with wrong token error = %v; want 401", err)
	}

	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadPort := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	unreachableCfg := config.Default()
	unreachableCfg.Role = config.RoleClient
	unreachableCfg.SyncDir = clientDir
	unreachableCfg.RemoteURL = fmt.Sprintf("http://127.0.0.1:%d", deadPort)
	unreachableCfg.AuthToken = "st_correct_token"
	unreachable := NewEngine(unreachableCfg, filepath.Join(tmp, "unreachable_state.json"), nil)
	defer unreachable.Stop()
	if err := unreachable.ReconcileWithRemote(context.Background()); err == nil {
		t.Fatal("expected an error for an unreachable remote endpoint")
	}
}

func TestSyncDirections(t *testing.T) {
	t.Run("push_only", func(t *testing.T) {
		tmp := t.TempDir()
		t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
		hostDir := filepath.Join(tmp, "host")
		clientDir := filepath.Join(tmp, "client")
		for _, dir := range []string{hostDir, clientDir} {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
		}
		mustWriteFile(t, filepath.Join(hostDir, "host_only.txt"), []byte("host"))
		mustWriteFile(t, filepath.Join(clientDir, "client_only.txt"), []byte("client"))

		const token = "st_push_only"
		host := startTestHost(t, hostDir, token)
		clientCfg := config.Default()
		clientCfg.Role = config.RoleClient
		clientCfg.SyncDir = clientDir
		clientCfg.RemoteURL = hostBaseURL(t, host)
		clientCfg.AuthToken = token
		clientCfg.SyncDirection = config.SyncPushOnly
		client := NewEngine(clientCfg, filepath.Join(tmp, "client_state.json"), nil)
		defer client.Stop()
		if err := client.ReconcileWithRemote(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(hostDir, "client_only.txt")); err != nil {
			t.Fatalf("push_only should upload client file: %v", err)
		}
		if _, err := os.Stat(filepath.Join(clientDir, "host_only.txt")); !os.IsNotExist(err) {
			t.Fatalf("push_only must not download host file, stat err=%v", err)
		}
	})

	t.Run("pull_only", func(t *testing.T) {
		tmp := t.TempDir()
		t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
		hostDir := filepath.Join(tmp, "host")
		clientDir := filepath.Join(tmp, "client")
		for _, dir := range []string{hostDir, clientDir} {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
		}
		mustWriteFile(t, filepath.Join(hostDir, "host_only.txt"), []byte("host"))
		mustWriteFile(t, filepath.Join(clientDir, "client_only.txt"), []byte("client"))

		const token = "st_pull_only"
		host := startTestHost(t, hostDir, token)
		clientCfg := config.Default()
		clientCfg.Role = config.RoleClient
		clientCfg.SyncDir = clientDir
		clientCfg.RemoteURL = hostBaseURL(t, host)
		clientCfg.AuthToken = token
		clientCfg.SyncDirection = config.SyncPullOnly
		client := NewEngine(clientCfg, filepath.Join(tmp, "client_state.json"), nil)
		defer client.Stop()
		if err := client.ReconcileWithRemote(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(clientDir, "host_only.txt")); err != nil {
			t.Fatalf("pull_only should download host file: %v", err)
		}
		if _, err := os.Stat(filepath.Join(hostDir, "client_only.txt")); !os.IsNotExist(err) {
			t.Fatalf("pull_only must not upload client file, stat err=%v", err)
		}
	})
}

func TestIgnoredFilesNeverSync(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	clientDir := filepath.Join(tmp, "client")
	for _, dir := range []string{hostDir, clientDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	mustWriteFile(t, filepath.Join(hostDir, "keep.txt"), []byte("keep"))
	mustWriteFile(t, filepath.Join(hostDir, "node_modules", "pkg.js"), []byte("js"))
	mustWriteFile(t, filepath.Join(hostDir, ".git", "config"), []byte("git"))
	mustWriteFile(t, filepath.Join(hostDir, "temp.tmp"), []byte("tmp"))

	const token = "st_ignore"
	host := startTestHost(t, hostDir, token)
	clientCfg := config.Default()
	clientCfg.Role = config.RoleClient
	clientCfg.SyncDir = clientDir
	clientCfg.RemoteURL = hostBaseURL(t, host)
	clientCfg.AuthToken = token
	client := NewEngine(clientCfg, filepath.Join(tmp, "client_state.json"), nil)
	defer client.Stop()
	if err := client.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(filepath.Join(clientDir, "keep.txt")); err != nil {
		t.Fatalf("keep.txt should be synced: %v", err)
	}
	for _, rel := range []string{"node_modules/pkg.js", ".git/config", "temp.tmp"} {
		if _, err := os.Stat(filepath.Join(clientDir, filepath.FromSlash(rel))); !os.IsNotExist(err) {
			t.Errorf("%s must not be synced, stat err=%v", rel, err)
		}
	}
}

func TestManualScanWhileStopped(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	dir := filepath.Join(tmp, "sync")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.SyncDir = dir
	engine := NewEngine(cfg, filepath.Join(tmp, "state.json"), nil)
	defer engine.Stop()

	mustWriteFile(t, filepath.Join(dir, "after_start.txt"), []byte("manual"))
	engine.TriggerManualSync()
	waitFor(t, 10*time.Second, "manual scan while stopped indexes the file", func() bool {
		_, ok := engine.index.FileMeta("after_start.txt")
		return ok
	})
}

func TestStartDoesNotRecreateVanishedIndexedFolder(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	dir := filepath.Join(tmp, "vanished")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	mustWriteFile(t, filepath.Join(dir, "important.txt"), []byte("keep"))
	cfg := config.Default()
	cfg.Role = config.RoleHost
	cfg.TunnelMode = config.TunnelDirect
	cfg.SyncDir = dir
	cfg.LocalPort = 0
	cfg.AuthToken = "st_vanished"
	engine := NewEngine(cfg, filepath.Join(tmp, "state.json"), nil)
	if count, _ := engine.index.Counts(); count != 1 {
		t.Fatalf("initial count = %d; want 1", count)
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := engine.Start(); err != nil {
		t.Fatalf("Start should tolerate a temporarily unavailable indexed folder: %v", err)
	}
	defer engine.Stop()

	if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
		t.Fatalf("Start must not recreate a vanished indexed folder, stat err=%v", statErr)
	}
	if count, _ := engine.index.Counts(); count != 1 {
		t.Fatalf("count after Start = %d; index must be preserved", count)
	}
	if tombs := engine.index.Snapshot().Tombstones; len(tombs) != 0 {
		t.Fatalf("tombstones = %v; want none", tombs)
	}
}

func TestStatsAndSpeedDecay(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	cfg := config.Default()
	cfg.SyncDir = filepath.Join(tmp, "sync")
	engine := NewEngine(cfg, filepath.Join(tmp, "state.json"), nil)
	defer engine.Stop()

	engine.index.MarkFileSynced(FileMeta{
		RelPath: "x.txt",
		Size:    1234,
		ModTime: time.Now().UTC().UnixMilli(),
		SHA256:  "abc",
	}, StatusSynced, "本地")
	engine.recordUpload("x.txt", 4096, 10*time.Millisecond)
	engine.recordDownload("x.txt", 2048, 10*time.Millisecond)
	engine.countUploadBytes(4096)
	engine.countDownloadBytes(2048)

	st := engine.Stats()
	if st.TotalFiles != 1 || st.TotalBytes != 1234 {
		t.Fatalf("stats totals = %d files / %d bytes", st.TotalFiles, st.TotalBytes)
	}
	if st.UploadSpeed <= 0 || st.DownloadSpeed <= 0 {
		t.Fatalf("speeds = up %d / down %d; want positive right after transfer", st.UploadSpeed, st.DownloadSpeed)
	}
	if st.FolderCount < 1 {
		t.Fatalf("folder count = %d", st.FolderCount)
	}

	engine.lastUploadAt.Store(time.Now().Add(-time.Minute).UnixNano())
	engine.lastDownloadAt.Store(time.Now().Add(-time.Minute).UnixNano())
	st = engine.Stats()
	if st.UploadSpeed != 0 || st.DownloadSpeed != 0 {
		t.Fatalf("speeds should decay to 0 after idle, got up %d / down %d", st.UploadSpeed, st.DownloadSpeed)
	}

	engine.setScanFailure(errors.New("scan boom"))
	if st = engine.Stats(); st.LastScanError == "" || st.ScanErrors == 0 {
		t.Fatalf("scan failure not surfaced: %+v", st)
	}
	engine.setScanSuccess()
	if st = engine.Stats(); st.LastScanError != "" {
		t.Fatalf("scan error should clear on success, got %q", st.LastScanError)
	}
}

func TestLogThrottleAndCapacity(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	cfg := config.Default()
	cfg.SyncDir = filepath.Join(tmp, "sync")
	engine := NewEngine(cfg, filepath.Join(tmp, "state.json"), nil)
	defer engine.Stop()

	before := engine.LogCount()
	engine.LogThrottled("dup-key", "WARN", "same message", time.Minute)
	engine.LogThrottled("dup-key", "WARN", "same message", time.Minute)
	if got := engine.LogCount(); got != before+1 {
		t.Fatalf("throttled logs = %d entries; want %d", got, before+1)
	}

	for i := 0; i < 500; i++ {
		engine.LogLevel("INFO", fmt.Sprintf("line %d", i))
	}
	if got := engine.LogCount(); got != maxLogEntries {
		t.Fatalf("log count = %d; want cap %d", got, maxLogEntries)
	}
	logs := engine.Logs()
	if len(logs) != maxLogEntries {
		t.Fatalf("logs copy = %d; want %d", len(logs), maxLogEntries)
	}
	for i := 1; i < len(logs); i++ {
		if logs[i].ID <= logs[i-1].ID {
			t.Fatalf("log IDs not ascending at %d: %d <= %d", i, logs[i].ID, logs[i-1].ID)
		}
	}

	engine.ClearLogs()
	if engine.LogCount() != 0 {
		t.Fatal("ClearLogs should empty the buffer")
	}
}

func TestImportFilesErrorReporting(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	dir := filepath.Join(tmp, "sync")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.SyncDir = dir
	engine := NewEngine(cfg, filepath.Join(tmp, "state.json"), nil)
	defer engine.Stop()

	good := filepath.Join(tmp, "dropped.txt")
	mustWriteFile(t, good, []byte("dropped"))
	missing := filepath.Join(tmp, "does-not-exist.txt")

	imported, err := engine.ImportFiles([]string{good, missing})
	if imported != 1 {
		t.Fatalf("imported = %d; want 1", imported)
	}
	if err != nil {
		t.Fatalf("partial import should not fail: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(dir, "dropped.txt")); err != nil || string(data) != "dropped" {
		t.Fatalf("imported file = %q, err=%v", string(data), err)
	}
	// The engine is stopped, but the dropped file should still appear in the
	// index immediately (the manual-scan fallback runs when no scan loop exists).
	waitFor(t, 5*time.Second, "imported file indexed while stopped", func() bool {
		_, ok := engine.index.FileMeta("dropped.txt")
		return ok
	})

	if imported, err = engine.ImportFiles([]string{missing}); imported != 0 || err == nil {
		t.Fatalf("all-failed import = %d, err=%v; want 0, non-nil", imported, err)
	}
	if imported, err = engine.ImportFiles([]string{tmp}); imported != 0 || err == nil {
		t.Fatalf("directory import = %d, err=%v; want 0, non-nil", imported, err)
	}
}

func TestRemoteEventsDoNotClobberNewerLocalFiles(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	dir := filepath.Join(tmp, "sync")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "guard.txt")
	mustWriteFile(t, target, []byte("local newer content"))
	future := time.Now().Add(10 * time.Second)
	if err := os.Chtimes(target, future, future); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.SyncDir = dir
	engine := NewEngine(cfg, filepath.Join(tmp, "state.json"), nil)
	defer engine.Stop()

	meta, ok := engine.index.FileMeta("guard.txt")
	if !ok {
		t.Fatal("guard.txt should be indexed")
	}

	// An older remote deletion must not remove a newer local edit.
	pending := map[string]SyncEvent{
		"guard.txt": {
			Type:      EventDelete,
			Tombstone: &Tombstone{RelPath: "guard.txt", DeletedAt: meta.ModTime - 5000},
		},
	}
	if !engine.processRemoteEventsLocked(context.Background(), pending) {
		t.Fatal("guarding a newer local file should request a reconciliation")
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("newer local file was deleted: %v", err)
	}

	// An older remote upsert must not overwrite a newer local file.
	pending = map[string]SyncEvent{
		"guard.txt": {
			Type: EventUpsert,
			File: &FileMeta{RelPath: "guard.txt", Size: 1, ModTime: meta.ModTime - 5000, SHA256: "older"},
		},
	}
	if !engine.processRemoteEventsLocked(context.Background(), pending) {
		t.Fatal("guarding a newer local file should request a reconciliation")
	}
	if data, err := os.ReadFile(target); err != nil || string(data) != "local newer content" {
		t.Fatalf("newer local file was overwritten: %q, err=%v", string(data), err)
	}
	if got, _ := engine.index.FileMeta("guard.txt"); got.SHA256 != meta.SHA256 {
		t.Fatalf("index hash changed from %q to %q", meta.SHA256, got.SHA256)
	}

	// A newer local deletion must not be undone by a stale remote upsert.
	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	deletedAt := time.Now().Add(30 * time.Second).UTC().UnixMilli()
	engine.index.MarkFileDeleted("guard.txt", deletedAt)
	pending = map[string]SyncEvent{
		"guard.txt": {
			Type: EventUpsert,
			File: &FileMeta{RelPath: "guard.txt", Size: 1, ModTime: meta.ModTime - 5000, SHA256: "older"},
		},
	}
	if !engine.processRemoteEventsLocked(context.Background(), pending) {
		t.Fatal("a stale upsert against a newer tombstone should request reconciliation")
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("stale upsert resurrected a deleted file, stat err=%v", err)
	}
	if _, ok := engine.index.Tombstone("guard.txt"); !ok {
		t.Fatal("newer tombstone should survive a stale upsert")
	}
}

func TestUpdateConfigWhileStoppedRefreshesIndex(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	mainDir := filepath.Join(tmp, "main")
	extraDir := filepath.Join(tmp, "extra")
	for _, dir := range []string{mainDir, extraDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cfg := config.Default()
	cfg.SyncDir = mainDir
	engine := NewEngine(cfg, filepath.Join(tmp, "state.json"), nil)
	defer engine.Stop()

	mustWriteFile(t, filepath.Join(extraDir, "extra.txt"), []byte("extra"))
	updated := engine.Config()
	updated.UpsertFolder(config.FolderMapping{ID: "extra", LocalPath: extraDir, Enabled: true})
	engine.UpdateConfig(updated)
	waitFor(t, 5*time.Second, "new folder indexed while stopped", func() bool {
		_, ok := engine.index.FileMeta("@extra/extra.txt")
		return ok
	})
}

func TestApplyPeerFolderMappings(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	cfg := config.Default()
	cfg.SyncDir = filepath.Join(tmp, "sync")
	target := filepath.Join(tmp, "peer-target")
	engine := NewEngine(cfg, filepath.Join(tmp, "state.json"), nil)
	defer engine.Stop()

	original := engine.Config()
	changed := engine.ApplyPeerFolderMappings([]config.FolderMapping{{
		ID:         "peer",
		Label:      "对端目录",
		LocalPath:  "/remote/path",
		RemotePath: target,
		Enabled:    true,
	}})
	if !changed {
		t.Fatal("expected mappings to change")
	}
	got := engine.Config()
	if _, ok := got.FindFolder("peer"); !ok {
		t.Fatal("peer mapping not applied")
	}
	if _, ok := original.FindFolder("peer"); ok {
		t.Fatal("Config() must return a deep copy, not the engine's live slices")
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("peer target directory should be created: %v", err)
	}
	if engine.ApplyPeerFolderMappings([]config.FolderMapping{{
		ID:         "peer",
		Label:      "对端目录",
		LocalPath:  "/remote/path",
		RemotePath: target,
		Enabled:    true,
	}}) {
		t.Fatal("re-applying identical mappings should report no change")
	}
}
