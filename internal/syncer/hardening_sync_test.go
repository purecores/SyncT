package syncer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"synct/internal/config"
)

func TestApplyPeerFolderMappings_RejectsExternalAndOverwriteByDefault(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))

	syncDir := filepath.Join(tmp, "sync")
	if err := os.MkdirAll(syncDir, 0o755); err != nil {
		t.Fatal(err)
	}
	outsideTarget := filepath.Join(tmp, "outside-evil")
	insideTarget := filepath.Join(syncDir, "nested-peer")

	cfg := config.Default()
	cfg.SyncDir = syncDir
	// AllowPeerRemotePath defaults to false.
	engine := NewEngine(cfg, filepath.Join(tmp, "state.json"), nil)
	defer engine.Stop()

	// 1. Peer tries to overwrite default folder LocalPath and map a new folder outside SyncDir.
	changed := engine.ApplyPeerFolderMappings([]config.FolderMapping{
		{
			ID:         config.DefaultFolderID,
			Label:      "主目录",
			LocalPath:  "/remote/main",
			RemotePath: outsideTarget,
			Enabled:    true,
		},
		{
			ID:         "ext",
			Label:      "外部目录",
			LocalPath:  "/remote/ext",
			RemotePath: outsideTarget,
			Enabled:    true,
		},
		{
			ID:         "inside",
			Label:      "内部目录",
			LocalPath:  "/remote/inside",
			RemotePath: insideTarget,
			Enabled:    true,
		},
	})
	if !changed {
		t.Fatal("expected new folders to be added")
	}

	got := engine.Config()
	defFolder, ok := got.FindFolder(config.DefaultFolderID)
	if !ok || filepath.Clean(defFolder.LocalPath) != filepath.Clean(syncDir) {
		t.Fatalf("existing default folder path must not be overwritten, got %+v", defFolder)
	}
	extFolder, ok := got.FindFolder("ext")
	if !ok {
		t.Fatal("expected ext folder to be mapped under SyncDir fallback")
	}
	wantFallback := filepath.Join(syncDir, "ext")
	if filepath.Clean(extFolder.LocalPath) != filepath.Clean(wantFallback) {
		t.Fatalf("expected external RemotePath to fall back to %q, got %q", wantFallback, extFolder.LocalPath)
	}
	if _, err := os.Stat(outsideTarget); !os.IsNotExist(err) {
		t.Fatalf("outside directory %q must not be created when AllowPeerRemotePath is false", outsideTarget)
	}
	inFolder, ok := got.FindFolder("inside")
	if !ok || filepath.Clean(inFolder.LocalPath) != filepath.Clean(insideTarget) {
		t.Fatalf("RemotePath inside SyncDir should be accepted, got %+v", inFolder)
	}

	// 2. Even with AllowPeerRemotePath=true, traversal, relative paths, roots, and overwriting existing folders are rejected.
	optInCfg := engine.Config()
	optInCfg.AllowPeerRemotePath = true
	engine.UpdateConfig(optInCfg)

	traversalPath := tmp + string(os.PathSeparator) + ".." + string(os.PathSeparator) + "escape"
	if _, ok := validatePeerRemotePath(syncDir, traversalPath, true); ok {
		t.Fatal("expected path with '..' segment to be rejected")
	}
	if _, ok := validatePeerRemotePath(syncDir, "relative/dir", true); ok {
		t.Fatal("expected relative path to be rejected")
	}
	rootPath := filepath.VolumeName(syncDir) + string(os.PathSeparator)
	if _, ok := validatePeerRemotePath(syncDir, rootPath, true); ok {
		t.Fatalf("expected filesystem root %q to be rejected", rootPath)
	}
	// Attempting to overwrite existing "ext" folder with a different path must still be ignored.
	engine.ApplyPeerFolderMappings([]config.FolderMapping{{
		ID:         "ext",
		Label:      "外部目录",
		LocalPath:  "/remote/ext",
		RemotePath: outsideTarget,
		Enabled:    true,
	}})
	gotAfter := engine.Config()
	extAfter, _ := gotAfter.FindFolder("ext")
	if filepath.Clean(extAfter.LocalPath) != filepath.Clean(wantFallback) {
		t.Fatalf("existing folder mapping must never be overwritten even with AllowPeerRemotePath, got %q", extAfter.LocalPath)
	}
}

func TestPushOnlyNodeRefusesPeerFolderMappings(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	syncDir := filepath.Join(tmp, "sync")
	if err := os.MkdirAll(syncDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.SyncDir = syncDir
	cfg.SyncDirection = config.SyncPushOnly
	engine := NewEngine(cfg, filepath.Join(tmp, "state.json"), nil)
	defer engine.Stop()

	changed := engine.ApplyPeerFolderMappings([]config.FolderMapping{{
		ID:        "peer-added",
		Label:     "对端新增",
		LocalPath: "/remote/peer-added",
		Enabled:   true,
	}})
	if changed {
		t.Fatal("push-only node must not accept folder mappings from the peer")
	}
	if _, ok := engine.Config().FindFolder("peer-added"); ok {
		t.Fatal("push-only node adopted a peer folder mapping")
	}
	if _, err := os.Stat(filepath.Join(syncDir, "peer-added")); !os.IsNotExist(err) {
		t.Fatal("push-only node created a peer folder directory")
	}
}

func TestEmptyDirDeletionPropagatesWithoutResurrection(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))

	hostDir := filepath.Join(tmp, "host")
	clientDir := filepath.Join(tmp, "client")
	for _, d := range []string{hostDir, clientDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	// Create an empty nested folder and a folder that will stay non-empty on host.
	if err := os.MkdirAll(filepath.Join(hostDir, "empty_parent", "empty_child"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(hostDir, "keep_dir"), 0o755); err != nil {
		t.Fatal(err)
	}

	token := "st_dir_tombstone_test"
	hostCfg := config.Default()
	hostCfg.Role = config.RoleHost
	hostCfg.TunnelMode = config.TunnelDirect
	hostCfg.SyncDir = hostDir
	hostCfg.LocalPort = 0
	hostCfg.AuthToken = token
	hostCfg.SyncDeletions = true

	hostEngine := NewEngine(hostCfg, filepath.Join(tmp, "host_state.json"), nil)
	if err := hostEngine.Start(); err != nil {
		t.Fatalf("hostEngine.Start failed: %v", err)
	}
	defer hostEngine.Stop()

	clientCfg := config.Default()
	clientCfg.Role = config.RoleClient
	clientCfg.SyncDir = clientDir
	clientCfg.RemoteURL = fmt.Sprintf("https://localhost:%d", hostEngine.Stats().LocalPort)
	clientCfg.AuthToken = token
	clientCfg.SyncDeletions = true

	clientEngine := NewEngine(clientCfg, filepath.Join(tmp, "client_state.json"), nil)
	defer clientEngine.Stop()

	// Step 1: Initial reconcile copies empty_parent/empty_child and keep_dir to client.
	if err := clientEngine.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatalf("initial reconcile failed: %v", err)
	}
	for _, rel := range []string{filepath.Join("empty_parent", "empty_child"), "keep_dir"} {
		if info, err := os.Stat(filepath.Join(clientDir, rel)); err != nil || !info.IsDir() {
			t.Fatalf("expected directory %s on client after initial reconcile, err=%v", rel, err)
		}
	}

	// Step 2: Client deletes empty_parent/empty_child and empty_parent, then reconciles.
	time.Sleep(15 * time.Millisecond)
	if err := os.RemoveAll(filepath.Join(clientDir, "empty_parent")); err != nil {
		t.Fatal(err)
	}
	if err := clientEngine.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatalf("reconcile after client dir delete failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(hostDir, "empty_parent")); !os.IsNotExist(err) {
		t.Fatalf("expected host empty_parent to be deleted, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(clientDir, "empty_parent")); !os.IsNotExist(err) {
		t.Fatalf("expected client empty_parent to stay deleted, stat err=%v", err)
	}

	// Step 3: Another reconcile must NOT resurrect the deleted empty folder.
	if err := clientEngine.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(hostDir, "empty_parent")); !os.IsNotExist(err) {
		t.Fatalf("host empty_parent resurrected after reconcile")
	}
	if _, err := os.Stat(filepath.Join(clientDir, "empty_parent")); !os.IsNotExist(err) {
		t.Fatalf("client empty_parent resurrected after reconcile")
	}

	// Step 4: Host deletes keep_dir while client has a file inside keep_dir ->
	// client must NOT force-delete the non-empty directory.
	if err := os.WriteFile(filepath.Join(clientDir, "keep_dir", "important.txt"), []byte("do not delete"), 0o644); err != nil {
		t.Fatal(err)
	}
	time.Sleep(15 * time.Millisecond)
	if err := os.Remove(filepath.Join(hostDir, "keep_dir")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := hostEngine.index.ScanFoldersIncremental(hostEngine.Config().EnabledFolders(), hostEngine.Config().IgnorePatterns); err != nil {
		t.Fatal(err)
	}
	if err := clientEngine.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatalf("reconcile with non-empty dir failed: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(clientDir, "keep_dir", "important.txt")); err != nil || string(got) != "do not delete" {
		t.Fatalf("non-empty directory file was lost or corrupted: %q (%v)", string(got), err)
	}

	// Step 5: Recreating empty_parent on client clears tombstone and propagates to host.
	time.Sleep(15 * time.Millisecond)
	if err := os.MkdirAll(filepath.Join(clientDir, "empty_parent"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := clientEngine.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatalf("reconcile after dir recreation failed: %v", err)
	}
	if info, err := os.Stat(filepath.Join(hostDir, "empty_parent")); err != nil || !info.IsDir() {
		t.Fatalf("expected recreated empty_parent to propagate to host, err=%v", err)
	}
}

func TestConcurrentEditPreservesConflictUsingSyncedHash(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))

	hostDir := filepath.Join(tmp, "host")
	clientDir := filepath.Join(tmp, "client")
	for _, d := range []string{hostDir, clientDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	baseTime := time.Now().Add(-10 * time.Second).Truncate(time.Second)
	hostFile := filepath.Join(hostDir, "doc.txt")
	if err := os.WriteFile(hostFile, []byte("v1 common ancestor"), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = os.Chtimes(hostFile, baseTime, baseTime)

	token := "st_conflict_base_hash"
	hostCfg := config.Default()
	hostCfg.Role = config.RoleHost
	hostCfg.TunnelMode = config.TunnelDirect
	hostCfg.SyncDir = hostDir
	hostCfg.LocalPort = 0
	hostCfg.AuthToken = token
	hostCfg.ConflictPolicy = config.ConflictKeepBoth

	hostEngine := NewEngine(hostCfg, filepath.Join(tmp, "host_state.json"), nil)
	if err := hostEngine.Start(); err != nil {
		t.Fatalf("hostEngine.Start failed: %v", err)
	}
	defer hostEngine.Stop()

	clientCfg := config.Default()
	clientCfg.Role = config.RoleClient
	clientCfg.SyncDir = clientDir
	clientCfg.RemoteURL = fmt.Sprintf("https://localhost:%d", hostEngine.Stats().LocalPort)
	clientCfg.AuthToken = token
	clientCfg.ConflictPolicy = config.ConflictKeepBoth

	clientEngine := NewEngine(clientCfg, filepath.Join(tmp, "client_state.json"), nil)
	defer clientEngine.Stop()

	// Initial sync establishes common ancestor hash in client's SyncedHash.
	if err := clientEngine.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatalf("initial reconcile failed: %v", err)
	}
	if clientEngine.index.SyncedHash("doc.txt") == "" {
		t.Fatal("expected SyncedHash to be recorded after initial sync")
	}

	// Both sides edit doc.txt independently; client has newer mtime so client pushes.
	hostEditTime := baseTime.Add(3 * time.Second)
	clientEditTime := baseTime.Add(6 * time.Second)

	if err := os.WriteFile(hostFile, []byte("v2 host independent edit"), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = os.Chtimes(hostFile, hostEditTime, hostEditTime)
	if _, _, err := hostEngine.index.ScanFoldersIncremental(hostEngine.Config().EnabledFolders(), hostEngine.Config().IgnorePatterns); err != nil {
		t.Fatal(err)
	}

	clientFile := filepath.Join(clientDir, "doc.txt")
	if err := os.WriteFile(clientFile, []byte("v2 client newer edit"), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = os.Chtimes(clientFile, clientEditTime, clientEditTime)

	if err := clientEngine.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatalf("reconcile after concurrent edit failed: %v", err)
	}

	// Host should have the client's newer version in doc.txt AND a preserved conflict copy of host's edit.
	gotHost, err := os.ReadFile(hostFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotHost) != "v2 client newer edit" {
		t.Fatalf("expected host doc.txt to have client's newer edit, got %q", string(gotHost))
	}

	entries, err := os.ReadDir(hostDir)
	if err != nil {
		t.Fatal(err)
	}
	foundConflict := false
	for _, ent := range entries {
		if strings.Contains(ent.Name(), ".conflict-") {
			data, err := os.ReadFile(filepath.Join(hostDir, ent.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if string(data) == "v2 host independent edit" {
				foundConflict = true
				break
			}
		}
	}
	if !foundConflict {
		t.Fatalf("expected conflict copy preserving host edit in %s; entries=%v", hostDir, entries)
	}
}

func TestResumableRangeDownloadRecoversFromInterruption(t *testing.T) {
	origThreshold := resumableDownloadThreshold
	origChunkSize := downloadChunkSize
	resumableDownloadThreshold = 1024
	downloadChunkSize = 1024
	t.Cleanup(func() {
		resumableDownloadThreshold = origThreshold
		downloadChunkSize = origChunkSize
	})

	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	clientDir := filepath.Join(tmp, "client")
	if err := os.MkdirAll(clientDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// 3.5 KiB payload across multiple Range chunks.
	payload := bytes.Repeat([]byte("SyncT-Resumable-Block-0123456789"), 112)
	sum := sha256.Sum256(payload)
	wantHash := hex.EncodeToString(sum[:])

	var (
		mu             sync.Mutex
		rangeHeaders   []string
		interruptedOne bool
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/file" {
			http.NotFound(w, r)
			return
		}
		rng := r.Header.Get("Range")
		mu.Lock()
		rangeHeaders = append(rangeHeaders, rng)
		doInterrupt := !interruptedOne && len(rangeHeaders) == 2
		if doInterrupt {
			interruptedOne = true
		}
		mu.Unlock()

		if doInterrupt {
			// Serve only half of the requested chunk and cut off the connection to simulate a mid-stream drop.
			start := int64(1024)
			end := int64(2047)
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
			w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(payload[start : start+300])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			// Returning early with fewer bytes than Content-Length triggers io.ErrUnexpectedEOF on the client.
			return
		}

		http.ServeContent(w, r, "big.bin", time.Now(), bytes.NewReader(payload))
	}))
	defer srv.Close()

	cfg := config.Default()
	cfg.Role = config.RoleClient
	cfg.SyncDir = clientDir
	cfg.RemoteURL = srv.URL
	cfg.AuthToken = "st_range_test"

	engine := NewEngine(cfg, filepath.Join(tmp, "state.json"), nil)
	defer engine.Stop()

	rem := FileMeta{
		FolderID: config.DefaultFolderID,
		RelPath:  "big.bin",
		Size:     int64(len(payload)),
		ModTime:  time.Now().UTC().UnixMilli(),
		SHA256:   wantHash,
	}

	if err := engine.pullFileFromRemote(context.Background(), engine.Config(), rem, false); err != nil {
		t.Fatalf("resumable pullFileFromRemote failed: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(clientDir, "big.bin"))
	if err != nil {
		t.Fatalf("read downloaded big.bin: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("downloaded payload mismatch: got %d bytes, want %d bytes", len(got), len(payload))
	}

	// Partial file must be cleaned up / renamed into place.
	partPath := partFilePath(clientDir, "big.bin", wantHash)
	if _, err := os.Stat(partPath); !os.IsNotExist(err) {
		t.Fatalf("expected part file %s to be removed after completion", partPath)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(rangeHeaders) < 4 {
		t.Fatalf("expected multiple Range requests including resume after interruption, got %v", rangeHeaders)
	}
	// After the 2nd request wrote 300 bytes starting at 1024, the 3rd request must resume at byte 1324!
	if rangeHeaders[2] != "bytes=1324-2347" {
		t.Fatalf("expected resume Range header bytes=1324-2347 after partial write, got %v", rangeHeaders)
	}
}
