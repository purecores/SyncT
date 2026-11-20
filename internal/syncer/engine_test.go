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

func TestBidirectionalSyncAndDeletions(t *testing.T) {
	tmpRoot := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmpRoot, "portable"))

	hostDir := filepath.Join(tmpRoot, "host_folder")
	clientDir := filepath.Join(tmpRoot, "client_folder")
	if err := os.MkdirAll(filepath.Join(hostDir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(clientDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create initial files on Host
	if err := os.WriteFile(filepath.Join(hostDir, "hello.txt"), []byte("hello from host"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hostDir, "sub", "readme.md"), []byte("# Subdir File"), 0o644); err != nil {
		t.Fatal(err)
	}

	token := "st_test_secret_token"

	hostCfg := config.Default()
	hostCfg.Role = config.RoleHost
	hostCfg.TunnelMode = config.TunnelDirect
	hostCfg.SyncDir = hostDir
	hostCfg.LocalPort = 0 // Let OS pick free port
	hostCfg.AuthToken = token

	hostEngine := NewEngine(hostCfg, filepath.Join(tmpRoot, "host_state.json"), nil)
	if err := hostEngine.Start(); err != nil {
		t.Fatalf("hostEngine.Start failed: %v", err)
	}
	defer hostEngine.Stop()

	port := hostEngine.Stats().LocalPort
	if port <= 0 {
		t.Fatalf("expected valid local port, got %d", port)
	}

	// Create initial file on Client before connecting
	if err := os.WriteFile(filepath.Join(clientDir, "client_only.txt"), []byte("created on client"), 0o644); err != nil {
		t.Fatal(err)
	}

	clientCfg := config.Default()
	clientCfg.Role = config.RoleClient
	clientCfg.SyncDir = clientDir
	clientCfg.RemoteURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	clientCfg.AuthToken = token
	clientCfg.SyncDirection = config.SyncBidirectional

	clientEngine := NewEngine(clientCfg, filepath.Join(tmpRoot, "client_state.json"), nil)
	if err := clientEngine.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatalf("ReconcileWithRemote failed: %v", err)
	}

	// Verify Client downloaded Host's files
	gotHello, err := os.ReadFile(filepath.Join(clientDir, "hello.txt"))
	if err != nil || string(gotHello) != "hello from host" {
		t.Fatalf("client hello.txt = %q, err = %v", string(gotHello), err)
	}
	gotSub, err := os.ReadFile(filepath.Join(clientDir, "sub", "readme.md"))
	if err != nil || string(gotSub) != "# Subdir File" {
		t.Fatalf("client sub/readme.md = %q, err = %v", string(gotSub), err)
	}

	// Verify Host received Client's file
	gotClientFile, err := os.ReadFile(filepath.Join(hostDir, "client_only.txt"))
	if err != nil || string(gotClientFile) != "created on client" {
		t.Fatalf("host client_only.txt = %q, err = %v", string(gotClientFile), err)
	}

	// Test conflict backup when both sides modify hello.txt
	time.Sleep(20 * time.Millisecond)
	if err := os.WriteFile(filepath.Join(clientDir, "hello.txt"), []byte("client concurrent edit"), 0o644); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(2 * time.Second)
	if err := os.WriteFile(filepath.Join(hostDir, "hello.txt"), []byte("host newer edit"), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = os.Chtimes(filepath.Join(hostDir, "hello.txt"), future, future)

	_, _, _, _ = hostEngine.index.ScanDirectory(hostDir, hostCfg.IgnorePatterns)
	if err := clientEngine.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatalf("second ReconcileWithRemote failed: %v", err)
	}

	// Client should now have "host newer edit" AND a .conflict- backup file
	updatedHello, err := os.ReadFile(filepath.Join(clientDir, "hello.txt"))
	if err != nil || string(updatedHello) != "host newer edit" {
		t.Fatalf("expected updated hello.txt on client, got %q (%v)", string(updatedHello), err)
	}
	entries, _ := os.ReadDir(clientDir)
	foundConflict := false
	for _, e := range entries {
		if strings.Contains(e.Name(), "hello.conflict-") {
			foundConflict = true
			break
		}
	}
	if !foundConflict {
		t.Errorf("expected conflict backup file in clientDir, got entries %v", entries)
	}

	// Test deletion propagation via tombstone
	if err := os.Remove(filepath.Join(hostDir, "sub", "readme.md")); err != nil {
		t.Fatal(err)
	}
	_, _, _, _ = hostEngine.index.ScanDirectory(hostDir, hostCfg.IgnorePatterns)
	if err := clientEngine.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatalf("deletion ReconcileWithRemote failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(clientDir, "sub", "readme.md")); !os.IsNotExist(err) {
		t.Errorf("expected sub/readme.md to be deleted on client, stat err = %v", err)
	}
}

func TestShareCodeRoundTrip(t *testing.T) {
	origURL := "https://sync.example.com"
	origToken := "st_9f8e7d6c5b4a"
	code := config.EncodeShareCode(origURL, origToken)
	if !strings.HasPrefix(code, "synct://connect/") {
		t.Fatalf("unexpected share code format: %s", code)
	}
	gotURL, gotToken, err := config.DecodeShareCode(code)
	if err != nil {
		t.Fatalf("DecodeShareCode failed: %v", err)
	}
	if gotURL != origURL || gotToken != origToken {
		t.Fatalf("got (%q, %q), want (%q, %q)", gotURL, gotToken, origURL, origToken)
	}

	// Also verify pasting raw st_ token pairs with DefaultPublicURL
	gotURL2, gotTok2, err := config.DecodeShareCode("st_work_pc_token_123")
	if err != nil || gotURL2 != config.DefaultPublicURL || gotTok2 != "st_work_pc_token_123" {
		t.Fatalf("raw token decode = (%q, %q, %v)", gotURL2, gotTok2, err)
	}
}

func TestLocalhostHTTPSAndHTTPDualProtocolSync(t *testing.T) {
	tmpRoot := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmpRoot, "portable"))

	hostDir := filepath.Join(tmpRoot, "host_https")
	clientDir := filepath.Join(tmpRoot, "client_https")
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(clientDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hostDir, "tls_verified.txt"), []byte("synced over https://localhost"), 0o644); err != nil {
		t.Fatal(err)
	}

	hostCfg := config.Default()
	hostCfg.SyncDir = hostDir
	hostCfg.LocalPort = 0
	hostCfg.AuthToken = "st_tls_token"

	hostEngine := NewEngine(hostCfg, filepath.Join(tmpRoot, "host_state.json"), nil)
	if err := hostEngine.Start(); err != nil {
		t.Fatalf("hostEngine.Start failed: %v", err)
	}
	defer hostEngine.Stop()

	port := hostEngine.Stats().LocalPort

	// Connect using https://localhost:<port> (just as cloudflared does for https://localhost:18920)
	clientCfg := config.Default()
	clientCfg.Role = config.RoleClient
	clientCfg.SyncDir = clientDir
	clientCfg.RemoteURL = fmt.Sprintf("https://localhost:%d", port)
	clientCfg.AuthToken = "st_tls_token"

	clientEngine := NewEngine(clientCfg, filepath.Join(tmpRoot, "client_state.json"), nil)
	if err := clientEngine.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatalf("ReconcileWithRemote over https://localhost:%d failed: %v", port, err)
	}

	got, err := os.ReadFile(filepath.Join(clientDir, "tls_verified.txt"))
	if err != nil || string(got) != "synced over https://localhost" {
		t.Fatalf("unexpected file content over HTTPS: %q (%v)", string(got), err)
	}
}

func TestMultiFolderAndCrossTargetMappingSync(t *testing.T) {
	tmpRoot := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmpRoot, "portable"))

	hostMainDir := filepath.Join(tmpRoot, "host_main")
	hostProjectsDir := filepath.Join(tmpRoot, "host_projects")
	clientMainDir := filepath.Join(tmpRoot, "client_main")
	clientProjectsTargetDir := filepath.Join(tmpRoot, "workpc_custom_projects_target")

	for _, d := range []string{hostMainDir, hostProjectsDir, clientMainDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	// Host has a file in default folder and a file in secondary "projects" folder
	// which is configured to map to clientProjectsTargetDir on the other PC.
	if err := os.WriteFile(filepath.Join(hostMainDir, "main.txt"), []byte("main folder file"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hostProjectsDir, "spec.md"), []byte("cross-mapped project spec"), 0o644); err != nil {
		t.Fatal(err)
	}

	token := "st_multi_folder_token"

	hostCfg := config.Default()
	hostCfg.Role = config.RoleHost
	hostCfg.TunnelMode = config.TunnelDirect
	hostCfg.SyncDir = hostMainDir
	hostCfg.LocalPort = 0
	hostCfg.AuthToken = token
	hostCfg.AllowPeerRemotePath = true
	hostCfg.SyncFolders = []config.FolderMapping{
		{
			ID:        config.DefaultFolderID,
			Label:     "主同步目录",
			LocalPath: hostMainDir,
			Enabled:   true,
		},
		{
			ID:         "projects",
			Label:      "项目文档库",
			LocalPath:  hostProjectsDir,
			RemotePath: clientProjectsTargetDir,
			Enabled:    true,
		},
	}

	hostEngine := NewEngine(hostCfg, filepath.Join(tmpRoot, "host_state.json"), nil)
	if err := hostEngine.Start(); err != nil {
		t.Fatalf("hostEngine.Start failed: %v", err)
	}
	defer hostEngine.Stop()

	port := hostEngine.Stats().LocalPort

	// Client starts with only its primary folder; it should automatically learn the "projects"
	// folder mapping with LocalPath = clientProjectsTargetDir from Host's manifest!
	clientCfg := config.Default()
	clientCfg.Role = config.RoleClient
	clientCfg.SyncDir = clientMainDir
	clientCfg.RemoteURL = fmt.Sprintf("https://localhost:%d", port)
	clientCfg.AuthToken = token
	clientCfg.AllowPeerRemotePath = true

	clientEngine := NewEngine(clientCfg, filepath.Join(tmpRoot, "client_state.json"), nil)
	if err := clientEngine.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatalf("client ReconcileWithRemote failed: %v", err)
	}

	// Verify primary folder synced
	gotMain, err := os.ReadFile(filepath.Join(clientMainDir, "main.txt"))
	if err != nil || string(gotMain) != "main folder file" {
		t.Fatalf("expected main.txt in clientMainDir, got %q (%v)", string(gotMain), err)
	}

	// Verify secondary mapped folder automatically synced into clientProjectsTargetDir
	gotSpec, err := os.ReadFile(filepath.Join(clientProjectsTargetDir, "spec.md"))
	if err != nil || string(gotSpec) != "cross-mapped project spec" {
		t.Fatalf("expected spec.md in clientProjectsTargetDir, got %q (%v)", string(gotSpec), err)
	}

	// Now Client adds a new folder mapping ("reports") pointing to a target directory on Host
	clientReportsDir := filepath.Join(tmpRoot, "client_reports")
	hostReportsTargetDir := filepath.Join(tmpRoot, "host_reports_target")
	if err := os.MkdirAll(clientReportsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(clientReportsDir, "weekly.txt"), []byte("weekly report from work pc"), 0o644); err != nil {
		t.Fatal(err)
	}

	updatedClientCfg := clientEngine.Config()
	updatedClientCfg.UpsertFolder(config.FolderMapping{
		ID:         "reports",
		Label:      "周报目录",
		LocalPath:  clientReportsDir,
		RemotePath: hostReportsTargetDir,
		Enabled:    true,
	})
	clientEngine.UpdateConfig(updatedClientCfg)

	if err := clientEngine.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatalf("second ReconcileWithRemote failed: %v", err)
	}

	// Verify Host received weekly.txt in hostReportsTargetDir
	gotWeekly, err := os.ReadFile(filepath.Join(hostReportsTargetDir, "weekly.txt"))
	if err != nil || string(gotWeekly) != "weekly report from work pc" {
		t.Fatalf("expected weekly.txt in hostReportsTargetDir, got %q (%v)", string(gotWeekly), err)
	}
}
