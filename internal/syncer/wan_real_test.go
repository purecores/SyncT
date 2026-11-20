package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"synct/internal/config"
)

// TestRealTunnelTransfer runs a genuine transfer against the running host
// behind the Cloudflare tunnel. It is manual because it needs a live host and
// its pairing key:
//
//	SYNCT_REAL_CONFIG=C:\SyncT-Host\data\config.json \
//	go test ./internal/syncer -run TestRealTunnelTransfer -v -timeout 30m
//
// It pushes one uniquely named probe file and checks the host's copy byte for
// byte, then removes it again. It never downloads the host's folders.
func TestRealTunnelTransfer(t *testing.T) {
	hostCfgPath := os.Getenv("SYNCT_REAL_CONFIG")
	publicURL := config.DefaultPublicURL
	password := os.Getenv("SYNCT_REAL_PASSWORD")
	if hostCfgPath == "" && password == "" {
		t.Skip("set SYNCT_REAL_CONFIG to the running host's data/config.json, or SYNCT_REAL_PASSWORD with SYNCT_REAL_TUNNEL")
	}

	var token, hostSyncDir string
	if hostCfgPath != "" {
		raw, err := os.ReadFile(hostCfgPath)
		if err != nil {
			t.Fatalf("read host config: %v", err)
		}
		var hc config.Config
		if err := json.Unmarshal(raw, &hc); err != nil {
			t.Fatalf("parse host config: %v", err)
		}
		hc.Normalize()
		token = hc.AuthToken
		hostSyncDir = hc.SyncDir
		if hc.CustomPublicURL != "" {
			publicURL = hc.CustomPublicURL
		}
	} else {
		token = config.DeriveToken(password)
	}
	if v := os.Getenv("SYNCT_REAL_TUNNEL"); v != "" {
		publicURL = v
	}
	if token == "" || hostSyncDir == "" {
		t.Fatalf("host config has no token or sync dir")
	}
	sizeMB := 16
	if v := os.Getenv("SYNCT_REAL_SIZE_MB"); v != "" {
		fmt.Sscanf(v, "%d", &sizeMB)
	}

	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	clientDir := filepath.Join(tmp, "client")
	if err := os.MkdirAll(clientDir, 0o755); err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("synct-real-probe-%d.bin", time.Now().Unix())
	size := int64(sizeMB) << 20
	makeLargeFile(t, filepath.Join(clientDir, name), size)
	wantHash, err := HashFile(filepath.Join(clientDir, name))
	if err != nil {
		t.Fatal(err)
	}
	hostPath := filepath.Join(hostSyncDir, name)
	t.Cleanup(func() {
		_ = os.Remove(hostPath)
	})

	cfg := config.Default()
	cfg.Role = config.RoleClient
	cfg.SyncDir = clientDir
	cfg.RemoteURL = publicURL
	cfg.AuthToken = token
	// Push-only keeps the host's real folders out of this client; the probe
	// is the only file that moves.
	cfg.SyncDirection = config.SyncPushOnly
	cfg.AutoStartSync = false
	client := NewEngine(cfg, filepath.Join(tmp, "client_state.json"), nil)
	defer client.Stop()

	t.Logf("tunnel %s -> host folder %s", publicURL, hostSyncDir)
	start := time.Now()
	if err := client.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	elapsed := time.Since(start)
	mbps := float64(size) / (1024 * 1024) / elapsed.Seconds()
	t.Logf("REAL WAN RESULT: %d MB uploaded in %s (%.2f MB/s) as %s", sizeMB, elapsed.Round(100*time.Millisecond), mbps, name)

	for _, entry := range client.Logs() {
		if entry.Level == "WARN" || entry.Level == "ERROR" {
			t.Logf("%s: %s", entry.Level, entry.Message)
		}
	}
	gotHash, err := HashFile(hostPath)
	if err != nil {
		t.Fatalf("host did not receive the probe: %v", err)
	}
	if gotHash != wantHash {
		t.Fatalf("host probe hash %s != source %s", gotHash, wantHash)
	}
	if info, err := os.Stat(hostPath); err != nil || info.Size() != size {
		t.Fatalf("host probe size mismatch: %v", err)
	}
	for _, f := range client.Files() {
		if f.RelPath == name && f.Status != StatusSynced {
			t.Fatalf("probe status = %s; want 已同步", f.Status)
		}
	}
	t.Logf("host copy verified by SHA-256: %s", gotHash)
}
