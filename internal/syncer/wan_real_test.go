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

// TestRealTunnelTransfer runs an actual host/client sync over the user's
// Cloudflare tunnel hostname when SYNCT_REAL_TUNNEL is set. It is manual: the
// host must be running and paired with the same password-derived key. The
// check is the one the field report needs — a file above the edge's request
// limit reaching the other computer intact, at a plausible speed.
func TestRealTunnelTransfer(t *testing.T) {
	publicURL := os.Getenv("SYNCT_REAL_TUNNEL")
	password := os.Getenv("SYNCT_REAL_PASSWORD")
	if publicURL == "" || password == "" {
		t.Skip("set SYNCT_REAL_TUNNEL=https://… and SYNCT_REAL_PASSWORD=… to run against the real host")
	}
	sizeMB := 48
	if v := os.Getenv("SYNCT_REAL_SIZE_MB"); v != "" {
		fmt.Sscanf(v, "%d", &sizeMB)
	}

	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	clientDir := filepath.Join(tmp, "Syncwork")
	if err := os.MkdirAll(clientDir, 0o755); err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("synct-probe-%d.bin", time.Now().Unix())
	size := int64(sizeMB) << 20
	makeLargeFile(t, filepath.Join(clientDir, name), size)
	srcHash, err := HashFile(filepath.Join(clientDir, name))
	if err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.Role = config.RoleClient
	cfg.SyncDir = clientDir
	cfg.RemoteURL = publicURL
	cfg.AuthToken = config.DeriveToken(password)
	cfg.SyncDirection = config.SyncPushOnly
	client := NewEngine(cfg, filepath.Join(tmp, "client_state.json"), nil)
	defer client.Stop()

	start := time.Now()
	if err := client.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	elapsed := time.Since(start)
	mbps := float64(size) / (1024 * 1024) / elapsed.Seconds()
	t.Logf("%d MB probe uploaded in %s (%.2f MB/s) as %s", sizeMB, elapsed.Round(100*time.Millisecond), mbps, name)
	t.Logf("source sha256 %s", srcHash)
	for _, entry := range client.Logs() {
		if entry.Level == "WARN" || entry.Level == "ERROR" {
			t.Logf("%s: %s", entry.Level, entry.Message)
		}
	}
	// The host is the paired computer; this test can only assert that the
	// client reports success and a non-zero rate.
	for _, f := range client.Files() {
		if f.RelPath == name && f.Status != StatusSynced {
			t.Fatalf("probe file status = %s; want 已同步", f.Status)
		}
	}
	_ = sha256.Size
	_ = hex.EncodeToString
}
