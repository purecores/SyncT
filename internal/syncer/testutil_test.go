package syncer

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"synct/internal/config"
)

func mustWriteFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout after %s waiting for %s", timeout, what)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func startTestHost(t *testing.T, syncDir, token string) *Engine {
	t.Helper()
	cfg := config.Default()
	cfg.Role = config.RoleHost
	cfg.TunnelMode = config.TunnelDirect
	cfg.SyncDir = syncDir
	cfg.LocalPort = 0 // OS-assigned free loopback port
	cfg.AuthToken = token
	cfg.AutoStartSync = false

	engine := NewEngine(cfg, filepath.Join(t.TempDir(), "host_state.json"), nil)
	if err := engine.Start(); err != nil {
		t.Fatalf("host engine Start: %v", err)
	}
	t.Cleanup(engine.Stop)
	return engine
}

func hostBaseURL(t *testing.T, engine *Engine) string {
	t.Helper()
	port := engine.Stats().LocalPort
	if port <= 0 {
		t.Fatalf("host engine has no listening port")
	}
	return "http://127.0.0.1:" + itoa(port)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
