package tunnel

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"synct/internal/config"
)

func TestLogBufferBounded(t *testing.T) {
	t.Setenv("SYNCT_DATA_DIR", t.TempDir())
	m := NewManager("", nil, nil)

	for i := 0; i < 400; i++ {
		m.appendLog(fmt.Sprintf("line-%03d", i))
	}
	if got := m.LogCount(); got != 300 {
		t.Fatalf("LogCount = %d; want 300", got)
	}
	logs := m.Logs()
	if len(logs) != 300 {
		t.Fatalf("Logs = %d entries; want 300", len(logs))
	}
	if !strings.Contains(logs[0], "line-100") {
		t.Fatalf("oldest kept log = %q; want line-100", logs[0])
	}
	if !strings.Contains(logs[len(logs)-1], "line-399") {
		t.Fatalf("newest log = %q; want line-399", logs[len(logs)-1])
	}

	m.ClearLogs()
	if m.LogCount() != 0 || len(m.Logs()) != 0 {
		t.Fatal("ClearLogs should empty the buffer")
	}
}

func TestMaskTokenArgs(t *testing.T) {
	args := []string{"tunnel", "run", "--token", "secret-token-value", "--no-tls-verify"}
	masked := maskTokenArgs(args)
	if strings.Contains(strings.Join(masked, " "), "secret-token-value") {
		t.Fatalf("token not redacted: %v", masked)
	}
	if !strings.Contains(masked[3], "****") {
		t.Fatalf("masked token = %q; want **** in the middle", masked[3])
	}
	if args[3] != "secret-token-value" {
		t.Fatal("maskTokenArgs must not mutate the input slice")
	}
}

func TestFindBinaryCustomPath(t *testing.T) {
	t.Setenv("SYNCT_DATA_DIR", t.TempDir())
	custom := filepath.Join(t.TempDir(), "cloudflared.exe")
	if err := os.WriteFile(custom, []byte("stub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if got := FindBinary(custom); got != custom {
		t.Fatalf("FindBinary(custom) = %q; want %q", got, custom)
	}
	if got := FindBinary(filepath.Join(t.TempDir(), "missing.exe")); got != "" {
		// A missing custom path falls through to PATH/portable locations. If a
		// real cloudflared happens to be installed, only assert it is not the
		// bogus custom path.
		if strings.EqualFold(got, custom) {
			t.Fatalf("FindBinary(missing) returned the bogus path %q", got)
		}
	}
}

func TestScanPipeQuickTunnelURL(t *testing.T) {
	var readyURL string
	m := &Manager{
		state:   TunnelStarting,
		onReady: func(url string) { readyURL = url },
	}
	cfg := config.Default()
	cfg.TunnelMode = config.TunnelQuick
	output := "2026-11-15T08:00:00Z INF Requesting new quick Tunnel\n" +
		"2026-11-15T08:00:01Z INF |  https://amber-sky-8492.trycloudflare.com  |\n"
	m.scanPipe(context.Background(), strings.NewReader(output), cfg)

	state, pubURL, _, lastErr := m.State()
	if state != TunnelRunning || pubURL != "https://amber-sky-8492.trycloudflare.com" {
		t.Fatalf("state/pubURL = %s / %q (err=%q)", state, pubURL, lastErr)
	}
	if readyURL != pubURL {
		t.Fatalf("onReady got %q; want %q", readyURL, pubURL)
	}
	if m.LogCount() < 2 {
		t.Fatalf("expected output lines to be logged, got %d", m.LogCount())
	}
}

func TestScanPipeTokenTunnelReady(t *testing.T) {
	var readyURL string
	m := &Manager{
		state:   TunnelStarting,
		onReady: func(url string) { readyURL = url },
	}
	cfg := config.Default()
	cfg.TunnelMode = config.TunnelToken
	cfg.CustomPublicURL = config.DefaultPublicURL
	m.scanPipe(context.Background(), strings.NewReader(
		"2026-11-15T08:00:00Z INF Registered tunnel connection connIndex=0\n"), cfg)

	state, pubURL, _, lastErr := m.State()
	if state != TunnelRunning || pubURL != config.DefaultPublicURL {
		t.Fatalf("state/pubURL = %s / %q (err=%q)", state, pubURL, lastErr)
	}
	if readyURL != config.DefaultPublicURL {
		t.Fatalf("onReady got %q", readyURL)
	}
}

func TestStateTransitionsOnStop(t *testing.T) {
	t.Setenv("SYNCT_DATA_DIR", t.TempDir())
	m := NewManager("", func() {}, nil)
	state, _, _, _ := m.State()
	if state != TunnelStopped {
		t.Fatalf("new manager state = %s; want stopped", state)
	}
	m.Stop()
	state, _, _, _ = m.State()
	if state != TunnelStopped {
		t.Fatalf("stopped manager state = %s", state)
	}
}
