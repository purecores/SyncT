package tunnel

import (
	"testing"

	"synct/internal/config"
)

func TestDirectModeStartAndStop(t *testing.T) {
	t.Setenv("SYNCT_DATA_DIR", t.TempDir())

	var updates int
	var readyURL string
	mgr := NewManager("", func() {
		updates++
	}, func(url string) {
		readyURL = url
	})

	cfg := config.Default()
	cfg.TunnelMode = config.TunnelDirect
	if err := mgr.Start(cfg, 18765); err != nil {
		t.Fatalf("mgr.Start in Direct mode failed: %v", err)
	}

	state, pubURL, lanURL, lastErr := mgr.State()
	if state != TunnelDirectOn {
		t.Fatalf("expected TunnelDirectOn, got %s (err=%s)", state, lastErr)
	}
	if pubURL == "" || lanURL == "" || pubURL != lanURL {
		t.Fatalf("expected non-empty matching pubURL and lanURL in direct mode, got pub=%q lan=%q", pubURL, lanURL)
	}
	if readyURL != lanURL {
		t.Fatalf("expected onReady callback with %q, got %q", lanURL, readyURL)
	}

	mgr.Stop()
	state, _, _, _ = mgr.State()
	if state != TunnelStopped {
		t.Fatalf("expected TunnelStopped after Stop(), got %s", state)
	}
}

func TestTryCloudflareURLRegex(t *testing.T) {
	line := "2026-11-15T08:00:00Z INF |  https://amber-sky-8492.trycloudflare.com  |"
	got := tryCloudflareRegex.FindString(line)
	want := "https://amber-sky-8492.trycloudflare.com"
	if got != want {
		t.Fatalf("tryCloudflareRegex.FindString(%q) = %q, want %q", line, got, want)
	}
}

func TestSystemTunnelModeReusesRunningCloudflared(t *testing.T) {
	t.Setenv("SYNCT_DATA_DIR", t.TempDir())

	sysInfo := DetectSystemCloudflared()
	t.Logf("DetectSystemCloudflared: running=%v pids=%v summary=%q", sysInfo.Running, sysInfo.PIDs, sysInfo.Summary)

	var readyURL string
	mgr := NewManager("", nil, func(url string) {
		readyURL = url
	})

	cfg := config.Default()
	cfg.TunnelMode = config.TunnelSystem
	if err := mgr.Start(cfg, config.DefaultLocalPort); err != nil {
		t.Fatalf("mgr.Start in TunnelSystem mode failed: %v", err)
	}
	defer mgr.Stop()

	state, pubURL, lanURL, lastErr := mgr.State()
	if state != TunnelRunning {
		t.Fatalf("expected TunnelRunning in TunnelSystem mode, got %s (err=%s)", state, lastErr)
	}
	if pubURL != config.DefaultPublicURL {
		t.Fatalf("expected pubURL=%q, got %q", config.DefaultPublicURL, pubURL)
	}
	if lanURL != config.DefaultLocalListenURL {
		t.Fatalf("expected lanURL=%q, got %q", config.DefaultLocalListenURL, lanURL)
	}
	if readyURL != config.DefaultPublicURL {
		t.Fatalf("expected readyURL=%q, got %q", config.DefaultPublicURL, readyURL)
	}
}

