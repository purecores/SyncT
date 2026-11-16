package syncer

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"synct/internal/config"
)

func TestRemoteStatusErrorExplains502(t *testing.T) {
	err := remoteStatusError("获取远端清单", http.StatusBadGateway, "error code: 502")
	if err == nil {
		t.Fatal("expected an error")
	}
	msg := err.Error()
	for _, want := range []string{"502", "源站不可达", "18920"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error = %q; want %q in it", msg, want)
		}
	}
	plain := remoteStatusError("上传文件", http.StatusInternalServerError, "")
	if !strings.Contains(plain.Error(), "500") {
		t.Fatalf("plain error = %q", plain.Error())
	}
}

func TestServerRefusesSilentPortFallback(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	dir := filepath.Join(tmp, "dir")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	busy := startTestHost(t, dir, "st_busy")
	busyPort := busy.Stats().LocalPort

	cfg := config.Default()
	cfg.Role = config.RoleHost
	cfg.TunnelMode = config.TunnelDirect
	cfg.SyncDir = dir
	cfg.LocalPort = busyPort
	cfg.AuthToken = "st_busy"
	second := NewEngine(cfg, filepath.Join(tmp, "second_state.json"), nil)
	defer second.Stop()

	err := second.Start()
	if err == nil {
		t.Fatal("Start must fail when the configured port is occupied instead of binding a random port")
	}
	if !strings.Contains(err.Error(), "502") {
		t.Fatalf("error = %v; want an explanation tying the busy port to the public 502", err)
	}
}

func TestOriginHealthLoopRebindsDeadListener(t *testing.T) {
	oldLocal, oldPublic := hostHealthInterval, publicProbeInterval
	hostHealthInterval = 100 * time.Millisecond
	publicProbeInterval = time.Hour
	t.Cleanup(func() {
		hostHealthInterval = oldLocal
		publicProbeInterval = oldPublic
	})

	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	dir := filepath.Join(tmp, "dir")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	host := startTestHost(t, dir, "st_health")
	port := host.Stats().LocalPort
	baseURL := hostBaseURL(t, host)
	token := host.Config().AuthToken
	statusOf := func() int {
		req, _ := http.NewRequest(http.MethodGet, baseURL+"/api/status", nil)
		req.Header.Set("X-SyncT-Token", token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	if got := statusOf(); got != http.StatusOK {
		t.Fatalf("initial status = %d", got)
	}

	// Simulate the origin dying the way a closed accept loop does: the
	// listener is gone, but the engine believes it is still running.
	host.server.lifeMu.Lock()
	listener := host.server.listener
	host.server.listener = nil
	host.server.lifeMu.Unlock()
	host.server.serving.Store(false)
	if listener != nil {
		_ = listener.Close()
	}

	waitFor(t, 10*time.Second, "origin rebound on the same port", func() bool {
		return statusOf() == http.StatusOK && host.server.IsServing()
	})
	if got := host.Stats().LocalPort; got != port {
		t.Fatalf("port after rebound = %d; want %d", got, port)
	}
}

func TestProbeLocalReportsHealthyOrigin(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	dir := filepath.Join(tmp, "dir")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	host := startTestHost(t, dir, "st_probe")
	if !host.probeLocal(host.Stats().LocalPort) {
		t.Fatal("probeLocal should report a healthy origin")
	}
	if host.probeLocal(1) {
		t.Fatal("probeLocal should report a closed port as unhealthy")
	}
}

func TestClientLoopJitterIsBounded(t *testing.T) {
	for _, backoff := range []time.Duration{0, time.Second, 2 * time.Second, 15 * time.Second} {
		for i := 0; i < 50; i++ {
			d := reconnectDelay(backoff)
			if d < backoff || d > backoff+backoff/2 {
				t.Fatalf("reconnectDelay(%s) = %s; want within [%s, %s]", backoff, d, backoff, backoff+backoff/2)
			}
		}
	}
}

func TestServerRestartKeepsServing(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	dir := filepath.Join(tmp, "dir")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	host := startTestHost(t, dir, "st_restart")
	port := host.Stats().LocalPort
	if err := host.server.Restart(); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if !host.server.IsServing() {
		t.Fatal("server should be serving after Restart")
	}
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/api/status", port), nil)
	req.Header.Set("X-SyncT-Token", host.Config().AuthToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("status after restart: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status after restart = %d", resp.StatusCode)
	}
}
