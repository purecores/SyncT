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

// Stopping the engine while many downloads are in flight must not write one
// warning per file: the work PC log showed 101 such lines from a single stop,
// burying the real errors.
func TestStopDuringTransfersStaysQuiet(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	clientDir := filepath.Join(tmp, "client")
	if err := os.MkdirAll(clientDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		sub := filepath.Join(hostDir, fmt.Sprintf("d%02d", i%4))
		if err := os.MkdirAll(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		mustWriteFile(t, filepath.Join(sub, fmt.Sprintf("f%03d.bin", i)), []byte(strings.Repeat("x", 512*1024)))
	}
	token := "st_noise_token"
	host := startTestHost(t, hostDir, token)
	port := host.Stats().LocalPort

	cfg := config.Default()
	cfg.Role = config.RoleClient
	cfg.SyncDir = clientDir
	cfg.RemoteURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	cfg.AuthToken = token
	client := NewEngine(cfg, filepath.Join(tmp, "client_state.json"), nil)
	if err := client.Start(); err != nil {
		t.Fatal(err)
	}
	// Let the initial reconcile start moving files, then stop mid-flight.
	waitFor(t, 10*time.Second, "transfers to start", func() bool { return client.Stats().DownloadSpeed > 0 })
	client.Stop()

	time.Sleep(300 * time.Millisecond)
	canceled := 0
	for _, entry := range client.Logs() {
		if strings.Contains(entry.Message, "context canceled") {
			canceled++
		}
	}
	if canceled > 0 {
		t.Fatalf("stop logged %d canceled-transfer warnings; want none", canceled)
	}
	// The touched files must be pending, not failed: a stop is not an error.
	for _, f := range client.Files() {
		if f.Status == StatusError {
			t.Fatalf("%s marked 错误 after a stop", f.RelPath)
		}
	}
}

func TestCanceledTransferClassified(t *testing.T) {
	if !transferCanceled(context.Background(), context.Canceled) {
		t.Fatal("context.Canceled should classify as a stop")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !transferCanceled(ctx, fmt.Errorf("wrapped: %w", context.Canceled)) {
		t.Fatal("a canceled context should classify as a stop")
	}
}
