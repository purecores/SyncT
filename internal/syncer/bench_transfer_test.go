package syncer

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"synct/internal/config"
)

type benchResult struct {
	label   string
	files   int
	bytes   int64
	elapsed time.Duration
}

func (r benchResult) rate() string {
	if r.elapsed <= 0 {
		return "-"
	}
	mbps := float64(r.bytes) / (1024 * 1024) / r.elapsed.Seconds()
	fps := float64(r.files) / r.elapsed.Seconds()
	return fmt.Sprintf("%8.1f MB/s  %8.1f files/s  %8d files  %7.1f MB  %s", mbps, fps, r.files, float64(r.bytes)/(1024*1024), r.elapsed.Round(time.Millisecond))
}

func makeManyFiles(t testing.TB, dir string, count, size int) int64 {
	t.Helper()
	var total int64
	payload := make([]byte, size)
	_, _ = rand.Read(payload)
	for i := 0; i < count; i++ {
		sub := filepath.Join(dir, fmt.Sprintf("d%02d", i%24))
		if err := os.MkdirAll(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(sub, fmt.Sprintf("f%05d.bin", i)), payload, 0o644); err != nil {
			t.Fatal(err)
		}
		total += int64(size)
	}
	return total
}

func makeLargeFile(t testing.TB, path string, size int64) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	chunk := make([]byte, 4*1024*1024)
	_, _ = rand.Read(chunk)
	for written := int64(0); written < size; {
		n := int64(len(chunk))
		if size-written < n {
			n = size - written
		}
		if _, err := f.Write(chunk[:n]); err != nil {
			t.Fatal(err)
		}
		written += n
	}
}

func newBenchClient(t testing.TB, root, hostDir, remote string, token string) (*Engine, string) {
	t.Helper()
	clientDir := filepath.Join(root, "client")
	if err := os.MkdirAll(clientDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Role = config.RoleClient
	cfg.SyncDir = clientDir
	cfg.RemoteURL = remote
	cfg.AuthToken = token
	cfg.AutoStartSync = false
	engine := NewEngine(cfg, filepath.Join(root, "client_state.json"), nil)
	return engine, clientDir
}

func TestTransferThroughputReport(t *testing.T) {
	if os.Getenv("SYNCT_BENCH") == "" {
		t.Skip("set SYNCT_BENCH=1 to run the real transfer report")
	}
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		t.Fatal(err)
	}

	token := "st_bench_token"
	old := maxParallelTransfers
	defer func() { maxParallelTransfers = old }()

	const smallCount = 4000
	const smallSize = 4 * 1024
	const largeSize = int64(256 * 1024 * 1024)

	// --- Small files only -------------------------------------------------
	smallHostDir := filepath.Join(tmp, "small-host")
	if err := os.MkdirAll(smallHostDir, 0o755); err != nil {
		t.Fatal(err)
	}
	smallBytes := makeManyFiles(t, filepath.Join(smallHostDir, "many"), smallCount, smallSize)
	start := time.Now()
	smallHost := startTestHost(t, smallHostDir, token)
	t.Logf("host startup scan (%d x 4KB): %s", smallCount, time.Since(start).Round(time.Millisecond))
	smallRemote := fmt.Sprintf("https://localhost:%d", smallHost.Stats().LocalPort)

	runSmall := func(label string, parallel int) {
		root := filepath.Join(tmp, fmt.Sprintf("small-%s-%d", label, parallel))
		if err := os.MkdirAll(root, 0o755); err != nil {
			t.Fatal(err)
		}
		client, clientDir := newBenchClient(t, root, smallHostDir, smallRemote, token)
		defer client.Stop()
		start := time.Now()
		if err := client.ReconcileWithRemote(context.Background()); err != nil {
			t.Fatal(err)
		}
		r := benchResult{label: label, files: smallCount, bytes: smallBytes, elapsed: time.Since(start)}
		t.Logf("%-28s %s", label, r.rate())
		if got, _ := client.index.Counts(); got != smallCount {
			t.Fatalf("%s: client indexed %d files; want %d", label, got, smallCount)
		}
		sample := filepath.Join("many", "d00", "f00000.bin")
		src, _ := os.ReadFile(filepath.Join(smallHostDir, sample))
		dst, _ := os.ReadFile(filepath.Join(clientDir, sample))
		if string(src) != string(dst) {
			t.Fatalf("%s: small sample differs", label)
		}
	}
	for _, n := range []int{1, 4, 8, 16, 64} {
		maxParallelTransfers = n
		runSmall(fmt.Sprintf("pull %d x 4KB, workers=%d", smallCount, n), n)
	}

	// --- One large file ---------------------------------------------------
	largeHostDir := filepath.Join(tmp, "large-host")
	if err := os.MkdirAll(largeHostDir, 0o755); err != nil {
		t.Fatal(err)
	}
	largePath := filepath.Join(largeHostDir, "large.bin")
	makeLargeFile(t, largePath, largeSize)
	start = time.Now()
	largeHost := startTestHost(t, largeHostDir, token)
	t.Logf("host startup scan (256MB): %s", time.Since(start).Round(time.Millisecond))
	largeRemote := fmt.Sprintf("https://localhost:%d", largeHost.Stats().LocalPort)

	maxParallelTransfers = 4
	root := filepath.Join(tmp, "large-client")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	largeClient, largeClientDir := newBenchClient(t, root, largeHostDir, largeRemote, token)
	defer largeClient.Stop()
	start = time.Now()
	if err := largeClient.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatal(err)
	}
	r := benchResult{label: "pull one 256MB file", files: 1, bytes: largeSize, elapsed: time.Since(start)}
	t.Logf("%-28s %s", r.label, r.rate())
	want, _ := HashFile(largePath)
	got, err := HashFile(filepath.Join(largeClientDir, "large.bin"))
	if err != nil || got != want {
		t.Fatalf("large file hash mismatch: %v", err)
	}

	// --- Push many small files -------------------------------------------
	// A fresh pair per run: deleting the host tree in place would be a real
	// deletion event, and the client would (correctly) delete its copies
	// before the next push instead of uploading again.
	runPush := func(n int) {
		label := fmt.Sprintf("push %d x 4KB, workers=%d", smallCount, n)
		root := filepath.Join(tmp, fmt.Sprintf("push-%d", n))
		hostDir := filepath.Join(root, "host")
		if err := os.MkdirAll(hostDir, 0o755); err != nil {
			t.Fatal(err)
		}
		pushHost := startTestHost(t, hostDir, token)
		defer pushHost.Stop()
		pushRoot := filepath.Join(root, "client")
		if err := os.MkdirAll(pushRoot, 0o755); err != nil {
			t.Fatal(err)
		}
		pushRemote := fmt.Sprintf("https://localhost:%d", pushHost.Stats().LocalPort)
		pushClient, pushClientDir := newBenchClient(t, pushRoot, hostDir, pushRemote, token)
		defer pushClient.Stop()
		makeManyFiles(t, filepath.Join(pushClientDir, "many"), smallCount, smallSize)

		// Separate the one-time local scan/hash from the upload itself: the
		// reconcile that follows keeps hashes from this scan.
		scanStart := time.Now()
		if _, _, err := pushClient.index.ScanFoldersIncremental(pushClient.Config().EnabledFolders(), pushClient.Config().IgnorePatterns); err != nil {
			t.Fatal(err)
		}
		t.Logf("%-28s scan %s", label, time.Since(scanStart).Round(time.Millisecond))

		maxParallelTransfers = n
		start := time.Now()
		if err := pushClient.ReconcileWithRemote(context.Background()); err != nil {
			t.Fatal(err)
		}
		r := benchResult{label: label, files: smallCount, bytes: smallBytes, elapsed: time.Since(start)}
		t.Logf("%-28s %s", label, r.rate())
		sample := filepath.Join("many", "d00", "f00000.bin")
		src, _ := os.ReadFile(filepath.Join(pushClientDir, sample))
		dst, err := os.ReadFile(filepath.Join(hostDir, sample))
		if err != nil || string(src) != string(dst) {
			t.Fatalf("%s: uploaded sample differs: %v", label, err)
		}
	}
	for _, n := range []int{1, 4, 8, 16, 64} {
		runPush(n)
	}
}
