package syncer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"synct/internal/config"
)

// startLatencyProxy gives the loopback transfer a WAN-like per-request delay.
// It stands in for the Cloudflare edge path when an external tunnel is not
// available, so the cost of one HTTP request per small file is visible.
func startLatencyProxy(t *testing.T, origin string, delay time.Duration) (string, func()) {
	t.Helper()
	target, err := url.Parse(origin)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
		proxy.ServeHTTP(w, r)
	}))
	return srv.URL, srv.Close
}

func TestLatencyTransferReport(t *testing.T) {
	if os.Getenv("SYNCT_BENCH") == "" {
		t.Skip("set SYNCT_BENCH=1 to run the latency transfer report")
	}
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		t.Fatal(err)
	}
	const count = 1000
	const size = 4 * 1024
	bytesTotal := makeManyFiles(t, filepath.Join(hostDir, "many"), count, size)
	token := "st_latency_token"

	start := time.Now()
	host := startTestHost(t, hostDir, token)
	t.Logf("host startup scan (%d x 4KB): %s", count, time.Since(start).Round(time.Millisecond))
	origin := fmt.Sprintf("http://127.0.0.1:%d", host.Stats().LocalPort)
	proxyURL, stopProxy := startLatencyProxy(t, origin, 40*time.Millisecond)
	defer stopProxy()

	old := maxParallelTransfers
	defer func() { maxParallelTransfers = old }()
	// Method B: one manifest request per file batch (the shipped protocol).
	for _, n := range []int{8, 32, 64} {
		root := filepath.Join(tmp, fmt.Sprintf("client-%d", n))
		if err := os.MkdirAll(root, 0o755); err != nil {
			t.Fatal(err)
		}
		client, _ := newBenchClient(t, root, hostDir, proxyURL, token)
		maxParallelTransfers = n
		start := time.Now()
		if err := client.ReconcileWithRemote(context.Background()); err != nil {
			t.Fatal(err)
		}
		r := benchResult{label: fmt.Sprintf("40ms latency, %d x 4KB, workers=%d", count, n), files: count, bytes: bytesTotal, elapsed: time.Since(start)}
		t.Logf("%-44s %s", r.label, r.rate())
		if got, _ := client.index.Counts(); got != count {
			t.Fatalf("client indexed %d files; want %d", got, count)
		}
		client.Stop()
	}

	// Direct per-file GET through the same proxy, no engine overhead: the
	// floor any client could reach when each small file is one request.
	reqs := make([]string, 0, count)
	for i := 0; i < count; i++ {
		reqs = append(reqs, fmt.Sprintf("%s/api/file?path=%s", proxyURL, url.QueryEscape(fmt.Sprintf("many/d%02d/f%05d.bin", i%24, i))))
	}
	reqClient := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 64}}
	for _, n := range []int{32, 64} {
		sem := make(chan struct{}, n)
		var wg sync.WaitGroup
		start := time.Now()
		for _, raw := range reqs {
			wg.Add(1)
			sem <- struct{}{}
			go func(raw string) {
				defer wg.Done()
				defer func() { <-sem }()
				req, _ := http.NewRequest(http.MethodGet, raw, nil)
				req.Header.Set("X-SyncT-Token", token)
				resp, err := reqClient.Do(req)
				if err != nil {
					return
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}(raw)
		}
		wg.Wait()
		r := benchResult{label: fmt.Sprintf("raw GET floor, %d x 4KB, conns=%d", count, n), files: count, bytes: bytesTotal, elapsed: time.Since(start)}
		t.Logf("%-44s %s", r.label, r.rate())
	}
}

// startEdgeCapProxy mimics Cloudflare's proxied-request limits: bodies over
// cap are refused with 413 and requests held longer than the origin timeout
// answer 524. It shows whether the chunked path actually survives an edge that
// rejects oversized or slow requests instead of only a fast loopback.
func startEdgeCapProxy(t *testing.T, origin string, capBytes int64, delay time.Duration) (string, func()) {
	t.Helper()
	target, err := url.Parse(origin)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > capBytes {
			http.Error(w, "413 Payload Too Large", http.StatusRequestEntityTooLarge)
			return
		}
		time.Sleep(delay)
		proxy.ServeHTTP(w, r)
	}))
	return srv.URL, srv.Close
}

func TestLargeUploadThroughEdgeCap(t *testing.T) {
	if os.Getenv("SYNCT_BENCH") == "" {
		t.Skip("set SYNCT_BENCH=1 to run the edge-cap upload report")
	}
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		t.Fatal(err)
	}
	token := "st_edge_cap_token"
	host := startTestHost(t, hostDir, token)
	origin := fmt.Sprintf("http://127.0.0.1:%d", host.Stats().LocalPort)
	// 3 MB cap: far below the test file, and no larger than the chunk size
	// the client uses, so nothing oversized may pass.
	proxyURL, stop := startEdgeCapProxy(t, origin, 3<<20, 20*time.Millisecond)
	defer stop()

	clientDir := filepath.Join(tmp, "client")
	if err := os.MkdirAll(clientDir, 0o755); err != nil {
		t.Fatal(err)
	}
	size := int64(24 << 20)
	makeLargeFile(t, filepath.Join(clientDir, "big.bin"), size)

	cfg := config.Default()
	cfg.Role = config.RoleClient
	cfg.SyncDir = clientDir
	cfg.RemoteURL = proxyURL
	cfg.AuthToken = token
	client := NewEngine(cfg, filepath.Join(tmp, "client_state.json"), nil)
	defer client.Stop()

	start := time.Now()
	if err := client.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatal(err)
	}
	r := benchResult{label: "24MB upload through 3MB edge cap", files: 1, bytes: size, elapsed: time.Since(start)}
	t.Logf("%-44s %s", r.label, r.rate())
	want, _ := HashFile(filepath.Join(clientDir, "big.bin"))
	got, err := HashFile(filepath.Join(hostDir, "big.bin"))
	if err != nil || got != want {
		for _, entry := range client.Logs() {
			t.Logf("client log: [%s] %s", entry.Level, entry.Message)
		}
		t.Fatalf("uploaded file differs: %v", err)
	}
}
