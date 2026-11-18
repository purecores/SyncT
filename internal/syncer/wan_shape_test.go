package syncer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"synct/internal/config"
)

// shapedProxy is a TCP proxy that rate-limits each direction, so a transfer
// test crosses a link like the work PC's instead of loopback. It also adds a
// fixed one-way delay, which is what makes many small files slow in reality.
type shapedProxy struct {
	ln    net.Listener
	up    int // bytes per second toward the origin, 0 = unlimited
	down  int
	delay time.Duration
	wg    sync.WaitGroup
}

func newShapedProxy(t *testing.T, origin string, upBps, downBps int, delay time.Duration) (*shapedProxy, string) {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &shapedProxy{ln: ln, up: upBps, down: downBps, delay: delay}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			upstream, err := net.Dial("tcp4", origin)
			if err != nil {
				_ = conn.Close()
				continue
			}
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				defer conn.Close()
				defer upstream.Close()
				pump(conn, upstream, p.up, p.delay)
			}()
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				defer conn.Close()
				defer upstream.Close()
				pump(upstream, conn, p.down, p.delay)
			}()
		}
	}()
	return p, "http://" + ln.Addr().String()
}

func (p *shapedProxy) Close() {
	_ = p.ln.Close()
	p.wg.Wait()
}

// pump copies src to dst at rate bytes per second, sleeping between chunks.
func pump(src, dst net.Conn, rate int, delay time.Duration) {
	buf := make([]byte, 32*1024)
	for {
		if delay > 0 {
			time.Sleep(delay)
		}
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
			if rate > 0 {
				// Pace after the write so a full buffer leaves at the link
				// speed rather than in one burst.
				time.Sleep(time.Duration(float64(n) / float64(rate) * float64(time.Second)))
			}
		}
		if err != nil {
			if nc, ok := dst.(*net.TCPConn); ok {
				_ = nc.CloseWrite()
			}
			return
		}
	}
}

// A 32 MB file over a shaped 8 Mbit/s link must arrive intact and at roughly
// the link rate, and a slow link must not trip the stall watchdog because the
// bytes keep moving.
func TestLargeUploadOverShapedLink(t *testing.T) {
	if testing.Short() {
		t.Skip("shaped link test moves 32 MB")
	}
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		t.Fatal(err)
	}
	token := "st_shaped_link"
	host := startTestHost(t, hostDir, token)
	origin := fmt.Sprintf("127.0.0.1:%d", host.Stats().LocalPort)

	const upBps = 1 << 20 // 8 Mbit/s
	proxy, proxyURL := newShapedProxy(t, origin, upBps, 0, 0)
	defer proxy.Close()

	clientDir := filepath.Join(tmp, "client")
	if err := os.MkdirAll(clientDir, 0o755); err != nil {
		t.Fatal(err)
	}
	size := int64(32 << 20)
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
		t.Fatalf("reconcile: %v", err)
	}
	elapsed := time.Since(start)

	want, _ := HashFile(filepath.Join(clientDir, "big.bin"))
	got, err := HashFile(filepath.Join(hostDir, "big.bin"))
	if err != nil || got != want {
		t.Fatalf("file over the shaped link differs: %v", err)
	}
	mbps := float64(size) / (1024 * 1024) / elapsed.Seconds()
	t.Logf("32 MB upload over 8 Mbit/s link: %s (%.1f MB/s)", elapsed.Round(time.Millisecond), mbps)
	if mbps < 0.6 || mbps > 2.0 {
		// The 8 Mbit/s link should carry about 1 MB/s; a much higher number
		// means the shaping was bypassed, a much lower one means a stall.
		t.Fatalf("throughput %.2f MB/s does not match the shaped 1 MB/s link", mbps)
	}
	if elapsed > 90*time.Second {
		t.Fatalf("32 MB over an 8 Mbit/s link took %s", elapsed)
	}
}

// A tunnel that freezes mid-body must be abandoned by the watchdog rather than
// left hanging; the status becomes a retryable failure.
func TestFrozenTransferIsAbandoned(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	dir := filepath.Join(tmp, "sync")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 2<<20)
	sum := sha256.Sum256(payload)
	wantHash := hex.EncodeToString(sum[:])

	srvURL, closeSrv := startFrozenFileServer(t, payload, wantHash)
	defer closeSrv()

	cfg := config.Default()
	cfg.Role = config.RoleClient
	cfg.SyncDir = dir
	cfg.RemoteURL = srvURL
	cfg.AuthToken = "st_frozen"
	cfg.SyncDirection = config.SyncPullOnly
	engine := NewEngine(cfg, filepath.Join(tmp, "state.json"), nil)
	defer engine.Stop()

	stallOverride.Store(int64(300 * time.Millisecond))
	defer stallOverride.Store(0)

	done := make(chan error, 1)
	go func() { done <- engine.ReconcileWithRemote(context.Background()) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("a frozen download never returned")
	}
	if _, err := os.Stat(filepath.Join(dir, "frozen.bin")); !os.IsNotExist(err) {
		t.Fatalf("an incomplete download was committed: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if len(e.Name()) > 0 && e.Name()[0] == '.' {
			continue
		}
		t.Fatalf("unexpected file after a frozen transfer: %s", e.Name())
	}
}

// startFrozenFileServer publishes a manifest for one file and then stalls
// mid-body, as a dead tunnel does.
func startFrozenFileServer(t *testing.T, payload []byte, wantHash string) (string, func()) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/manifest":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"node_id":"peer","files":{"frozen.bin":{"rel_path":"frozen.bin","size":%d,"mod_time":1,"sha256":"%s"}},"tombstones":{}}`,
				len(payload), wantHash)
		case "/api/file":
			w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(payload[:len(payload)/2])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			// Stop sending; keep the connection open until the client gives up.
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	return srv.URL, srv.Close
}
