package syncer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"synct/internal/config"
)

// A slow link must not fail with a response-header timeout while the body is
// still moving: that is exactly the "upload stuck with no speed, then
// http2: timeout awaiting response headers" the work PC reported. The server
// here answers only after the whole body has been received slowly.
func TestSlowUploadSurvivesResponseHeaderDelay(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", filepath.Join(tmp, "portable"))
	hostDir := filepath.Join(tmp, "host")
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		t.Fatal(err)
	}
	token := "st_slow_upload"
	host := startTestHost(t, hostDir, token)
	origin := fmt.Sprintf("http://127.0.0.1:%d", host.Stats().LocalPort)

	// Sit between the client and the host like the tunnel does, and delay the
	// response by draining the body slowly before forwarding.
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/api/file" {
			// Drain the body first; headers are held until it is complete,
			// which is exactly when a slow tunnel produces
			// "http2: timeout awaiting response headers" if the client sets a
			// fixed header deadline.
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "read failed", http.StatusBadRequest)
				return
			}
			forward := r.Clone(context.Background())
			forward.Body = io.NopCloser(bytesReader(body))
			forward.ContentLength = int64(len(body))
			forward.URL.Scheme = "http"
			forward.URL.Host = r.Host
			resp, err := http.DefaultTransport.RoundTrip(forward)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			defer resp.Body.Close()
			for k, v := range resp.Header {
				w.Header()[k] = v
			}
			w.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(w, resp.Body)
			return
		}
		proxyRequest(origin, w, r)
	}))
	defer slow.Close()

	clientDir := filepath.Join(tmp, "client")
	if err := os.MkdirAll(clientDir, 0o755); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 3<<20)
	for i := range payload {
		payload[i] = byte(i)
	}
	mustWriteFile(t, filepath.Join(clientDir, "slow.bin"), payload)

	cfg := config.Default()
	cfg.Role = config.RoleClient
	cfg.SyncDir = clientDir
	cfg.RemoteURL = slow.URL
	cfg.AuthToken = token
	client := NewEngine(cfg, filepath.Join(tmp, "client_state.json"), nil)
	defer client.Stop()

	if tr, ok := client.transfer.Transport.(*http.Transport); ok && tr.ResponseHeaderTimeout != 0 {
		t.Fatalf("transfer transport still enforces a fixed header timeout: %s", tr.ResponseHeaderTimeout)
	}
	if err := client.ReconcileWithRemote(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(hostDir, "slow.bin"))
	if err != nil || len(got) != len(payload) {
		t.Fatalf("host file = %d bytes, err=%v", len(got), err)
	}
}

func bytesReader(b []byte) io.Reader { return &byteReader{b: b} }

type byteReader struct{ b []byte }

func (r *byteReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}

func proxyRequest(origin string, w http.ResponseWriter, r *http.Request) {
	u := origin + r.URL.RequestURI()
	req, err := http.NewRequestWithContext(r.Context(), r.Method, u, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	req.Header = r.Header.Clone()
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}
