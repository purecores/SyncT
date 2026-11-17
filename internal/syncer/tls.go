package syncer

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/egoist/mygo/netproxy"

	"synct/internal/config"
)

// newSyncHTTPTransport creates an *http.Transport that respects the Windows system proxy
// and trusts both Windows system root CAs (for https://synctreei.suosmanthus.cyou)
// and the local self-signed certificate (for https://localhost:18920).
func newSyncHTTPTransport() *http.Transport {
	tr := netproxy.Transport()
	_, certPath, err := EnsureLocalTLSConfig()
	if err == nil && certPath != "" {
		if pemData, rerr := os.ReadFile(certPath); rerr == nil {
			pool, perr := x509.SystemCertPool()
			if perr != nil || pool == nil {
				pool = x509.NewCertPool()
			}
			pool.AppendCertsFromPEM(pemData)
			if tr.TLSClientConfig == nil {
				tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
			} else {
				tr.TLSClientConfig = tr.TLSClientConfig.Clone()
			}
			tr.TLSClientConfig.RootCAs = pool
		}
	}
	// Transfers are bulk bytes over one tunnel; a handful of reused
	// connections overlap latency between files without opening a new TLS
	// handshake (and a new Cloudflare edge round trip) per file.
	if tr.MaxIdleConnsPerHost < 8 {
		tr.MaxIdleConnsPerHost = 8
	}
	if tr.MaxIdleConns < 32 {
		tr.MaxIdleConns = 32
	}
	tr.DisableCompression = true
	return tr
}

// InstallLocalTrust installs the local origin certificate into the machine's
// trusted root store so the cloudflared service accepts it without
// No TLS Verify. It is invoked from the UI on an explicit user action and
// shows a UAC prompt; the certificate is generated first if missing.
func InstallLocalTrust() error {
	_, certPath, err := EnsureLocalTLSConfig()
	if err != nil {
		return err
	}
	if certPath == "" {
		return errors.New("本地证书路径不可用")
	}
	if _, err := os.Stat(certPath); err != nil {
		return fmt.Errorf("读取本地证书失败: %w", err)
	}
	return installCACert(certPath)
}

// EnsureLocalTLSConfig loads or generates a self-signed TLS certificate for
// `https://localhost:18920` (covering `localhost`, `127.0.0.1`, `::1`, and `synctreei.suosmanthus.cyou`).
func EnsureLocalTLSConfig() (*tls.Config, string, error) {
	certPath, keyPath := config.LocalCertPaths()
	if cert, err := tls.LoadX509KeyPair(certPath, keyPath); err == nil {
		return &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}, certPath, nil
	}

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, "", err
	}

	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		serialNumber = big.NewInt(time.Now().UnixNano())
	}

	now := time.Now().Add(-1 * time.Hour)
	tmpl := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"SyncT Portable Local Origin"},
			CommonName:   "localhost",
		},
		NotBefore:             now,
		NotAfter:              now.Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames: []string{
			"localhost",
			config.DefaultPublicDomain,
		},
		IPAddresses: []net.IP{
			net.ParseIP("127.0.0.1"),
			net.ParseIP("::1"),
		},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		return nil, "", err
	}

	keyBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return nil, "", err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

	_ = os.WriteFile(certPath, certPEM, 0o600)
	_ = os.WriteFile(keyPath, keyPEM, 0o600)

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, "", err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, certPath, nil
}

// peekedConn replays the initial peeked byte before reading from the underlying net.Conn.
type peekedConn struct {
	net.Conn
	firstByte byte
	hasByte   bool
}

func (c *peekedConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if c.hasByte {
		p[0] = c.firstByte
		c.hasByte = false
		return 1, nil
	}
	return c.Conn.Read(p)
}

// originFirstByteTimeout bounds how long a fresh origin connection may stay
// silent before it is dropped. cloudflared and corporate HTTPS proxies can
// pre-open connections, so a very short deadline would randomly kill the
// tunnel's request path and surface as intermittent 502s.
const originFirstByteTimeout = 30 * time.Second

// errOriginListenerFailed ends http.Server.Serve when every accept loop gave
// up, distinguishing a dead origin from an intentional close.
var errOriginListenerFailed = errors.New("本地回环监听器已失效")

// dualProtocolListener accepts connections from one or more loopback listeners (IPv4 127.0.0.1 and IPv6 ::1)
// and transparently serves both HTTPS (TLS 1.2/1.3) and HTTP on the same port by inspecting the first byte.
type dualProtocolListener struct {
	listeners []net.Listener
	tlsConfig *tls.Config
	connCh    chan net.Conn
	closeOnce sync.Once
	closedCh  chan struct{}
	failedCh  chan struct{}
	failOnce  sync.Once
	onError   func(error)
}

func newDualProtocolListener(listeners []net.Listener, tlsConfig *tls.Config, onError func(error)) *dualProtocolListener {
	dl := &dualProtocolListener{
		listeners: listeners,
		tlsConfig: tlsConfig,
		connCh:    make(chan net.Conn, 64),
		closedCh:  make(chan struct{}),
		failedCh:  make(chan struct{}),
		onError:   onError,
	}
	for _, ln := range listeners {
		go dl.acceptLoop(ln)
	}
	return dl
}

// reportFailure wakes Accept with a permanent error once no accept loop can
// make progress, so http.Server.Serve returns and the origin is rebound
// instead of staying silently unreachable behind the tunnel (502).
func (dl *dualProtocolListener) reportFailure(err error) {
	if dl.onError != nil {
		dl.onError(err)
	}
	dl.failOnce.Do(func() { close(dl.failedCh) })
}

func (dl *dualProtocolListener) acceptLoop(ln net.Listener) {
	var failures int
	backoff := 5 * time.Millisecond
	for {
		rawConn, err := ln.Accept()
		if err != nil {
			select {
			case <-dl.closedCh:
				return
			default:
			}
			// Accept can fail transiently on Windows (e.g. WSAECONNRESET when
			// a client aborts before the handshake, or a momentary resource
			// shortage). Returning here used to kill the only listener while
			// the app still looked healthy; retry instead, and only give up
			// after sustained failures so Serve can rebind the origin.
			failures++
			acceptErr := fmt.Errorf("监听 %s 接收连接失败 (第 %d 次): %w", ln.Addr(), failures, err)
			if failures >= 10 {
				// Sustained failure: let Serve return so the origin is rebound.
				dl.reportFailure(acceptErr)
				return
			}
			if dl.onError != nil {
				dl.onError(acceptErr)
			}
			select {
			case <-dl.closedCh:
				return
			case <-time.After(backoff):
			}
			if backoff < time.Second {
				backoff *= 2
			}
			continue
		}
		failures = 0
		backoff = 5 * time.Millisecond
		go func(c net.Conn) {
			defer func() {
				if r := recover(); r != nil {
					_ = c.Close()
					if dl.onError != nil {
						dl.onError(fmt.Errorf("连接处理异常: %v", r))
					}
				}
			}()
			dl.dispatchConn(c)
		}(rawConn)
	}
}

func (dl *dualProtocolListener) dispatchConn(rawConn net.Conn) {
	_ = rawConn.SetReadDeadline(time.Now().Add(originFirstByteTimeout))
	var b [1]byte
	n, err := rawConn.Read(b[:])
	_ = rawConn.SetReadDeadline(time.Time{})
	if err != nil || n == 0 {
		_ = rawConn.Close()
		return
	}

	pc := &peekedConn{
		Conn:      rawConn,
		firstByte: b[0],
		hasByte:   true,
	}

	var finalConn net.Conn = pc
	// 0x16 is the TLS Record Type for Handshake (ClientHello)
	if b[0] == 0x16 && dl.tlsConfig != nil {
		finalConn = tls.Server(pc, dl.tlsConfig)
	}

	select {
	case dl.connCh <- finalConn:
	case <-dl.closedCh:
		_ = finalConn.Close()
	}
}

func (dl *dualProtocolListener) Accept() (net.Conn, error) {
	select {
	case c, ok := <-dl.connCh:
		if !ok {
			return nil, io.EOF
		}
		return c, nil
	case <-dl.failedCh:
		return nil, errOriginListenerFailed
	case <-dl.closedCh:
		return nil, net.ErrClosed
	}
}

func (dl *dualProtocolListener) Close() error {
	var firstErr error
	dl.closeOnce.Do(func() {
		close(dl.closedCh)
		for _, ln := range dl.listeners {
			if err := ln.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	})
	return firstErr
}

func (dl *dualProtocolListener) Addr() net.Addr {
	if len(dl.listeners) > 0 {
		return dl.listeners[0].Addr()
	}
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: config.DefaultLocalPort}
}
