package syncer

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/egoist/mygo/netproxy"

	"synct/internal/config"
)

// localCertPaths returns the local TLS cert/key paths. Tests override this to
// isolate certificate generation inside t.TempDir().
var localCertPaths = config.LocalCertPaths

// installCACertFn points to the OS-specific root store installer. Tests may
// override this to inspect the generated temp file without showing a UAC prompt.
var installCACertFn = installCACert

// newSyncHTTPTransport creates an *http.Transport that respects the Windows system proxy
// and trusts both Windows system root CAs (for public HTTPS tunnel endpoints)
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
	if tr.MaxIdleConnsPerHost < 64 {
		tr.MaxIdleConnsPerHost = 64
	}
	if tr.MaxIdleConns < 128 {
		tr.MaxIdleConns = 128
	}
	// Fail fast on a dead tunnel instead of waiting for the kernel's SYN
	// timeout, which would hold a worker for over 20 seconds per file.
	if tr.DialContext != nil && tr.TLSHandshakeTimeout == 0 {
		tr.TLSHandshakeTimeout = 15 * time.Second
	}
	tr.DisableCompression = true
	return tr
}

// InstallLocalTrust installs ONLY the name-constrained localhost Root CA
// certificate into the machine's trusted root store so the cloudflared service
// accepts the local origin without No TLS Verify. It is invoked from the UI on
// an explicit user action and shows a UAC prompt; the certificate chain is
// generated (or upgraded from a legacy CA-as-leaf cert) first if needed.
func InstallLocalTrust() error {
	_, certPath, err := EnsureLocalTLSConfig()
	if err != nil {
		return err
	}
	if certPath == "" {
		return errors.New("本地证书路径不可用")
	}
	_, keyPath := localCertPaths()

	caPEM, caCert, err := extractConstrainedCACertPEM(certPath)
	if err != nil {
		return fmt.Errorf("提取受约束本地 CA 证书失败: %w", err)
	}
	if err := verifyKeyDoesNotMatchCert(keyPath, caCert); err != nil {
		return err
	}

	var rnd [16]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return fmt.Errorf("生成临时证书文件名失败: %w", err)
	}
	tempPath := filepath.Join(os.TempDir(), fmt.Sprintf("synct-localhost-ca-%s.cer", hex.EncodeToString(rnd[:])))
	f, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("创建临时 CA 证书文件失败: %w", err)
	}
	if _, err := f.Write(caPEM); err != nil {
		_ = f.Close()
		_ = os.Remove(tempPath)
		return fmt.Errorf("写入临时 CA 证书文件失败: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("关闭临时 CA 证书文件失败: %w", err)
	}

	if err := installCACertFn(tempPath); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	return nil
}

// isLoopbackSingleHostNet reports whether n restricts IP SANs to a single
// loopback address (127.0.0.1/32 or ::1/128).
func isLoopbackSingleHostNet(n *net.IPNet) bool {
	if n == nil || n.IP == nil || n.Mask == nil {
		return false
	}
	ones, bits := n.Mask.Size()
	if ip4 := n.IP.To4(); ip4 != nil {
		return ip4.Equal(net.IPv4(127, 0, 0, 1)) && ones == 32 && bits == 32
	}
	if len(n.IP) == net.IPv6len {
		return n.IP.Equal(net.IPv6loopback) && ones == 128 && bits == 128
	}
	return false
}

// validateConstrainedLocalCA verifies that cert is a localhost-only,
// loopback-only, zero-path-length CA certificate safe to trust locally.
func validateConstrainedLocalCA(cert *x509.Certificate) error {
	if cert == nil {
		return errors.New("CA 证书为空")
	}
	if !cert.BasicConstraintsValid || !cert.IsCA {
		return errors.New("证书不是有效的 CA 证书")
	}
	if cert.MaxPathLen != 0 || !cert.MaxPathLenZero {
		return errors.New("CA 证书缺少 MaxPathLen=0 约束")
	}
	if !cert.PermittedDNSDomainsCritical {
		return errors.New("CA 证书名称约束未标记为 Critical")
	}
	if len(cert.PermittedDNSDomains) != 1 || cert.PermittedDNSDomains[0] != "localhost" {
		return fmt.Errorf("CA 证书 DNS 名称约束非法: %v", cert.PermittedDNSDomains)
	}
	if len(cert.PermittedIPRanges) == 0 {
		return errors.New("CA 证书缺少回环 IP 约束")
	}
	for _, ipNet := range cert.PermittedIPRanges {
		if !isLoopbackSingleHostNet(ipNet) {
			return fmt.Errorf("CA 证书包含非回环 IP 约束: %v", ipNet)
		}
	}
	return nil
}

// extractConstrainedCACertPEM reads a PEM certificate bundle and returns ONLY
// the single name-constrained CA certificate block (never the leaf cert).
func extractConstrainedCACertPEM(certPath string) ([]byte, *x509.Certificate, error) {
	data, err := os.ReadFile(certPath)
	if err != nil {
		return nil, nil, fmt.Errorf("读取本地证书失败: %w", err)
	}

	var caCert *x509.Certificate
	rest := data
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		parsed, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, nil, fmt.Errorf("解析证书链失败: %w", err)
		}
		if parsed.IsCA {
			if caCert != nil {
				return nil, nil, errors.New("证书链包含多个 CA 证书")
			}
			caCert = parsed
		}
	}

	if caCert == nil {
		return nil, nil, errors.New("证书链中未找到 CA 证书")
	}
	if err := validateConstrainedLocalCA(caCert); err != nil {
		return nil, nil, err
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCert.Raw})
	return caPEM, caCert, nil
}

func publicKeysEqual(a, b crypto.PublicKey) bool {
	if a == nil || b == nil {
		return false
	}
	type equalKey interface {
		Equal(crypto.PublicKey) bool
	}
	if eq, ok := a.(equalKey); ok {
		return eq.Equal(b)
	}
	ab, aerr := x509.MarshalPKIXPublicKey(a)
	bb, berr := x509.MarshalPKIXPublicKey(b)
	return aerr == nil && berr == nil && bytes.Equal(ab, bb)
}

func parsePrivateKeyPEM(keyPEM []byte) (crypto.Signer, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, errors.New("私钥 PEM 格式无效")
	}
	if key, err := x509.ParseECPrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		if signer, ok := key.(crypto.Signer); ok {
			return signer, nil
		}
		return nil, errors.New("不支持的 PKCS8 私钥类型")
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	return nil, errors.New("无法解析本地私钥")
}

// verifyKeyDoesNotMatchCert ensures the private key stored on disk does not
// correspond to the CA certificate's public key.
func verifyKeyDoesNotMatchCert(keyPath string, caCert *x509.Certificate) error {
	if keyPath == "" {
		return errors.New("本地私钥路径不可用")
	}
	keyData, err := os.ReadFile(keyPath)
	if err != nil {
		return fmt.Errorf("读取本地私钥失败: %w", err)
	}
	signer, err := parsePrivateKeyPEM(keyData)
	if err != nil {
		return err
	}
	if publicKeysEqual(signer.Public(), caCert.PublicKey) {
		return errors.New("安全校验失败: 磁盘私钥与 CA 证书匹配，拒绝安装信任")
	}
	return nil
}

// validateCertUtilPath verifies that pemPath is a clean absolute path free of
// shell/argument-breaking characters before it is passed to certutil.exe.
func validateCertUtilPath(pemPath string) (string, error) {
	if pemPath == "" {
		return "", errors.New("证书路径为空")
	}
	const forbiddenChars = "\"'&|<>%^;`\r\n\x00$"
	// Reject explicit metacharacters and any ASCII control characters.
	for i := 0; i < len(pemPath); i++ {
		c := pemPath[i]
		if c < 0x20 || strings.ContainsRune(forbiddenChars, rune(c)) {
			return "", fmt.Errorf("证书路径包含非法字符: %q", pemPath)
		}
	}
	if strings.HasSuffix(pemPath, `\`) || strings.HasSuffix(pemPath, `/`) {
		return "", fmt.Errorf("证书路径不能以路径分隔符结尾: %q", pemPath)
	}
	cleanPath := filepath.Clean(pemPath)
	if !filepath.IsAbs(cleanPath) {
		return "", fmt.Errorf("证书路径必须为绝对路径: %q", pemPath)
	}
	if strings.HasSuffix(cleanPath, `\`) || strings.HasSuffix(cleanPath, `/`) {
		return "", fmt.Errorf("证书路径不能以路径分隔符结尾: %q", pemPath)
	}
	for i := 0; i < len(cleanPath); i++ {
		c := cleanPath[i]
		if c < 0x20 || strings.ContainsRune(forbiddenChars, rune(c)) {
			return "", fmt.Errorf("证书路径包含非法字符: %q", cleanPath)
		}
	}
	return cleanPath, nil
}

func randomCertSerial() *big.Int {
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialLimit)
	if err != nil || serialNumber.Sign() <= 0 {
		return big.NewInt(time.Now().UnixNano())
	}
	return serialNumber
}

func isValidHardenedLocalChain(cert tls.Certificate) bool {
	if len(cert.Certificate) < 2 {
		return false
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return false
	}
	if leaf.IsCA || leaf.KeyUsage&x509.KeyUsageCertSign != 0 {
		return false
	}
	if len(leaf.DNSNames) != 1 || leaf.DNSNames[0] != "localhost" {
		return false
	}
	renewalCutoff := time.Now().Add(30 * 24 * time.Hour)
	if renewalCutoff.After(leaf.NotAfter) || time.Now().Before(leaf.NotBefore) {
		return false
	}

	ca, err := x509.ParseCertificate(cert.Certificate[1])
	if err != nil {
		return false
	}
	if err := validateConstrainedLocalCA(ca); err != nil {
		return false
	}
	if renewalCutoff.After(ca.NotAfter) || time.Now().Before(ca.NotBefore) {
		return false
	}
	if err := leaf.CheckSignatureFrom(ca); err != nil {
		return false
	}
	if signer, ok := cert.PrivateKey.(crypto.Signer); ok {
		if publicKeysEqual(signer.Public(), ca.PublicKey) {
			return false
		}
	}
	return true
}

// EnsureLocalTLSConfig loads or generates a hardened TLS certificate chain for
// `https://localhost:18920` (covering `localhost`, `127.0.0.1`, and `::1`).
// An ephemeral name-constrained Root CA signs a non-CA leaf certificate; the
// CA private key exists only in memory during signing and is never written to disk.
func EnsureLocalTLSConfig() (*tls.Config, string, error) {
	certPath, keyPath := localCertPaths()
	if cert, err := tls.LoadX509KeyPair(certPath, keyPath); err == nil {
		if isValidHardenedLocalChain(cert) {
			return &tls.Config{
				Certificates: []tls.Certificate{cert},
				MinVersion:   tls.VersionTLS12,
			}, certPath, nil
		}
	}

	// Generate an ephemeral P-256 CA keypair strictly in memory; never persist it.
	caPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, "", err
	}
	defer func() {
		if caPriv != nil && caPriv.D != nil {
			caPriv.D.SetInt64(0)
		}
		caPriv = nil
	}()

	now := time.Now().Add(-1 * time.Hour)
	caTmpl := x509.Certificate{
		SerialNumber: randomCertSerial(),
		Subject: pkix.Name{
			Organization: []string{"SyncT Local Origin CA"},
			CommonName:   "SyncT Localhost Root CA",
		},
		NotBefore:                   now,
		NotAfter:                    now.Add(3 * 365 * 24 * time.Hour),
		KeyUsage:                    x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid:       true,
		IsCA:                        true,
		MaxPathLen:                  0,
		MaxPathLenZero:              true,
		PermittedDNSDomainsCritical: true,
		PermittedDNSDomains:         []string{"localhost"},
		PermittedIPRanges: []*net.IPNet{
			{IP: net.IPv4(127, 0, 0, 1).To4(), Mask: net.CIDRMask(32, 32)},
			{IP: net.IPv6loopback, Mask: net.CIDRMask(128, 128)},
		},
	}

	caDER, err := x509.CreateCertificate(rand.Reader, &caTmpl, &caTmpl, &caPriv.PublicKey, caPriv)
	if err != nil {
		return nil, "", err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, "", err
	}

	// Generate a separate P-256 keypair for the non-CA leaf server certificate.
	leafPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, "", err
	}

	leafTmpl := x509.Certificate{
		SerialNumber: randomCertSerial(),
		Subject: pkix.Name{
			Organization: []string{"SyncT Portable Local Origin"},
			CommonName:   "localhost",
		},
		NotBefore:             now,
		NotAfter:              now.Add(2 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
		DNSNames:              []string{"localhost"},
		IPAddresses: []net.IP{
			net.ParseIP("127.0.0.1"),
			net.ParseIP("::1"),
		},
	}

	leafDER, err := x509.CreateCertificate(rand.Reader, &leafTmpl, caCert, &leafPriv.PublicKey, caPriv)
	if err != nil {
		return nil, "", err
	}

	// Explicitly discard the ephemeral CA private key before encoding/writing anything.
	if caPriv.D != nil {
		caPriv.D.SetInt64(0)
	}
	caPriv = nil

	leafKeyBytes, err := x509.MarshalECPrivateKey(leafPriv)
	if err != nil {
		return nil, "", err
	}

	leafPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	certPEM := make([]byte, 0, len(leafPEM)+len(caPEM))
	certPEM = append(certPEM, leafPEM...)
	certPEM = append(certPEM, caPEM...)
	leafKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: leafKeyBytes})

	if certDir := filepath.Dir(certPath); certDir != "" {
		_ = os.MkdirAll(certDir, 0o755)
	}
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		return nil, "", fmt.Errorf("写入本地证书失败: %w", err)
	}
	if err := os.WriteFile(keyPath, leafKeyPEM, 0o600); err != nil {
		return nil, "", fmt.Errorf("写入本地私钥失败: %w", err)
	}

	cert, err := tls.X509KeyPair(certPEM, leafKeyPEM)
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
