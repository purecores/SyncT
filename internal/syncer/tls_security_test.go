package syncer

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"synct/internal/config"
)

func withTempLocalCertPaths(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	certPath := filepath.Join(dir, "localhost-cert.pem")
	keyPath := filepath.Join(dir, "localhost-key.pem")
	orig := localCertPaths
	localCertPaths = func() (string, string) {
		return certPath, keyPath
	}
	t.Cleanup(func() {
		localCertPaths = orig
	})
	return certPath, keyPath
}

func TestTLS_EnsureLocalTLSConfig_HardenedChain(t *testing.T) {
	certPath, keyPath := withTempLocalCertPaths(t)

	tlsCfg, gotPath, err := EnsureLocalTLSConfig()
	if err != nil {
		t.Fatalf("EnsureLocalTLSConfig failed: %v", err)
	}
	if gotPath != certPath {
		t.Fatalf("certPath = %q, want %q", gotPath, certPath)
	}
	if tlsCfg.MinVersion != tls.VersionTLS12 {
		t.Fatalf("MinVersion = %v, want TLS 1.2", tlsCfg.MinVersion)
	}
	if len(tlsCfg.Certificates) != 1 {
		t.Fatalf("Certificates len = %d, want 1", len(tlsCfg.Certificates))
	}
	chain := tlsCfg.Certificates[0].Certificate
	if len(chain) != 2 {
		t.Fatalf("chain len = %d, want 2 (leaf + constrained CA)", len(chain))
	}

	leaf, err := x509.ParseCertificate(chain[0])
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	ca, err := x509.ParseCertificate(chain[1])
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}

	// Leaf certificate checks.
	if leaf.IsCA {
		t.Fatal("leaf cert must have IsCA == false")
	}
	if !leaf.BasicConstraintsValid {
		t.Fatal("leaf cert must have BasicConstraintsValid == true")
	}
	if leaf.KeyUsage&x509.KeyUsageCertSign != 0 {
		t.Fatal("leaf cert must NOT have KeyUsageCertSign")
	}
	if leaf.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
		t.Fatal("leaf cert must have KeyUsageDigitalSignature")
	}
	if len(leaf.DNSNames) != 1 || leaf.DNSNames[0] != "localhost" {
		t.Fatalf("leaf DNSNames = %v, want [localhost]", leaf.DNSNames)
	}
	for _, dns := range leaf.DNSNames {
		if dns == config.DefaultPublicDomain {
			t.Fatalf("leaf cert must NOT include public domain %q", config.DefaultPublicDomain)
		}
	}
	var hasV4, hasV6 bool
	for _, ip := range leaf.IPAddresses {
		if ip.Equal(net.ParseIP("127.0.0.1")) {
			hasV4 = true
		}
		if ip.Equal(net.ParseIP("::1")) {
			hasV6 = true
		}
	}
	if !hasV4 || !hasV6 {
		t.Fatalf("leaf IPAddresses = %v, want both 127.0.0.1 and ::1", leaf.IPAddresses)
	}

	// CA certificate checks.
	if !ca.IsCA || !ca.BasicConstraintsValid {
		t.Fatal("CA cert must have IsCA == true and BasicConstraintsValid == true")
	}
	if ca.MaxPathLen != 0 || !ca.MaxPathLenZero {
		t.Fatalf("CA MaxPathLen=%d MaxPathLenZero=%v, want 0/true", ca.MaxPathLen, ca.MaxPathLenZero)
	}
	if !ca.PermittedDNSDomainsCritical {
		t.Fatal("CA PermittedDNSDomainsCritical must be true")
	}
	if len(ca.PermittedDNSDomains) != 1 || ca.PermittedDNSDomains[0] != "localhost" {
		t.Fatalf("CA PermittedDNSDomains = %v, want [localhost]", ca.PermittedDNSDomains)
	}
	if len(ca.PermittedIPRanges) != 2 {
		t.Fatalf("CA PermittedIPRanges = %v, want 2 loopback entries", ca.PermittedIPRanges)
	}
	if err := validateConstrainedLocalCA(ca); err != nil {
		t.Fatalf("validateConstrainedLocalCA failed: %v", err)
	}

	// Verify key on disk matches ONLY leaf public key, NOT CA public key.
	keyBytes, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("read keyPath: %v", err)
	}
	signer, err := parsePrivateKeyPEM(keyBytes)
	if err != nil {
		t.Fatalf("parsePrivateKeyPEM: %v", err)
	}
	if !publicKeysEqual(signer.Public(), leaf.PublicKey) {
		t.Fatal("localhost-key.pem must match leaf public key")
	}
	if publicKeysEqual(signer.Public(), ca.PublicKey) {
		t.Fatal("localhost-key.pem must NOT match CA public key (CA key must be ephemeral)")
	}
	if err := verifyKeyDoesNotMatchCert(keyPath, ca); err != nil {
		t.Fatalf("verifyKeyDoesNotMatchCert failed: %v", err)
	}

	// Calling EnsureLocalTLSConfig again should reuse the valid chain without regenerating.
	tlsCfg2, _, err := EnsureLocalTLSConfig()
	if err != nil {
		t.Fatalf("second EnsureLocalTLSConfig failed: %v", err)
	}
	leaf2, err := x509.ParseCertificate(tlsCfg2.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatalf("parse second leaf: %v", err)
	}
	if leaf2.SerialNumber.Cmp(leaf.SerialNumber) != 0 {
		t.Fatalf("expected existing valid chain to be reused; serial changed %v -> %v", leaf.SerialNumber, leaf2.SerialNumber)
	}
}

func TestTLS_EnsureLocalTLSConfig_RegeneratesLegacyCAAsLeaf(t *testing.T) {
	certPath, keyPath := withTempLocalCertPaths(t)

	// Write a legacy insecure self-signed CA-as-leaf certificate.
	legacyPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate legacy key: %v", err)
	}
	now := time.Now().Add(-1 * time.Hour)
	legacySerial := big.NewInt(424242)
	legacyTmpl := x509.Certificate{
		SerialNumber:          legacySerial,
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             now,
		NotAfter:              now.Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost", config.DefaultPublicDomain},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	legacyDER, err := x509.CreateCertificate(rand.Reader, &legacyTmpl, &legacyTmpl, &legacyPriv.PublicKey, legacyPriv)
	if err != nil {
		t.Fatalf("create legacy cert: %v", err)
	}
	legacyKeyBytes, err := x509.MarshalECPrivateKey(legacyPriv)
	if err != nil {
		t.Fatalf("marshal legacy key: %v", err)
	}
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: legacyDER}), 0o600); err != nil {
		t.Fatalf("write legacy cert: %v", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: legacyKeyBytes}), 0o600); err != nil {
		t.Fatalf("write legacy key: %v", err)
	}

	tlsCfg, _, err := EnsureLocalTLSConfig()
	if err != nil {
		t.Fatalf("EnsureLocalTLSConfig failed to upgrade legacy cert: %v", err)
	}
	chain := tlsCfg.Certificates[0].Certificate
	if len(chain) != 2 {
		t.Fatalf("upgraded chain len = %d, want 2", len(chain))
	}
	leaf, err := x509.ParseCertificate(chain[0])
	if err != nil {
		t.Fatalf("parse upgraded leaf: %v", err)
	}
	ca, err := x509.ParseCertificate(chain[1])
	if err != nil {
		t.Fatalf("parse upgraded ca: %v", err)
	}
	if leaf.SerialNumber.Cmp(legacySerial) == 0 {
		t.Fatal("legacy cert was not regenerated")
	}
	if leaf.IsCA {
		t.Fatal("upgraded leaf must not be CA")
	}
	if err := validateConstrainedLocalCA(ca); err != nil {
		t.Fatalf("upgraded CA failed validation: %v", err)
	}
	if err := verifyKeyDoesNotMatchCert(keyPath, ca); err != nil {
		t.Fatalf("upgraded key unexpectedly matched CA: %v", err)
	}
}

func TestTLS_EnsureLocalTLSConfig_RegeneratesExpiringChain(t *testing.T) {
	certPath, keyPath := withTempLocalCertPaths(t)

	caPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen ca key: %v", err)
	}
	now := time.Now().Add(-1 * time.Hour)
	caTmpl := x509.Certificate{
		SerialNumber:                big.NewInt(1001),
		Subject:                     pkix.Name{CommonName: "SyncT Localhost Root CA"},
		NotBefore:                   now,
		NotAfter:                    now.Add(365 * 24 * time.Hour),
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
		t.Fatalf("create ca: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}

	leafPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen leaf key: %v", err)
	}
	expiringSerial := big.NewInt(2002)
	leafTmpl := x509.Certificate{
		SerialNumber:          expiringSerial,
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             now,
		NotAfter:              time.Now().Add(10 * 24 * time.Hour), // within 30-day renewal window
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, &leafTmpl, caCert, &leafPriv.PublicKey, caPriv)
	if err != nil {
		t.Fatalf("create leaf: %v", err)
	}
	leafKeyBytes, err := x509.MarshalECPrivateKey(leafPriv)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}
	bundle := append(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})...,
	)
	if err := os.WriteFile(certPath, bundle, 0o600); err != nil {
		t.Fatalf("write bundle: %v", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: leafKeyBytes}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	tlsCfg, _, err := EnsureLocalTLSConfig()
	if err != nil {
		t.Fatalf("EnsureLocalTLSConfig failed: %v", err)
	}
	leaf, err := x509.ParseCertificate(tlsCfg.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatalf("parse regenerated leaf: %v", err)
	}
	if leaf.SerialNumber.Cmp(expiringSerial) == 0 {
		t.Fatal("expiring leaf cert was not regenerated")
	}
	if time.Now().Add(365 * 24 * time.Hour).After(leaf.NotAfter) {
		t.Fatalf("regenerated leaf NotAfter = %v, expected ~2 years", leaf.NotAfter)
	}
}

func TestTLS_NameConstraintsEnforced(t *testing.T) {
	withTempLocalCertPaths(t)

	tlsCfg, _, err := EnsureLocalTLSConfig()
	if err != nil {
		t.Fatalf("EnsureLocalTLSConfig: %v", err)
	}
	leaf, err := x509.ParseCertificate(tlsCfg.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	ca, err := x509.ParseCertificate(tlsCfg.Certificates[0].Certificate[1])
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(ca)

	for _, host := range []string{"localhost", "127.0.0.1", "::1"} {
		if _, err := leaf.Verify(x509.VerifyOptions{
			Roots:     pool,
			DNSName:   host,
			KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}); err != nil {
			t.Fatalf("leaf.Verify(%q) failed: %v", host, err)
		}
	}

	// Prove that even if an attacker somehow held a constrained CA private key,
	// any certificate signed for an external domain or non-loopback IP is rejected by X.509 verification.
	simCAPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen sim CA key: %v", err)
	}
	now := time.Now().Add(-1 * time.Hour)
	simCATmpl := x509.Certificate{
		SerialNumber:                randomCertSerial(),
		Subject:                     pkix.Name{CommonName: "SyncT Localhost Root CA"},
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
	simCADER, err := x509.CreateCertificate(rand.Reader, &simCATmpl, &simCATmpl, &simCAPriv.PublicKey, simCAPriv)
	if err != nil {
		t.Fatalf("create sim CA: %v", err)
	}
	simCA, err := x509.ParseCertificate(simCADER)
	if err != nil {
		t.Fatalf("parse sim CA: %v", err)
	}
	simPool := x509.NewCertPool()
	simPool.AddCert(simCA)

	for _, badDomain := range []string{"github.com", "evil.example.com"} {
		roguePriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("gen rogue key: %v", err)
		}
		rogueTmpl := x509.Certificate{
			SerialNumber:          randomCertSerial(),
			Subject:               pkix.Name{CommonName: badDomain},
			NotBefore:             now,
			NotAfter:              now.Add(24 * time.Hour),
			KeyUsage:              x509.KeyUsageDigitalSignature,
			ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			BasicConstraintsValid: true,
			IsCA:                  false,
			DNSNames:              []string{badDomain},
		}
		rogueDER, err := x509.CreateCertificate(rand.Reader, &rogueTmpl, simCA, &roguePriv.PublicKey, simCAPriv)
		if err != nil {
			t.Fatalf("sign rogue cert for %s: %v", badDomain, err)
		}
		rogueCert, err := x509.ParseCertificate(rogueDER)
		if err != nil {
			t.Fatalf("parse rogue cert: %v", err)
		}
		if _, err := rogueCert.Verify(x509.VerifyOptions{
			Roots:     simPool,
			DNSName:   badDomain,
			KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}); err == nil {
			t.Fatalf("expected name-constrained CA to reject rogue cert for %q, but Verify succeeded", badDomain)
		}
	}

	// Also ensure non-loopback IP SAN is rejected.
	rogueIPPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen rogue IP key: %v", err)
	}
	rogueIPTmpl := x509.Certificate{
		SerialNumber:          randomCertSerial(),
		Subject:               pkix.Name{CommonName: "8.8.8.8"},
		NotBefore:             now,
		NotAfter:              now.Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
		IPAddresses:           []net.IP{net.ParseIP("8.8.8.8")},
	}
	rogueIPDER, err := x509.CreateCertificate(rand.Reader, &rogueIPTmpl, simCA, &rogueIPPriv.PublicKey, simCAPriv)
	if err != nil {
		t.Fatalf("sign rogue IP cert: %v", err)
	}
	rogueIPCert, err := x509.ParseCertificate(rogueIPDER)
	if err != nil {
		t.Fatalf("parse rogue IP cert: %v", err)
	}
	if _, err := rogueIPCert.Verify(x509.VerifyOptions{
		Roots:     simPool,
		DNSName:   "8.8.8.8",
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err == nil {
		t.Fatal("expected name-constrained CA to reject rogue IP cert for 8.8.8.8, but Verify succeeded")
	}
}

func TestTLS_LoopbackHTTPSWithNewSyncHTTPTransport(t *testing.T) {
	withTempLocalCertPaths(t)

	tlsCfg, _, err := EnsureLocalTLSConfig()
	if err != nil {
		t.Fatalf("EnsureLocalTLSConfig: %v", err)
	}

	rawLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen loopback: %v", err)
	}
	tlsLn := tls.NewListener(rawLn, tlsCfg)
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("synct-tls-ok"))
		}),
	}
	defer srv.Close()
	go func() {
		_ = srv.Serve(tlsLn)
	}()

	tr := newSyncHTTPTransport()
	defer tr.CloseIdleConnections()
	if tr.TLSClientConfig == nil || tr.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("newSyncHTTPTransport must use verified TLS config without InsecureSkipVerify")
	}

	client := &http.Client{
		Transport: tr,
		Timeout:   5 * time.Second,
	}
	url := "https://" + rawLn.Addr().String() + "/health"
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("HTTPS GET %s failed: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != http.StatusOK || string(body) != "synct-tls-ok" {
		t.Fatalf("unexpected response: status=%d body=%q", resp.StatusCode, string(body))
	}
}

func TestTLS_InstallLocalTrust_ExtractsOnlyConstrainedCA(t *testing.T) {
	withTempLocalCertPaths(t)

	var capturedPath string
	origInstaller := installCACertFn
	installCACertFn = func(pemPath string) error {
		capturedPath = pemPath
		return nil
	}
	t.Cleanup(func() {
		installCACertFn = origInstaller
		if capturedPath != "" {
			_ = os.Remove(capturedPath)
		}
	})

	if err := InstallLocalTrust(); err != nil {
		t.Fatalf("InstallLocalTrust failed: %v", err)
	}
	if capturedPath == "" {
		t.Fatal("expected installCACertFn to be called with temp CA cert path")
	}
	base := filepath.Base(capturedPath)
	pattern := regexp.MustCompile(`^synct-localhost-ca-[0-9a-f]{32}\.cer$`)
	if !pattern.MatchString(base) {
		t.Fatalf("temp CA filename %q does not match expected clean hex pattern", base)
	}
	if _, err := buildCertUtilArgs(capturedPath); err != nil {
		t.Fatalf("temp CA path %q rejected by buildCertUtilArgs: %v", capturedPath, err)
	}

	raw, err := os.ReadFile(capturedPath)
	if err != nil {
		t.Fatalf("read temp CA cert: %v", err)
	}
	block, rest := pem.Decode(raw)
	if block == nil || block.Type != "CERTIFICATE" {
		t.Fatalf("temp CA cert has invalid PEM")
	}
	if extra, _ := pem.Decode(rest); extra != nil {
		t.Fatalf("temp CA cert must contain ONLY 1 certificate (CA only, not leaf)")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse temp CA cert: %v", err)
	}
	if err := validateConstrainedLocalCA(cert); err != nil {
		t.Fatalf("temp CA cert failed validateConstrainedLocalCA: %v", err)
	}
}

func TestTLS_BuildCertUtilArgs_RejectsCommandInjection(t *testing.T) {
	badPaths := []string{
		"",
		`C:\Users\a" & calc.exe & "b.pem`,
		`C:\a|b.pem`,
		`C:\a%TEMP%\b.pem`,
		`C:\a&whoami.pem`,
		`C:\a;calc.pem`,
		`C:\a>out.pem`,
		`C:\a<in.pem`,
		`C:\a^b.pem`,
		"C:\\a`id`.pem",
		`C:\a$env.pem`,
		`C:\a'b.pem`,
		"C:\\a\r\nb.pem",
		"C:\\a\x00b.pem",
		`relative\ca.pem`,
		`./ca.pem`,
		`../ca.pem`,
		filepath.Join(t.TempDir(), "sub") + string(os.PathSeparator),
	}

	for _, p := range badPaths {
		if args, err := buildCertUtilArgs(p); err == nil {
			t.Errorf("buildCertUtilArgs(%q) unexpectedly succeeded with %q", p, args)
		}
	}

	goodPath := filepath.Join(t.TempDir(), "synct-localhost-ca-0123456789abcdef0123456789abcdef.cer")
	args, err := buildCertUtilArgs(goodPath)
	if err != nil {
		t.Fatalf("buildCertUtilArgs(%q) failed: %v", goodPath, err)
	}
	want := `-addstore -f Root "` + filepath.Clean(goodPath) + `"`
	if args != want {
		t.Fatalf("buildCertUtilArgs(%q) = %q, want %q", goodPath, args, want)
	}
	if strings.Contains(strings.ToLower(args), "cmd") || strings.Contains(args, "&") {
		t.Fatalf("buildCertUtilArgs output must not contain shell chaining: %q", args)
	}
}
