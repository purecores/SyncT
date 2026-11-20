//go:build windows

package syncer

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

var (
	modShell32        = syscall.NewLazyDLL("shell32.dll")
	procShellExecuteW = modShell32.NewProc("ShellExecuteW")
)

const swShowNormal = 1

// buildCertUtilArgs validates pemPath and returns the exact argument string for
// invoking certutil.exe directly (without cmd.exe).
func buildCertUtilArgs(pemPath string) (string, error) {
	cleanPath, err := validateCertUtilPath(pemPath)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`-addstore -f Root "%s"`, cleanPath), nil
}

// installCACert adds the name-constrained localhost Root CA certificate to the
// machine-wide "Trusted Root Certification Authorities" store by invoking
// certutil.exe directly via ShellExecuteW with the "runas" verb (never cmd.exe).
//
// The store write affects every user on this PC and therefore needs
// administrator rights: ShellExecuteW with the "runas" verb asks Windows to
// show its UAC consent prompt. A declined prompt simply does nothing, which
// is why the caller must verify the tunnel recovers rather than assume it.
func installCACert(pemPath string) error {
	args, err := buildCertUtilArgs(pemPath)
	if err != nil {
		return fmt.Errorf("证书路径安全校验失败: %w", err)
	}

	data, err := os.ReadFile(pemPath)
	if err != nil {
		return fmt.Errorf("读取证书文件失败: %w", err)
	}
	block, rest := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		return fmt.Errorf("证书文件格式无效: %s", pemPath)
	}
	if extra, _ := pem.Decode(rest); extra != nil {
		return fmt.Errorf("证书文件只能包含单个 CA 证书: %s", pemPath)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("证书内容无效: %w", err)
	}
	if err := validateConstrainedLocalCA(cert); err != nil {
		return fmt.Errorf("拒绝安装不受约束的 CA 证书: %w", err)
	}

	verb, err := syscall.UTF16PtrFromString("runas")
	if err != nil {
		return err
	}
	file, err := syscall.UTF16PtrFromString("certutil.exe")
	if err != nil {
		return err
	}
	params, err := syscall.UTF16PtrFromString(args)
	if err != nil {
		return err
	}

	ret, _, callErr := procShellExecuteW.Call(
		0,
		uintptr(unsafe.Pointer(verb)),
		uintptr(unsafe.Pointer(file)),
		uintptr(unsafe.Pointer(params)),
		0,
		swShowNormal,
	)
	if ret <= 32 {
		return fmt.Errorf("请求管理员权限失败 (代码 %d): %v", ret, callErr)
	}
	return nil
}
