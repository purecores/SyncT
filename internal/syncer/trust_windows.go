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

// installCACert adds the local origin certificate to the machine-wide
// "Trusted Root Certification Authorities" store and restarts the cloudflared
// service so its cached root pool picks the certificate up.
//
// The store write affects every user on this PC and therefore needs
// administrator rights: ShellExecuteW with the "runas" verb asks Windows to
// show its UAC consent prompt. A declined prompt simply does nothing, which
// is why the caller must verify the tunnel recovers rather than assume it.
func installCACert(pemPath string) error {
	data, err := os.ReadFile(pemPath)
	if err != nil {
		return fmt.Errorf("读取证书文件失败: %w", err)
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		return fmt.Errorf("证书文件格式无效: %s", pemPath)
	}
	if _, err := x509.ParseCertificate(block.Bytes); err != nil {
		return fmt.Errorf("证书内容无效: %w", err)
	}

	verb, err := syscall.UTF16PtrFromString("runas")
	if err != nil {
		return err
	}
	file, err := syscall.UTF16PtrFromString("cmd.exe")
	if err != nil {
		return err
	}
	// The certificate is self-signed for localhost and only used by the local
	// cloudflared origin connection; restarting the service makes cloudflared
	// re-read the Windows root store.
	params, err := syscall.UTF16PtrFromString(fmt.Sprintf(
		`/c certutil -addstore -f Root "%s" & net stop cloudflared & net start cloudflared & timeout /t 6 >nul`,
		pemPath,
	))
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
