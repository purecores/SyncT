//go:build !windows

package syncer

import (
	"errors"
	"fmt"
)

// buildCertUtilArgs mirrors the Windows helper so cross-platform unit tests
// can exercise certutil argument validation on any OS.
func buildCertUtilArgs(pemPath string) (string, error) {
	cleanPath, err := validateCertUtilPath(pemPath)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`-addstore -f Root "%s"`, cleanPath), nil
}

// installCACert is only implemented on Windows, where cloudflared's origin
// client reads the system trust store.
func installCACert(string) error {
	return errors.New("仅支持 Windows 证书信任安装")
}
