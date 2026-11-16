//go:build !windows

package syncer

import "errors"

// installCACert is only implemented on Windows, where cloudflared's origin
// client reads the system trust store.
func installCACert(string) error {
	return errors.New("仅支持 Windows 证书信任安装")
}
