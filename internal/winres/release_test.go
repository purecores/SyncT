package winres

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"synct/internal/config"
)

func TestPackagePortableReleaseZIPs(t *testing.T) {
	rootDir, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}

	exePath := filepath.Join(rootDir, "SyncT.exe")
	info, err := os.Stat(exePath)
	if err != nil || info.Size() == 0 {
		t.Skip("SyncT.exe not built yet; skipping release packaging")
	}

	// Clean up any bundled bin/cloudflared.exe since the Host machine already runs system cloudflared
	_ = os.RemoveAll(filepath.Join(rootDir, "bin"))

	releaseDir := filepath.Join(rootDir, "release")
	if err := os.MkdirAll(releaseDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Preserve existing auth_token if root data/config.json already has one, or generate a new shared token
	sharedToken := config.GenerateToken()
	rootCfgPath := filepath.Join(rootDir, "data", "config.json")
	if raw, err := os.ReadFile(rootCfgPath); err == nil {
		var existing config.Config
		if json.Unmarshal(raw, &existing) == nil && existing.AuthToken != "" {
			sharedToken = existing.AuthToken
		}
	}

	// 1. Prepare Host (Server) Config: Reuses system's already-running Cloudflared -> synctreei.suosmanthus.cyou -> https://localhost:18920
	hostCfg := config.Default()
	hostCfg.Role = config.RoleHost
	hostCfg.TunnelMode = config.TunnelSystem
	hostCfg.CustomPublicURL = config.DefaultPublicURL
	hostCfg.RemoteURL = config.DefaultPublicURL
	hostCfg.LocalPort = config.DefaultLocalPort
	hostCfg.LocalTLS = true
	hostCfg.LowPrivilegeMode = true
	hostCfg.AuthToken = sharedToken
	hostCfg.SyncDir = "./SyncFolder"
	// The host must be listening whenever the public hostname is opened;
	// otherwise every first request from the work PC is a 502.
	hostCfg.AutoStartSync = true

	// Also save to root ./data/config.json for in-place use on this Host machine
	_ = os.MkdirAll(filepath.Join(rootDir, "data"), 0o755)
	hostCfgBytes, _ := json.MarshalIndent(hostCfg, "", "  ")
	_ = os.WriteFile(rootCfgPath, hostCfgBytes, 0o600)

	// 2. Prepare Work PC (Client) Config: User-privilege restrained mode, pre-paired with Host's AuthToken
	clientCfg := config.Default()
	clientCfg.Role = config.RoleClient
	clientCfg.TunnelMode = config.TunnelSystem
	clientCfg.CustomPublicURL = config.DefaultPublicURL
	clientCfg.RemoteURL = config.DefaultPublicURL
	clientCfg.LocalPort = config.DefaultLocalPort
	clientCfg.LocalTLS = true
	clientCfg.LowPrivilegeMode = true
	clientCfg.AuthToken = sharedToken
	clientCfg.SyncDir = "./SyncFolder"
	clientCfgBytes, _ := json.MarshalIndent(clientCfg, "", "  ")

	shareCode := config.EncodeShareCode(config.DefaultPublicURL, sharedToken)

	guideContent := fmt.Sprintf(`# SyncT 个人双机专属便携版 (Portable Release) 使用说明

## 1. 架构概览

- **专属公网域名**: `+"`https://synctreei.suosmanthus.cyou`"+`
- **本机隧道模式**: **直接复用本机系统已运行的 Cloudflared 服务/进程**（无需再次填写 Token，也无需由软件重复拉起 `+"`cloudflared.exe`"+`）
- **本机服务端监听**: `+"`https://localhost:18920`"+` (仅绑定 `+"`127.0.0.1` / `::1`"+` 本地回环，内建自签名 TLS 证书并同时兼容 HTTPS 与 HTTP)
- **预配对安全访问密钥 (Auth Token)**: `+"`%s`"+`
- **预配对一键连接码 (Share Code)**:
  `+"```text\n  %s\n  ```"+`

---

## 2. 本机服务端 (`+"`SyncT-Host-Server`"+`) 使用步骤

1. 本机系统的 `+"`cloudflared`"+` 已在后台运行并将 `+"`synctreei.suosmanthus.cyou`"+` 转发至 `+"`https://localhost:18920`"+`（入站规则需配置 `+"`noTLSVerify: true`"+`，或直接转发到 `+"`http://localhost:18920`"+`）。
2. 双击运行 `+"`SyncT.exe`"+`（已默认预设为 **主机模式 + 复用系统已运行的 Cloudflared (`+"`System Tunnel`"+`) + `+"`https://synctreei.suosmanthus.cyou`"+` + 端口 `+"`18920`"+`，并已默认开启「自动启动同步」，保证公网入口被访问时本地源站正在监听）。
3. 选择需要同步的本地文件夹，直接点击右上角 **「启动隧道并同步」** 即可（软件会自动检测并复用系统正在运行的 `+"`cloudflared`"+` 进程，无需填写 Token）。

> **出现 502 Bad Gateway 时**：说明 Cloudflared 隧道在线，但它无法回源到 `+"`https://localhost:18920`"+`。请依次检查：
> 1. 主机端 SyncT 是否已点击「启动隧道并同步」（状态卡片是否显示「隧道已就绪」与监听端口）；
> 2. 端口 `+"`18920`"+` 是否被其他程序占用（SyncT 日志会提示，此时不会再静默改用随机端口）；
> 3. 系统 `+"`cloudflared`"+` 服务的入站规则是否仍指向 `+"`localhost:18920`"+`，自签名证书是否配置了 `+"`noTLSVerify: true`"+`；
> 4. SyncT 会每 60 秒自动探测公网地址，并在概览页顶部直接给出 502 的具体原因；本地源站异常时会自动重建监听。

---

## 3. 工作电脑客户端 (`+"`SyncT-WorkPC-Client`"+` · User 权限克制版) 使用步骤

专为仅具标准 **User 权限** 的工作电脑定制：
- **显式内嵌 `+"`asInvoker`"+` 权限清单**：双击运行绝不触发管理员 UAC 提权弹窗。
- **零端口监听**：工作电脑作为客户端，不调用任何 `+"`net.Listen`"+`，绝不触发 Windows 防火墙管理员弹窗。
- **零子进程与无 `+"`cloudflared.exe`"+`**：通过标准 HTTPS (443) 直接连接 `+"`https://synctreei.suosmanthus.cyou`"+`，不附带 `+"`cloudflared.exe`"+`，避免触发企业终端安全软件告警。
- **零注册表与零系统目录写入**：所有配置文件、文件哈希索引、窗口位置状态 (`+"`window-state.json`"+`) 100%% 隔离保存在程序同级的 `+"`./data/`"+` 目录中。

**操作方法**：
1. 将 `+"`SyncT-WorkPC-Client-UserMode-Portable.zip`"+` 解压到工作电脑当前用户有读写权限的任意目录（如 `+"`D:\\Tools\\SyncT`"+` 或个人桌面/文档目录，请勿放在 `+"`C:\\Program Files`"+`）。
2. 双击 `+"`SyncT.exe`"+`（已预先配好 **节点模式**、域名 `+"`https://synctreei.suosmanthus.cyou`"+` 及与本机一致的 **访问密钥**）。
3. 点击 **「浏览选择…」** 指定工作电脑上要同步的文件夹，然后点击右上角 **「连接隧道并同步」** 即可开始双向实时同步！
`, sharedToken, shareCode)

	// Build 1: SyncT-WorkPC-Client-UserMode-Portable.zip
	workZipPath := filepath.Join(releaseDir, "SyncT-WorkPC-Client-UserMode-Portable.zip")
	if err := createZipArchive(workZipPath, func(zw *zip.Writer) error {
		if err := addFileToZip(zw, exePath, "SyncT-WorkPC-Client/SyncT.exe"); err != nil {
			return err
		}
		if err := addBytesToZip(zw, "SyncT-WorkPC-Client/data/config.json", clientCfgBytes); err != nil {
			return err
		}
		if err := addBytesToZip(zw, "SyncT-WorkPC-Client/使用说明-工作电脑User权限版.md", []byte(guideContent)); err != nil {
			return err
		}
		if err := addBytesToZip(zw, "SyncT-WorkPC-Client/SyncFolder/.keep", []byte("")); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("create WorkPC zip failed: %v", err)
	}

	// Build 2: SyncT-Host-Server-Portable.zip (uses system's already-running cloudflared)
	hostZipPath := filepath.Join(releaseDir, "SyncT-Host-Server-Portable.zip")
	if err := createZipArchive(hostZipPath, func(zw *zip.Writer) error {
		if err := addFileToZip(zw, exePath, "SyncT-Host-Server/SyncT.exe"); err != nil {
			return err
		}
		if err := addBytesToZip(zw, "SyncT-Host-Server/data/config.json", hostCfgBytes); err != nil {
			return err
		}
		if err := addBytesToZip(zw, "SyncT-Host-Server/使用说明-本机服务端.md", []byte(guideContent)); err != nil {
			return err
		}
		if err := addBytesToZip(zw, "SyncT-Host-Server/SyncFolder/.keep", []byte("")); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("create Host zip failed: %v", err)
	}

	// Build 3: Combined Master Release ZIP (SyncT-Windows11-Portable-Release.zip)
	masterZipPath := filepath.Join(releaseDir, "SyncT-Windows11-Portable-Release.zip")
	if err := createZipArchive(masterZipPath, func(zw *zip.Writer) error {
		if err := addBytesToZip(zw, "使用说明-本机与工作电脑配置指南.md", []byte(guideContent)); err != nil {
			return err
		}
		// Host folder
		if err := addFileToZip(zw, exePath, "SyncT-Host-Server/SyncT.exe"); err != nil {
			return err
		}
		if err := addBytesToZip(zw, "SyncT-Host-Server/data/config.json", hostCfgBytes); err != nil {
			return err
		}
		if err := addBytesToZip(zw, "SyncT-Host-Server/SyncFolder/.keep", []byte("")); err != nil {
			return err
		}
		// Work PC Client folder
		if err := addFileToZip(zw, exePath, "SyncT-WorkPC-Client/SyncT.exe"); err != nil {
			return err
		}
		if err := addBytesToZip(zw, "SyncT-WorkPC-Client/data/config.json", clientCfgBytes); err != nil {
			return err
		}
		if err := addBytesToZip(zw, "SyncT-WorkPC-Client/SyncFolder/.keep", []byte("")); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("create Master release zip failed: %v", err)
	}
}

func createZipArchive(zipPath string, populate func(zw *zip.Writer) error) error {
	f, err := os.OpenFile(zipPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	zw := zip.NewWriter(f)
	if err := populate(zw); err != nil {
		_ = zw.Close()
		return err
	}
	return zw.Close()
}

func addFileToZip(zw *zip.Writer, srcPath, zipEntryPath string) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()

	info, err := src.Stat()
	if err != nil {
		return err
	}
	hdr, err := zip.FileInfoHeader(info)
	if err != nil {
		return err
	}
	hdr.Name = zipEntryPath
	hdr.Method = zip.Deflate

	w, err := zw.CreateHeader(hdr)
	if err != nil {
		return err
	}
	_, err = io.Copy(w, src)
	return err
}

func addBytesToZip(zw *zip.Writer, zipEntryPath string, content []byte) error {
	hdr := &zip.FileHeader{
		Name:   zipEntryPath,
		Method: zip.Deflate,
	}
	w, err := zw.CreateHeader(hdr)
	if err != nil {
		return err
	}
	_, err = w.Write(content)
	return err
}
