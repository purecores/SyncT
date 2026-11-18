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
	hostCfg.SyncDir = config.PreferredSyncDir
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
	clientCfg.SyncDir = config.PreferredSyncDir
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
2. 双击 `+"`SyncT.exe`"+`（已预先配好 **节点模式**、域名 `+"`https://synctreei.suosmanthus.cyou`"+` 及与本机一致的 **访问密钥**）。默认同步目录为 `+"`D:\\Code\\Syncwork`"+`，两台电脑相同；该路径不可用时回退到程序目录下的 `+"`SyncFolder`"+`。
3. 点击 **「浏览选择…」** 指定工作电脑上要同步的文件夹，然后点击右上角 **「连接同步」** 即可开始双向实时同步！

---

## 4. 本版界面与同步行为变化

- **更清爽的主界面**：概览页只保留实时吞吐、受控文件/未同步/映射目录/累计流量四项指标，以及同步文件夹与连接状态；全部配置收进「设置」页的可折叠分组（文件夹与方向、工作模式与连接、文件规则、运行与后台、多文件夹与跨机映射）。
- **实时速率真实可信**：上传/下载速率改为按实际流过的字节做 2 秒滑动窗口统计，大文件传输过程中持续更新，传输结束约 3 秒内回落到 0，不再出现「瞬间爆表」的虚高数值。
- **1 秒刷新**：运行期间每 250ms 刷新一次界面，状态栏与概览的速率、未同步数量随传输即时变化。
- **未同步文件**：侧边栏新增「未同步」页，列出待同步、传输中、冲突与失败的文件；状态栏同时显示未同步数量。主机端直接提供文件下载，因此主机本地文件视为已同步；可向对端上传的客户端会先标记「待同步」，确认后转为「已同步」。
- **空文件夹同步**：会在两端之间同步空文件夹（含多级嵌套），新增空文件夹也会通过实时事件推送，无需等待下一次对账。
- **状态栏提示开关**：右下角新增「后台提示」开关，控制关闭主窗口时是否在通知中心提示仍在后台同步；设置页「运行与后台」中有同名开关，二者是同一配置。
- **传输效率**：小文件使用 64 个并发传输槽位、大文件使用 4 个；扫描时并行计算新/变更文件的 SHA-256，扫描结果直接用于上传，减少重复读盘与重复哈希，并复用 HTTP 连接、关闭传输层压缩。
- **大文件分块续传**：超过 8MB 的文件分块上传，避开 Cloudflare 的单请求 100MB 上限与约 100 秒的 524 超时；遇到更低的边缘上限会自动缩小分块重试，断线从中断处续传。
- **日志按批聚合**：一次同步上千个文件只写一行汇总，停止同步不再刷出大量 context canceled 告警，边缘错误页会压缩成一行说明。
- **冲突与恢复更稳**：下载摘要不符会先重试一次，重试不再生成重复的 .conflict- 副本；从备份恢复的文件按「本端首次看到时间」判断，不会被对端较新的删除记录误删。
- **大文件不再卡住**：移除了传输的固定 45 秒响应头超时（大文件慢速上传会被误判为超时），改为每个请求自己的期限 + 3 分钟停滞看门狗；1MB 以上的文件都走分块上传，每块 4MB，遇边缘限制自动减半。
- **文字可选中复制**：整个窗口的文字都可以用鼠标选中、Ctrl+A 全选、Ctrl+C 复制。
- **日志文件**：程序同级目录的 SyncT.log，上限 2MB，超过后重新开始。
- **同步密码**：两端输入同一密码即可派生出相同的访问密钥（PBKDF2-SHA256 12 万轮），密码本身不保存。
- **默认目录**：两端默认 D:\Code\Syncwork。
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
