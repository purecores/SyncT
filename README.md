# SyncT — Windows 11 个人双机专属实时文件夹同步工具 (Portable Release)

**SyncT** 是一款面向 **Windows 11** 平台构建的**纯原生单文件便携版 (Portable)** 实时文件夹同步桌面应用，底层基于 [MyGo](https://mygo.run/zh) 原生 UI 框架（无 CGO、无 WebView、直接编译为原生 Win32/Direct3D 11 硬件加速界面）与 **Cloudflare Tunnel (`cloudflared`)** 构建。

---

## 专属定制规格（本机服务端 <-> 工作电脑客户端）

1. **直接复用本机已运行的系统 Cloudflared (`synctreei.suosmanthus.cyou` -> `https://localhost:18920`)**
   - **复用系统隧道 (`TunnelSystem`)**：通过纯 Go Win32 `CreateToolhelp32Snapshot` 实时检测本机系统已在后台运行的 `cloudflared.exe` 服务/进程（无需提权、无需再次填写 Token、绝不重复启动第二个 `cloudflared` 进程，停止同步时也不会干扰系统正在运行的 `cloudflared` 服务）。
   - **专属公网域名**：默认内置绑定 `https://synctreei.suosmanthus.cyou`。
   - **本地监听端点**：服务端默认监听 **`https://localhost:18920`**（同时绑定 `127.0.0.1:18920` 与 `[::1]:18920` 回环接口）。
   - **双协议自适应 + 自签名 TLS 证书**：内建自动生成 `localhost` / `synctreei.suosmanthus.cyou` 的 ECDSA P-256 自签名 TLS 证书（保存在 `./data/localhost-cert.pem`），通过首字节嗅探在同一端口 `18920` 同时兼容 `https://localhost:18920` 与 `http://localhost:18920`。

2. **工作电脑 User 权限克制设计 (`asInvoker`)**
   - **内嵌 `asInvoker` Windows 权限清单 (`RT_MANIFEST`)**：PE 资源段中显式声明 `<requestedExecutionLevel level="asInvoker" uiAccess="false"/>`，在仅具普通 `User` 权限的工作电脑上运行**绝不弹出 UAC 管理员提权窗口**。
   - **工作电脑端零监听端口 & 零子进程**：工作电脑处于客户端模式（`RoleClient`），仅通过标准出站 HTTPS (`443`) 访问 `https://synctreei.suosmanthus.cyou`，不监听任何本地端口（绝不触发 Windows 防火墙管理员弹窗），也不需要启动或附带 `cloudflared.exe`。
   - **100% 便携目录隔离**：所有配置文件 (`config.json`)、状态索引 (`state.json`)、窗口尺寸状态 (`window-state.json`)、缓存与日志均通过 `mygo.App.SetPath` 强制重定向至程序同级的 `./data/` 目录，**零注册表写入、零 `%AppData%` 写入、零系统目录访问**。

3. **多文件夹同步与跨机目录映射 (`LocalPath` ↔ `RemotePath`)**
   - **同时同步多个独立目录**：支持在主目录之外添加任意数量的受控同步文件夹，按目录标签与逻辑映射 ID (`default`, `projects`, `docs` 等) 独立开启/关闭与检索。
   - **跨电脑异构目录映射**：支持指定本机某个文件夹同步到另一台电脑的不同目标文件夹路径（对端连接握手时自动识别映射 ID 并创建/绑定对应的目标文件夹）。
   - **左下角实时同步遥测面板**：侧边栏左下角与底部状态栏常驻展示实时上传/下载速率 (`↑` / `↓`)、受控文件总数与总容量、累计传输流量、活跃目录数以及最近同步的文件名与大小。

4. **专属应用图标 (`bailian-color.png`)**
   - 已将 `D:\Code\Nikki\light\bailian-color.png` 转换为多尺寸高清图标（`16px ~ 256px`）编译进 `rsrc_windows_amd64.syso`（EXE 文件图标、任务栏图标、窗口图标），同时内嵌至软件顶栏与 Windows 11 系统托盘图标。

---

## Release ZIP 发布包说明 (`./release/`)

| 发布包文件 | 适用电脑 | 内容说明 |
| :--- | :--- | :--- |
| `release/SyncT-Windows11-Portable-Release.zip` | **完整双机合集包** | 同时包含 `SyncT-Host-Server/`（本机服务端）、`SyncT-WorkPC-Client/`（工作电脑端）及双机配对说明 |
| `release/SyncT-Host-Server-Portable.zip` | **本机（服务端）** | 含 `SyncT.exe` 及预设为 `Host + 复用系统已运行的 Cloudflared + https://synctreei.suosmanthus.cyou + https://localhost:18920` 的配置（无需打包额外的 `cloudflared.exe`） |
| `release/SyncT-WorkPC-Client-UserMode-Portable.zip` | **工作电脑（User 权限客户端）** | 含 `SyncT.exe` 与已预先配对相同 `auth_token` 及 `https://synctreei.suosmanthus.cyou` 的客户端配置 |

---

## 编译与打包命令

一键生成图标资源、编译 `SyncT.exe` 并打包全部 Release ZIP：

```bash
cmd /c build-portable.bat
```

或使用 Go 命令执行完整测试与打包：

```bash
CGO_ENABLED=0 go test -count=1 -v ./...
```
