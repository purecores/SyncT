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
   - **左下角实时吞吐面板**：侧边栏左下角与底部状态栏常驻展示实时上传/下载速率 (`↑` / `↓`)、文件与文件夹数量、总容量以及尚未同步的文件数。

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

## v1.2.0 界面与同步行为

- **主界面重做**：概览页只保留一块实时吞吐读数与 30 秒速率轨迹、四项关键指标（受控文件 / 未同步 / 映射目录 / 累计流量），以及同步文件夹与连接状态两张卡片。原先堆在概览里的隧道表单、密钥、端口、分享码和多文件夹编辑器全部收进「设置」页，按「文件夹与方向」「工作模式与连接」「文件规则」「运行与后台」「多文件夹与跨机映射」分组折叠。顶栏只留开始/停止、扫描与「更多」菜单。
- **速率真实**：速率不再按「单个文件传输耗时」折算（小文件会显示成数 GB/s 的虚高值），而是统计实际流过的字节在 2 秒滑动窗口内的速率；传输中持续更新，空闲约 3 秒后归零。
- **界面刷新**：引擎运行期间每 250ms 请求一帧，状态栏与概览的速率、未同步数量随手更新，不再等下一次文件传完。
- **未同步文件页**：侧边栏新增「未同步」，列出待同步、上传/下载中、冲突与失败的文件，可直接定位到资源管理器；状态栏同时显示未同步数量。主机端直接提供文件下载，因此主机本地文件视为已同步；可向对端上传的客户端会把新文件标为「待同步」，确认写入远端后转为「已同步」。
- **空文件夹同步**：清单新增目录列表，主机与节点都会创建对端存在的空文件夹（含多级嵌套）；新建空文件夹还会通过 SSE / `POST /api/dirs` 实时推送。
- **后台提示开关**：状态栏右下角新增「后台提示」开关，设置页「运行与后台」中也有同一开关，控制关闭主窗口隐藏到托盘时是否弹出一条系统通知（默认关闭）。
- **传输效率**：对账与批量上传最多 4 个文件并发传输，HTTP 连接复用（`MaxIdleConnsPerHost 8`）并关闭传输层压缩；扫描后预取变更文件的 SHA-256，下一轮扫描与上传直接复用摘要，避免重复读盘与重复哈希。

## 502 Bad Gateway 排查与自愈

工作电脑报「远端返回状态码 502」表示 **Cloudflare 边缘已连上隧道，但 cloudflared 无法从主机端取得响应**。SyncT 现在会在主机端自动区分并处理两类原因：

1. **主机端 SyncT 未运行或本地端口未监听**：客户端错误信息会直接提示「主机端 SyncT 可能未运行，或 Cloudflared 无法连接本地源站」；主机端每 15 秒自检本地源站（`http://127.0.0.1:18920`），监听意外失效时自动重建。固定端口不再静默改用随机端口——端口被占用会明确报错，因为随机端口必然导致公网 502。
2. **cloudflared 拒绝本地自签名证书**（`x509: certificate signed by unknown authority`）：本地证书只对回环服务有效，Cloudflare 托管隧道的源站若配置为 `https://localhost:18920` 且未开启 `No TLS Verify`，cloudflared 会拒绝自签名证书。主机端会读取本地 TLS 握手错误并在日志/概览中提示，工具栏出现 **「修复 502 (信任本地证书)」** 按钮：点击后以管理员权限把 `./data/localhost-cert.pem` 安装到「受信任的根证书颁发机构」并重启 `cloudflared` 服务。也可以不改本机证书，直接在 Cloudflare Zero Trust 控制台把该隧道的源站改为 `http://localhost:18920`，或在源站设置中开启 `No TLS Verify`。

主机端每 60 秒自动探测一次公网地址，502 恢复后会自动清除告警。建议在主机端设置中开启 **「自动启动同步」**（默认已开启）和 **「开机自动运行」**，保证登录 Windows 后源站与隧道始终在线。

## 性能与可靠性优化

- **增量文件夹刷新**：每个映射目录独立增量扫描；未变化文件按 `(大小, 毫秒级修改时间)` 复用已持久化的 SHA-256，不再重复读取文件内容，也不会为一次目录变更重建整张索引。
- **更低延迟的实时监听**：使用 MyGo `HintsOnly` 目录提示（无逐文件句柄/快照，适合大目录），80~400ms 去抖后再合并 180ms 扫描窗口；新增/移除/修改同步目录时自动重建监听器，监听失败自动回退轮询并定期重连。
- **克制内存**：上传与下载全程流式传输（不再把整个文件读入内存）；状态索引写入 500ms 去抖合并；日志缓冲区原地裁剪；删除墓碑保留 30 天且最多 10000 条；UI 每帧的统计读取为 O(1)，文件列表与筛选/排序结果按索引版本缓存。
- **全面的错误处理**：断开的盘符、被占用的文件、不可读子目录或已迁移/重命名的同步目录不会被误判为删除（不生成墓碑、不清空索引、也不会自动重建空目录）；传输进行中的文件在整个传输期间受保护，不会被轮询扫描误删；损坏的 `config.json` / `state.json` 自动备份后恢复默认；上传/下载校验 SHA-256，失败会标记状态并在下次扫描重试；SSE 空闲 60 秒自动重连，事件缓冲区满时自动触发全量对账；冲突副本保留失败会记录日志；重复错误按时间窗口限流。
- **避免覆盖较新的本地修改**：SSE 收到过期的 upsert/delete 事件时先比较本地修改时间/墓碑时间，交由常规双向对账决定方向，而不是直接覆盖；已停用的文件夹映射不会再被远端写入。
- **一致的文件列表缓存**：文件条目与其版本号在同一把锁内返回，避免 UI 把旧列表固定在一个新版本号下而长时间不刷新。
- **Windows 路径安全**：拒绝 `..` 路径穿越；符号链接/Junction 作为同步根目录时可正确遍历（不会误判为空目录），嵌套链接不会被跟随；清理空目录时严格校验根目录边界。
- **索引存储选型**：状态索引继续使用「去抖 + 原子重写」的紧凑 JSON，而不是 SQLite DLL。扫描/对账必须在内存中持有完整清单，SQLite 不会降低内存占用，却会引入 DLL 依赖、破坏 `CGO_ENABLED=0` 单文件便携构建；实测 2 万个文件时状态仅 3.9MB，保存 19ms、加载 25ms，远低于去抖窗口。若未来索引规模达到十万级以上，再考虑增量日志或嵌入式存储。

## 编译与打包命令

一键生成图标资源、编译 `SyncT.exe` 并打包全部 Release ZIP：

```bash
cmd /c build-portable.bat
```

或使用 Go 命令执行完整测试与打包：

```bash
CGO_ENABLED=0 go test -count=1 -v ./...
```

补充测试命令（数据竞争检测与性能基准）：

```bash
CGO_ENABLED=1 go test -race -count=1 ./...
go test ./internal/syncer -run '^$' -bench . -benchmem
```
