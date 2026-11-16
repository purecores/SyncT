package tunnel

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/egoist/mygo/netproxy"

	"synct/internal/config"
)

// TunnelState describes the current status of the Cloudflared tunnel.
type TunnelState string

const (
	TunnelStopped  TunnelState = "未启动"
	TunnelStarting TunnelState = "正在建立隧道"
	TunnelRunning  TunnelState = "隧道已就绪"
	TunnelDirectOn TunnelState = "局域网直连已就绪"
	TunnelError    TunnelState = "隧道异常"
)

var tryCloudflareRegex = regexp.MustCompile(`https://[a-zA-Z0-9-]+\.trycloudflare\.com`)

// SystemProcessInfo describes an already-running system `cloudflared` process/service.
type SystemProcessInfo struct {
	Running bool
	PIDs    []uint32
	Summary string
}

// DetectSystemCloudflared checks whether `cloudflared` is already running on this machine
// (e.g. as a Windows Service or background daemon) without spawning any child process.
func DetectSystemCloudflared() SystemProcessInfo {
	pids := findRunningCloudflaredPIDs()
	if len(pids) == 0 {
		return SystemProcessInfo{
			Running: false,
			Summary: "未检测到系统运行中的 cloudflared 进程",
		}
	}
	parts := make([]string, len(pids))
	for i, pid := range pids {
		parts[i] = fmt.Sprintf("%d", pid)
	}
	return SystemProcessInfo{
		Running: true,
		PIDs:    pids,
		Summary: fmt.Sprintf("系统 Cloudflared 正在运行 (PID: %s)", strings.Join(parts, ", ")),
	}
}

// Manager manages the portable or system `cloudflared` process and its output stream.
type Manager struct {
	mu         sync.RWMutex
	state      TunnelState
	pubURL     string
	lanURL     string
	lastErr    string
	binPath    string
	version    string
	sysRunning bool
	sysPIDs    []uint32
	logs       []string
	onUpdate   func()
	onReady    func(publicURL string)

	cmdCancel   context.CancelFunc
	downloading atomic.Bool
	dlProgress  atomic.Int64 // 0..1000
}

// NewManager creates a Cloudflared tunnel manager.
// To respect low-privilege User environments on work PCs, it does not spawn any child process
// at construction time. On the Host machine, it automatically detects the running system `cloudflared`.
func NewManager(customPath string, onUpdate func(), onReady func(string)) *Manager {
	m := &Manager{
		state:    TunnelStopped,
		onUpdate: onUpdate,
		onReady:  onReady,
	}
	sysInfo := DetectSystemCloudflared()
	bin := FindBinary(customPath)
	if sysInfo.Running {
		m.sysRunning = true
		m.sysPIDs = sysInfo.PIDs
		if bin != "" {
			m.binPath = bin
		} else {
			m.binPath = "系统运行中服务/进程 (cloudflared.exe)"
		}
		m.version = sysInfo.Summary
	} else if bin != "" {
		m.binPath = bin
		m.version = "系统/便携组件已就绪 (" + filepath.Base(bin) + ")"
	}
	return m
}

// SystemStatus returns the latest detected running state of the system's `cloudflared` process.
func (m *Manager) SystemStatus() SystemProcessInfo {
	info := DetectSystemCloudflared()
	m.mu.Lock()
	m.sysRunning = info.Running
	m.sysPIDs = info.PIDs
	if info.Running {
		if m.binPath == "" {
			m.binPath = "系统运行中服务/进程 (cloudflared.exe)"
		}
		m.version = info.Summary
	}
	m.mu.Unlock()
	return info
}

// RefreshBinary searches for a running system `cloudflared` process first, then `cloudflared.exe` paths.
func (m *Manager) RefreshBinary(customPath string) (string, string) {
	sysInfo := DetectSystemCloudflared()
	bin := FindBinary(customPath)
	ver := ""
	if sysInfo.Running {
		ver = sysInfo.Summary
		if bin == "" {
			bin = "系统运行中服务/进程 (cloudflared.exe)"
		}
	} else if bin != "" {
		ver = queryVersion(bin)
	}
	m.mu.Lock()
	m.sysRunning = sysInfo.Running
	m.sysPIDs = sysInfo.PIDs
	m.binPath = bin
	m.version = ver
	m.mu.Unlock()
	m.notify()
	return bin, ver
}

// FindBinary locates `cloudflared` in portable folders first, then PATH and Windows install dirs.
func FindBinary(customPath string) string {
	candidates := make([]string, 0, 10)
	if strings.TrimSpace(customPath) != "" {
		candidates = append(candidates, strings.TrimSpace(customPath))
	}
	exeName := "cloudflared"
	if runtime.GOOS == "windows" {
		exeName = "cloudflared.exe"
	}

	// 1. Portable locations next to SyncT
	candidates = append(candidates,
		filepath.Join(config.BinDir(), exeName),
		filepath.Join(config.PortableRoot(), exeName),
	)

	// 2. System PATH
	if p, err := exec.LookPath(exeName); err == nil && p != "" {
		candidates = append(candidates, p)
	}
	if p, err := exec.LookPath("cloudflared"); err == nil && p != "" {
		candidates = append(candidates, p)
	}

	// 3. Common Windows 11 installation locations
	if runtime.GOOS == "windows" {
		if localAppData := os.Getenv("LOCALAPPDATA"); localAppData != "" {
			candidates = append(candidates,
				filepath.Join(localAppData, "Microsoft", "WinGet", "Links", "cloudflared.exe"),
			)
		}
		candidates = append(candidates,
			`C:\Program Files (x86)\cloudflared\cloudflared.exe`,
			`C:\Program Files\cloudflared\cloudflared.exe`,
		)
	}

	for _, c := range candidates {
		if c == "" {
			continue
		}
		if info, err := os.Stat(c); err == nil && !info.IsDir() {
			return c
		}
	}
	return ""
}

func queryVersion(binPath string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binPath, "--version")
	configureCmdPlatform(cmd)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "已检测到可执行文件"
	}
	line := strings.TrimSpace(string(out))
	if idx := strings.IndexAny(line, "\r\n"); idx >= 0 {
		line = line[:idx]
	}
	return line
}

// BinaryInfo returns the detected `cloudflared` path and version string.
func (m *Manager) BinaryInfo() (string, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.binPath, m.version
}

// State returns the current TunnelState, public URL, LAN URL, and last error.
func (m *Manager) State() (TunnelState, string, string, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state, m.pubURL, m.lanURL, m.lastErr
}

// Logs returns a copy of the Cloudflared process logs.
func (m *Manager) Logs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, len(m.logs))
	copy(out, m.logs)
	return out
}

// LogCount returns the number of buffered log lines without copying them.
func (m *Manager) LogCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.logs)
}

// ClearLogs clears the tunnel output log buffer.
func (m *Manager) ClearLogs() {
	m.mu.Lock()
	m.logs = nil
	m.mu.Unlock()
	m.notify()
}

// Start attaches to the system's already-running `cloudflared` (TunnelSystem) or launches
// a managed tunnel in Quick, Token, or Direct mode for `localPort` (`https://localhost:18920`).
func (m *Manager) Start(cfg config.Config, localPort int) error {
	m.Stop()

	lanIP := detectLANIP()
	lanURL := fmt.Sprintf("https://localhost:%d", localPort)
	if cfg.TunnelMode == config.TunnelDirect && lanIP != "" {
		// LAN peers cannot validate the localhost-only self-signed certificate,
		// so advertise the HTTP endpoint the dual-protocol origin also serves.
		lanURL = fmt.Sprintf("http://%s:%d", lanIP, localPort)
	}

	if cfg.TunnelMode == config.TunnelDirect {
		m.mu.Lock()
		m.state = TunnelDirectOn
		m.lanURL = lanURL
		m.pubURL = lanURL
		m.lastErr = ""
		m.mu.Unlock()
		m.appendLog(fmt.Sprintf("[直连模式] 本地/局域网访问地址已就绪: %s", lanURL))
		m.notify()
		if m.onReady != nil {
			m.onReady(lanURL)
		}
		return nil
	}

	// Reuse system's already-running Cloudflared when in TunnelSystem mode,
	// or when in TunnelToken mode with no separate token specified (or when system cloudflared is already running).
	sysInfo := m.SystemStatus()
	if cfg.TunnelMode == config.TunnelSystem || (cfg.TunnelMode == config.TunnelToken && strings.TrimSpace(cfg.TunnelToken) == "") {
		pubURL := config.NormalizeRemoteURL(cfg.CustomPublicURL)
		if pubURL == "" {
			pubURL = config.DefaultPublicURL
		}

		ctx, cancel := context.WithCancel(context.Background())
		m.mu.Lock()
		m.cmdCancel = cancel
		m.lanURL = lanURL
		m.pubURL = pubURL
		m.lastErr = ""
		if sysInfo.Running {
			m.state = TunnelRunning
		} else {
			// Do not claim a healthy tunnel when no connector process exists:
			// the public hostname answers 502 until cloudflared comes back.
			m.state = TunnelStarting
			m.lastErr = "未检测到系统 cloudflared 进程，公网入口当前会返回 502"
		}
		m.mu.Unlock()

		if sysInfo.Running {
			m.appendLog(fmt.Sprintf("[系统隧道] 已复用本机正在运行的 Cloudflared (%s)", sysInfo.Summary))
			m.appendLog(fmt.Sprintf("[映射就绪] %s -> %s (无需重复启动子进程)", pubURL, lanURL))
		} else {
			m.appendLog(fmt.Sprintf("[系统隧道] 本地端点 %s 已就绪，但未检测到系统 cloudflared 进程；启动服务前 %s 会返回 502", lanURL, pubURL))
		}
		m.notify()
		if m.onReady != nil {
			m.onReady(pubURL)
		}

		// Monitor system cloudflared process status in background without touching or stopping it
		go m.monitorSystemProcess(ctx, pubURL, lanURL)
		return nil
	}

	bin, _ := m.RefreshBinary(cfg.CloudflaredPath)
	if bin == "" || strings.HasPrefix(bin, "系统运行中") {
		err := errors.New("未找到独立的 cloudflared.exe，可切换为「复用系统已运行的 Cloudflared」模式")
		m.mu.Lock()
		m.state = TunnelError
		m.lastErr = err.Error()
		m.mu.Unlock()
		m.appendLog("[错误] " + err.Error())
		m.notify()
		return err
	}

	var args []string
	switch cfg.TunnelMode {
	case config.TunnelQuick:
		args = []string{
			"tunnel",
			"--url", fmt.Sprintf("https://localhost:%d", localPort),
			"--no-tls-verify",
			"--no-autoupdate",
			"--metrics", "127.0.0.1:0",
		}
	case config.TunnelToken:
		tok := strings.TrimSpace(cfg.TunnelToken)
		if tok == "" {
			err := errors.New("Token Tunnel 模式需要填写 Cloudflare 隧道 Token (用于绑定 synctreei.suosmanthus.cyou -> https://localhost:18920)")
			m.mu.Lock()
			m.state = TunnelError
			m.lastErr = err.Error()
			m.mu.Unlock()
			m.appendLog("[提示] " + err.Error())
			m.notify()
			return err
		}
		args = []string{
			"tunnel",
			"--no-autoupdate",
			"--metrics", "127.0.0.1:0",
			"run",
			"--no-tls-verify",
			"--token", tok,
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.cmdCancel = cancel
	m.state = TunnelStarting
	m.lanURL = lanURL
	m.pubURL = ""
	if cfg.TunnelMode == config.TunnelToken && strings.TrimSpace(cfg.CustomPublicURL) != "" {
		m.pubURL = config.NormalizeRemoteURL(cfg.CustomPublicURL)
	}
	m.lastErr = ""
	m.mu.Unlock()
	m.notify()

	// Run cloudflared under a supervisor: a crashed connector produces 502 on
	// the public hostname until someone notices, so restart it with backoff.
	go m.superviseTunnel(ctx, bin, args, cfg)
	return nil
}

// superviseTunnel keeps one managed cloudflared process alive until Stop.
func (m *Manager) superviseTunnel(ctx context.Context, bin string, args []string, cfg config.Config) {
	backoff := time.Second
	started := false
	for ctx.Err() == nil {
		if started {
			if cfg.TunnelMode == config.TunnelQuick {
				m.mu.Lock()
				m.pubURL = ""
				m.mu.Unlock()
			}
			m.appendLog(fmt.Sprintf("[重连] %s 后重新启动 Cloudflared 隧道…", backoff.Round(time.Second)))
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}
		started = true

		began := time.Now()
		m.runTunnelProcess(ctx, bin, args, cfg)
		if ctx.Err() != nil {
			return
		}
		m.mu.Lock()
		if m.lastErr == "" {
			m.lastErr = "cloudflared 进程已意外结束，正在自动重启"
		}
		m.state = TunnelError
		m.mu.Unlock()
		m.appendLog("[退出] Cloudflared 隧道进程已退出，将自动重启")
		m.notify()

		// A process that ran for a while is not crash-looping; retry promptly.
		if time.Since(began) > time.Minute {
			backoff = time.Second
		} else if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// runTunnelProcess starts one cloudflared process and blocks until it exits.
func (m *Manager) runTunnelProcess(ctx context.Context, bin string, args []string, cfg config.Config) {
	cmd := exec.CommandContext(ctx, bin, args...)
	configureCmdPlatform(cmd)

	certPath, _ := config.LocalCertPaths()
	env := append(os.Environ(), "NO_TLS_VERIFY=true")
	if _, statErr := os.Stat(certPath); statErr == nil {
		// cloudflared expects a directory of PEM files here, not a single file.
		env = append(env, "TUNNEL_ORIGIN_CA_POOL="+filepath.Dir(certPath))
	}
	cmd.Env = env

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		m.appendLog(fmt.Sprintf("[错误] 无法读取 cloudflared 输出: %v", err))
		return
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		m.appendLog(fmt.Sprintf("[错误] 无法读取 cloudflared 错误输出: %v", err))
		return
	}

	if err := cmd.Start(); err != nil {
		m.mu.Lock()
		m.state = TunnelError
		m.lastErr = fmt.Sprintf("启动 cloudflared 失败: %v", err)
		m.mu.Unlock()
		m.appendLog(fmt.Sprintf("[错误] 启动 cloudflared 失败: %v", err))
		m.notify()
		return
	}

	m.mu.Lock()
	if ctx.Err() == nil {
		m.state = TunnelStarting
		m.lastErr = ""
	}
	m.mu.Unlock()
	m.appendLog(fmt.Sprintf("[启动] %s %s", filepath.Base(bin), strings.Join(maskTokenArgs(args), " ")))
	m.notify()

	go m.scanPipe(ctx, stdoutPipe, cfg)
	go m.scanPipe(ctx, stderrPipe, cfg)

	waitErr := cmd.Wait()
	if ctx.Err() != nil {
		return
	}
	m.mu.Lock()
	if waitErr != nil {
		m.lastErr = fmt.Sprintf("cloudflared 进程退出: %v", waitErr)
	} else {
		m.lastErr = "cloudflared 进程已意外结束"
	}
	m.mu.Unlock()
}

func (m *Manager) scanPipe(ctx context.Context, r io.Reader, cfg config.Config) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		if ctx.Err() != nil {
			return
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		m.appendLog(line)

		if cfg.TunnelMode == config.TunnelQuick {
			if match := tryCloudflareRegex.FindString(line); match != "" && !strings.Contains(match, "api.trycloudflare.com") {
				m.mu.Lock()
				alreadySet := m.pubURL != ""
				m.pubURL = match
				if m.state != TunnelStopped {
					m.state = TunnelRunning
					m.lastErr = ""
				}
				m.mu.Unlock()
				if !alreadySet {
					m.appendLog("[就绪] 公网临时隧道已分配: " + match)
					m.notify()
					if m.onReady != nil {
						m.onReady(match)
					}
				}
			}
		} else if cfg.TunnelMode == config.TunnelToken {
			if strings.Contains(line, "Registered tunnel connection") || strings.Contains(line, "Updated to new configuration") {
				m.mu.Lock()
				wasRunning := m.state == TunnelRunning
				if m.state != TunnelStopped {
					m.state = TunnelRunning
					m.lastErr = ""
				}
				if m.pubURL == "" && strings.TrimSpace(cfg.CustomPublicURL) != "" {
					m.pubURL = config.NormalizeRemoteURL(cfg.CustomPublicURL)
				}
				pub := m.pubURL
				m.mu.Unlock()
				if !wasRunning {
					m.appendLog("[就绪] Cloudflare Token 固定隧道已连接成功")
					m.notify()
					if m.onReady != nil && pub != "" {
						m.onReady(pub)
					}
				}
			}
		}
	}
}

// monitorSystemProcess periodically checks the running system `cloudflared` process
// without ever stopping or interfering with it.
func (m *Manager) monitorSystemProcess(ctx context.Context, pubURL, lanURL string) {
	wasRunning := DetectSystemCloudflared().Running
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			info := m.SystemStatus()
			if info.Running && !wasRunning {
				wasRunning = true
				m.mu.Lock()
				if m.state != TunnelStopped {
					m.state = TunnelRunning
					m.lastErr = ""
				}
				m.mu.Unlock()
				m.appendLog(fmt.Sprintf("[系统隧道] 检测到系统 Cloudflared 已恢复运行 (%s) · %s -> %s", info.Summary, pubURL, lanURL))
				m.notify()
			} else if !info.Running && wasRunning {
				wasRunning = false
				m.mu.Lock()
				if m.state != TunnelStopped {
					m.state = TunnelStarting
					m.lastErr = "系统 cloudflared 进程已退出，公网入口将返回 502"
				}
				m.mu.Unlock()
				m.appendLog(fmt.Sprintf("[提示] 系统 Cloudflared 进程已退出：公网 %s 将返回 502 (本地 %s 仍保持监听)", pubURL, lanURL))
				m.notify()
			}
		}
	}
}

// Stop terminates a child `cloudflared` process if SyncT started one,
// or detaches cleanly from the system's already-running `cloudflared` without stopping it.
func (m *Manager) Stop() {
	m.mu.Lock()
	cancel := m.cmdCancel
	m.cmdCancel = nil
	m.state = TunnelStopped
	m.pubURL = ""
	m.lanURL = ""
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	m.notify()
}

// IsDownloading reports whether a portable cloudflared binary download is in progress and its progress (0..1).
func (m *Manager) IsDownloading() (bool, float64) {
	if !m.downloading.Load() {
		return false, 0
	}
	return true, float64(m.dlProgress.Load()) / 1000.0
}

// DownloadPortableBinary downloads the official `cloudflared-windows-amd64.exe` into `./bin/cloudflared.exe`.
func (m *Manager) DownloadPortableBinary(onDone func(path string, err error)) {
	if !m.downloading.CompareAndSwap(false, true) {
		return
	}
	m.dlProgress.Store(10)
	m.notify()

	go func() {
		defer func() {
			m.downloading.Store(false)
			m.notify()
		}()

		dlURL := "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe"
		targetPath := filepath.Join(config.BinDir(), "cloudflared.exe")
		tmpPath := targetPath + ".download"

		m.appendLog("[下载] 正在下载便携版 cloudflared.exe: " + dlURL)
		client := &http.Client{
			Transport: netproxy.Transport(),
			Timeout:   10 * time.Minute,
		}
		resp, err := client.Get(dlURL)
		if err != nil {
			m.appendLog(fmt.Sprintf("[错误] 下载失败: %v", err))
			if onDone != nil {
				onDone("", err)
			}
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			err := fmt.Errorf("HTTP %d", resp.StatusCode)
			m.appendLog(fmt.Sprintf("[错误] 下载返回状态异常: %v", err))
			if onDone != nil {
				onDone("", err)
			}
			return
		}

		out, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
		if err != nil {
			m.appendLog(fmt.Sprintf("[错误] 无法创建下载临时文件: %v", err))
			if onDone != nil {
				onDone("", err)
			}
			return
		}

		total := resp.ContentLength
		var written int64
		buf := make([]byte, 64*1024)
		for {
			n, rerr := resp.Body.Read(buf)
			if n > 0 {
				if _, werr := out.Write(buf[:n]); werr != nil {
					_ = out.Close()
					_ = os.Remove(tmpPath)
					m.appendLog(fmt.Sprintf("[错误] 写入 cloudflared.exe 失败: %v", werr))
					if onDone != nil {
						onDone("", werr)
					}
					return
				}
				written += int64(n)
				if total > 0 {
					m.dlProgress.Store(written * 1000 / total)
					m.notify()
				}
			}
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				_ = out.Close()
				_ = os.Remove(tmpPath)
				m.appendLog(fmt.Sprintf("[错误] 下载中断: %v", rerr))
				if onDone != nil {
					onDone("", rerr)
				}
				return
			}
		}
		_ = out.Close()
		if total > 0 && written != total {
			err := fmt.Errorf("下载不完整: %d/%d 字节", written, total)
			_ = os.Remove(tmpPath)
			m.appendLog("[错误] " + err.Error())
			if onDone != nil {
				onDone("", err)
			}
			return
		}

		_ = os.Remove(targetPath)
		if err := os.Rename(tmpPath, targetPath); err != nil {
			_ = os.Remove(tmpPath)
			m.appendLog(fmt.Sprintf("[错误] 保存 cloudflared.exe 失败: %v", err))
			if onDone != nil {
				onDone("", err)
			}
			return
		}

		m.RefreshBinary(targetPath)
		m.appendLog("[完成] 便携版 cloudflared.exe 已保存至: " + targetPath)
		if onDone != nil {
			onDone(targetPath, nil)
		}
	}()
}

func (m *Manager) appendLog(line string) {
	stamp := time.Now().Format("15:04:05")
	entry := fmt.Sprintf("[%s] %s", stamp, line)
	m.mu.Lock()
	m.logs = append(m.logs, entry)
	if len(m.logs) > 300 {
		// Trim in place: no per-line allocation once the buffer is full.
		copy(m.logs, m.logs[len(m.logs)-300:])
		m.logs = m.logs[:300]
	}
	m.mu.Unlock()
	m.notify()
}

func (m *Manager) notify() {
	if m.onUpdate != nil {
		m.onUpdate()
	}
}

func maskTokenArgs(args []string) []string {
	out := make([]string, len(args))
	copy(out, args)
	for i := 0; i < len(out)-1; i++ {
		if out[i] == "--token" && len(out[i+1]) > 8 {
			out[i+1] = out[i+1][:4] + "****" + out[i+1][len(out[i+1])-4:]
		}
	}
	return out
}

func detectLANIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, a := range addrs {
		ipNet, ok := a.(*net.IPNet)
		if !ok || ipNet.IP.IsLoopback() {
			continue
		}
		ip4 := ipNet.IP.To4()
		if ip4 == nil {
			continue
		}
		if ip4[0] == 169 && ip4[1] == 254 {
			continue
		}
		return ip4.String()
	}
	return ""
}
