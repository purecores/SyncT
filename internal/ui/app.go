package ui

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/egoist/mygo"
	"github.com/egoist/mygo/ui"

	"synct/internal/config"
	"synct/internal/syncer"
	"synct/internal/tunnel"
)

// Sidebar section IDs
const (
	SectionOverview = "overview"
	SectionFiles    = "files"
	SectionPending  = "pending"
	SectionLogs     = "logs"
	SectionSettings = "settings"
)

// transferAccent marks downloads. Uploads use the theme accent (indigo, like
// the app icon's upper face); downloads use the icon's cyan so the two halves
// of the throughput trace read apart at a glance.
var transferAccent = ui.Hex("#0891b2")

// Select option labels
var (
	tunnelModeOptions = []string{
		"复用系统 Cloudflared (推荐)",
		"托管 Token 隧道",
		"临时 Quick Tunnel",
		"仅局域网直连",
	}
	syncDirOptions = []string{
		"双向实时同步",
		"仅发送 (本地 -> 远端)",
		"仅接收 (远端 -> 本地)",
	}
	conflictOptions = []string{
		"保留双方冲突副本",
		"以最新修改时间为准",
	}
)

// speedSample is one point of the throughput trace.
type speedSample struct {
	up   int64
	down int64
}

// App holds the native MyGo UI state and coordinates the sync engine and Cloudflared tunnel manager.
type App struct {
	cfg    config.Config
	engine *syncer.Engine
	tunnel *tunnel.Manager
	win    *mygo.Window

	// Navigation & Layout
	section       string
	navOpen       bool
	toolsOpen     bool
	roleIndex     int
	logTab        int
	portInput     string
	shareInput    string
	showToken     bool
	passwordInput string

	// statusBarNoticeSeen mirrors the status bar toggle so a click there is
	// persisted and confirmed, exactly as the settings page does.
	statusBarNoticeSeen bool

	// Settings disclosure sections
	settingsFoldersOpen  bool
	settingsConnOpen     bool
	settingsRulesOpen    bool
	settingsRuntimeOpen  bool
	settingsAdvancedOpen bool
	addFolderOpen        bool

	pendingToast string

	// Multi-folder mapping editor inputs
	newFolderID     string
	newFolderLabel  string
	newFolderLocal  string
	newFolderRemote string

	// File Table state
	fileSearch       string
	fileFolderFilter string
	fileFilterIdx    int
	fileTableState   ui.ListState
	fileSort         ui.SortOrder
	selectedFileRow  int

	// Unsynced files table state
	pendingTableState  ui.ListState
	pendingSort        ui.SortOrder
	selectedPendingRow int

	// Logs List state
	syncLogList   ui.ListState
	tunnelLogList ui.ListState

	// Confirmation Dialogs
	confirmClearLogsOpen bool
	confirmResetCfgOpen  bool

	// Power keep-awake release callback
	keepAwakeRelease func()

	// fileCache reuses the filtered/sorted file table across frames. The view
	// rebuilds only when the index generation or a filter actually changes.
	fileCache fileViewCache

	// Throughput trace: the last minute of sampled rates, kept in the view so
	// the dashboard stays useful after switching sections.
	speedHistory []speedSample
	lastSpeedAt  time.Time

	// binaryRefreshTimer debounces cloudflared --version probes while typing.
	binaryRefreshTimer *time.Timer

	// Optional hook for directory picker (allows deterministic testing)
	PickFolderFunc func(current string) (string, error)
	// OpenPathFunc/RevealPathFunc replace the OS shell in tests.
	OpenPathFunc     func(path string)
	RevealPathFunc   func(path string)
	ClipboardWriteFn func(text string)
}

type fileViewCache struct {
	files    []syncer.FileEntry
	version  uint64
	search   string
	filter   int
	sortCol  string
	sortDesc bool
	result   []syncer.FileEntry
	valid    bool
}

// NewApp constructs the SyncT application state.
func NewApp(cfg config.Config, statePath string) *App {
	cfg.Normalize()
	app := &App{
		cfg:                 cfg,
		section:             SectionOverview,
		navOpen:             true,
		toolsOpen:           true,
		settingsFoldersOpen: true,
		settingsConnOpen:    true,
		portInput:           strconv.Itoa(cfg.LocalPort),
		selectedFileRow:     -1,
		selectedPendingRow:  -1,
	}
	app.statusBarNoticeSeen = cfg.TrayCloseNotice
	if cfg.Role == config.RoleClient {
		app.roleIndex = 1
	}

	app.engine = syncer.NewEngine(cfg, statePath, app.requestRedraw)
	app.engine.SetConfigChangeHandler(func(updated config.Config) {
		win := app.win
		if win == nil {
			return // Headless test mode
		}
		win.Update(func() {
			app.cfg = updated
			app.requestRedraw()
		})
	})
	app.tunnel = tunnel.NewManager(cfg.CloudflaredPath, app.requestRedraw, func(pubURL string) {
		app.engine.Logf("Cloudflared 公网隧道已就绪: %s", pubURL)
		app.requestRedraw()
	})

	return app
}

// SetWindow binds the native mygo.Window to the App so background goroutines can trigger frames.
func (app *App) SetWindow(win *mygo.Window) {
	app.win = win
	// Refresh the portable exe path in the per-user Run entry after the app
	// was moved to another folder, but never create one the user opted out of.
	if app.cfg.OpenAtLogin {
		if err := mygo.App.SetOpenAtLogin(true); err != nil {
			app.engine.LogLevel("WARN", "更新开机自启登录项失败: "+err.Error())
		}
	}
	if app.cfg.AutoStartSync {
		app.StartAll()
	}
}

// Engine returns the underlying syncer.Engine.
func (app *App) Engine() *syncer.Engine {
	return app.engine
}

// Tunnel returns the underlying tunnel.Manager.
func (app *App) Tunnel() *tunnel.Manager {
	return app.tunnel
}

// Config returns the current configuration.
func (app *App) Config() config.Config {
	return app.cfg
}

func (app *App) openPath(path string) {
	if app.OpenPathFunc != nil {
		app.OpenPathFunc(path)
		return
	}
	if app.win == nil {
		return
	}
	go mygo.Shell.OpenPath(path)
}

func (app *App) revealPath(path string) {
	if app.RevealPathFunc != nil {
		app.RevealPathFunc(path)
		return
	}
	if app.win == nil {
		return
	}
	go mygo.Shell.ShowItemInFolder(path)
}

func (app *App) writeClipboard(text string) {
	if app.ClipboardWriteFn != nil {
		app.ClipboardWriteFn(text)
		return
	}
	if app.win == nil {
		return
	}
	mygo.Clipboard.WriteText(text)
}

func (app *App) requestRedraw() {
	if app.win != nil {
		app.win.Invalidate()
	}
}

// saveConfig persists the UI's configuration copy and surfaces failures instead
// of silently dropping user settings.
func (app *App) saveConfig() {
	if err := config.Save(app.cfg); err != nil {
		app.engine.LogLevel("WARN", "保存配置失败: "+err.Error())
		app.pendingToast = "保存配置失败: " + err.Error()
		app.requestRedraw()
	}
}

// StartAll starts the sync engine and (when in Host mode) the Cloudflared tunnel.
func (app *App) StartAll() error {
	app.syncFieldsToConfig()
	app.saveConfig()
	app.engine.UpdateConfig(app.cfg)

	if err := app.engine.Start(); err != nil {
		return err
	}

	if app.cfg.Role == config.RoleHost {
		stats := app.engine.Stats()
		if err := app.tunnel.Start(app.cfg, stats.LocalPort); err != nil {
			app.engine.LogLevel("WARN", err.Error())
		}
		// Reuse the system cloudflared only if it is actually running; when the
		// service is stopped the public hostname answers 502 and the user needs
		// to know that it is the tunnel, not SyncT, that must be started.
		if app.cfg.TunnelMode == config.TunnelSystem && !tunnel.DetectSystemCloudflared().Running {
			app.engine.LogLevel("WARN", fmt.Sprintf("未检测到运行中的系统 cloudflared 服务：公网地址 %s 将返回 502，请先启动 cloudflared 服务/进程后重新连接", app.cfg.CustomPublicURL))
		}
	}
	app.updateKeepAwake(true)
	return nil
}

// StopAll stops both the Cloudflared tunnel and the sync engine.
func (app *App) StopAll() {
	app.tunnel.Stop()
	app.engine.Stop()
	app.updateKeepAwake(false)
}

// fixTunnelTLS installs the portable origin certificate into the Windows
// trusted root store (with a UAC prompt) and restarts cloudflared, so its
// origin connection stops failing with "certificate signed by unknown
// authority" — the cause of the public 502.
func (app *App) fixTunnelTLS() {
	if err := syncer.InstallLocalTrust(); err != nil {
		app.engine.LogLevel("ERROR", "修复 502 失败: "+err.Error())
		app.pendingToast = "修复 502 失败: " + err.Error()
		app.requestRedraw()
		return
	}
	app.engine.Logf("已请求安装本地证书并重启 cloudflared；约 10 秒后会自动复检公网入口")
	app.pendingToast = "已提交证书信任修复，公网入口将在 cloudflared 重启后恢复"
	app.requestRedraw()
}

func (app *App) updateKeepAwake(running bool) {
	if app.win == nil {
		return // Headless test mode: do not call OS power service
	}
	if app.keepAwakeRelease != nil {
		app.keepAwakeRelease()
		app.keepAwakeRelease = nil
	}
	if running && app.cfg.KeepAwake {
		app.keepAwakeRelease = mygo.Power.KeepAwake("SyncT 正在通过 Cloudflared 实时同步文件夹", false)
	}
}

func (app *App) syncFieldsToConfig() {
	if p, err := strconv.Atoi(strings.TrimSpace(app.portInput)); err == nil && p > 0 && p <= 65535 {
		app.cfg.LocalPort = p
	}
	if app.roleIndex == 0 {
		app.cfg.Role = config.RoleHost
	} else {
		app.cfg.Role = config.RoleClient
	}
	app.cfg.Normalize()
}

// sampleSpeeds appends at most one trace point every half second.
func (app *App) sampleSpeeds(stats syncer.Stats) {
	now := time.Now()
	if !app.lastSpeedAt.IsZero() && now.Sub(app.lastSpeedAt) < 500*time.Millisecond {
		return
	}
	app.lastSpeedAt = now
	app.speedHistory = append(app.speedHistory, speedSample{up: stats.UploadSpeed, down: stats.DownloadSpeed})
	if len(app.speedHistory) > 60 {
		app.speedHistory = append([]speedSample(nil), app.speedHistory[len(app.speedHistory)-60:]...)
	}
}

// View builds the entire Windows 11 native interface for each frame.
func (app *App) View(c *ui.Context) {
	// The app icon is indigo/cyan; using its indigo as the accent keeps the
	// chrome and the mark in the same family instead of fighting an unrelated
	// warm accent against it.
	t := *c.Theme().WithAccent(ui.Hex("#4f46e5"))
	c.SetTheme(&t)
	c.SetMenuPresentation(ui.PopoverMenus)

	if app.pendingToast != "" {
		c.Toast(app.pendingToast)
		app.pendingToast = ""
	}

	stats := app.engine.Stats()
	app.sampleSpeeds(stats)

	root := ui.Column(c).Fill().Background(t.Background).Selectable()
	if paths := root.DroppedFiles(); len(paths) > 0 {
		count, err := app.engine.ImportFiles(paths)
		if err != nil {
			c.Toast("导入拖放文件失败: " + err.Error())
		} else if count > 0 {
			c.Toast(fmt.Sprintf("已将 %d 个文件导入同步文件夹并触发同步", count))
		}
	}

	root.Children(func() {
		app.buildHeader(c, &t, stats)
		ui.Divider(c)

		ui.Split(
			c,
			&app.cfg.SidebarWidth,
			func() { app.buildSidebar(c, &t, stats) },
			func() { app.buildMainPane(c, &t, stats) },
		).SplitReset(220).Grow(1)

		ui.Divider(c)
		app.buildStatusBar(c, &t, stats)
	})

	// Confirmation dialogs
	switch ui.AlertDialog(
		c,
		&app.confirmClearLogsOpen,
		"清空所有运行日志？",
		"此操作将清除当前内存中的同步活动记录与 Cloudflared 隧道输出日志，不会影响已同步的文件。",
		"取消",
		"确认清空",
	) {
	case 1:
		app.engine.ClearLogs()
		app.tunnel.ClearLogs()
		c.Toast("已清空所有运行日志")
	}

	switch ui.AlertDialog(
		c,
		&app.confirmResetCfgOpen,
		"恢复默认便携配置？",
		"此操作将停止当前同步并将端口、忽略规则及同步策略恢复为出厂默认值，您的本地文件夹内容不会被删除。",
		"取消",
		"恢复默认",
	) {
	case 1:
		app.StopAll()
		app.cfg = config.Default()
		app.roleIndex = 0
		app.portInput = strconv.Itoa(app.cfg.LocalPort)
		app.statusBarNoticeSeen = app.cfg.TrayCloseNotice
		app.saveConfig()
		app.engine.UpdateConfig(app.cfg)
		c.Toast("已恢复默认便携配置")
	}
}

func (app *App) buildHeader(c *ui.Context, t *ui.Theme, stats syncer.Stats) {
	running := app.engine.IsRunning()

	ui.Row(c).Padding(t.Space(2), t.Space(4)).Gap(t.Space(2.5)).Background(t.Surface).Children(func() {
		if AppLogoBitmap != nil {
			ui.Image(c, AppLogoBitmap).Size(t.Space(6), t.Space(6)).Fit(ui.Contain)
		} else {
			ui.Icon(c, IconCloud).TextColor(t.Accent)
		}
		ui.Column(c).Gap(0).Children(func() {
			ui.Text(c, "SyncT").FontSize(t.Rem(1.15)).Bold()
		})

		ui.Spacer(c)
		app.buildStatusPill(c, t, stats)

		if running {
			if ui.Button(c.Key("btn-stop"), "停止同步").Clicked() {
				app.StopAll()
				c.Toast("已停止实时同步与隧道连接")
			}
		} else {
			startLabel := "开始同步"
			if app.roleIndex == 1 {
				startLabel = "连接同步"
			}
			if ui.PrimaryButton(c.Key("btn-start"), startLabel).Clicked() {
				if err := app.StartAll(); err != nil {
					c.Toast("启动失败: " + err.Error())
				} else {
					c.Toast("实时同步已启动")
				}
			}
		}

		if ui.Button(c.Key("btn-scan"), "扫描").Tooltip("立即增量扫描本地文件夹并与远端对账").Clicked() {
			app.engine.TriggerManualSync()
			c.Toast("已触发文件夹增量扫描与对账")
		}

		ui.MenuButton(c.Key("menu-more"), "更多", func(m *ui.Menu) {
			if m.Item("立即扫描同步").Chosen() {
				app.engine.TriggerManualSync()
				app.pendingToast = "已触发文件夹增量扫描与对账"
				app.requestRedraw()
			}
			if m.Item("打开同步目录").Chosen() {
				app.openPath(app.cfg.SyncDir)
				app.pendingToast = "正在打开目录: " + app.cfg.SyncDir
				app.requestRedraw()
			}
			m.Separator()
			if m.Item("复制连接码").Chosen() {
				app.copyShareCode(c)
			}
			if app.cfg.Role == config.RoleHost && stats.TLSRejected {
				if m.Item("修复 502 (信任本地证书)").Chosen() {
					go app.fixTunnelTLS()
					app.pendingToast = "正在请求管理员权限安装本地证书并重启 cloudflared…"
					app.requestRedraw()
				}
			}
		}).Tooltip("扫描、打开目录、复制连接码")
	})
}

func (app *App) buildStatusPill(c *ui.Context, t *ui.Theme, stats syncer.Stats) {
	color := app.stateColor(t, stats.State)
	ui.Row(c).Padding(t.Space(1), t.Space(2.5)).Gap(t.Space(1.5)).Radius(t.Radius * 2).
		Background(color.Alpha(0.12)).Children(func() {
		ui.Box(c).Size(t.Space(1.75), t.Space(1.75)).Radius(t.Space(1)).Background(color)
		ui.Text(c, string(stats.State)).TextColor(color).Bold().SingleLine()
	})
}

func (app *App) buildSidebar(c *ui.Context, t *ui.Theme, stats syncer.Stats) {
	logsCount := app.engine.LogCount() + app.tunnel.LogCount()

	ui.Column(c).Fill().Background(t.Surface).Children(func() {
		ui.Sidebar(c, &app.section, func() {
			ui.SidebarSection(c, "同步", &app.navOpen, func() {
				ui.SidebarItem(c, SectionOverview, IconCloud, "概览").Children(func() {
					if app.engine.IsRunning() {
						ui.Badge(c, "运行中").Background(t.Success.Alpha(0.16)).TextColor(t.Success)
					}
				})
				ui.SidebarItem(c, SectionFiles, IconFolder, "文件").Children(func() {
					ui.Badge(c, strconv.Itoa(stats.TotalFiles))
				})
				ui.SidebarItem(c, SectionPending, IconClock, "未同步").Children(func() {
					if stats.UnsyncedFiles > 0 {
						ui.Badge(c, strconv.Itoa(stats.UnsyncedFiles)).
							Background(t.Warning.Alpha(0.18)).TextColor(t.Warning)
					}
				})
				ui.SidebarItem(c, SectionLogs, IconActivity, "日志").Children(func() {
					if logsCount > 0 {
						ui.Badge(c, strconv.Itoa(logsCount))
					}
				})
			})
			ui.SidebarSection(c, "配置", &app.toolsOpen, func() {
				ui.SidebarItem(c, SectionSettings, IconSettings, "设置")
			})
		}).Grow(1)

		ui.Divider(c)
		app.buildBottomLeftSyncInfo(c, t, stats)
	})
}

func (app *App) buildBottomLeftSyncInfo(c *ui.Context, t *ui.Theme, stats syncer.Stats) {
	ui.Column(c).Padding(t.Space(3)).Gap(t.Space(1.5)).Background(t.Surface).Children(func() {
		ui.Row(c).Gap(t.Space(1.5)).Children(func() {
			ui.Text(c, "实时吞吐").Bold().Grow(1)
			ui.Badge(c, fmt.Sprintf("%d 个目录", max(stats.FolderCount, 1))).
				Background(t.Accent.Alpha(0.12)).TextColor(t.Accent)
		})

		ui.Row(c).Gap(t.Space(2)).Children(func() {
			ui.Textf(c, "↑ %s", syncer.FormatSpeed(stats.UploadSpeed)).TextColor(t.Accent).Bold().Grow(1).SingleLine()
			ui.Textf(c, "↓ %s", syncer.FormatSpeed(stats.DownloadSpeed)).TextColor(transferAccent).Bold().Grow(1).SingleLine()
		})

		ui.Textf(c, "%d 个文件 · %d 个文件夹", stats.TotalFiles, stats.TotalDirs).
			TextColor(t.TextMuted).FontSize(t.Rem(0.82)).SingleLine()

		ui.Row(c).Gap(t.Space(1)).Children(func() {
			ui.Textf(c, "总容量 %s", syncer.FormatBytes(stats.TotalBytes)).TextColor(t.TextMuted).FontSize(t.Rem(0.82)).Grow(1).SingleLine()
			if stats.UnsyncedFiles > 0 {
				ui.Textf(c, "%d 个未同步", stats.UnsyncedFiles).TextColor(t.Warning).FontSize(t.Rem(0.82)).SingleLine()
			}
		})
	})
}

func (app *App) buildMainPane(c *ui.Context, t *ui.Theme, stats syncer.Stats) {
	switch app.section {
	case SectionFiles:
		app.buildFilesPage(c, t)
	case SectionPending:
		app.buildPendingPage(c, t, stats)
	case SectionLogs:
		app.buildLogsPage(c, t)
	case SectionSettings:
		app.buildSettingsPage(c, t)
	default:
		app.buildOverviewPage(c, t, stats)
	}
}

func (app *App) buildOverviewPage(c *ui.Context, t *ui.Theme, stats syncer.Stats) {
	tunState, pubURL, lanURL, tunErr := app.tunnel.State()
	binPath, binVer := app.tunnel.BinaryInfo()
	downloading, dlProg := app.tunnel.IsDownloading()

	ui.Scroll(c.Key("overview-scroll")).Fill().Children(func() {
		ui.Column(c).Padding(t.Space(5)).Gap(t.Space(4)).Children(func() {
			if msg := app.errorBannerText(stats, tunErr); msg != "" {
				ui.Row(c).Padding(t.Space(3)).Gap(t.Space(2)).Radius(t.Radius).
					Background(t.Danger.Alpha(0.10)).Border(1, t.Danger.Alpha(0.5)).Children(func() {
					ui.Badge(c, "异常提示").Background(t.Danger).TextColor(t.AccentText)
					ui.Text(c, msg).TextColor(t.Danger).Grow(1)
				})
			}

			app.buildThroughputHero(c, t, stats)
			app.buildLedger(c, t, stats)

			ui.Grid(c).Columns(2).Gap(t.Space(4)).Children(func() {
				app.buildFolderPanel(c, t)
				app.buildConnectionPanel(c, t, stats, tunState, pubURL, lanURL)
			})

			// When cloudflared is present and idle this is setup trivia; show
			// the panel only while it needs attention.
			if downloading || binPath == "" {
				app.buildCloudflaredPanel(c, t, binPath, binVer, downloading, dlProg)
			}
		})
	})
}

func (app *App) buildThroughputHero(c *ui.Context, t *ui.Theme, stats syncer.Stats) {
	ui.Column(c).FillWidth().Padding(t.Space(5)).Gap(t.Space(3)).Radius(t.Radius).
		Background(t.Surface).Border(1, t.Border).Children(func() {
		ui.Row(c).AlignItems(ui.End).Gap(t.Space(3)).Children(func() {
			ui.Column(c).Gap(t.Space(1)).Grow(1).Children(func() {
				ui.Text(c, "当前吞吐").TextColor(t.TextMuted).FontSize(t.Rem(0.84))
				ui.Row(c).Gap(t.Space(4)).Children(func() {
					ui.Row(c).AlignItems(ui.Baseline).Gap(t.Space(1.5)).Children(func() {
						ui.Textf(c, "↑ %s", syncer.FormatSpeed(stats.UploadSpeed)).
							FontSize(t.Rem(1.75)).Bold().TextColor(t.Accent).SingleLine()
						ui.Text(c, "上传").TextColor(t.TextMuted).FontSize(t.Rem(0.84))
					})
					ui.Row(c).AlignItems(ui.Baseline).Gap(t.Space(1.5)).Children(func() {
						ui.Textf(c, "↓ %s", syncer.FormatSpeed(stats.DownloadSpeed)).
							FontSize(t.Rem(1.75)).Bold().TextColor(transferAccent).SingleLine()
						ui.Text(c, "下载").TextColor(t.TextMuted).FontSize(t.Rem(0.84))
					})
				})
			})

			ui.Column(c).Gap(t.Space(0.5)).Children(func() {
				ui.Text(c, string(stats.State)).Bold().TextColor(app.stateColor(t, stats.State)).SingleLine()
				ui.Text(c, app.stateSubtext(stats)).TextColor(t.TextMuted).FontSize(t.Rem(0.84)).SingleLine()
			})
		})

		if app.traceHasActivity() {
			app.buildThroughputTrace(c, t, stats)
		} else {
			ui.Row(c).FillWidth().Height(t.Space(8)).Center().Children(func() {
				ui.Text(c, "有文件传输时，这里会绘制最近 30 秒的上行与下行速率。").
					TextColor(t.TextMuted).FontSize(t.Rem(0.82))
			})
		}

		ui.Row(c).Gap(t.Space(3)).Children(func() {
			app.traceLegend(c, t, t.Accent, "上传")
			app.traceLegend(c, t, transferAccent, "下载")
			ui.Spacer(c)
			ui.Text(c, "最近 30 秒").TextColor(t.TextMuted).FontSize(t.Rem(0.78))
		})
	})
}

func (app *App) traceLegend(c *ui.Context, t *ui.Theme, color ui.Color, label string) {
	ui.Row(c).Gap(t.Space(1)).Children(func() {
		ui.Box(c).Size(t.Space(1.5), t.Space(1.5)).Radius(t.Space(0.75)).Background(color)
		ui.Text(c, label).TextColor(t.TextMuted).FontSize(t.Rem(0.8))
	})
}

func (app *App) traceHasActivity() bool {
	for _, s := range app.speedHistory {
		if s.up > 0 || s.down > 0 {
			return true
		}
	}
	return false
}

// buildThroughputTrace paints the two rate series directly. A chart library
// would need axes and labels that this quiet instrument does not show.
func (app *App) buildThroughputTrace(c *ui.Context, t *ui.Theme, stats syncer.Stats) {
	trace := ui.Box(c).FillWidth().Height(t.Space(13))
	trace.Draw(func(p *ui.Painter, r ui.Rect) {
		for i := 0; i <= 2; i++ {
			y := r.Y + r.H*float32(i)/2
			p.Line(r.X, y, r.X+r.W, y, 1, t.Border.Alpha(0.55))
		}
		samples := app.speedHistory
		if len(samples) < 2 {
			return
		}
		var peak int64 = 1
		for _, s := range samples {
			if s.up > peak {
				peak = s.up
			}
			if s.down > peak {
				peak = s.down
			}
		}
		step := r.W / float32(len(samples)-1)
		up := make([][2]float32, 0, len(samples))
		down := make([][2]float32, 0, len(samples))
		for i, s := range samples {
			x := r.X + step*float32(i)
			up = append(up, [2]float32{x, r.Y + r.H - float32(s.up)/float32(peak)*r.H})
			down = append(down, [2]float32{x, r.Y + r.H - float32(s.down)/float32(peak)*r.H})
		}
		p.StrokePolyline(up, 1.6, t.Accent)
		p.StrokePolyline(down, 1.6, transferAccent)
	})
}

func (app *App) buildLedger(c *ui.Context, t *ui.Theme, stats syncer.Stats) {
	ui.Row(c).FillWidth().Padding(t.Space(3), t.Space(4)).Gap(t.Space(4)).
		Radius(t.Radius).Background(t.Surface).Border(1, t.Border).
		Dividers(1, t.Border).Children(func() {
		app.metric(c, t, "受控文件", fmt.Sprintf("%d", stats.TotalFiles), "个文件")
		app.metric(c, t, "未同步", fmt.Sprintf("%d", stats.UnsyncedFiles), "个待对端确认")
		app.metric(c, t, "映射目录", fmt.Sprintf("%d", max(stats.FolderCount, 1)), fmt.Sprintf("含 %d 个子文件夹", stats.TotalDirs))
		app.metric(c, t, "本次累计流量",
			fmt.Sprintf("↑ %s", syncer.FormatBytes(stats.BytesUploaded)),
			fmt.Sprintf("↓ %s", syncer.FormatBytes(stats.BytesDownloaded)))
	})
}

func (app *App) metric(c *ui.Context, t *ui.Theme, label, value, sub string) {
	ui.Column(c).Grow(1).Gap(t.Space(0.5)).Children(func() {
		ui.Text(c, label).TextColor(t.TextMuted).FontSize(t.Rem(0.8)).SingleLine()
		ui.Text(c, value).FontSize(t.Rem(1.15)).Bold().SingleLine()
		ui.Text(c, sub).TextColor(t.TextMuted).FontSize(t.Rem(0.78)).SingleLine()
	})
}

func (app *App) buildFolderPanel(c *ui.Context, t *ui.Theme) {
	ui.Column(c).Padding(t.Space(4)).Gap(t.Space(2.5)).Radius(t.Radius).
		Background(t.Surface).Border(1, t.Border).Children(func() {
		ui.Row(c).Gap(t.Space(2)).Children(func() {
			ui.Icon(c, IconFolder).TextColor(t.Accent)
			ui.Text(c, "同步文件夹").Bold().Grow(1)
			if ui.Button(c.Key("btn-manage-folders"), "管理").Clicked() {
				app.section = SectionSettings
				app.settingsFoldersOpen = true
			}
		})

		folders := app.cfg.EnabledFolders()
		shown := folders
		if len(shown) > 4 {
			shown = shown[:4]
		}
		for i := range shown {
			folder := shown[i]
			ui.Row(c).Gap(t.Space(2)).Children(func() {
				ui.Box(c).Size(t.Space(1.5), t.Space(1.5)).Radius(t.Space(0.75)).Background(t.Success).Margin(t.Space(0.5), 0, 0, 0)
				ui.Column(c).Gap(0).Grow(1).Children(func() {
					ui.Text(c, folder.Label).SingleLine()
					ui.Text(c, folder.LocalPath).TextColor(t.TextMuted).FontSize(t.Rem(0.78)).SingleLine()
				})
				if ui.Button(c.Key(fmt.Sprintf("btn-ov-open-%d", i)), "打开").Clicked() {
					app.openPath(folder.LocalPath)
					c.Toast("正在打开目录: " + folder.LocalPath)
				}
			})
		}
		if len(folders) > len(shown) {
			ui.Textf(c, "还有 %d 个文件夹，在设置中管理", len(folders)-len(shown)).TextColor(t.TextMuted).FontSize(t.Rem(0.8))
		}
		if len(folders) == 0 {
			ui.Text(c, "尚未选择同步文件夹。").TextColor(t.TextMuted)
		}
	})
}

func (app *App) buildConnectionPanel(c *ui.Context, t *ui.Theme, stats syncer.Stats, tunState tunnel.TunnelState, pubURL, lanURL string) {
	ui.Column(c).Padding(t.Space(4)).Gap(t.Space(2.5)).Radius(t.Radius).
		Background(t.Surface).Border(1, t.Border).Children(func() {
		ui.Row(c).Gap(t.Space(2)).Children(func() {
			ui.Icon(c, IconCloud).TextColor(t.Accent)
			ui.Text(c, "连接").Bold().Grow(1)
			ui.Badge(c, string(tunState)).Background(app.tunnelColor(t, tunState).Alpha(0.14)).
				TextColor(app.tunnelColor(t, tunState))
		})

		activeURL := app.activeShareURL(stats, pubURL, lanURL)
		if activeURL == "" {
			ui.Text(c, app.tunnelSubtext(pubURL, lanURL)).TextColor(t.TextMuted)
		} else {
			ui.Text(c, activeURL).Bold().SingleLine()
		}

		detail := app.stateSubtext(stats)
		if app.cfg.Role == config.RoleHost {
			detail = fmt.Sprintf("在线节点 %d · 本地端口 %d", stats.ConnectedPeers, max(stats.LocalPort, 0))
		}
		ui.Text(c, detail).TextColor(t.TextMuted).FontSize(t.Rem(0.82)).SingleLine()

		ui.Row(c).Gap(t.Space(2)).Children(func() {
			if ui.Button(c.Key("btn-copy-code"), "复制连接码").Clicked() {
				app.copyShareCode(c)
			}
			if app.cfg.Role == config.RoleHost && stats.TLSRejected {
				if ui.Button(c.Key("btn-fix-502"), "修复 502").Tooltip("将本地自签名证书安装到受信任根，并重启 cloudflared").Clicked() {
					go app.fixTunnelTLS()
					c.Toast("正在请求管理员权限安装本地证书并重启 cloudflared…")
				}
			}
		})
	})
}

func (app *App) buildCloudflaredPanel(c *ui.Context, t *ui.Theme, binPath, binVer string, downloading bool, dlProg float64) {
	ui.Column(c).Padding(t.Space(4)).Gap(t.Space(2)).Radius(t.Radius).
		Background(t.Surface).Border(1, t.Border).Children(func() {
		ui.Row(c).Gap(t.Space(2)).Children(func() {
			ui.Icon(c, IconShield).TextColor(t.Accent)
			ui.Text(c, "Cloudflared 组件").Bold().Grow(1)
			if binPath != "" {
				ui.Badge(c, "就绪").Background(t.Success.Alpha(0.16)).TextColor(t.Success)
			} else {
				ui.Badge(c, "未检测到").Background(t.Warning.Alpha(0.16)).TextColor(t.Warning)
			}
		})
		if binVer != "" {
			ui.Text(c, binVer).TextColor(t.TextMuted).FontSize(t.Rem(0.8)).SingleLine()
		}
		if downloading {
			ui.Progress(c, dlProg).Label("正在下载便携版 cloudflared.exe")
		}
		ui.Row(c).Gap(t.Space(2)).Children(func() {
			if ui.Button(c.Key("btn-detect-cf"), "重新检测").Clicked() {
				app.refreshBinaryAsync()
				c.Toast("正在重新检测 Cloudflared 组件…")
			}
			if ui.Button(c.Key("btn-dl-cf"), "下载便携版").Disabled(downloading).Clicked() {
				c.Toast("开始下载官方便携版 cloudflared.exe 至 bin 目录…")
				app.tunnel.DownloadPortableBinary(func(savedPath string, err error) {
					if err != nil {
						app.pendingToast = "下载 cloudflared.exe 失败: " + err.Error()
					} else {
						app.pendingToast = "便携版 cloudflared.exe 下载完成！"
					}
					app.requestRedraw()
				})
			}
		})
	})
}

func (app *App) buildFilesPage(c *ui.Context, t *ui.Theme) {
	allFiles, version := app.engine.FilesWithVersion()
	filtered := app.cachedVisibleFiles(allFiles, version)
	unsynced := 0
	for _, f := range allFiles {
		if f.Status != syncer.StatusSynced {
			unsynced++
		}
	}

	ui.Column(c).Fill().Padding(t.Space(4)).Gap(t.Space(3)).Children(func() {
		ui.Row(c).Gap(t.Space(2.5)).Children(func() {
			ui.SearchField(c.Key("search-files"), &app.fileSearch).
				Placeholder("搜索文件名、目录或状态…").
				Label("搜索同步文件").
				Grow(1)

			ui.Segmented(c.Key("seg-file-filter"), &app.fileFilterIdx, "全部", "已同步", "未同步").
				Label("按状态筛选")

			if ui.Button(c.Key("btn-open-dir"), "打开目录").Clicked() {
				app.openPath(app.cfg.SyncDir)
				c.Toast("正在打开目录: " + app.cfg.SyncDir)
			}
		})

		ui.Row(c).Gap(t.Space(2)).Children(func() {
			ui.Textf(c, "共 %d 个文件 · %d 个尚未确认同步", len(allFiles), unsynced).
				TextColor(t.TextMuted).FontSize(t.Rem(0.82))
			ui.Spacer(c)
			if ui.Button(c.Key("btn-rescan-files"), "刷新").Clicked() {
				app.engine.TriggerManualSync()
				c.Toast("已刷新本地同步文件索引")
			}
		})

		ui.Column(c).Grow(1).Radius(t.Radius).Background(t.Surface).Border(1, t.Border).Children(func() {
			if len(filtered) == 0 {
				app.emptyState(c, t, IconFolder,
					app.emptyFilesText(len(allFiles)), "打开同步目录", func() {
						app.openPath(app.cfg.SyncDir)
						c.Toast("正在打开同步目录: " + app.cfg.SyncDir)
					})
				return
			}

			cols := []ui.TableColumn{
				{ID: "folder", Title: "目录", Width: 108, Sortable: true},
				{ID: "path", Title: "文件", Sortable: true},
				{ID: "size", Title: "大小", Width: 92, Align: ui.End, Sortable: true},
				{ID: "mtime", Title: "修改时间", Width: 150, Sortable: true},
				{ID: "status", Title: "状态", Width: 96, Sortable: true},
			}

			app.fileTableState.Sort = &app.fileSort
			app.fileTableState.Selected = &app.selectedFileRow
			app.fileTableState.Key = func(row int) any {
				if row >= 0 && row < len(filtered) {
					return filtered[row].RelPath
				}
				return row
			}

			var actionOpenPath string
			var actionRevealPath string
			var actionCopyRel string

			app.fileTableState.CellMenu = func(m *ui.Menu, row, _ int) {
				if row < 0 || row >= len(filtered) {
					return
				}
				entry := filtered[row]
				full := app.resolveEntryFullPath(entry.RelPath)
				if m.Item("打开文件").Chosen() {
					actionOpenPath = full
				}
				if m.Item("在资源管理器中显示").Chosen() {
					actionRevealPath = full
				}
				m.Separator()
				if m.Item("复制相对路径").Chosen() {
					actionCopyRel = entry.RelPath
				}
			}

			if ui.Table(c, &app.fileTableState, cols, len(filtered), func(row, col int) {
				item := filtered[row]
				switch col {
				case 0:
					ui.Text(c, item.FolderLabel).TextColor(t.TextMuted).SingleLine()
				case 1:
					ui.Text(c, item.RelPath).SingleLine()
				case 2:
					ui.Text(c, syncer.FormatBytes(item.Size)).SingleLine()
				case 3:
					ui.Text(c, item.ModTime.Format("2006-01-02 15:04")).TextColor(t.TextMuted).SingleLine()
				case 4:
					if item.Status == syncer.StatusSynced {
						ui.Text(c, string(item.Status)).TextColor(t.TextMuted).SingleLine()
					} else {
						ui.Text(c, string(item.Status)).TextColor(app.fileStatusColor(t, item.Status)).Bold().SingleLine()
					}
				}
			}).Grow(1).Submitted() {
				if app.selectedFileRow >= 0 && app.selectedFileRow < len(filtered) {
					actionRevealPath = app.resolveEntryFullPath(filtered[app.selectedFileRow].RelPath)
				}
			}

			app.applyFileActions(c, actionOpenPath, actionRevealPath, actionCopyRel)
		})
	})
}

func (app *App) emptyFilesText(total int) string {
	if total == 0 {
		return "同步文件夹还是空的。放入文件，或把文件直接拖进这个窗口。"
	}
	return "没有文件匹配当前的搜索或筛选条件。"
}

func (app *App) buildPendingPage(c *ui.Context, t *ui.Theme, stats syncer.Stats) {
	allFiles, _ := app.engine.FilesWithVersion()
	pending := pendingFiles(allFiles)
	filtered := pending
	if col := app.pendingSort.Column; col != "" {
		// Sorting is offered on this table too; the small list keeps the same
		// comparator as the files page.
		sortPending(filtered, app.pendingSort)
	}

	ui.Column(c).Fill().Padding(t.Space(4)).Gap(t.Space(3)).Children(func() {
		ui.Row(c).AlignItems(ui.End).Gap(t.Space(3)).Children(func() {
			ui.Column(c).Gap(t.Space(0.5)).Grow(1).Children(func() {
				ui.Text(c, "未同步文件").FontSize(t.Rem(1.15)).Bold()
				ui.Text(c, "这些文件还没有在对端确认。运行同步后，它们会自动上传或下载；错误与冲突会留在列表中等待处理。").
					TextColor(t.TextMuted).FontSize(t.Rem(0.84))
			})
			if ui.Button(c.Key("btn-pending-scan"), "立即扫描并同步").Clicked() {
				app.engine.TriggerManualSync()
				c.Toast("已触发文件夹增量扫描与对账")
			}
		})

		ui.Column(c).Grow(1).Radius(t.Radius).Background(t.Surface).Border(1, t.Border).Children(func() {
			if len(filtered) == 0 {
				app.emptyState(c, t, IconCheckCircle, "所有文件都已确认同步，没有待处理项。", "", nil)
				return
			}

			cols := []ui.TableColumn{
				{ID: "path", Title: "文件", Sortable: true},
				{ID: "folder", Title: "目录", Width: 110, Sortable: true},
				{ID: "status", Title: "状态", Width: 100, Sortable: true},
				{ID: "size", Title: "大小", Width: 92, Align: ui.End, Sortable: true},
				{ID: "mtime", Title: "修改时间", Width: 150, Sortable: true},
			}

			app.pendingTableState.Sort = &app.pendingSort
			app.pendingTableState.Selected = &app.selectedPendingRow
			app.pendingTableState.Key = func(row int) any {
				if row >= 0 && row < len(filtered) {
					return filtered[row].RelPath
				}
				return row
			}

			var actionRevealPath string
			if ui.Table(c, &app.pendingTableState, cols, len(filtered), func(row, col int) {
				item := filtered[row]
				switch col {
				case 0:
					ui.Text(c, item.RelPath).SingleLine()
				case 1:
					ui.Text(c, item.FolderLabel).TextColor(t.TextMuted).SingleLine()
				case 2:
					ui.Text(c, string(item.Status)).TextColor(app.fileStatusColor(t, item.Status)).Bold().SingleLine()
				case 3:
					ui.Text(c, syncer.FormatBytes(item.Size)).SingleLine()
				case 4:
					ui.Text(c, item.ModTime.Format("2006-01-02 15:04")).TextColor(t.TextMuted).SingleLine()
				}
			}).Grow(1).Submitted() {
				if app.selectedPendingRow >= 0 && app.selectedPendingRow < len(filtered) {
					actionRevealPath = app.resolveEntryFullPath(filtered[app.selectedPendingRow].RelPath)
				}
			}
			if actionRevealPath != "" {
				app.revealPath(actionRevealPath)
				c.Toast("已在资源管理器中定位: " + filepath.Base(actionRevealPath))
			}
		})
	})
}

func (app *App) emptyState(c *ui.Context, t *ui.Theme, icon *ui.SVG, text, actionLabel string, action func()) {
	ui.Column(c).Fill().Center().Gap(t.Space(3)).Padding(t.Space(6)).Children(func() {
		ui.Icon(c, icon).TextColor(t.TextMuted).FontSize(t.Space(7))
		ui.Text(c, text).TextColor(t.TextMuted)
		if actionLabel != "" && action != nil {
			if ui.Button(c.Key("btn-empty-action"), actionLabel).Clicked() {
				action()
			}
		}
	})
}

func (app *App) applyFileActions(c *ui.Context, openPath, revealPath, copyRel string) {
	if openPath != "" {
		app.openPath(openPath)
		c.Toast("已打开文件: " + filepath.Base(openPath))
	}
	if revealPath != "" {
		app.revealPath(revealPath)
		c.Toast("已在资源管理器中定位: " + filepath.Base(revealPath))
	}
	if copyRel != "" {
		app.writeClipboard(copyRel)
		c.Toast("已复制路径: " + copyRel)
	}
}

func (app *App) buildLogsPage(c *ui.Context, t *ui.Theme) {
	syncLogs := app.engine.Logs()
	tunLogs := app.tunnel.Logs()

	ui.Column(c).Fill().Padding(t.Space(4)).Gap(t.Space(3)).Children(func() {
		ui.Row(c).Gap(t.Space(3)).Children(func() {
			ui.Tabs(c, &app.logTab, "同步活动", "隧道输出")
			ui.Spacer(c)
			if ui.Button(c.Key("btn-copy-logs"), "复制").Clicked() {
				text := app.formatCurrentLogsText(syncLogs, tunLogs)
				if text != "" {
					app.writeClipboard(text)
				}
				c.Toast("已复制当前日志内容至剪贴板")
			}
			if ui.Button(c.Key("btn-clear-logs"), "清空日志").Disabled(len(syncLogs) == 0 && len(tunLogs) == 0).Clicked() {
				app.confirmClearLogsOpen = true
			}
		})

		ui.Column(c).Grow(1).Radius(t.Radius).Background(t.Surface).Border(1, t.Border).Children(func() {
			if app.logTab == 0 {
				if len(syncLogs) == 0 {
					app.emptyState(c, t, IconActivity, "暂无同步活动，启动同步或扫描文件夹后会实时显示。", "", nil)
					return
				}
				app.syncLogList.FollowEnd = true
				app.syncLogList.Key = func(row int) any {
					if row >= 0 && row < len(syncLogs) {
						return syncLogs[row].ID
					}
					return row
				}
				ui.List(c, &app.syncLogList, len(syncLogs), func(i int) {
					entry := syncLogs[i]
					ui.Row(c).Padding(t.Space(1.5), t.Space(3)).Gap(t.Space(2)).Children(func() {
						ui.Text(c, entry.Time.Format("15:04:05.000")).TextColor(t.TextMuted).FontSize(t.Rem(0.8))
						ui.Badge(c, entry.Level).Background(app.logLevelColor(t, entry.Level).Alpha(0.14)).
							TextColor(app.logLevelColor(t, entry.Level))
						ui.Text(c, entry.Message).Grow(1)
					})
				}).Selectable().Grow(1)
			} else {
				if len(tunLogs) == 0 {
					app.emptyState(c, t, IconCloud, "暂无 Cloudflared 隧道输出，在主机模式下启动隧道后会显示。", "", nil)
					return
				}
				app.tunnelLogList.FollowEnd = true
				app.tunnelLogList.Key = func(row int) any { return row }
				ui.List(c, &app.tunnelLogList, len(tunLogs), func(i int) {
					ui.Text(c, tunLogs[i]).Padding(t.Space(1), t.Space(3))
				}).Selectable().Grow(1)
			}
		})
	})
}

func (app *App) buildSettingsPage(c *ui.Context, t *ui.Theme) {
	ui.Scroll(c.Key("settings-scroll")).Selectable().Fill().Children(func() {
		ui.Column(c).Padding(t.Space(5)).Gap(t.Space(3)).Children(func() {
			ui.Row(c).AlignItems(ui.End).Children(func() {
				ui.Column(c).Gap(t.Space(0.5)).Grow(1).Children(func() {
					ui.Text(c, "设置").FontSize(t.Rem(1.2)).Bold()
					ui.Text(c, "所有配置与索引都保存在程序同级的 data 目录，不写入注册表或其他用户目录。").
						TextColor(t.TextMuted).FontSize(t.Rem(0.82))
				})
				if ui.Button(c.Key("btn-reset-defaults"), "恢复默认设置").Clicked() {
					app.confirmResetCfgOpen = true
				}
			})

			app.settingsSection(c, t, "文件夹与方向", &app.settingsFoldersOpen, func() {
				ui.Form(c, func() {
					var dirErr string
					if strings.TrimSpace(app.cfg.SyncDir) == "" {
						dirErr = "请选择或输入需要实时同步的本地文件夹路径"
					}
					ui.Field(c, "主同步目录", func() {
						ui.Row(c).Gap(t.Space(2)).Children(func() {
							if ui.TextInput(c.Key("input-sync-dir"), &app.cfg.SyncDir).Placeholder(`例如 D:\Work\SharedFolder`).Grow(1).Changed() {
								if len(app.cfg.SyncFolders) > 0 {
									app.cfg.SyncFolders[0].LocalPath = app.cfg.SyncDir
								}
								app.saveConfig()
							}
							if ui.Button(c.Key("btn-browse-dir"), "浏览…").Clicked() {
								app.pickSyncDirectory(c)
							}
						})
					}).Description("也可以把文件从资源管理器直接拖进 SyncT 窗口导入这里").Error(dirErr)

					dirIdx := syncDirectionIndex(app.cfg.SyncDirection)
					ui.Field(c, "同步方向", func() {
						choice := syncDirOptions[dirIdx]
						if ui.Select(c.Key("sel-sync-dir"), &choice, syncDirOptions).Label("同步方向").Changed() {
							app.cfg.SyncDirection = parseSyncDirection(choice)
							app.saveConfig()
							app.engine.UpdateConfig(app.cfg)
							c.Toast("同步方向已更新为: " + choice)
						}
					}).Description("双向模式会比较修改时间与 SHA-256；冲突时按下面的规则处理")
				})
			})

			app.settingsSection(c, t, "工作模式与连接", &app.settingsConnOpen, func() {
				ui.Form(c, func() {
					ui.Field(c, "工作模式", func() {
						if ui.Segmented(c.Key("seg-role"), &app.roleIndex, "主机模式 (发布隧道)", "节点模式 (连接远端)").Changed() {
							running := app.engine.IsRunning()
							if running {
								app.StopAll()
							}
							app.syncFieldsToConfig()
							app.saveConfig()
							app.engine.UpdateConfig(app.cfg)
							if app.cfg.Role == config.RoleHost {
								c.Toast("已切换为主机模式：由本机发布 Cloudflared 隧道")
							} else {
								c.Toast("已切换为节点模式：连接远端 Cloudflared 隧道")
							}
						}
					}).Description("主机模式监听本地端口并复用系统 Cloudflared；节点模式只发起出站 HTTPS，不监听端口")

					ui.Field(c, "同步密码", func() {
						ui.Row(c).Gap(t.Space(2)).Children(func() {
							inp := ui.TextInput(c.Key("input-auth-token"), &app.passwordInput).
								Placeholder("两端输入同一个密码").Grow(1)
							inp.Password()
							if ui.Button(c.Key("btn-apply-password"), "应用密码").
								Disabled(strings.TrimSpace(app.passwordInput) == "").Clicked() {
								app.applyPassword(c)
							}
							if ui.Button(c.Key("btn-gen-token"), "随机生成").Clicked() {
								app.cfg.AuthToken = config.GenerateToken()
								app.passwordInput = ""
								app.saveConfig()
								app.engine.UpdateConfig(app.cfg)
								c.Toast("已生成新的随机访问密钥")
							}
						})
					}).Description("两端输入同一个密码即可配对；密码经 PBKDF2-SHA256 派生为访问密钥，密码本身不会保存或发送。也可以直接粘贴连接码或 st_ 密钥")

					if app.roleIndex == 0 {
						tmIdx := tunnelModeIndex(app.cfg.TunnelMode)
						ui.Field(c, "隧道方式", func() {
							choice := tunnelModeOptions[tmIdx]
							if ui.Select(c.Key("sel-tunnel-mode"), &choice, tunnelModeOptions).Label("隧道方式").Changed() {
								app.cfg.TunnelMode = parseTunnelMode(choice)
								app.saveConfig()
								c.Toast("已切换隧道方式: " + choice)
							}
						}).Description("默认复用本机已运行的系统 Cloudflared，无需再次启动进程或填写 Token")

						if app.cfg.TunnelMode == config.TunnelSystem || app.cfg.TunnelMode == config.TunnelToken {
							ui.Field(c, "公网域名", func() {
								if ui.TextInput(c.Key("input-custom-pub-url"), &app.cfg.CustomPublicURL).Placeholder("https://synctreei.suosmanthus.cyou").Changed() {
									app.saveConfig()
								}
							}).Description("Cloudflared 入站规则绑定的域名")
						}
						if app.cfg.TunnelMode == config.TunnelToken {
							ui.Field(c, "Tunnel Token", func() {
								ui.TextInput(c.Key("input-tunnel-token"), &app.cfg.TunnelToken).Placeholder("eyJhIjoi... (Cloudflare Zero Trust 隧道 Token)").Password()
							}).Description("在 Cloudflare Zero Trust 控制台创建的固定隧道 Token")
						}

						var portErr string
						if p, err := strconv.Atoi(strings.TrimSpace(app.portInput)); err != nil || p <= 0 || p > 65535 {
							portErr = "请输入 1 ~ 65535 之间的有效端口号"
						}
						ui.Field(c, "本地端口", func() {
							ui.Row(c).Gap(t.Space(2)).Children(func() {
								if ui.TextInput(c.Key("input-local-port"), &app.portInput).Placeholder("18920").Grow(1).Changed() {
									app.syncFieldsToConfig()
									app.saveConfig()
								}
								ui.Badge(c, fmt.Sprintf("localhost:%s", strings.TrimSpace(app.portInput))).
									Background(t.Accent.Alpha(0.12)).TextColor(t.Accent)
							})
						}).Description("只绑定 127.0.0.1 / ::1；内建自签名 TLS，同时兼容 HTTPS 与 HTTP").Error(portErr)
					} else {
						ui.Field(c, "一键连接码", func() {
							ui.Row(c).Gap(t.Space(2)).Children(func() {
								ui.TextInput(c.Key("input-share-code"), &app.shareInput).
									Placeholder("粘贴 synct://connect/... 或 st_... 密钥").Grow(1)
								if ui.Button(c.Key("btn-paste-code"), "从剪贴板粘贴").Clicked() {
									if app.win != nil {
										clip := strings.TrimSpace(mygo.Clipboard.ReadText())
										if clip != "" {
											app.shareInput = clip
										}
									}
									app.applyShareCodeInput(c)
								}
								if ui.Button(c.Key("btn-parse-code"), "解析填充").Disabled(strings.TrimSpace(app.shareInput) == "").Clicked() {
									app.applyShareCodeInput(c)
								}
							})
						}).Description("在主机端点击「复制连接码」，然后在这里导入域名与访问密钥")

						var remoteErr string
						if strings.TrimSpace(app.cfg.RemoteURL) == "" {
							remoteErr = "请填写 Cloudflared 隧道域名"
						}
						ui.Field(c, "远端地址", func() {
							if ui.TextInput(c.Key("input-remote-url"), &app.cfg.RemoteURL).Placeholder("https://synctreei.suosmanthus.cyou").Changed() {
								app.saveConfig()
								app.engine.UpdateConfig(app.cfg)
							}
						}).Description("工作电脑只通过标准 HTTPS (443) 出站连接这个地址").Error(remoteErr)
					}
				})
			})

			app.settingsSection(c, t, "文件规则", &app.settingsRulesOpen, func() {
				ui.Form(c, func() {
					ui.Field(c, "忽略规则", func() {
						if ui.TokenField(c.Key("token-ignore"), &app.cfg.IgnorePatterns, config.PresetIgnoreSuggestions()).
							Placeholder("输入通配符 (如 *.tmp, .git) 后回车…").
							Label("忽略规则列表").
							Changed() {
							app.saveConfig()
							app.engine.UpdateConfig(app.cfg)
							c.Toast("已更新忽略规则")
						}
					}).Description("匹配的文件夹或文件不会被扫描、上传或下载；.synct* 临时文件始终忽略")

					confIdx := conflictPolicyIndex(app.cfg.ConflictPolicy)
					ui.Field(c, "冲突策略", func() {
						choice := conflictOptions[confIdx]
						if ui.Select(c.Key("sel-conflict-policy"), &choice, conflictOptions).Label("冲突策略").Changed() {
							app.cfg.ConflictPolicy = parseConflictPolicy(choice)
							app.saveConfig()
							app.engine.UpdateConfig(app.cfg)
							c.Toast("冲突策略已更新: " + choice)
						}
					}).Description("保留副本会生成 .conflict-时间戳 文件，任何一端的改动都不会丢失")

					ui.Field(c, "同步删除", func() {
						if ui.Switch(c.Key("sw-sync-del"), &app.cfg.SyncDeletions).Label("同步删除").Changed() {
							app.saveConfig()
							app.engine.UpdateConfig(app.cfg)
							c.Toast("已更新文件删除同步开关")
						}
					}).Description("一端删除文件后，对端对应的文件也会被移除（保留 30 天删除记录用于离线对账）")
				})
			})

			app.settingsSection(c, t, "运行与后台", &app.settingsRuntimeOpen, func() {
				ui.Form(c, func() {
					ui.Field(c, "自动启动同步", func() {
						if ui.Switch(c.Key("sw-autostart"), &app.cfg.AutoStartSync).Label("自动启动同步").Changed() {
							app.saveConfig()
							c.Toast("已保存自动启动同步选项")
						}
					}).Description("打开 SyncT 后立即按上次配置启动隧道与实时同步")

					ui.Field(c, "开机自动运行", func() {
						if ui.Switch(c.Key("sw-openatlogin"), &app.cfg.OpenAtLogin).Label("登录 Windows 后自动启动").Changed() {
							app.saveConfig()
							if app.win != nil {
								if err := mygo.App.SetOpenAtLogin(app.cfg.OpenAtLogin); err != nil {
									c.Toast("设置开机自启失败: " + err.Error())
								} else {
									c.Toast("已更新 Windows 登录自启动设置")
								}
							}
						}
					}).Description("通过当前用户登录项启动，无需管理员权限")

					ui.Field(c, "防止系统休眠", func() {
						if ui.Switch(c.Key("sw-keepawake"), &app.cfg.KeepAwake).Label("防止系统休眠").Changed() {
							app.saveConfig()
							app.updateKeepAwake(app.engine.IsRunning())
							c.Toast("已更新电源保持策略")
						}
					}).Description("同步运行期间阻止系统睡眠，避免公网隧道中断")

					ui.Field(c, "最小化到托盘", func() {
						if ui.Switch(c.Key("sw-tray"), &app.cfg.MinimizeToTray).Label("最小化到托盘").Changed() {
							app.saveConfig()
							c.Toast("已更新关闭窗口时的后台运行设置")
						}
					}).Description("关闭窗口时隐藏到系统托盘并继续同步")

					ui.Field(c, "后台提示", func() {
						if ui.Switch(c.Key("sw-tray-notice"), &app.cfg.TrayCloseNotice).Label("关闭窗口时提示").Changed() {
							app.saveConfig()
							if app.cfg.TrayCloseNotice {
								c.Toast("已开启：关闭窗口时在通知中心提示仍在后台同步")
							} else {
								c.Toast("已关闭：关闭窗口后静默在后台同步")
							}
						}
					}).Description("只在最小化到托盘时显示一条系统通知，不影响同步本身")

					ui.Field(c, "用户权限克制", func() {
						if ui.Switch(c.Key("sw-lowpriv"), &app.cfg.LowPrivilegeMode).Label("用户权限克制模式").Changed() {
							app.saveConfig()
							c.Toast("已更新用户权限克制模式")
						}
					}).Description("强制 asInvoker 清单、只绑定回环地址，数据全部隔离在 ./data/")
				})
			})

			app.settingsSection(c, t, "多文件夹与跨机映射", &app.settingsAdvancedOpen, func() {
				app.buildFolderMappingsFields(c, t)
			})

			ui.Column(c).Gap(t.Space(0.5)).Children(func() {
				ui.Textf(c, "数据目录  %s", config.DataDir()).TextColor(t.TextMuted).FontSize(t.Rem(0.78)).SingleLine()
				ui.Textf(c, "配置  %s", config.ConfigPath()).TextColor(t.TextMuted).FontSize(t.Rem(0.78)).SingleLine()
				ui.Textf(c, "索引  %s", config.StatePath()).TextColor(t.TextMuted).FontSize(t.Rem(0.78)).SingleLine()
				ui.Textf(c, "日志  %s (上限 2MB)", filepath.Join(config.PortableRoot(), "SyncT.log")).TextColor(t.TextMuted).FontSize(t.Rem(0.78)).SingleLine()
				ui.Textf(c, "默认同步目录  %s（两台电脑相同）", config.PreferredSyncDir).TextColor(t.TextMuted).FontSize(t.Rem(0.78)).SingleLine()
			})
		})
	})
}

func (app *App) settingsSection(c *ui.Context, t *ui.Theme, title string, open *bool, body func()) {
	ui.Column(c).FillWidth().Padding(t.Space(3.5)).Radius(t.Radius).
		Background(t.Surface).Border(1, t.Border).Children(func() {
		ui.Collapsible(c, title, open, body)
	})
}

func (app *App) buildStatusBar(c *ui.Context, t *ui.Theme, stats syncer.Stats) {
	tunState, pubURL, lanURL, _ := app.tunnel.State()
	url := app.activeShareURL(stats, pubURL, lanURL)
	if url == "" {
		url = string(tunState)
	}

	ui.Row(c).Padding(t.Space(1.25), t.Space(3)).Gap(t.Space(2.5)).Background(t.Surface).Children(func() {
		ui.Box(c).Size(t.Space(1.5), t.Space(1.5)).Radius(t.Space(0.75)).
			Background(app.stateColor(t, stats.State)).Margin(0, t.Space(0.5), 0, 0)
		ui.Text(c, string(stats.State)).TextColor(app.stateColor(t, stats.State)).Bold().SingleLine()

		ui.Textf(c, "↑ %s", syncer.FormatSpeed(stats.UploadSpeed)).TextColor(t.Accent).SingleLine()
		ui.Textf(c, "↓ %s", syncer.FormatSpeed(stats.DownloadSpeed)).TextColor(transferAccent).SingleLine()

		if stats.UnsyncedFiles > 0 {
			ui.Textf(c, "未同步 %d", stats.UnsyncedFiles).TextColor(t.Warning).SingleLine()
		} else {
			ui.Text(c, "已全部同步").TextColor(t.TextMuted).SingleLine()
		}

		ui.Textf(c, "%d 文件 · %d 文件夹 · %s", stats.TotalFiles, stats.TotalDirs, syncer.FormatBytes(stats.TotalBytes)).
			TextColor(t.TextMuted).SingleLine()

		ui.Spacer(c)

		ui.Text(c, url).TextColor(t.TextMuted).SingleLine().MaxWidth(t.Space(44))

		if ui.Toggle(c.Key("toggle-tray-notice"), &app.cfg.TrayCloseNotice, "后台提示").
			Tooltip("关闭主窗口时是否在通知中心提示 SyncT 仍在后台同步").
			Label("关闭窗口时显示后台提示").
			Changed() {
			app.statusBarNoticeSeen = app.cfg.TrayCloseNotice
			app.saveConfig()
			c.Toast(app.trayNoticeToast(app.cfg.TrayCloseNotice))
		} else if app.cfg.TrayCloseNotice != app.statusBarNoticeSeen {
			// The settings page owns this same value; adopt its change without
			// raising a second toast for one action.
			app.statusBarNoticeSeen = app.cfg.TrayCloseNotice
		}
	})
}

func (app *App) trayNoticeToast(on bool) string {
	if on {
		return "已开启：关闭窗口时在通知中心提示 SyncT 仍在后台同步"
	}
	return "已关闭：关闭窗口后静默在后台同步"
}

func (app *App) buildFolderMappingsFields(c *ui.Context, t *ui.Theme) {
	ui.Text(c, "除主目录外，还可以同步任意数量的独立文件夹，并指定它在本机与另一台电脑上的对应路径。").
		TextColor(t.TextMuted).FontSize(t.Rem(0.82))

	removeID := ""
	changed := false

	for i := range app.cfg.SyncFolders {
		idx := i
		folder := &app.cfg.SyncFolders[idx]
		isDefault := config.NormalizeFolderID(folder.ID) == config.DefaultFolderID

		ui.Column(c).Key(fmt.Sprintf("folder-map-%d", idx)).Padding(t.Space(2.5)).Gap(t.Space(2)).
			Radius(t.Radius).Background(t.Background).Border(1, t.Border).Children(func() {
			ui.Row(c).Gap(t.Space(2)).Children(func() {
				if ui.Switch(c.Key(fmt.Sprintf("sw-f-en-%d", idx)), &folder.Enabled).Label("启用").Changed() {
					changed = true
				}
				ui.Badge(c, "ID: "+folder.ID).Background(t.Accent.Alpha(0.12)).TextColor(t.Accent)
				if ui.TextInput(c.Key(fmt.Sprintf("inp-f-lbl-%d", idx)), &folder.Label).Placeholder("目录显示名称").Grow(1).Changed() {
					changed = true
				}
				if ui.Button(c.Key(fmt.Sprintf("btn-f-open-%d", idx)), "打开").Clicked() {
					if strings.TrimSpace(folder.LocalPath) != "" {
						app.openPath(folder.LocalPath)
					}
				}
				if !isDefault {
					if ui.Button(c.Key(fmt.Sprintf("btn-f-del-%d", idx)), "移除").Clicked() {
						removeID = folder.ID
					}
				}
			})

			ui.Row(c).Gap(t.Space(2)).Children(func() {
				ui.Text(c, "本机").TextColor(t.TextMuted).Width(t.Space(8))
				if ui.TextInput(c.Key(fmt.Sprintf("inp-f-loc-%d", idx)), &folder.LocalPath).
					Placeholder(`本机文件夹路径，例如 D:\Projects\Docs`).Grow(1).Changed() {
					if isDefault {
						app.cfg.SyncDir = folder.LocalPath
					}
					changed = true
				}
				if ui.Button(c.Key(fmt.Sprintf("btn-f-browse-%d", idx)), "浏览…").Clicked() {
					curPath := folder.LocalPath
					app.pickFolderWithCallback(c, curPath, func(chosen string) {
						if idx < len(app.cfg.SyncFolders) {
							app.cfg.SyncFolders[idx].LocalPath = chosen
							if isDefault {
								app.cfg.SyncDir = chosen
							}
							app.saveConfig()
							app.engine.UpdateConfig(app.cfg)
						}
					})
				}
			})

			ui.Row(c).Gap(t.Space(2)).Children(func() {
				ui.Text(c, "对端").TextColor(t.TextMuted).Width(t.Space(8))
				if ui.TextInput(c.Key(fmt.Sprintf("inp-f-rem-%d", idx)), &folder.RemotePath).
					Placeholder("映射至另一台电脑的目标文件夹，留空则由对端决定").Grow(1).Changed() {
					changed = true
				}
			})
		})
	}

	if removeID != "" {
		app.cfg.RemoveFolder(removeID)
		changed = true
		c.Toast("已移除同步文件夹映射: " + removeID)
	}

	if changed {
		app.cfg.Normalize()
		app.saveConfig()
		app.engine.UpdateConfig(app.cfg)
	}

	ui.Collapsible(c, "添加同步文件夹", &app.addFolderOpen, func() {
		ui.Column(c).Gap(t.Space(2)).Children(func() {
			ui.Row(c).Gap(t.Space(2)).Children(func() {
				ui.TextInput(c.Key("inp-new-f-id"), &app.newFolderID).
					Placeholder("映射 ID (可留空自动生成)").Grow(1)
				ui.TextInput(c.Key("inp-new-f-lbl"), &app.newFolderLabel).
					Placeholder("显示名称").Grow(1)
			})
			ui.Row(c).Gap(t.Space(2)).Children(func() {
				ui.TextInput(c.Key("inp-new-f-loc"), &app.newFolderLocal).
					Placeholder(`本机文件夹路径，例如 D:\Code\MyNotes`).Grow(1)
				if ui.Button(c.Key("btn-new-f-browse"), "选择…").Clicked() {
					app.pickFolderWithCallback(c, app.newFolderLocal, func(chosen string) {
						app.newFolderLocal = chosen
						if strings.TrimSpace(app.newFolderLabel) == "" {
							app.newFolderLabel = filepath.Base(chosen)
						}
						if strings.TrimSpace(app.newFolderID) == "" {
							app.newFolderID = config.NormalizeFolderID(filepath.Base(chosen))
						}
					})
				}
			})
			ui.Row(c).Gap(t.Space(2)).Children(func() {
				ui.TextInput(c.Key("inp-new-f-rem"), &app.newFolderRemote).
					Placeholder("另一台电脑的目标文件夹（选填）").Grow(1)
				if ui.Button(c.Key("btn-add-folder-map"), "添加").
					Disabled(strings.TrimSpace(app.newFolderLocal) == "").Clicked() {
					id := config.NormalizeFolderID(app.newFolderID)
					if app.newFolderID == "" || id == config.DefaultFolderID {
						base := config.NormalizeFolderID(filepath.Base(strings.TrimSpace(app.newFolderLocal)))
						if base == "" || base == config.DefaultFolderID {
							base = fmt.Sprintf("folder-%d", len(app.cfg.SyncFolders)+1)
						}
						id = base
					}
					lbl := strings.TrimSpace(app.newFolderLabel)
					if lbl == "" {
						lbl = filepath.Base(strings.TrimSpace(app.newFolderLocal))
					}
					app.cfg.UpsertFolder(config.FolderMapping{
						ID:         id,
						Label:      lbl,
						LocalPath:  strings.TrimSpace(app.newFolderLocal),
						RemotePath: strings.TrimSpace(app.newFolderRemote),
						Enabled:    true,
					})
					app.newFolderID = ""
					app.newFolderLabel = ""
					app.newFolderLocal = ""
					app.newFolderRemote = ""
					app.saveConfig()
					app.engine.UpdateConfig(app.cfg)
					c.Toast(fmt.Sprintf("已添加同步文件夹映射: %s (%s)", lbl, id))
				}
			})
		})
	})
}

func (app *App) resolveEntryFullPath(relPath string) string {
	if _, _, fullPath, ok := syncer.ResolveFolderFullPath(app.cfg, relPath); ok {
		return fullPath
	}
	return filepath.Join(app.cfg.SyncDir, filepath.FromSlash(relPath))
}

func (app *App) pickFolderWithCallback(c *ui.Context, defaultDir string, onChosen func(string)) {
	if defaultDir == "" {
		defaultDir = app.cfg.SyncDir
	}
	if app.PickFolderFunc != nil {
		chosen, err := app.PickFolderFunc(defaultDir)
		if err == nil && chosen != "" {
			onChosen(chosen)
			c.Toast("已选择文件夹: " + chosen)
		}
		return
	}
	if app.win == nil {
		return
	}
	go func() {
		paths, err := mygo.Dialog.Open(mygo.OpenDialogOptions{
			Parent:      app.win,
			Title:       "选择需要实时同步的文件夹",
			Directory:   true,
			DefaultPath: defaultDir,
		})
		if err != nil || len(paths) == 0 || paths[0] == "" {
			return
		}
		chosen := paths[0]
		app.win.Update(func() {
			onChosen(chosen)
			app.pendingToast = "已选择同步文件夹: " + chosen
		})
	}()
}

func (app *App) pickSyncDirectory(c *ui.Context) {
	app.pickFolderWithCallback(c, app.cfg.SyncDir, func(chosen string) {
		app.cfg.SyncDir = chosen
		if len(app.cfg.SyncFolders) > 0 {
			app.cfg.SyncFolders[0].LocalPath = chosen
		}
		app.saveConfig()
		app.engine.UpdateConfig(app.cfg)
	})
}

// applyPassword derives the shared key from a user password. Both computers
// run the same derivation, so typing the same password on each side pairs them
// without anyone copying a long token.
func (app *App) applyPassword(c *ui.Context) {
	password := strings.TrimSpace(app.passwordInput)
	if password == "" {
		c.Toast("请先输入同步密码")
		return
	}
	if len(password) < 6 {
		c.Toast("密码至少 6 位，避免两端误配到同一弱密钥")
		return
	}
	token := config.DeriveTokenFromInput(password)
	app.cfg.AuthToken = token
	app.passwordInput = ""
	app.saveConfig()
	app.engine.UpdateConfig(app.cfg)
	c.Toast("已根据密码生成同步密钥，另一端输入同一密码即可连接")
}

func (app *App) applyShareCodeInput(c *ui.Context) {
	remoteURL, tok, err := config.DecodeShareCode(app.shareInput)
	if err != nil {
		c.Toast("解析连接码失败: " + err.Error())
		return
	}
	app.cfg.RemoteURL = remoteURL
	if tok != "" {
		app.cfg.AuthToken = tok
	}
	app.shareInput = ""
	app.saveConfig()
	app.engine.UpdateConfig(app.cfg)
	c.Toast("已成功导入隧道地址与访问密钥")
}

// copyShareCode encodes the current public/local endpoint into a synct:// code.
func (app *App) copyShareCode(c *ui.Context) {
	stats := app.engine.Stats()
	_, pubURL, lanURL, _ := app.tunnel.State()
	activeURL := app.activeShareURL(stats, pubURL, lanURL)
	shareCode := config.EncodeShareCode(activeURL, app.cfg.AuthToken)
	if activeURL == "" && stats.LocalPort > 0 {
		activeURL = fmt.Sprintf("https://localhost:%d", stats.LocalPort)
		shareCode = config.EncodeShareCode(activeURL, app.cfg.AuthToken)
	}
	if shareCode == "" {
		c.Toast("请先启动隧道或填写远端地址后再复制连接码")
		return
	}
	app.writeClipboard(shareCode)
	c.Toast("已复制 synct:// 一键连接码 (" + activeURL + ")")
}

func (app *App) activeShareURL(stats syncer.Stats, pubURL, lanURL string) string {
	activeURL := pubURL
	if activeURL == "" && app.cfg.CustomPublicURL != "" {
		activeURL = app.cfg.CustomPublicURL
	}
	if activeURL == "" {
		activeURL = lanURL
	}
	if app.cfg.Role == config.RoleClient && app.cfg.RemoteURL != "" {
		activeURL = app.cfg.RemoteURL
	}
	if activeURL == "" && stats.LocalPort > 0 {
		activeURL = fmt.Sprintf("https://localhost:%d", stats.LocalPort)
	}
	return activeURL
}

func (app *App) errorBannerText(stats syncer.Stats, tunErr string) string {
	if stats.LargeUploadBlocked {
		return "有大文件因隧道单次请求上限被拒：对端 SyncT 版本较旧，不支持分块上传。请把两端都升级到 v1.3.0 或更新版本。"
	}
	if stats.LastError != "" {
		return stats.LastError
	}
	if stats.HealthWarning != "" {
		return stats.HealthWarning
	}
	if stats.LastScanError != "" {
		return stats.LastScanError
	}
	return tunErr
}

// scheduleBinaryRefresh debounces cloudflared version probes triggered by typing.
func (app *App) scheduleBinaryRefresh() {
	path := app.cfg.CloudflaredPath
	if app.binaryRefreshTimer != nil {
		app.binaryRefreshTimer.Stop()
	}
	app.binaryRefreshTimer = time.AfterFunc(600*time.Millisecond, func() {
		binPath, version := app.tunnel.RefreshBinary(path)
		app.postBinaryRefreshToast(binPath, version)
	})
}

// refreshBinaryAsync probes the binary off the UI thread (button click).
func (app *App) refreshBinaryAsync() {
	path := app.cfg.CloudflaredPath
	go func() {
		binPath, version := app.tunnel.RefreshBinary(path)
		app.postBinaryRefreshToast(binPath, version)
	}()
}

func (app *App) postBinaryRefreshToast(binPath, version string) {
	if app.win == nil {
		return // Headless test mode
	}
	app.win.Update(func() {
		if binPath != "" {
			app.pendingToast = "已检测到 Cloudflared: " + version
		} else {
			app.pendingToast = "未检测到 cloudflared.exe"
		}
	})
}

// cachedVisibleFiles avoids re-filtering and re-sorting the whole table on
// every frame; it only rebuilds when the index version or a filter changes.
func (app *App) cachedVisibleFiles(files []syncer.FileEntry, version uint64) []syncer.FileEntry {
	cache := &app.fileCache
	if cache.valid &&
		cache.version == version &&
		cache.search == app.fileSearch &&
		cache.filter == app.fileFilterIdx &&
		cache.sortCol == app.fileSort.Column &&
		cache.sortDesc == app.fileSort.Descending &&
		len(cache.files) == len(files) {
		return cache.result
	}
	result := app.filterAndSortFiles(files)
	cache.files = files
	cache.version = version
	cache.search = app.fileSearch
	cache.filter = app.fileFilterIdx
	cache.sortCol = app.fileSort.Column
	cache.sortDesc = app.fileSort.Descending
	cache.result = result
	cache.valid = true
	return result
}

func (app *App) filterAndSortFiles(files []syncer.FileEntry) []syncer.FileEntry {
	q := strings.ToLower(strings.TrimSpace(app.fileSearch))
	out := make([]syncer.FileEntry, 0, len(files))
	for _, f := range files {
		switch app.fileFilterIdx {
		case 1:
			if f.Status != syncer.StatusSynced {
				continue
			}
		case 2:
			if f.Status == syncer.StatusSynced {
				continue
			}
		}
		if q != "" {
			if !strings.Contains(strings.ToLower(f.RelPath), q) &&
				!strings.Contains(strings.ToLower(f.FolderLabel), q) &&
				!strings.Contains(strings.ToLower(string(f.Status)), q) &&
				!strings.Contains(strings.ToLower(f.Direction), q) {
				continue
			}
		}
		out = append(out, f)
	}

	col := app.fileSort.Column
	desc := app.fileSort.Descending
	sort.SliceStable(out, func(i, j int) bool {
		var less bool
		switch col {
		case "folder":
			less = out[i].FolderLabel < out[j].FolderLabel
		case "size":
			less = out[i].Size < out[j].Size
		case "mtime":
			less = out[i].ModTime.Before(out[j].ModTime)
		case "status":
			less = out[i].Status < out[j].Status
		default:
			less = out[i].RelPath < out[j].RelPath
		}
		if desc {
			return !less
		}
		return less
	})
	return out
}

func pendingFiles(files []syncer.FileEntry) []syncer.FileEntry {
	out := make([]syncer.FileEntry, 0, 16)
	for _, f := range files {
		if f.Status != syncer.StatusSynced {
			out = append(out, f)
		}
	}
	return out
}

func sortPending(files []syncer.FileEntry, order ui.SortOrder) {
	sort.SliceStable(files, func(i, j int) bool {
		var less bool
		switch order.Column {
		case "folder":
			less = files[i].FolderLabel < files[j].FolderLabel
		case "size":
			less = files[i].Size < files[j].Size
		case "mtime":
			less = files[i].ModTime.Before(files[j].ModTime)
		case "status":
			less = files[i].Status < files[j].Status
		default:
			less = files[i].RelPath < files[j].RelPath
		}
		if order.Descending {
			return !less
		}
		return less
	})
}

func (app *App) formatCurrentLogsText(syncLogs []syncer.LogEntry, tunLogs []string) string {
	var sb strings.Builder
	if app.logTab == 0 {
		for _, l := range syncLogs {
			sb.WriteString(fmt.Sprintf("[%s] [%s] %s\n", l.Time.Format("15:04:05.000"), l.Level, l.Message))
		}
	} else {
		for _, l := range tunLogs {
			sb.WriteString(l + "\n")
		}
	}
	return sb.String()
}

func (app *App) stateSubtext(stats syncer.Stats) string {
	if app.cfg.Role == config.RoleHost {
		if stats.LocalPort > 0 {
			return fmt.Sprintf("监听 localhost:%d · 在线节点 %d", stats.LocalPort, stats.ConnectedPeers)
		}
		return "本机服务端 · 待启动"
	}
	if stats.RemoteConnected {
		return "已连接远端主机 " + stats.RemoteNodeID
	}
	return "工作电脑客户端 · 待连接"
}

func (app *App) tunnelSubtext(pubURL, lanURL string) string {
	if pubURL != "" {
		return pubURL
	}
	if app.cfg.Role == config.RoleHost && app.cfg.CustomPublicURL != "" {
		return app.cfg.CustomPublicURL + " → localhost:" + strconv.Itoa(app.cfg.LocalPort)
	}
	if lanURL != "" {
		return lanURL
	}
	if app.cfg.Role == config.RoleClient {
		if app.cfg.RemoteURL != "" {
			return app.cfg.RemoteURL
		}
		return config.DefaultPublicURL
	}
	return "点击「开始同步」启动隧道"
}

func (app *App) stateColor(t *ui.Theme, st syncer.EngineState) ui.Color {
	switch st {
	case syncer.StateRunning, syncer.StateSyncing:
		return t.Success
	case syncer.StateStarting, syncer.StateConnecting:
		return t.Warning
	case syncer.StateError:
		return t.Danger
	default:
		return t.TextMuted
	}
}

func (app *App) tunnelColor(t *ui.Theme, st tunnel.TunnelState) ui.Color {
	switch st {
	case tunnel.TunnelRunning, tunnel.TunnelDirectOn:
		return t.Success
	case tunnel.TunnelStarting:
		return t.Warning
	case tunnel.TunnelError:
		return t.Danger
	default:
		return t.TextMuted
	}
}

func (app *App) fileStatusColor(t *ui.Theme, st syncer.FileStatus) ui.Color {
	switch st {
	case syncer.StatusSynced:
		return t.Success
	case syncer.StatusUploading, syncer.StatusDownloading, syncer.StatusPending:
		return t.Accent
	case syncer.StatusConflict:
		return t.Warning
	case syncer.StatusError:
		return t.Danger
	default:
		return t.Text
	}
}

func (app *App) logLevelColor(t *ui.Theme, level string) ui.Color {
	switch level {
	case "SYNC":
		return t.Success
	case "WARN":
		return t.Warning
	case "ERROR":
		return t.Danger
	default:
		return t.Accent
	}
}

func tunnelModeIndex(m config.TunnelMode) int {
	switch m {
	case config.TunnelSystem:
		return 0
	case config.TunnelToken:
		return 1
	case config.TunnelQuick:
		return 2
	case config.TunnelDirect:
		return 3
	default:
		return 0
	}
}

func parseTunnelMode(label string) config.TunnelMode {
	switch {
	case strings.HasPrefix(label, "复用系统"), strings.HasPrefix(label, "System"):
		return config.TunnelSystem
	case strings.HasPrefix(label, "托管"), strings.HasPrefix(label, "Token"):
		return config.TunnelToken
	case strings.HasPrefix(label, "临时"), strings.HasPrefix(label, "Quick"):
		return config.TunnelQuick
	case strings.HasPrefix(label, "仅局域网"), strings.HasPrefix(label, "LAN"):
		return config.TunnelDirect
	default:
		return config.TunnelSystem
	}
}

func syncDirectionIndex(d config.SyncDirection) int {
	switch d {
	case config.SyncPushOnly:
		return 1
	case config.SyncPullOnly:
		return 2
	default:
		return 0
	}
}

func parseSyncDirection(label string) config.SyncDirection {
	switch {
	case strings.HasPrefix(label, "仅发送"):
		return config.SyncPushOnly
	case strings.HasPrefix(label, "仅接收"):
		return config.SyncPullOnly
	default:
		return config.SyncBidirectional
	}
}

func conflictPolicyIndex(p config.ConflictPolicy) int {
	if p == config.ConflictNewerWins {
		return 1
	}
	return 0
}

func parseConflictPolicy(label string) config.ConflictPolicy {
	if strings.HasPrefix(label, "以最新") {
		return config.ConflictNewerWins
	}
	return config.ConflictKeepBoth
}
