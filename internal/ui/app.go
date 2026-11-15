package ui

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

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
	SectionLogs     = "logs"
	SectionSettings = "settings"
)

// Select option labels
var (
	tunnelModeOptions = []string{
		"System Tunnel (复用本机已运行的系统 Cloudflared)",
		"Token Tunnel (由本软件托管启动 Cloudflared Token 隧道)",
		"Quick Tunnel (免配置临时公网隧道)",
		"LAN Direct (仅局域网/本地直连)",
	}
	syncDirOptions = []string{
		"双向实时同步 (本地 <-> 远端)",
		"仅发送镜像 (本地 -> 远端)",
		"仅接收镜像 (远端 -> 本地)",
	}
	conflictOptions = []string{
		"保留双方冲突副本 (.conflict)",
		"最新修改时间覆盖 (Last-Write-Wins)",
	}
)

// App holds the native MyGo UI state and coordinates the sync engine and Cloudflared tunnel manager.
type App struct {
	cfg    config.Config
	engine *syncer.Engine
	tunnel *tunnel.Manager
	win    *mygo.Window

	// Navigation & Layout
	section      string
	navOpen      bool
	toolsOpen    bool
	roleIndex    int
	logTab       int
	portInput    string
	shareInput   string
	showToken    bool
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

	// Logs List state
	syncLogList   ui.ListState
	tunnelLogList ui.ListState

	// Confirmation Dialogs
	confirmClearLogsOpen bool
	confirmResetCfgOpen  bool

	// Power keep-awake release callback
	keepAwakeRelease func()

	// Optional hook for directory picker (allows deterministic testing)
	PickFolderFunc func(current string) (string, error)
}

// NewApp constructs the SyncT application state.
func NewApp(cfg config.Config, statePath string) *App {
	cfg.Normalize()
	app := &App{
		cfg:             cfg,
		section:         SectionOverview,
		navOpen:         true,
		toolsOpen:       true,
		portInput:       strconv.Itoa(cfg.LocalPort),
		selectedFileRow: -1,
	}
	if cfg.Role == config.RoleClient {
		app.roleIndex = 1
	}

	app.engine = syncer.NewEngine(cfg, statePath, app.requestRedraw)
	app.tunnel = tunnel.NewManager(cfg.CloudflaredPath, app.requestRedraw, func(pubURL string) {
		app.engine.Logf("Cloudflared 公网隧道已就绪: %s", pubURL)
		app.requestRedraw()
	})

	return app
}

// SetWindow binds the native mygo.Window to the App so background goroutines can trigger frames.
func (app *App) SetWindow(win *mygo.Window) {
	app.win = win
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

func (app *App) requestRedraw() {
	if app.win != nil {
		app.win.Invalidate()
	}
}

// StartAll starts the sync engine and (when in Host mode) the Cloudflared tunnel.
func (app *App) StartAll() error {
	app.syncFieldsToConfig()
	_ = config.Save(app.cfg)
	app.engine.UpdateConfig(app.cfg)

	if err := app.engine.Start(); err != nil {
		return err
	}

	if app.cfg.Role == config.RoleHost {
		stats := app.engine.Stats()
		if err := app.tunnel.Start(app.cfg, stats.LocalPort); err != nil {
			app.engine.LogLevel("WARN", err.Error())
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

// View builds the entire Windows 11 native interface for each frame.
func (app *App) View(c *ui.Context) {
	// Apply Cloudflare-inspired warm orange/amber accent while respecting desktop light/dark mode
	t := *c.Theme().WithAccent(ui.Hex("#ea580c"))
	c.SetTheme(&t)
	c.SetMenuPresentation(ui.PopoverMenus)

	if app.pendingToast != "" {
		c.Toast(app.pendingToast)
		app.pendingToast = ""
	}

	root := ui.Column(c).Fill().Background(t.Background)
	if paths := root.DroppedFiles(); len(paths) > 0 {
		count, err := app.engine.ImportFiles(paths)
		if err != nil {
			c.Toast("导入拖放文件失败: " + err.Error())
		} else if count > 0 {
			c.Toast(fmt.Sprintf("已将 %d 个文件导入同步文件夹并触发同步", count))
		}
	}

	root.Children(func() {
		app.buildToolbar(c, &t)
		ui.Divider(c)

		ui.Split(
			c,
			&app.cfg.SidebarWidth,
			func() { app.buildSidebar(c, &t) },
			func() { app.buildMainPane(c, &t) },
		).SplitReset(220).Grow(1)

		ui.Divider(c)
		app.buildStatusBar(c, &t)
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
		_ = config.Save(app.cfg)
		app.engine.UpdateConfig(app.cfg)
		c.Toast("已恢复默认便携配置")
	}
}

func (app *App) buildToolbar(c *ui.Context, t *ui.Theme) {
	running := app.engine.IsRunning()
	stats := app.engine.Stats()
	tunState, pubURL, lanURL, _ := app.tunnel.State()

	ui.Row(c).Padding(t.Space(2), t.Space(3)).Gap(t.Space(2)).Background(t.Surface).Children(func() {
		if AppLogoBitmap != nil {
			ui.Image(c, AppLogoBitmap).Size(22, 22).Fit(ui.Contain)
		} else {
			ui.Icon(c, IconCloud).TextColor(t.Accent)
		}
		ui.Text(c, "SyncT").FontSize(t.Rem(1.1)).Bold()
		ui.Badge(c, "Portable").Background(t.Accent.Alpha(0.15)).TextColor(t.Accent)
		if app.cfg.LowPrivilegeMode {
			ui.Badge(c, "User 权限克制").Background(t.Success.Alpha(0.15)).TextColor(t.Success)
		}

		if ui.Segmented(c, &app.roleIndex, "主机模式 (发布隧道)", "节点模式 (连接远端)").Label("工作模式").Changed() {
			if running {
				app.StopAll()
			}
			app.syncFieldsToConfig()
			_ = config.Save(app.cfg)
			app.engine.UpdateConfig(app.cfg)
			if app.cfg.Role == config.RoleHost {
				c.Toast("已切换为主机模式：开启 Cloudflared 隧道共享文件夹")
			} else {
				c.Toast("已切换为节点模式：连接到远端 Cloudflared 隧道")
			}
		}

		ui.Spacer(c)

		ui.Toolbar(c, func() {
			if !running {
				startLabel := "启动隧道并同步"
				if app.roleIndex == 1 {
					startLabel = "连接隧道并同步"
				}
				if ui.PrimaryButton(c.Key("btn-start"), startLabel).Clicked() {
					if err := app.StartAll(); err != nil {
						c.Toast("启动失败: " + err.Error())
					} else {
						c.Toast("实时同步已启动")
					}
				}
			} else {
				if ui.Button(c.Key("btn-stop"), "停止同步").Clicked() {
					app.StopAll()
					c.Toast("已停止实时同步与隧道连接")
				}
			}

			if ui.Button(c.Key("btn-scan"), "立即扫描同步").Clicked() {
				app.engine.TriggerManualSync()
				c.Toast("已触发文件夹增量扫描与对账")
			}

			if ui.Button(c.Key("btn-open-dir"), "打开同步目录").Clicked() {
				if app.win != nil {
					go mygo.Shell.OpenPath(app.cfg.SyncDir)
				}
				c.Toast("正在打开目录: " + app.cfg.SyncDir)
			}

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
			shareCode := config.EncodeShareCode(activeURL, app.cfg.AuthToken)
			if ui.Button(c.Key("btn-copy-code"), "复制连接码").Disabled(shareCode == "" && tunState == tunnel.TunnelStopped && stats.LocalPort == 0).Clicked() {
				if shareCode == "" && stats.LocalPort > 0 {
					shareCode = config.EncodeShareCode(fmt.Sprintf("https://localhost:%d", stats.LocalPort), app.cfg.AuthToken)
				}
				if shareCode != "" {
					if app.win != nil {
						mygo.Clipboard.WriteText(shareCode)
					}
					c.Toast("已复制 synct:// 一键连接码到剪贴板 (" + activeURL + ")")
				} else {
					c.Toast("请先启动隧道或填写远端地址后再复制连接码")
				}
			}
		}).Label("主操作工具栏")
	})
}

func (app *App) buildSidebar(c *ui.Context, t *ui.Theme) {
	stats := app.engine.Stats()
	logsCount := len(app.engine.Logs()) + len(app.tunnel.Logs())

	ui.Column(c).Fill().Background(t.Surface).Children(func() {
		ui.Sidebar(c, &app.section, func() {
			ui.SidebarSection(c, "实时同步工作台", &app.navOpen, func() {
				ui.SidebarItem(c, SectionOverview, IconCloud, "概览与隧道").Children(func() {
					if app.engine.IsRunning() {
						ui.Badge(c, "运行中").Background(t.Success).TextColor(t.AccentText)
					}
				})
				ui.SidebarItem(c, SectionFiles, IconFolder, "同步文件列表").Children(func() {
					ui.Badge(c, strconv.Itoa(stats.TotalFiles))
				})
				ui.SidebarItem(c, SectionLogs, IconActivity, "实时传输日志").Children(func() {
					if logsCount > 0 {
						ui.Badge(c, strconv.Itoa(logsCount))
					}
				})
			})
			ui.SidebarSection(c, "便携配置与规则", &app.toolsOpen, func() {
				ui.SidebarItem(c, SectionSettings, IconSettings, "忽略规则与设置").Children(func() {
					ui.Badge(c, strconv.Itoa(len(app.cfg.IgnorePatterns)))
				})
			})
		}).Grow(1)

		ui.Divider(c)
		app.buildBottomLeftSyncInfo(c, t, stats)
	})
}

func (app *App) buildBottomLeftSyncInfo(c *ui.Context, t *ui.Theme, stats syncer.Stats) {
	folderCnt := max(stats.FolderCount, len(app.cfg.EnabledFolders()))
	ui.Column(c).Padding(t.Space(2.5)).Gap(t.Space(1.5)).Background(t.Surface).Children(func() {
		ui.Row(c).Gap(t.Space(1.5)).Children(func() {
			ui.Icon(c, IconActivity).TextColor(t.Accent)
			ui.Text(c, "实时同步信息").Bold().Grow(1)
			ui.Badge(c, fmt.Sprintf("%d 目录", folderCnt)).Background(t.Accent.Alpha(0.15)).TextColor(t.Accent)
		})

		// Real-time Upload / Download Speeds
		ui.Row(c).Gap(t.Space(2)).Children(func() {
			ui.Textf(c, "↑ %s", syncer.FormatSpeed(stats.UploadSpeed)).TextColor(t.Success).Bold().Grow(1).SingleLine()
			ui.Textf(c, "↓ %s", syncer.FormatSpeed(stats.DownloadSpeed)).TextColor(t.Accent).Bold().Grow(1).SingleLine()
		})

		// File Count & Total File Size
		ui.Row(c).Gap(t.Space(1)).Children(func() {
			ui.Text(c, "文件总计:").TextColor(t.TextMuted)
			ui.Spacer(c)
			ui.Textf(c, "%d 个 · %s", stats.TotalFiles, syncer.FormatBytes(stats.TotalBytes)).Bold().SingleLine()
		})

		// Cumulative Upload / Download Size
		ui.Row(c).Gap(t.Space(1)).Children(func() {
			ui.Text(c, "累计流量:").TextColor(t.TextMuted)
			ui.Spacer(c)
			ui.Textf(c, "↑ %s / ↓ %s", syncer.FormatBytes(stats.BytesUploaded), syncer.FormatBytes(stats.BytesDownloaded)).TextColor(t.TextMuted).SingleLine()
		})

		// Latest synced file & its size
		if stats.LastSyncFile != "" {
			ui.Row(c).Gap(t.Space(1)).Children(func() {
				ui.Text(c, "最近文件:").TextColor(t.TextMuted)
				ui.Spacer(c)
				ui.Textf(c, "%s (%s)", filepath.Base(stats.LastSyncFile), syncer.FormatBytes(stats.LastSyncBytes)).TextColor(t.TextMuted).SingleLine()
			})
		} else {
			ui.Row(c).Gap(t.Space(1)).Children(func() {
				ui.Text(c, "同步状态:").TextColor(t.TextMuted)
				ui.Spacer(c)
				ui.Text(c, string(stats.State)).TextColor(app.stateColor(t, stats.State)).SingleLine()
			})
		}
	})
}

func (app *App) buildMainPane(c *ui.Context, t *ui.Theme) {
	switch app.section {
	case SectionFiles:
		app.buildFilesPage(c, t)
	case SectionLogs:
		app.buildLogsPage(c, t)
	case SectionSettings:
		app.buildSettingsPage(c, t)
	default:
		app.buildOverviewPage(c, t)
	}
}

func (app *App) buildOverviewPage(c *ui.Context, t *ui.Theme) {
	stats := app.engine.Stats()
	tunState, pubURL, lanURL, tunErr := app.tunnel.State()
	binPath, binVer := app.tunnel.BinaryInfo()
	downloading, dlProg := app.tunnel.IsDownloading()

	ui.Scroll(c.Key("overview-scroll")).Fill().Children(func() {
		ui.Column(c).Padding(t.Space(4)).Gap(t.Space(4)).Children(func() {
			// 1. Live Status KPI Cards Row
			ui.Grid(c).Columns(4).Gap(t.Space(3)).Children(func() {
				app.buildStatCard(c, t, "同步状态", string(stats.State), app.stateSubtext(stats), app.stateColor(t, stats.State))
				app.buildStatCard(c, t, "Cloudflared 隧道", string(tunState), app.tunnelSubtext(pubURL, lanURL), app.tunnelColor(t, tunState))
				app.buildStatCard(c, t, "受控文件总数", fmt.Sprintf("%d 个文件 (%d 目录)", stats.TotalFiles, max(stats.FolderCount, 1)), "总容量 "+syncer.FormatBytes(stats.TotalBytes), t.Accent)
				app.buildStatCard(c, t, "累计实时传输", fmt.Sprintf("↑ %s / ↓ %s", syncer.FormatBytes(stats.BytesUploaded), syncer.FormatBytes(stats.BytesDownloaded)), fmt.Sprintf("速率 ↑ %s · ↓ %s (%d 次事件)", syncer.FormatSpeed(stats.UploadSpeed), syncer.FormatSpeed(stats.DownloadSpeed), stats.SyncedEvents), t.Text)
			})

			// Error banner if any
			if stats.LastError != "" || tunErr != "" {
				errMsg := stats.LastError
				if errMsg == "" {
					errMsg = tunErr
				}
				ui.Row(c).Padding(t.Space(3)).Gap(t.Space(2)).Radius(t.Radius).
					Background(t.Danger.Alpha(0.12)).Border(1, t.Danger).Children(func() {
					ui.Badge(c, "异常提示").Background(t.Danger).TextColor(t.AccentText)
					ui.Text(c, errMsg).TextColor(t.Danger).Grow(1)
				})
			}

			// 2. Folder & Connection Configuration Card
			ui.Column(c).Padding(t.Space(4)).Gap(t.Space(3)).Radius(t.Radius).
				Background(t.Surface).Border(1, t.Border).Children(func() {
				ui.Row(c).Gap(t.Space(2)).Children(func() {
					ui.Icon(c, IconFolder).TextColor(t.Accent)
					ui.Text(c, "同步文件夹与隧道连接配置").FontSize(t.Rem(1.05)).Bold().Grow(1)
					if ui.Button(c.Key("btn-save-cfg"), "保存配置").Clicked() {
						app.syncFieldsToConfig()
						if err := config.Save(app.cfg); err != nil {
							c.Toast("保存配置失败: " + err.Error())
						} else {
							app.engine.UpdateConfig(app.cfg)
							c.Toast("已保存便携配置至 data/config.json")
						}
					}
				})

				ui.Divider(c)

				ui.Form(c, func() {
					// Sync Folder Picker
					var dirErr string
					if strings.TrimSpace(app.cfg.SyncDir) == "" {
						dirErr = "请选择或输入需要实时同步的本地文件夹路径"
					}
					ui.Field(c, "指定同步文件夹", func() {
						ui.Row(c).Gap(t.Space(2)).Children(func() {
							if ui.TextInput(c.Key("input-sync-dir"), &app.cfg.SyncDir).Placeholder("例如 D:\\Work\\SharedFolder").Grow(1).Changed() {
								if len(app.cfg.SyncFolders) > 0 {
									app.cfg.SyncFolders[0].LocalPath = app.cfg.SyncDir
								}
								_ = config.Save(app.cfg)
							}
							if ui.Button(c.Key("btn-browse-dir"), "浏览选择…").Clicked() {
								app.pickSyncDirectory(c)
							}
						})
					}).Description("支持直接将文件从 Windows 资源管理器拖入本窗口自动同步").Error(dirErr)

					// Sync Direction
					dirIdx := syncDirectionIndex(app.cfg.SyncDirection)
					ui.Field(c, "实时同步模式", func() {
						choice := syncDirOptions[dirIdx]
						if ui.Select(c.Key("sel-sync-dir"), &choice, syncDirOptions).Label("实时同步模式").Changed() {
							app.cfg.SyncDirection = parseSyncDirection(choice)
							_ = config.Save(app.cfg)
							app.engine.UpdateConfig(app.cfg)
							c.Toast("同步方向已更新为: " + choice)
						}
					}).Description("双向模式自动比对修改时间与哈希；冲突时默认保留副本防止误覆盖")

					// Shared Auth Token
					ui.Field(c, "安全访问密钥", func() {
						ui.Row(c).Gap(t.Space(2)).Children(func() {
							inp := ui.TextInput(c.Key("input-auth-token"), &app.cfg.AuthToken).Placeholder("输入用于两端认证的密钥").Grow(1)
							if !app.showToken {
								inp.Password()
							}
							ui.Toggle(c.Key("toggle-show-token"), &app.showToken, "显示密钥")
							if ui.Button(c.Key("btn-gen-token"), "随机生成").Clicked() {
								app.cfg.AuthToken = config.GenerateToken()
								_ = config.Save(app.cfg)
								app.engine.UpdateConfig(app.cfg)
								c.Toast("已生成新的随机访问密钥")
							}
						})
					}).Description("两端必须使用相同的访问密钥 (X-SyncT-Token) 才能读写同步文件")

					// Mode-specific fields
					if app.roleIndex == 0 {
						// Host Mode settings
						ui.Fieldset(c, "Cloudflared 隧道发布设置 (主机端)", func() {
							tmIdx := tunnelModeIndex(app.cfg.TunnelMode)
							ui.Field(c, "隧道运行方式", func() {
								choice := tunnelModeOptions[tmIdx]
								if ui.Select(c.Key("sel-tunnel-mode"), &choice, tunnelModeOptions).Label("隧道运行方式").Changed() {
									app.cfg.TunnelMode = parseTunnelMode(choice)
									_ = config.Save(app.cfg)
									c.Toast("已切换隧道模式: " + choice)
								}
							}).Description("默认直接复用本机系统已在运行的 Cloudflared 服务，无需重复启动进程或填写 Token")

							if app.cfg.TunnelMode == config.TunnelSystem {
								ui.Row(c).Padding(t.Space(2.5)).Gap(t.Space(2)).Radius(t.Radius).
									Background(t.Success.Alpha(0.10)).Border(1, t.Success.Alpha(0.4)).Children(func() {
									ui.Badge(c, "复用系统隧道").Background(t.Success).TextColor(t.AccentText)
									statusText := "已自动对接本机正在运行的系统 Cloudflared 服务，直接将 https://localhost:18920 映射至 https://synctreei.suosmanthus.cyou。"
									if binVer != "" {
										statusText = fmt.Sprintf("%s · 直接复用系统隧道将 https://localhost:18920 发布至 %s，无需再次启动 cloudflared。", binVer, app.cfg.CustomPublicURL)
									}
									ui.Text(c, statusText).TextColor(t.Text).Grow(1)
								})

								ui.Field(c, "绑定的公网域名", func() {
									if ui.TextInput(c.Key("input-custom-pub-url"), &app.cfg.CustomPublicURL).Placeholder("https://synctreei.suosmanthus.cyou").Changed() {
										_ = config.Save(app.cfg)
									}
								}).Description("已预设系统 Cloudflared 绑定的专属域名 https://synctreei.suosmanthus.cyou")
							}

							if app.cfg.TunnelMode == config.TunnelToken {
								ui.Field(c, "Tunnel Token", func() {
									ui.TextInput(c.Key("input-tunnel-token"), &app.cfg.TunnelToken).Placeholder("eyJhIjoi... (Cloudflare Zero Trust 隧道 Token)").Password()
								}).Description("在 Cloudflare Zero Trust 控制台创建的固定隧道 Token")

								ui.Field(c, "绑定的公网域名", func() {
									if ui.TextInput(c.Key("input-custom-pub-url"), &app.cfg.CustomPublicURL).Placeholder("https://synctreei.suosmanthus.cyou").Changed() {
										_ = config.Save(app.cfg)
									}
								}).Description("已预设自用专属域名 https://synctreei.suosmanthus.cyou")
							}

							var portErr string
							if p, err := strconv.Atoi(strings.TrimSpace(app.portInput)); err != nil || p <= 0 || p > 65535 {
								portErr = "请输入 1 ~ 65535 之间的有效本地端口号"
							}
							ui.Field(c, "本地监听端口", func() {
								ui.Row(c).Gap(t.Space(2)).Children(func() {
									if ui.TextInput(c.Key("input-local-port"), &app.portInput).Placeholder("18920").Grow(1).Changed() {
										app.syncFieldsToConfig()
										_ = config.Save(app.cfg)
									}
									ui.Badge(c, fmt.Sprintf("https://localhost:%s", strings.TrimSpace(app.portInput))).
										Background(t.Accent.Alpha(0.15)).TextColor(t.Accent)
								})
							}).Description("仅绑定本机回环 (127.0.0.1 / ::1)，内建自签名 TLS 证书并同时兼容 HTTPS 与 HTTP，不触发防火墙提权弹窗").Error(portErr)
						})
					} else {
						// Client Mode settings
						ui.Fieldset(c, "远端 Cloudflared 隧道连接设置 (节点端 · 工作电脑 User 低权限)", func() {
							ui.Row(c).Padding(t.Space(2.5)).Gap(t.Space(2)).Radius(t.Radius).
								Background(t.Success.Alpha(0.10)).Border(1, t.Success.Alpha(0.4)).Children(func() {
								ui.Badge(c, "User 权限克制").Background(t.Success).TextColor(t.AccentText)
								ui.Text(c, "工作电脑客户端模式：零本地端口监听、无需启动 cloudflared.exe、零注册表写入，仅通过标准 HTTPS (443) 双向同步 https://synctreei.suosmanthus.cyou。").
									TextColor(t.Text).Grow(1)
							})

							ui.Field(c, "一键连接码解析", func() {
								ui.Row(c).Gap(t.Space(2)).Children(func() {
									ui.TextInput(c.Key("input-share-code"), &app.shareInput).Placeholder("粘贴 synct://connect/... 连接码或直接粘贴 st_... 访问密钥").Grow(1)
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
							}).Description("在本机服务端点击「复制连接码」后，可在工作电脑一键导入隧道域名与访问密钥")

							var remoteErr string
							if strings.TrimSpace(app.cfg.RemoteURL) == "" {
								remoteErr = "请填写 https://synctreei.suosmanthus.cyou 或其他隧道域名"
							}
							ui.Field(c, "远端隧道地址", func() {
								if ui.TextInput(c.Key("input-remote-url"), &app.cfg.RemoteURL).Placeholder("https://synctreei.suosmanthus.cyou").Changed() {
									_ = config.Save(app.cfg)
									app.engine.UpdateConfig(app.cfg)
								}
							}).Description("已默认预设为 https://synctreei.suosmanthus.cyou").Error(remoteErr)
						})
					}

					app.buildFolderMappingsSection(c, t)
				})
			})

			// 3. Share Code & Cloudflared Environment Card
			ui.Column(c).Padding(t.Space(4)).Gap(t.Space(3)).Radius(t.Radius).
				Background(t.Surface).Border(1, t.Border).Children(func() {
				ui.Row(c).Gap(t.Space(2)).Children(func() {
					ui.Icon(c, IconShield).TextColor(t.Accent)
					ui.Text(c, "系统 Cloudflared 状态与双机连接分享").FontSize(t.Rem(1.05)).Bold().Grow(1)
					if binPath != "" {
						ui.Badge(c, "系统隧道就绪").Background(t.Success).TextColor(t.AccentText)
					} else {
						ui.Badge(c, "仅客户端/未检测").Background(t.Warning).TextColor(t.AccentText)
					}
				})

				ui.Divider(c)

				if binPath != "" {
					ui.Textf(c, "当前 Cloudflared 来源: %s", binPath).TextColor(t.Text)
					if binVer != "" {
						ui.Textf(c, "运行状态与版本: %s", binVer).TextColor(t.TextMuted)
					}
				} else {
					ui.Text(c, "当前未检测到本地 cloudflared 进程（工作电脑客户端无需安装或运行 cloudflared）。").TextColor(t.TextMuted)
				}

				if downloading {
					ui.Progress(c, dlProg).Label("正在下载便携版 cloudflared.exe")
				}

				activeURL := pubURL
				if activeURL == "" {
					activeURL = lanURL
				}
				if app.cfg.Role == config.RoleClient && app.cfg.RemoteURL != "" {
					activeURL = app.cfg.RemoteURL
				}
				shareCode := config.EncodeShareCode(activeURL, app.cfg.AuthToken)
				if activeURL != "" {
					ui.Column(c).Padding(t.Space(3)).Gap(t.Space(1)).Radius(t.Radius).
						Background(t.Background).Border(1, t.Border).Children(func() {
						ui.Textf(c, "当前活动隧道地址: %s", activeURL).Bold()
						ui.Textf(c, "一键分享连接码: %s", shareCode).TextColor(t.TextMuted).SingleLine()
					})
				}

				ui.Row(c).Gap(t.Space(2)).Children(func() {
					if ui.Button(c.Key("btn-detect-cf"), "重新检测组件").Clicked() {
						p, v := app.tunnel.RefreshBinary(app.cfg.CloudflaredPath)
						if p != "" {
							c.Toast("已检测到 Cloudflared: " + v)
						} else {
							c.Toast("未检测到 cloudflared.exe")
						}
					}
					if ui.Button(c.Key("btn-dl-cf"), "自动下载便携版 cloudflared.exe").Disabled(downloading).Clicked() {
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
		})
	})
}

func (app *App) buildFilesPage(c *ui.Context, t *ui.Theme) {
	allFiles := app.engine.Files()
	filtered := app.filterAndSortFiles(allFiles)

	ui.Column(c).Fill().Padding(t.Space(4)).Gap(t.Space(3)).Children(func() {
		// Search & filter bar
		ui.Row(c).Gap(t.Space(3)).Children(func() {
			ui.SearchField(c.Key("search-files"), &app.fileSearch).
				Placeholder("搜索文件名、相对路径或同步方向…").
				Label("搜索同步文件").
				Grow(1)

			ui.Segmented(c.Key("seg-file-filter"), &app.fileFilterIdx, "全部文件", "已同步", "传输中/冲突").
				Label("按状态筛选")

			if ui.Button(c.Key("btn-rescan-files"), "刷新列表").Clicked() {
				app.engine.TriggerManualSync()
				c.Toast("已刷新本地同步文件索引")
			}
		})

		// Table container with surface face
		ui.Column(c).Grow(1).Radius(t.Radius).Background(t.Surface).Border(1, t.Border).Children(func() {
			if len(filtered) == 0 {
				ui.Column(c).Fill().Center().Gap(t.Space(3)).Padding(t.Space(6)).Children(func() {
					ui.Icon(c, IconFolder).TextColor(t.TextMuted)
					if len(allFiles) == 0 {
						ui.Text(c, "当前同步文件夹中暂无文件，您可以放入文件或直接将文件拖入窗口。").TextColor(t.TextMuted)
					} else {
						ui.Text(c, "没有匹配当前搜索条件或状态筛选的文件。").TextColor(t.TextMuted)
					}
					if ui.Button(c.Key("btn-empty-open"), "打开同步目录").Clicked() {
						if app.win != nil {
							go mygo.Shell.OpenPath(app.cfg.SyncDir)
						}
						c.Toast("正在打开同步目录: " + app.cfg.SyncDir)
					}
				})
				return
			}

			cols := []ui.TableColumn{
				{ID: "folder", Title: "所属目录", Width: 130, Sortable: true},
				{ID: "path", Title: "相对路径", Sortable: true},
				{ID: "size", Title: "大小", Width: 105, Align: ui.End, Sortable: true},
				{ID: "mtime", Title: "最后修改时间", Width: 160, Sortable: true},
				{ID: "dir", Title: "同步方向", Width: 115, Sortable: true},
				{ID: "status", Title: "状态", Width: 105, Sortable: true},
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
					ui.Text(c, item.FolderLabel).TextColor(t.Accent).SingleLine()
				case 1:
					ui.Text(c, item.RelPath).SingleLine()
				case 2:
					ui.Text(c, syncer.FormatBytes(item.Size)).SingleLine()
				case 3:
					ui.Text(c, item.ModTime.Format("2006-01-02 15:04:05")).TextColor(t.TextMuted).SingleLine()
				case 4:
					ui.Text(c, item.Direction).TextColor(t.TextMuted).SingleLine()
				case 5:
					ui.Text(c, string(item.Status)).TextColor(app.fileStatusColor(t, item.Status)).Bold().SingleLine()
				}
			}).Grow(1).Submitted() {
				if app.selectedFileRow >= 0 && app.selectedFileRow < len(filtered) {
					full := app.resolveEntryFullPath(filtered[app.selectedFileRow].RelPath)
					actionRevealPath = full
				}
			}

			if actionOpenPath != "" {
				if app.win != nil {
					go mygo.Shell.OpenPath(actionOpenPath)
				}
				c.Toast("已打开文件: " + filepath.Base(actionOpenPath))
			}
			if actionRevealPath != "" {
				if app.win != nil {
					go mygo.Shell.ShowItemInFolder(actionRevealPath)
				}
				c.Toast("已在资源管理器中定位: " + filepath.Base(actionRevealPath))
			}
			if actionCopyRel != "" {
				if app.win != nil {
					mygo.Clipboard.WriteText(actionCopyRel)
				}
				c.Toast("已复制路径: " + actionCopyRel)
			}
		})
	})
}

func (app *App) buildLogsPage(c *ui.Context, t *ui.Theme) {
	syncLogs := app.engine.Logs()
	tunLogs := app.tunnel.Logs()

	ui.Column(c).Fill().Padding(t.Space(4)).Gap(t.Space(3)).Children(func() {
		ui.Row(c).Gap(t.Space(3)).Children(func() {
			ui.Tabs(c, &app.logTab, "实时同步活动日志", "Cloudflared 隧道控制台输出")
			ui.Spacer(c)
			if ui.Button(c.Key("btn-copy-logs"), "复制当前日志").Clicked() {
				text := app.formatCurrentLogsText(syncLogs, tunLogs)
				if app.win != nil && text != "" {
					mygo.Clipboard.WriteText(text)
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
					ui.Column(c).Fill().Center().Gap(t.Space(2)).Padding(t.Space(6)).Children(func() {
						ui.Icon(c, IconActivity).TextColor(t.TextMuted)
						ui.Text(c, "暂无同步活动日志，启动同步或扫描文件夹后将实时滚动显示。").TextColor(t.TextMuted)
					})
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
						ui.Text(c, entry.Time.Format("15:04:05.000")).TextColor(t.TextMuted)
						ui.Badge(c, entry.Level).Background(app.logLevelColor(t, entry.Level).Alpha(0.16)).TextColor(app.logLevelColor(t, entry.Level))
						ui.Text(c, entry.Message).Grow(1)
					})
				}).Grow(1)
			} else {
				if len(tunLogs) == 0 {
					ui.Column(c).Fill().Center().Gap(t.Space(2)).Padding(t.Space(6)).Children(func() {
						ui.Icon(c, IconCloud).TextColor(t.TextMuted)
						ui.Text(c, "暂无 Cloudflared 隧道输出，在主机模式下启动隧道后将实时显示。").TextColor(t.TextMuted)
					})
					return
				}
				app.tunnelLogList.FollowEnd = true
				app.tunnelLogList.Key = func(row int) any { return row }
				ui.List(c, &app.tunnelLogList, len(tunLogs), func(i int) {
					ui.Text(c, tunLogs[i]).Padding(t.Space(1), t.Space(3))
				}).Grow(1)
			}
		})
	})
}

func (app *App) buildSettingsPage(c *ui.Context, t *ui.Theme) {
	ui.Scroll(c.Key("settings-scroll")).Fill().Children(func() {
		ui.Column(c).Padding(t.Space(4)).Gap(t.Space(4)).Children(func() {
			ui.Column(c).Padding(t.Space(4)).Gap(t.Space(3)).Radius(t.Radius).
				Background(t.Surface).Border(1, t.Border).Children(func() {
				ui.Row(c).Gap(t.Space(2)).Children(func() {
					ui.Icon(c, IconSettings).TextColor(t.Accent)
					ui.Text(c, "忽略规则、冲突处理与 Windows 11 便携设置").FontSize(t.Rem(1.05)).Bold().Grow(1)
					if ui.Button(c.Key("btn-reset-defaults"), "恢复默认设置").Clicked() {
						app.confirmResetCfgOpen = true
					}
				})

				ui.Divider(c)

				ui.Form(c, func() {
					ui.Fieldset(c, "文件过滤与冲突保护策略", func() {
						ui.Field(c, "忽略规则列表", func() {
							if ui.TokenField(c.Key("token-ignore"), &app.cfg.IgnorePatterns, config.PresetIgnoreSuggestions()).
								Placeholder("输入通配符 (如 *.tmp, .git) 按回车添加…").
								Label("忽略规则列表").
								Changed() {
								_ = config.Save(app.cfg)
								app.engine.UpdateConfig(app.cfg)
								c.Toast("已更新忽略规则并重新过滤文件")
							}
						}).Description("匹配的文件夹或文件名将不会被扫描、上传或下载 (.synct* 临时文件始终自动忽略)")

						confIdx := conflictPolicyIndex(app.cfg.ConflictPolicy)
						ui.Field(c, "版本冲突策略", func() {
							choice := conflictOptions[confIdx]
							if ui.Select(c.Key("sel-conflict-policy"), &choice, conflictOptions).Label("版本冲突策略").Changed() {
								app.cfg.ConflictPolicy = parseConflictPolicy(choice)
								_ = config.Save(app.cfg)
								app.engine.UpdateConfig(app.cfg)
								c.Toast("冲突策略已更新: " + choice)
							}
						}).Description("当两端同时修改同一文件时，保留冲突副本可确保任何一端的改动都不会丢失")

						ui.Field(c, "同步文件删除", func() {
							if ui.Switch(c.Key("sw-sync-del"), &app.cfg.SyncDeletions).Label("同步文件删除").Changed() {
								_ = config.Save(app.cfg)
								app.engine.UpdateConfig(app.cfg)
								c.Toast("已更新文件删除同步开关")
							}
						}).Description("开启后，一端删除文件会产生删除墓碑 (Tombstone) 并同步移除另一端的对应文件")
					})

					ui.Fieldset(c, "Windows 11 便携版运行行为", func() {
						ui.Field(c, "Cloudflared 路径", func() {
							ui.Row(c).Gap(t.Space(2)).Children(func() {
								if ui.TextInput(c.Key("input-cf-path"), &app.cfg.CloudflaredPath).
									Placeholder("留空则自动按 ./bin/cloudflared.exe -> 系统 PATH 顺序查找").
									Grow(1).Changed() {
									_ = config.Save(app.cfg)
									app.tunnel.RefreshBinary(app.cfg.CloudflaredPath)
								}
							})
						}).Description("便携移动使用时可将 cloudflared.exe 放在软件同级的 bin 目录中")

						ui.Field(c, "自动启动同步", func() {
							if ui.Switch(c.Key("sw-autostart"), &app.cfg.AutoStartSync).Label("自动启动同步").Changed() {
								_ = config.Save(app.cfg)
								c.Toast("已保存自动启动同步选项")
							}
						}).Description("打开 SyncT.exe 时立即按照上次保存的配置启动隧道与实时同步")

						ui.Field(c, "防止系统休眠", func() {
							if ui.Switch(c.Key("sw-keepawake"), &app.cfg.KeepAwake).Label("防止系统休眠").Changed() {
								_ = config.Save(app.cfg)
								app.updateKeepAwake(app.engine.IsRunning())
								c.Toast("已更新电源唤醒保持策略")
							}
						}).Description("同步运行期间调用 Windows 电源服务防止电脑睡眠中断公网隧道")

						ui.Field(c, "最小化到托盘", func() {
							if ui.Switch(c.Key("sw-tray"), &app.cfg.MinimizeToTray).Label("最小化到托盘").Changed() {
								_ = config.Save(app.cfg)
								c.Toast("已更新关闭窗口最小化至系统托盘设置")
							}
						}).Description("点击窗口关闭按钮时隐藏到 Windows 11 右下角系统托盘继续后台实时同步")

						ui.Field(c, "User 权限克制模式", func() {
							if ui.Switch(c.Key("sw-lowpriv"), &app.cfg.LowPrivilegeMode).Label("User 权限克制模式").Changed() {
								_ = config.Save(app.cfg)
								c.Toast("已更新 User 权限克制模式策略")
							}
						}).Description("专为受限 User 权限工作电脑设计：强制 asInvoker 权限清单、仅绑定回环地址、窗口状态与缓存全部隔离在 ./data/ 目录")
					})
				})

				ui.Divider(c)
				ui.Column(c).Gap(t.Space(1)).Children(func() {
					ui.Textf(c, "便携数据目录: %s", config.DataDir()).TextColor(t.TextMuted)
					ui.Textf(c, "便携配置文件: %s", config.ConfigPath()).TextColor(t.TextMuted)
					ui.Textf(c, "同步状态索引: %s", config.StatePath()).TextColor(t.TextMuted)
				})
			})
		})
	})
}

func (app *App) buildStatusBar(c *ui.Context, t *ui.Theme) {
	stats := app.engine.Stats()
	tunState, pubURL, lanURL, _ := app.tunnel.State()

	ui.Row(c).Padding(t.Space(1.5), t.Space(3)).Gap(t.Space(3)).Background(t.Surface).Children(func() {
		ui.Badge(c, string(stats.State)).Background(app.stateColor(t, stats.State).Alpha(0.18)).TextColor(app.stateColor(t, stats.State))
		ui.Textf(c, "↑ %s  ↓ %s", syncer.FormatSpeed(stats.UploadSpeed), syncer.FormatSpeed(stats.DownloadSpeed)).TextColor(t.Accent).Bold().SingleLine()
		ui.Textf(c, "文件: %d 个 (%s) · %d 个映射目录", stats.TotalFiles, syncer.FormatBytes(stats.TotalBytes), max(stats.FolderCount, 1)).TextColor(t.Text).SingleLine()
		ui.Textf(c, "主目录: %s", app.cfg.SyncDir).TextColor(t.TextMuted).SingleLine().Grow(1)

		if pubURL != "" {
			ui.Textf(c, "公网隧道: %s", pubURL).TextColor(t.Accent).SingleLine()
		} else if lanURL != "" {
			ui.Textf(c, "本地地址: %s", lanURL).TextColor(t.TextMuted).SingleLine()
		} else {
			ui.Textf(c, "隧道: %s", tunState).TextColor(t.TextMuted).SingleLine()
		}

		if !stats.LastSyncTime.IsZero() {
			ui.Textf(c, "最近同步: %s", stats.LastSyncTime.Format("15:04:05")).TextColor(t.TextMuted)
		}
	})
}

func (app *App) buildStatCard(c *ui.Context, t *ui.Theme, title, value, sub string, accentColor ui.Color) {
	ui.Column(c).Padding(t.Space(3)).Gap(t.Space(1)).Radius(t.Radius).
		Background(t.Surface).Border(1, t.Border).Children(func() {
		ui.Text(c, title).TextColor(t.TextMuted)
		ui.Text(c, value).FontSize(t.Rem(1.15)).Bold().TextColor(accentColor).SingleLine()
		ui.Text(c, sub).TextColor(t.TextMuted).SingleLine()
	})
}

func (app *App) buildFolderMappingsSection(c *ui.Context, t *ui.Theme) {
	ui.Fieldset(c, "多文件夹同步与跨机目录映射 (本机文件夹 <-> 另一台电脑目标文件夹)", func() {
		ui.Text(c, "支持同时同步多个独立文件夹，并可指定本机文件夹在另一台电脑上对应的目标文件夹路径（对端连接时将按映射标识自动创建或绑定目标目录）。").
			TextColor(t.TextMuted)

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
					ui.Badge(c, "ID: "+folder.ID).Background(t.Accent.Alpha(0.15)).TextColor(t.Accent)
					if ui.TextInput(c.Key(fmt.Sprintf("inp-f-lbl-%d", idx)), &folder.Label).Placeholder("目录显示名称").Grow(1).Changed() {
						changed = true
					}
					if ui.Button(c.Key(fmt.Sprintf("btn-f-open-%d", idx)), "打开目录").Clicked() {
						if app.win != nil && strings.TrimSpace(folder.LocalPath) != "" {
							go mygo.Shell.OpenPath(folder.LocalPath)
						}
					}
					if !isDefault {
						if ui.Button(c.Key(fmt.Sprintf("btn-f-del-%d", idx)), "移除").Clicked() {
							removeID = folder.ID
						}
					}
				})

				ui.Row(c).Gap(t.Space(2)).Children(func() {
					ui.Text(c, "本机目录:").TextColor(t.TextMuted)
					if ui.TextInput(c.Key(fmt.Sprintf("inp-f-loc-%d", idx)), &folder.LocalPath).
						Placeholder("本机要同步的文件夹路径，例如 D:\\Projects\\Docs").Grow(1).Changed() {
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
								_ = config.Save(app.cfg)
								app.engine.UpdateConfig(app.cfg)
							}
						})
					}
				})

				ui.Row(c).Gap(t.Space(2)).Children(func() {
					ui.Text(c, "对端映射:").TextColor(t.TextMuted)
					if ui.TextInput(c.Key(fmt.Sprintf("inp-f-rem-%d", idx)), &folder.RemotePath).
						Placeholder("映射至另一台电脑的目标文件夹路径 (例如 C:\\Users\\Work\\Desktop\\Docs，留空则由对端指定)").Grow(1).Changed() {
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
			_ = config.Save(app.cfg)
			app.engine.UpdateConfig(app.cfg)
		}

		// Add new mapped folder card
		ui.Column(c).Padding(t.Space(2.5)).Gap(t.Space(2)).Radius(t.Radius).
			Background(t.Surface).Border(1, t.Border).Children(func() {
			ui.Text(c, "新增同步文件夹 / 跨电脑文件夹映射").Bold()
			ui.Row(c).Gap(t.Space(2)).Children(func() {
				ui.TextInput(c.Key("inp-new-f-id"), &app.newFolderID).
					Placeholder("映射标识 ID (如 projects, reports，可留空自动生成)").Grow(1)
				ui.TextInput(c.Key("inp-new-f-lbl"), &app.newFolderLabel).
					Placeholder("显示名称 (如 工作项目文档)").Grow(1)
			})
			ui.Row(c).Gap(t.Space(2)).Children(func() {
				ui.TextInput(c.Key("inp-new-f-loc"), &app.newFolderLocal).
					Placeholder("本机文件夹路径 (例如 D:\\Code\\MyNotes)").Grow(1)
				if ui.Button(c.Key("btn-new-f-browse"), "选择本机目录…").Clicked() {
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
					Placeholder("另一台电脑的目标文件夹路径 (选填，例如 C:\\Users\\WorkUser\\Documents\\Notes)").Grow(1)
				if ui.Button(c.Key("btn-add-folder-map"), "添加文件夹映射").
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
					_ = config.Save(app.cfg)
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
		_ = config.Save(app.cfg)
		app.engine.UpdateConfig(app.cfg)
	})
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
	_ = config.Save(app.cfg)
	app.engine.UpdateConfig(app.cfg)
	c.Toast("已成功导入隧道地址与访问密钥")
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
		case "dir":
			less = out[i].Direction < out[j].Direction
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
			return fmt.Sprintf("监听 https://localhost:%d · 在线节点 %d", stats.LocalPort, stats.ConnectedPeers)
		}
		return "本机服务端 · 待启动 (https://localhost:18920)"
	}
	if stats.RemoteConnected {
		return "已连接远端主机 " + stats.RemoteNodeID
	}
	return "工作电脑客户端 (User低权限) · 待连接"
}

func (app *App) tunnelSubtext(pubURL, lanURL string) string {
	if pubURL != "" {
		return pubURL
	}
	if app.cfg.Role == config.RoleHost && app.cfg.CustomPublicURL != "" {
		return app.cfg.CustomPublicURL + " -> https://localhost:18920"
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
	return "点击上方按钮启动隧道"
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
	case strings.HasPrefix(label, "System"):
		return config.TunnelSystem
	case strings.HasPrefix(label, "Token"):
		return config.TunnelToken
	case strings.HasPrefix(label, "Quick"):
		return config.TunnelQuick
	case strings.HasPrefix(label, "LAN"):
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
	if strings.HasPrefix(label, "最新修改") {
		return config.ConflictNewerWins
	}
	return config.ConflictKeepBoth
}
