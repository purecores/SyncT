package main

import (
	"fmt"
	"log"
	"path/filepath"

	"github.com/egoist/mygo"
	"github.com/egoist/mygo/ui"

	"synct/internal/applog"
	"synct/internal/config"
	appui "synct/internal/ui"
)

// appVersion is stamped by hand with each release; it is only used in logs.
const appVersion = "1.4.0"

func main() {
	mygo.App.SetName("SyncT")

	// Strictly isolate all user data, window states, caches, and logs inside portable ./data/
	// so running on a User-privilege work computer never touches %AppData% or system directories.
	dataDir := config.DataDir()
	mygo.App.SetPath(mygo.PathUserData, dataDir)
	mygo.App.SetPath(mygo.PathCache, filepath.Join(dataDir, "cache"))
	mygo.App.SetPath(mygo.PathLogs, filepath.Join(dataDir, "logs"))

	applog.Init(filepath.Join(config.PortableRoot(), "SyncT.log"))
	defer applog.Close()
	applog.Write("INFO", fmt.Sprintf("SyncT %s 启动，数据目录 %s", appVersion, dataDir))

	cfg, err := config.Load()
	if err != nil {
		log.Printf("加载配置文件警告: %v", err)
	}

	app := appui.NewApp(cfg, config.StatePath())

	var mainWin *mygo.Window
	var quitting bool

	if !mygo.App.RequestSingleInstanceLock() {
		return
	}

	mygo.App.OnSecondInstance(func(_ []string, _ string) {
		if mainWin != nil {
			mainWin.Show()
			mainWin.Focus()
		}
	})

	mygo.App.OnDidBecomeActive(func() {
		mygo.ClearNotifications()
	})

	mygo.App.OnBeforeQuit(func(*mygo.QuitEvent) {
		quitting = true
		app.StopAll()
		applog.Write("INFO", "SyncT 退出")
		applog.Close()
	})

	mygo.App.WhenReady(func() {
		mainWin = mygo.NewWindow(mygo.WindowOptions{
			Title:     "SyncT",
			Width:     1060,
			Height:    720,
			MinWidth:  820,
			MinHeight: 540,
			StateKey:  "synct-main-window",
			Content:   ui.View(app.View),
		})
		app.SetWindow(mainWin)

		mainWin.OnClose(func(e *mygo.CloseEvent) {
			if !quitting && app.Config().MinimizeToTray {
				e.PreventDefault()
				mainWin.Hide()
				// Closing to the tray is a deliberate background action; the
				// notification is optional and toggled from the status bar.
				if app.Config().TrayCloseNotice && mygo.NotificationsSupported() {
					n := mygo.NewNotification(mygo.NotificationOptions{
						ID:     "synct-tray-notice",
						Title:  "SyncT 正在后台运行",
						Body:   "实时文件夹同步与 Cloudflared 隧道保持在系统托盘运行中。",
						Silent: true,
					})
					n.OnClick(func() {
						mainWin.Show()
						mainWin.Focus()
						n.Close()
					})
					_ = n.Show()
				}
			}
		})

		trayMenu := mygo.NewMenu([]*mygo.MenuItem{
			{
				Label: "显示 SyncT 主窗口",
				Click: func(*mygo.MenuItem, *mygo.Window) {
					mainWin.Show()
					mainWin.Focus()
				},
			},
			{
				Label: "立即扫描同步",
				Click: func(*mygo.MenuItem, *mygo.Window) {
					app.Engine().TriggerManualSync()
				},
			},
			{
				Label: "打开同步文件夹",
				Click: func(*mygo.MenuItem, *mygo.Window) {
					go mygo.Shell.OpenPath(app.Config().SyncDir)
				},
			},
			mygo.Separator(),
			{
				Label: "退出 SyncT",
				Click: func(*mygo.MenuItem, *mygo.Window) {
					quitting = true
					mygo.App.Quit()
				},
			},
		})

		tray, trayErr := mygo.NewTray(mygo.TrayOptions{
			ID:      "synct-tray",
			Icon:    appui.TrayIconPNG(),
			ToolTip: "SyncT",
			Menu:    trayMenu,
		})
		if trayErr == nil {
			tray.OnClick(func() {
				mainWin.Show()
				mainWin.Focus()
			})
		}
	})

	if err := mygo.App.Run(); err != nil {
		log.Fatal(err)
	}
}
