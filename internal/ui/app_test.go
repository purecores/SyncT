package ui

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/egoist/mygo/ui"

	"synct/internal/config"
)

// newTestApp builds a host app over a temporary folder with the given files.
func newTestApp(t *testing.T, root string, files map[string]string) *App {
	t.Helper()
	return newTestAppRole(t, root, files, config.RoleHost)
}

func newTestAppRole(t *testing.T, root string, files map[string]string, role config.Role) *App {
	t.Helper()
	t.Setenv("SYNCT_DATA_DIR", root)
	syncDir := filepath.Join(root, "SyncFolder")
	if err := os.MkdirAll(syncDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range files {
		full := filepath.Join(syncDir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cfg := config.Default()
	cfg.SyncDir = syncDir
	cfg.Role = role
	cfg.TunnelMode = config.TunnelDirect
	app := NewApp(cfg, filepath.Join(root, "state.json"))
	app.OpenPathFunc = func(string) {}
	app.RevealPathFunc = func(string) {}
	app.ClipboardWriteFn = func(string) {}
	return app
}

func TestAppOverviewShowsQuietDashboard(t *testing.T) {
	app := newTestApp(t, t.TempDir(), map[string]string{
		"report.docx":    "quarterly report content",
		"notes.txt":      "meeting notes",
		"archive/a.json": "{}",
	})
	defer app.StopAll()

	tt := ui.NewTester(app.View, 1060, 720)
	for _, want := range []string{"SyncT", "当前吞吐", "受控文件", "同步文件夹", "连接"} {
		if !tt.HasText(want) {
			t.Fatalf("overview is missing %q, texts=%v", want, tt.Texts())
		}
	}
	// Folder management moved to settings; the dense connection form is gone
	// from the overview.
	if tt.HasText("多文件夹同步与跨机目录映射 (本机文件夹 <-> 另一台电脑目标文件夹)") {
		t.Fatal("overview must not embed the full folder-mapping form")
	}
	if tt.FramePending() {
		t.Error("expected a settled overview frame")
	}
}

func TestAppClientModeAndShareCode(t *testing.T) {
	app := newTestApp(t, t.TempDir(), map[string]string{"a.txt": "a"})
	defer app.StopAll()

	tt := ui.NewTester(app.View, 1100, 760)
	if err := tt.Click("设置"); err != nil {
		t.Fatalf("open settings: %v", err)
	}
	if err := tt.Click("节点模式 (连接远端)"); err != nil {
		t.Fatalf("click client mode: %v", err)
	}
	if app.Config().Role != config.RoleClient {
		t.Fatalf("role = %s; want client", app.Config().Role)
	}

	app.shareInput = config.EncodeShareCode("https://demo-sync.trycloudflare.com", "st_new_secret_999")
	tt.Frame()
	if err := tt.Click("解析填充"); err != nil {
		t.Fatalf("apply share code: %v", err)
	}
	if got := app.Config().RemoteURL; got != "https://demo-sync.trycloudflare.com" {
		t.Fatalf("remote url = %q", got)
	}
	if got := app.Config().AuthToken; got != "st_new_secret_999" {
		t.Fatalf("token = %q", got)
	}
}

func TestAppStartStopFromHeader(t *testing.T) {
	app := newTestApp(t, t.TempDir(), map[string]string{"a.txt": "a"})
	defer app.StopAll()

	tt := ui.NewTester(app.View, 1100, 760)
	if err := tt.Click("开始同步"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if !app.Engine().IsRunning() {
		t.Fatal("engine should run after 开始同步")
	}
	if err := tt.Click("停止同步"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if app.Engine().IsRunning() {
		t.Fatal("engine should stop after 停止同步")
	}
}

func TestAppFilesAndPendingPages(t *testing.T) {
	// A client that has not connected yet has local files waiting to go out.
	app := newTestAppRole(t, t.TempDir(), map[string]string{
		"report.docx": "quarterly report content",
		"notes.txt":   "meeting notes",
	}, config.RoleClient)
	defer app.StopAll()

	tt := ui.NewTester(app.View, 1060, 720)
	if err := tt.Click("文件"); err != nil {
		t.Fatalf("open files: %v", err)
	}
	if !tt.HasText("report.docx") || !tt.HasText("notes.txt") {
		t.Fatalf("files table is missing entries, texts=%v", tt.Texts())
	}

	app.fileSearch = "report"
	tt.Frame()
	if !tt.HasText("report.docx") || tt.HasText("notes.txt") {
		t.Fatalf("search filter kept the wrong rows, texts=%v", tt.Texts())
	}
	app.fileSearch = ""
	tt.Frame()

	if err := tt.Click("未同步"); err != nil {
		t.Fatalf("open pending: %v", err)
	}
	// Both files are new and have not reached a peer yet.
	if !tt.HasText("未同步文件") || !tt.HasText("report.docx") {
		t.Fatalf("pending page should list unconfirmed files, texts=%v", tt.Texts())
	}
	if err := tt.Click("立即扫描并同步"); err != nil {
		t.Fatalf("pending scan: %v", err)
	}
}

func TestAppLogsClearConfirmation(t *testing.T) {
	app := newTestApp(t, t.TempDir(), map[string]string{"a.txt": "a"})
	defer app.StopAll()
	app.Engine().Logf("hello log")

	tt := ui.NewTester(app.View, 1060, 720)
	if err := tt.Click("日志"); err != nil {
		t.Fatalf("open logs: %v", err)
	}
	if err := tt.Click("清空日志"); err != nil {
		t.Fatalf("clear logs: %v", err)
	}
	if !app.confirmClearLogsOpen {
		t.Fatal("expected the confirmation dialog")
	}
	if err := tt.Click("确认清空"); err != nil {
		t.Fatalf("confirm clear: %v", err)
	}
	if got := len(app.Engine().Logs()); got != 0 {
		t.Fatalf("logs = %d after clearing", got)
	}
}

func TestAppSettingsSectionsAndTrayNoticeToggle(t *testing.T) {
	app := newTestApp(t, t.TempDir(), map[string]string{"a.txt": "a"})
	defer app.StopAll()

	tt := ui.NewTester(app.View, 1060, 720)
	if err := tt.Click("设置"); err != nil {
		t.Fatalf("open settings: %v", err)
	}
	tt.Advance(2 * time.Second)
	for _, want := range []string{"文件夹与方向", "工作模式与连接", "文件规则", "运行与后台", "多文件夹与跨机映射"} {
		if !tt.HasText(want) {
			t.Fatalf("settings is missing section %q", want)
		}
	}

	// The status bar switch is the same setting as the form's.
	before := app.Config().TrayCloseNotice
	if err := tt.Click("后台提示"); err != nil {
		t.Fatalf("click status bar notice toggle: %v", err)
	}
	if app.Config().TrayCloseNotice == before {
		t.Fatal("status bar toggle did not change TrayCloseNotice")
	}
}

func TestAppMinimumWindowSize(t *testing.T) {
	app := newTestApp(t, t.TempDir(), map[string]string{"a.txt": "a"})
	defer app.StopAll()

	tt := ui.NewTester(app.View, 820, 540)
	tt.Advance(1 * time.Second)
	if _, ok := tt.Find("开始同步"); !ok {
		t.Fatal("primary action must stay visible at the minimum window size")
	}
	if tt.FramePending() {
		t.Error("expected a settled frame at 820x540")
	}
}
