package ui

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/egoist/mygo/ui"

	"synct/internal/config"
)

func TestAppViewHeadlessNavigationAndInteractions(t *testing.T) {
	tmpRoot := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", tmpRoot)

	syncDir := filepath.Join(tmpRoot, "SyncFolder")
	if err := os.MkdirAll(syncDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(syncDir, "report.docx"), []byte("quarterly report content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(syncDir, "notes.txt"), []byte("meeting notes"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.SyncDir = syncDir
	cfg.TunnelMode = config.TunnelDirect

	app := NewApp(cfg, filepath.Join(tmpRoot, "state.json"))
	defer app.StopAll()

	// 1. Test at default window size (1060 x 720)
	tt := ui.NewTester(app.View, 1060, 720)
	if !tt.HasText("SyncT") || !tt.HasText("同步文件夹与隧道连接配置") {
		t.Fatalf("expected overview page texts, got %v", tt.Texts())
	}
	// Verify bottom-left real-time sync info panel and multi-folder mapping section are rendered
	if !tt.HasText("实时同步信息") || !tt.HasText("文件总计:") || !tt.HasText("累计流量:") {
		t.Fatalf("expected bottom-left sync info telemetry in sidebar, got %v", tt.Texts())
	}
	if !tt.HasText("多文件夹同步与跨机目录映射 (本机文件夹 <-> 另一台电脑目标文件夹)") {
		t.Fatalf("expected multi-folder mapping section, got %v", tt.Texts())
	}
	if tt.FramePending() {
		t.Error("expected settled overview frame with FramePending() == false")
	}

	// 2. Switch from Host Mode to Client Mode via Segmented control and parse a ShareCode
	if err := tt.Click("节点模式 (连接远端)"); err != nil {
		t.Fatalf("click Client mode segment: %v", err)
	}
	if app.Config().Role != config.RoleClient {
		t.Fatalf("expected RoleClient after clicking segment, got %s", app.Config().Role)
	}

	sampleCode := config.EncodeShareCode("https://demo-sync.trycloudflare.com", "st_new_secret_999")
	app.shareInput = sampleCode
	tt.Frame()
	if err := tt.Click("解析填充"); err != nil {
		t.Fatalf("click 解析填充: %v", err)
	}
	if app.Config().RemoteURL != "https://demo-sync.trycloudflare.com" || app.Config().AuthToken != "st_new_secret_999" {
		t.Fatalf("share code not applied: url=%q token=%q", app.Config().RemoteURL, app.Config().AuthToken)
	}

	// Switch back to Host Mode and test starting/stopping direct sync
	if err := tt.Click("主机模式 (发布隧道)"); err != nil {
		t.Fatalf("click Host mode segment: %v", err)
	}
	if err := tt.Click("启动隧道并同步"); err != nil {
		t.Fatalf("click 启动隧道并同步: %v", err)
	}
	if !app.Engine().IsRunning() {
		t.Fatal("expected engine to be running after clicking start")
	}
	if err := tt.Click("停止同步"); err != nil {
		t.Fatalf("click 停止同步: %v", err)
	}
	if app.Engine().IsRunning() {
		t.Fatal("expected engine to be stopped after clicking stop")
	}

	// 3. Navigate to Files section and verify Table + SearchField
	if err := tt.Click("同步文件列表"); err != nil {
		t.Fatalf("click 同步文件列表: %v", err)
	}
	if !tt.HasText("report.docx") || !tt.HasText("notes.txt") {
		t.Fatalf("expected both files in table, got %v", tt.Texts())
	}

	// Filter files via search query
	app.fileSearch = "report"
	tt.Frame()
	if !tt.HasText("report.docx") || tt.HasText("notes.txt") {
		t.Fatalf("expected only report.docx when filtering by 'report', got %v", tt.Texts())
	}
	app.fileSearch = ""
	tt.Frame()

	// 4. Navigate to Logs section and test AlertDialog confirmation on Clear Logs
	if err := tt.Click("实时传输日志"); err != nil {
		t.Fatalf("click 实时传输日志: %v", err)
	}
	if err := tt.Click("清空日志"); err != nil {
		t.Fatalf("click 清空日志: %v", err)
	}
	if !app.confirmClearLogsOpen {
		t.Fatal("expected confirmClearLogsOpen to be true")
	}
	if err := tt.Click("确认清空"); err != nil {
		t.Fatalf("click 确认清空 in AlertDialog: %v", err)
	}
	if len(app.Engine().Logs()) != 0 {
		t.Fatalf("expected logs to be cleared, got %d entries", len(app.Engine().Logs()))
	}

	// 5. Navigate to Settings section
	if err := tt.Click("忽略规则与设置"); err != nil {
		t.Fatalf("click 忽略规则与设置: %v", err)
	}
	tt.Advance(2 * time.Second)
	if !tt.HasText("文件过滤与冲突保护策略") {
		t.Fatalf("expected settings page content, got %v", tt.Texts())
	}
	if tt.FramePending() {
		t.Error("expected settled settings frame with FramePending() == false")
	}

	// 6. Verify layout at minimum window size (820 x 540)
	tt.SetSize(820, 540)
	tt.Advance(1 * time.Second)
	if _, ok := tt.Find("主操作工具栏"); !ok {
		// Check primary buttons remain visible and clickable at minimum window size
		if _, okBtn := tt.Find("立即扫描同步"); !okBtn {
			t.Errorf("expected toolbar controls visible at minimum window size 820x540")
		}
	}
	if tt.FramePending() {
		t.Error("expected FramePending() == false at minimum window size")
	}
}
