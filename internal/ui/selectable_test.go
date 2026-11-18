package ui

import (
	"strings"
	"testing"
	"time"

	"github.com/egoist/mygo/ui"

	"synct/internal/config"
)

// Every piece of text in the window must be selectable and copyable with the
// pointer, as the user asked: a path, a log line, a rate all need to reach the
// clipboard without a custom "copy" button.
func TestWindowTextIsSelectable(t *testing.T) {
	app := newTestApp(t, t.TempDir(), map[string]string{"notes.txt": "hello"})
	defer app.StopAll()

	tt := ui.NewTester(app.View, 1100, 760)
	tt.Advance(100 * time.Millisecond)

	box, ok := tt.Find("SyncT")
	if !ok {
		t.Fatalf("app name not visible; texts=%v", tt.Texts())
	}
	// Drag from the left of the label to its right, then copy.
	startX, startY := box.X+1, box.Y+box.H/2
	endX := box.X + box.W - 1
	tt.Press(startX, startY)
	for x := startX; x <= endX; x += 4 {
		tt.Move(x, startY)
	}
	tt.Release(endX, startY)
	tt.Key(ui.Cmd, ui.KeyC)

	clip := tt.Clipboard()
	if !strings.Contains(clip, "SyncT") {
		t.Fatalf("Ctrl+C after selecting the app name copied %q", clip)
	}
}

// The settings page carries the addresses and keys a user may need to paste
// elsewhere; selecting across paragraphs must join them.
func TestSettingsTextSelectAllCopies(t *testing.T) {
	app := newTestApp(t, t.TempDir(), map[string]string{"a.txt": "a"})
	defer app.StopAll()

	tt := ui.NewTester(app.View, 1100, 760)
	if err := tt.Click("设置"); err != nil {
		t.Fatal(err)
	}
	tt.Advance(100 * time.Millisecond)
	first, ok := tt.Find("所有配置与索引都保存在程序同级的 data 目录，不写入注册表或其他用户目录。")
	if !ok {
		t.Fatalf("settings subtitle not visible; texts=%v", tt.Texts())
	}
	second, ok := tt.Find("双向模式会比较修改时间与 SHA-256；冲突时按下面的规则处理")
	if !ok {
		t.Fatalf("settings section not visible; texts=%v", tt.Texts())
	}
	startX, startY := first.X+1, first.Y+first.H/2
	endX, endY := second.X+second.W-1, second.Y+second.H/2
	tt.Press(startX, startY)
	for step := 1; step <= 12; step++ {
		x := startX + (endX-startX)*float32(step)/12
		y := startY + (endY-startY)*float32(step)/12
		tt.Move(x, y)
	}
	tt.Release(endX, endY)
	tt.Key(ui.Cmd, ui.KeyC)
	clip := tt.Clipboard()
	if !strings.Contains(clip, "所有配置与索引") || !strings.Contains(clip, "双向模式") {
		t.Fatalf("cross-paragraph selection copied %q", clip)
	}
	_ = config.Default
}

// A password typed into the settings field becomes a stable derived key, and
// the field clears so the password is not left on screen.
func TestSettingsPasswordDerivesKey(t *testing.T) {
	app := newTestApp(t, t.TempDir(), map[string]string{"a.txt": "a"})
	defer app.StopAll()

	tt := ui.NewTester(app.View, 1100, 760)
	if err := tt.Click("设置"); err != nil {
		t.Fatal(err)
	}
	app.passwordInput = "work-pc-pairing-password"
	tt.Frame()
	if err := tt.Click("应用密码"); err != nil {
		t.Fatalf("apply password: %v", err)
	}
	token := app.Config().AuthToken
	if !config.IsDerivedToken(token) {
		t.Fatalf("auth token is not derived: %q", token)
	}
	if token != config.DeriveToken("work-pc-pairing-password") {
		t.Fatal("the host and the work PC would derive different keys")
	}
	if app.passwordInput != "" {
		t.Fatal("the password field should clear after applying")
	}
}
