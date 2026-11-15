package ui

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/egoist/mygo/ui"

	"synct/internal/config"
	"synct/internal/syncer"
)

func TestCachedVisibleFiles(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", tmp)
	syncDir := filepath.Join(tmp, "SyncFolder")
	if err := os.MkdirAll(syncDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(syncDir, "alpha.txt"), []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(syncDir, "beta.log"), []byte("beta"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.SyncDir = syncDir
	cfg.TunnelMode = config.TunnelDirect
	app := NewApp(cfg, filepath.Join(tmp, "state.json"))
	defer app.StopAll()

	files, version := app.engine.FilesWithVersion()
	if len(files) != 2 {
		t.Fatalf("initial files = %d; want 2", len(files))
	}
	r1 := app.cachedVisibleFiles(files, version)
	r2 := app.cachedVisibleFiles(files, version)
	if len(r1) != 2 {
		t.Fatalf("visible files = %d; want 2", len(r1))
	}
	if reflect.ValueOf(r1).Pointer() != reflect.ValueOf(r2).Pointer() {
		t.Fatal("unchanged filters should reuse the cached result")
	}

	app.fileSearch = "alpha"
	r3 := app.cachedVisibleFiles(files, version)
	if len(r3) != 1 || r3[0].RelPath != "alpha.txt" {
		t.Fatalf("search filter = %+v", r3)
	}
	r4 := app.cachedVisibleFiles(files, version)
	if reflect.ValueOf(r3).Pointer() != reflect.ValueOf(r4).Pointer() {
		t.Fatal("unchanged search should reuse the cached filtered result")
	}

	app.fileSearch = ""
	app.fileFilterIdx = 1 // 已同步 only
	r5 := app.cachedVisibleFiles(files, version)
	if len(r5) != 2 {
		t.Fatalf("status filter = %d; want 2 synced", len(r5))
	}

	// A new file changes the index generation and must invalidate the cache.
	if err := os.WriteFile(filepath.Join(syncDir, "gamma.md"), []byte("gamma"), 0o644); err != nil {
		t.Fatal(err)
	}
	app.engine.TriggerManualSync()
	deadline := time.Now().Add(10 * time.Second)
	for {
		files, version = app.engine.FilesWithVersion()
		if len(files) == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("manual scan did not pick up new file, files=%d", len(files))
		}
		time.Sleep(20 * time.Millisecond)
	}
	app.fileFilterIdx = 0
	all := app.cachedVisibleFiles(files, version)
	if len(all) != 3 {
		t.Fatalf("visible files after rescan = %d; want 3", len(all))
	}
}

func TestOverviewShowsScanErrorBanner(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SYNCT_DATA_DIR", tmp)
	badRoot := filepath.Join(tmp, "not-a-folder")
	if err := os.WriteFile(badRoot, []byte("regular file"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.SyncDir = badRoot
	cfg.SyncFolders = []config.FolderMapping{{
		ID:        config.DefaultFolderID,
		Label:     "主同步目录",
		LocalPath: badRoot,
		Enabled:   true,
	}}
	app := NewApp(cfg, filepath.Join(tmp, "state.json"))
	defer app.StopAll()

	stats := app.Engine().Stats()
	if stats.LastScanError == "" {
		t.Fatal("expected the initial scan error to be surfaced in Stats")
	}

	tt := ui.NewTester(app.View, 1060, 720)
	if !tt.HasText("异常提示") {
		t.Fatalf("overview should show the scan error banner, texts=%v", tt.Texts())
	}
}

func TestAppFileStatusColors(t *testing.T) {
	theme := ui.Theme{
		Text:    ui.Hex("#111111"),
		Accent:  ui.Hex("#ea580c"),
		Warning: ui.Hex("#f59e0b"),
		Success: ui.Hex("#16a34a"),
		Danger:  ui.Hex("#dc2626"),
	}
	app := &App{}
	cases := []struct {
		status syncer.FileStatus
		want   ui.Color
	}{
		{syncer.StatusSynced, theme.Success},
		{syncer.StatusUploading, theme.Accent},
		{syncer.StatusDownloading, theme.Accent},
		{syncer.StatusPending, theme.Accent},
		{syncer.StatusConflict, theme.Warning},
		{syncer.StatusError, theme.Danger},
		{syncer.FileStatus("unknown"), theme.Text},
	}
	for _, tc := range cases {
		if got := app.fileStatusColor(&theme, tc.status); got != tc.want {
			t.Errorf("status %q color = %+v; want %+v", tc.status, got, tc.want)
		}
	}
}
