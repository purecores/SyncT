package applog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCapKeepsFileAtOrBelowTwoMegabytes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "SyncT.log")
	Init(path)
	defer Close()

	line := strings.Repeat("x", 1000)
	for i := 0; i < 3000; i++ {
		Write("INFO", line)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > MaxSize {
		t.Fatalf("log grew to %d bytes; cap is %d", info.Size(), MaxSize)
	}
	if MaxSize != 2<<20 {
		t.Fatalf("cap = %d; want 2MB", MaxSize)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "日志超过 2MB") {
		t.Fatal("a rotated file should say why it restarted")
	}
}

func TestExistingOversizeFileIsTruncatedOnInit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "SyncT.log")
	if err := os.WriteFile(path, make([]byte, MaxSize+1), 0o600); err != nil {
		t.Fatal(err)
	}
	Init(path)
	Write("INFO", "after restart")
	Close()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > MaxSize {
		t.Fatalf("init left %d bytes; want at most %d", info.Size(), MaxSize)
	}
	data, _ := os.ReadFile(path)
	if !strings.Contains(string(data), "after restart") {
		t.Fatal("logging stopped after a rotated file")
	}
}
