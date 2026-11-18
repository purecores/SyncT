// Package applog writes the portable activity log next to the executable.
// The in-memory log is for the UI; this file is what survives a crash and can
// be attached to a report. It is capped at 2 MB so a long-running sync cannot
// fill the USB stick or portable folder.
package applog

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// MaxSize is the cap for the single log file. When the next line would pass
// it, the file is truncated and starts over with a marker.
const MaxSize = 2 << 20

var (
	mu      sync.Mutex
	file    *os.File
	size    int64
	logPath string
)

// Init opens p for appending and trims it if a previous run left it over the
// cap. A failure to open is ignored: logging must never stop the app.
func Init(p string) {
	mu.Lock()
	defer mu.Unlock()
	if file != nil {
		_ = file.Close()
		file = nil
	}
	logPath = p
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return
	}
	file = f
	size = 0
	if info, statErr := f.Stat(); statErr == nil {
		size = info.Size()
	}
	if size > MaxSize {
		resetLocked()
	}
}

// Write appends one timestamped line.
func Write(level, msg string) {
	stamp := time.Now().Format("15:04:05.000")
	writeLocked(fmt.Sprintf("[%s] [%s] %s", stamp, level, msg))
}

func writeLocked(line string) {
	mu.Lock()
	defer mu.Unlock()
	if file == nil {
		return
	}
	if !endsWithNewline(line) {
		line += "\n"
	}
	if size+int64(len(line)) > MaxSize {
		resetLocked()
		if file == nil {
			return
		}
	}
	n, err := file.WriteString(line)
	if err == nil {
		size += int64(n)
	}
}

// resetLocked starts the file over. Truncate alone does not work on an append
// handle: its next write still goes to the old end offset, which on Windows
// leaves a sparse gap. Closing and recreating the file makes the reset
// explicit and keeps the cap exact.
func resetLocked() {
	if file != nil {
		_ = file.Close()
		file = nil
	}
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return
	}
	file = f
	size = 0
	marker := "--- 日志超过 2MB，已从新文件继续 ---\n"
	if n, err := f.WriteString(marker); err == nil {
		size = int64(n)
	}
}

func endsWithNewline(s string) bool {
	return len(s) > 0 && s[len(s)-1] == '\n'
}

// Close flushes and closes the file.
func Close() {
	mu.Lock()
	defer mu.Unlock()
	if file == nil {
		return
	}
	_ = file.Sync()
	_ = file.Close()
	file = nil
}
