//go:build windows

package tunnel

import (
	"os/exec"
	"strings"
	"syscall"
	"unsafe"
)

func configureCmdPlatform(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		HideWindow:    true,
		CreationFlags: 0x08000000, // CREATE_NO_WINDOW
	}
}

const th32csSnapProcess = 0x00000002

type processEntry32W struct {
	Size            uint32
	CntUsage        uint32
	ProcessID       uint32
	DefaultHeapID   uintptr
	ModuleID        uint32
	Threads         uint32
	ParentProcessID uint32
	PriClassBase    int32
	Flags           uint32
	ExeFile         [260]uint16
}

var (
	modKernel32                  = syscall.NewLazyDLL("kernel32.dll")
	procCreateToolhelp32Snapshot = modKernel32.NewProc("CreateToolhelp32Snapshot")
	procProcess32FirstW          = modKernel32.NewProc("Process32FirstW")
	procProcess32NextW           = modKernel32.NewProc("Process32NextW")
)

// findRunningCloudflaredPIDs enumerates active Windows processes in pure Go
// without spawning any child processes or requiring Administrator privileges,
// returning the PIDs of all running `cloudflared.exe` instances (including Windows Services).
func findRunningCloudflaredPIDs() []uint32 {
	snap, _, _ := procCreateToolhelp32Snapshot.Call(th32csSnapProcess, 0)
	if snap == 0 || snap == uintptr(syscall.InvalidHandle) {
		return nil
	}
	defer syscall.CloseHandle(syscall.Handle(snap))

	var entry processEntry32W
	entry.Size = uint32(unsafe.Sizeof(entry))

	ret, _, _ := procProcess32FirstW.Call(snap, uintptr(unsafe.Pointer(&entry)))
	if ret == 0 {
		return nil
	}

	var pids []uint32
	for {
		name := strings.ToLower(syscall.UTF16ToString(entry.ExeFile[:]))
		if name == "cloudflared.exe" || name == "cloudflared" {
			pids = append(pids, entry.ProcessID)
		}
		entry.Size = uint32(unsafe.Sizeof(entry))
		ret, _, _ = procProcess32NextW.Call(snap, uintptr(unsafe.Pointer(&entry)))
		if ret == 0 {
			break
		}
	}
	return pids
}
