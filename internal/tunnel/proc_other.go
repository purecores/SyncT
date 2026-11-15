//go:build !windows

package tunnel

import "os/exec"

func configureCmdPlatform(_ *exec.Cmd) {}

func findRunningCloudflaredPIDs() []uint32 {
	return nil
}
