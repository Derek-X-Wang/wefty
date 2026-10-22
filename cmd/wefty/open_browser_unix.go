//go:build unix

package main

import (
	"os/exec"
	"syscall"
)

func newOpenBrowserCommand(command, url string) *exec.Cmd {
	process := exec.Command(command, url)
	process.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return process
}
