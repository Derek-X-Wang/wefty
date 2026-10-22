//go:build !unix

package main

import "os/exec"

func newOpenBrowserCommand(command, url string) *exec.Cmd {
	return exec.Command(command, url)
}
