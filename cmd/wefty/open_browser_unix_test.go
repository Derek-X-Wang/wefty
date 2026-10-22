//go:build unix

package main

import "testing"

func TestOpenBrowserCommandHasOwnProcessGroup(t *testing.T) {
	command := newOpenBrowserCommand("browser-opener", "http://127.0.0.1:18480")
	if command.Process != nil {
		t.Fatal("browser process was launched")
	}
	if len(command.Args) != 2 || command.Args[1] != "http://127.0.0.1:18480" {
		t.Fatalf("opener args: %v", command.Args)
	}
	if command.SysProcAttr == nil || !command.SysProcAttr.Setpgid {
		t.Fatal("browser opener shares the foreground process group")
	}
}
