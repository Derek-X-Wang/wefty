//go:build darwin || linux

package ocihelper

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestWorkloadValidationRejectsFIFOAndSocketMounts(t *testing.T) {
	temporaryRoot, err := os.MkdirTemp("/tmp", "wefty-oci-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(temporaryRoot) })
	root, err := filepath.EvalSymlinks(temporaryRoot)
	if err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(root, "fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(root, "socket")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = listener.Close()
		_ = os.Remove(socket)
	})

	for _, source := range []string{fifo, socket} {
		input := WorkloadInput{
			ImageDigest:    "sha256:" + strings.Repeat("a", 64),
			OperatorMounts: []OperatorMount{{NodePath: source, ContainerPath: "/data"}},
		}
		if err := validateWorkload(input, []string{root}); err == nil {
			t.Fatalf("non-regular mount source %q was accepted", source)
		}
	}
}
