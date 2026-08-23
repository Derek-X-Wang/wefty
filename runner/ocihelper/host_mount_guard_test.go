package ocihelper

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestHostMountGuardRejectsUnsafeMacSourcesAndRevalidates(t *testing.T) {
	createdRoot, err := os.MkdirTemp("/tmp", "wefty-mount-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(createdRoot) })
	root, err := filepath.EvalSymlinks(createdRoot)
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, "safe")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	guard, err := OpenHostMountGuard([]string{directory}, root)
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()
	if err := guard.Revalidate(); err != nil {
		t.Fatal(err)
	}

	symlink := filepath.Join(root, "link")
	if err := os.Symlink(directory, symlink); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(root, "socket")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	fifo := filepath.Join(root, "fifo")
	if err := makeTestFIFO(fifo); err != nil {
		t.Fatal(err)
	}
	for _, unsafe := range []string{root, filepath.Join(root, "..", filepath.Base(root)), symlink, socketPath, fifo, "/dev/null"} {
		if rejected, err := OpenHostMountGuard([]string{unsafe}, root); err == nil {
			_ = rejected.Close()
			t.Errorf("accepted unsafe host mount %q", unsafe)
		}
	}
}
