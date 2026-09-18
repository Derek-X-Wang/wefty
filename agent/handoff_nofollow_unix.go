//go:build unix

package agent

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

const noFollowOpenFlag = syscall.O_NOFOLLOW

// openHandoffDirectory opens the final component without following it, then
// derives os.Root from the still-open descriptor, never by reopening its name.
func openHandoffDirectory(parent *os.Root, name string) (*os.Root, error) {
	expected, err := parent.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !expected.IsDir() || expected.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("handoff path %q is not a directory (symbolic links are refused)", name)
	}
	file, err := openHandoffFile(parent, name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_DIRECTORY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !opened.IsDir() || !os.SameFile(expected, opened) {
		return nil, fmt.Errorf("handoff directory %q changed identity while opening", name)
	}
	// Go has no os.Root constructor taking an existing descriptor. These OS
	// descriptor aliases refer to the held file even if its pathname is renamed.
	prefix := "/dev/fd"
	if runtime.GOOS == "linux" {
		prefix = "/proc/self/fd"
	}
	root, err := os.OpenRoot(fmt.Sprintf("%s/%d", prefix, file.Fd()))
	if err != nil {
		return nil, err
	}
	actual, err := root.Stat(".")
	if err != nil || !os.SameFile(opened, actual) {
		root.Close()
		return nil, fmt.Errorf("handoff directory %q changed descriptor identity: %v", name, err)
	}
	return root, nil
}

// os.Root.OpenFile resolves symlinks internally, even when its caller passes
// O_NOFOLLOW. Use openat directly for these single-component no-follow opens.
func openHandoffFile(parent *os.Root, name string, flags int, mode os.FileMode) (*os.File, error) {
	if name == "" || name == ".." || strings.ContainsAny(name, "/\\") {
		return nil, fmt.Errorf("handoff file name %q is not one component", name)
	}
	directory, err := parent.Open(".")
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	var fd int
	for {
		fd, err = unix.Openat(int(directory.Fd()), name, flags|unix.O_CLOEXEC, uint32(mode.Perm()))
		if err != unix.EINTR {
			break
		}
	}
	if err != nil {
		return nil, &os.PathError{Op: "openat", Path: name, Err: err}
	}
	return os.NewFile(uintptr(fd), name), nil
}
