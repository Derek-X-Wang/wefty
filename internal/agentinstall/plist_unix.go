//go:build unix

package agentinstall

import (
	"errors"
	"io"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

// readRetainedPlist opens a catalog-derived single component relative to the
// held installation directory. Root.OpenFile follows in-root symlinks, so use
// openat directly. Nonblocking open prevents a replaced FIFO from hanging the
// catalog lock. Validate and read the same descriptor, never reopen its name.
func readRetainedPlist(parent *os.Root, name string) ([]byte, error) {
	return readOwnedFile(parent, name, 0644)
}

// ReadPrivateFile reads and validates one descriptor relative to a held parent.
func ReadPrivateFile(parent *os.Root, name string) ([]byte, error) {
	return readOwnedFile(parent, name, 0600)
}

func readOwnedFile(parent *os.Root, name string, mode os.FileMode) ([]byte, error) {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\\") {
		return nil, &VerificationRefusal{Message: "owned record requires a single component"}
	}
	directory, err := parent.Open(".")
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	var fd int
	for {
		fd, err = unix.Openat(int(directory.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
		if !errors.Is(err, unix.EINTR) {
			break
		}
	}
	if err != nil {
		if errors.Is(err, unix.ELOOP) {
			return nil, &VerificationRefusal{Message: "retained generation plist must not be a symbolic link"}
		}
		return nil, &os.PathError{Op: "openat", Path: name, Err: err}
	}
	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("wrap retained generation plist descriptor")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !owned(info) || !info.Mode().IsRegular() || info.Mode().Perm() != mode {
		return nil, &VerificationRefusal{Message: "retained generation plist ownership, type or mode changed"}
	}
	return io.ReadAll(file)
}

// ReadOwnedRegularFile supplies the same no-follow descriptor check for an
// installation completion file whose setup mode is already known.
func ReadOwnedRegularFile(parent *os.Root, name string, mode os.FileMode) ([]byte, error) {
	return readOwnedFile(parent, name, mode)
}
