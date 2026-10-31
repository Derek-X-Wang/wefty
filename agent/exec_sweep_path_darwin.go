package agent

import (
	"bytes"
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

func execMailboxPhysicalPath(directory *os.File) (string, error) {
	// F_GETPATH writes MAXPATHLEN bytes and derives the current name from the
	// open vnode, including after a rename. Darwin's /dev/fd cannot be a cwd.
	var path [1024]byte
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, directory.Fd(), syscall.F_GETPATH, uintptr(unsafe.Pointer(&path[0])))
	if errno != 0 {
		return "", errno
	}
	end := bytes.IndexByte(path[:], 0)
	if end <= 0 {
		return "", fmt.Errorf("F_GETPATH returned no terminated physical path")
	}
	return string(path[:end]), nil
}
