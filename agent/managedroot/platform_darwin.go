//go:build darwin

package managedroot

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func platformMountIdentityFD(fd int) (mountIdentity, error) {
	var stat unix.Statfs_t
	if err := unix.Fstatfs(fd, &stat); err != nil {
		return mountIdentity{}, fmt.Errorf("inspect mount identity: %w", err)
	}
	return mountIdentity{key: fmt.Sprintf("%d:%d", stat.Fsid.Val[0], stat.Fsid.Val[1])}, nil
}

func platformMountIdentityAt(parentFD int, name string) (mountIdentity, error) {
	fd, err := unix.Openat(parentFD, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return mountIdentity{}, fmt.Errorf("open entry to inspect mount identity: %w", err)
	}
	defer unix.Close(fd)
	return platformMountIdentityFD(fd)
}

func renameNoReplace(fromFD int, from string, toFD int, to string) error {
	return unix.RenameatxNp(fromFD, from, toFD, to, unix.RENAME_EXCL)
}
