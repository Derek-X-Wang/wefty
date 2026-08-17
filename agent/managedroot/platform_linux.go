//go:build linux

package managedroot

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func platformMountIdentityFD(fd int) (mountIdentity, error) {
	return linuxMountIdentity(fd, "", unix.AT_EMPTY_PATH|unix.AT_SYMLINK_NOFOLLOW)
}

func platformMountIdentityAt(parentFD int, name string) (mountIdentity, error) {
	return linuxMountIdentity(parentFD, name, unix.AT_SYMLINK_NOFOLLOW)
}

func linuxMountIdentity(parentFD int, name string, flags int) (mountIdentity, error) {
	var stat unix.Statx_t
	if err := unix.Statx(parentFD, name, flags, unix.STATX_BASIC_STATS|unix.STATX_MNT_ID, &stat); err != nil {
		return mountIdentity{}, fmt.Errorf("inspect mount identity: %w", err)
	}
	if stat.Mask&unix.STATX_MNT_ID == 0 {
		return mountIdentity{}, fmt.Errorf("%w: kernel did not return a mount ID", ErrMountBoundary)
	}
	return mountIdentity{key: fmt.Sprintf("%d:%d:%d", stat.Dev_major, stat.Dev_minor, stat.Mnt_id)}, nil
}

func renameNoReplace(fromFD int, from string, toFD int, to string) error {
	return unix.Renameat2(fromFD, from, toFD, to, unix.RENAME_NOREPLACE)
}
