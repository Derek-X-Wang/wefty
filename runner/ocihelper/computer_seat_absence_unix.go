//go:build linux || darwin

package ocihelper

import (
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"strconv"
)

func confirmSeatFolderAbsent(root string, place int) error {
	anchor, err := os.Open(root)
	if err != nil {
		return err
	}
	defer anchor.Close()
	fd := int(anchor.Fd())
	owned := -1
	defer func() {
		if owned >= 0 {
			unix.Close(owned)
		}
	}()
	for _, component := range []string{"wefty", "service", "seats"} {
		next, err := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		if owned >= 0 {
			unix.Close(owned)
		}
		fd, owned = next, next
	}
	var st unix.Stat_t
	err = unix.Fstatat(fd, strconv.Itoa(place), &st, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New("Seat folder remains")
}
