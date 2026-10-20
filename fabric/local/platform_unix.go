//go:build darwin || linux

package local

import (
	"errors"
	"os"
	"syscall"
)

func ownedByUser(info os.FileInfo, uid uint32) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uid
}
func socketRefused(err error) bool { return errors.Is(err, syscall.ECONNREFUSED) }
