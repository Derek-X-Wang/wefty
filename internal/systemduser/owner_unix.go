//go:build unix

package systemduser

import (
	"io/fs"
	"syscall"
)

func rootOwned(info fs.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == 0
}
