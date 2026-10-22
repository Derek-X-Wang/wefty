//go:build unix

package main

import (
	"io/fs"
	"syscall"
)

func setupFolderOwnedBy(info fs.FileInfo, uid int) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && uint64(stat.Uid) == uint64(uid)
}
