//go:build unix

package main

import (
	"io/fs"
	"syscall"
)

// Chmod of retained files must not change an inode outside the history tree.
func conversionSingleLink(info fs.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Nlink == 1
}

func conversionDoubleLink(info fs.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Nlink == 2
}
