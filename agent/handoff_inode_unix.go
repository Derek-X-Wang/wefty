//go:build unix

package agent

import (
	"os"
	"syscall"
)

// inodeOf reports a file's identity and link count, so a hard-linked file is
// charged to the node once rather than once per name.
func inodeOf(info os.FileInfo) (inodeIdentity, uint64, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return inodeIdentity{}, 0, false
	}
	return inodeIdentity{device: uint64(stat.Dev), inode: uint64(stat.Ino)}, uint64(stat.Nlink), true
}
