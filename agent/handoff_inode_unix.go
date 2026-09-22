//go:build unix

package agent

import (
	"os"
	"syscall"
)

// handoffInode identifies one file on this node, so a pass over the whole
// handoff root charges a hard-linked file once rather than once per name that
// reaches it. Two runs that link the same 100 MiB artifact occupy 100 MiB, and
// the node figure has to say so.
//
// Identity is (device, inode) and never a path: two names under one root are
// the same storage exactly when they are the same inode on the same device.
// It is not compared across roots -- the OCI handoff root is a different
// filesystem, and on a Mac node a different machine entirely.
type handoffInode struct {
	device uint64
	inode  uint64
}

// handoffInodeOf reports the identity of a file more than one name can reach,
// and whether it is such a file at all.
//
// A file with a single link cannot be reached twice by a walk that never
// follows a symlink, so it is deliberately not identified. That is what keeps
// the pass's map small on the tree this slice's charged-byte floor exists for:
// a million single-linked empty files add a million entries to the accounting
// and none at all to the map.
func handoffInodeOf(info os.FileInfo) (handoffInode, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink <= 1 {
		return handoffInode{}, false
	}
	return handoffInode{device: uint64(stat.Dev), inode: uint64(stat.Ino)}, true
}
