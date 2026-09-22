//go:build !unix

package agent

import "os"

// handoffInode identifies one file on this node. Off Unix there is no portable
// way to ask, and retained handoff directories already require Unix no-follow
// handles (handoff_nofollow_other.go), so nothing here ever runs.
type handoffInode struct {
	device uint64
	inode  uint64
}

// handoffInodeOf reports no identity rather than a guessed one: charging a
// hard-linked file twice overstates a node's storage, and pretending two
// different files are one understates it. Overstating is the safe direction,
// and it is what a nil identity produces.
func handoffInodeOf(os.FileInfo) (handoffInode, bool) {
	return handoffInode{}, false
}
