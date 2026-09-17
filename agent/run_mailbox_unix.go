//go:build unix

package agent

import "syscall"

// runMailboxNonBlockingOpen keeps an open on a FIFO — which a workload can
// plant in its own mailbox — from blocking until a writer appears. Regular
// files ignore the flag.
const runMailboxNonBlockingOpen = syscall.O_NONBLOCK
