//go:build !unix

package agent

// runMailboxNonBlockingOpen has no portable equivalent outside unix; the
// regular-file proof around every mailbox open carries the guarantee there.
const runMailboxNonBlockingOpen = 0
