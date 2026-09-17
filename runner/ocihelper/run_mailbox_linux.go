//go:build linux

package ocihelper

import "context"

// The ContainerdEngine serves the mailbox out of its own managed root. The
// confinement itself is platform-neutral and lives in run_mailbox.go, so it is
// exercised by the same tests on every platform the agent builds for.
//
// Each operation honours its context. The server hands it the operation
// context, so a session that is closing or an attempt whose reap has begun
// cannot keep an in-flight descent or read alive behind it.

func (engine *ContainerdEngine) ListRunMailbox(ctx context.Context, request ListRunMailboxRequest) (ListRunMailboxResponse, error) {
	return listRunMailbox(ctx, engine.config.RuntimeRoot, request)
}

func (engine *ContainerdEngine) ReadRunMailbox(ctx context.Context, request ReadRunMailboxRequest) (ReadRunMailboxResponse, error) {
	return readRunMailbox(ctx, engine.config.RuntimeRoot, request)
}

func (engine *ContainerdEngine) RemoveRunMailboxEntry(ctx context.Context, request RemoveRunMailboxEntryRequest) (RemoveRunMailboxEntryResponse, error) {
	return removeRunMailboxEntry(ctx, engine.config.RuntimeRoot, request)
}
