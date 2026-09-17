//go:build linux

package ocihelper

import "context"

// The ContainerdEngine serves the mailbox out of its own managed root. The
// confinement itself is platform-neutral and lives in run_mailbox.go, so it is
// exercised by the same tests on every platform the agent builds for.
//
// Each operation honours its context cooperatively: the server hands it the
// operation context and the steps below check it between filesystem calls, so a
// closing session or a begun reap stops the next step rather than interrupting
// one already executing. Admission is what the attempt's liveness fences; an
// operation already admitted may run to completion.

func (engine *ContainerdEngine) ListRunMailbox(ctx context.Context, request ListRunMailboxRequest) (ListRunMailboxResponse, error) {
	return listRunMailbox(ctx, engine.config.RuntimeRoot, request)
}

func (engine *ContainerdEngine) ReadRunMailbox(ctx context.Context, request ReadRunMailboxRequest) (ReadRunMailboxResponse, error) {
	return readRunMailbox(ctx, engine.config.RuntimeRoot, request)
}

func (engine *ContainerdEngine) RemoveRunMailboxEntry(ctx context.Context, request RemoveRunMailboxEntryRequest) (RemoveRunMailboxEntryResponse, error) {
	return removeRunMailboxEntry(ctx, engine.config.RuntimeRoot, request)
}
