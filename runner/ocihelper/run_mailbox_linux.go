//go:build linux

package ocihelper

import "context"

// The ContainerdEngine serves the mailbox out of its own managed root. The
// confinement itself is platform-neutral and lives in run_mailbox.go, so it is
// exercised by the same tests on every platform the agent builds for.

func (engine *ContainerdEngine) ListRunMailbox(_ context.Context, request ListRunMailboxRequest) (ListRunMailboxResponse, error) {
	return listRunMailbox(engine.config.RuntimeRoot, request)
}

func (engine *ContainerdEngine) ReadRunMailbox(_ context.Context, request ReadRunMailboxRequest) (ReadRunMailboxResponse, error) {
	return readRunMailbox(engine.config.RuntimeRoot, request)
}

func (engine *ContainerdEngine) RemoveRunMailboxEntry(_ context.Context, request RemoveRunMailboxEntryRequest) (RemoveRunMailboxEntryResponse, error) {
	return removeRunMailboxEntry(engine.config.RuntimeRoot, request)
}
