//go:build darwin || linux

package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
)

// recordingMailboxFS decorates a real implementation and records every call the
// publisher makes, so a test can prove the publisher reaches the events
// directory only through this seam.
type recordingMailboxFS struct {
	inner mailboxFS
	mu    sync.Mutex
	calls []string
}

func (fs *recordingMailboxFS) record(call string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.calls = append(fs.calls, call)
}

func (fs *recordingMailboxFS) snapshot() []string {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return slices.Clone(fs.calls)
}

func (fs *recordingMailboxFS) list(limit int) ([]string, bool, error) {
	fs.record("list")
	return fs.inner.list(limit)
}

func (fs *recordingMailboxFS) read(name string, limit int) ([]byte, bool, error) {
	fs.record("read:" + name)
	return fs.inner.read(name, limit)
}

func (fs *recordingMailboxFS) remove(name string) error {
	fs.record("remove:" + name)
	return fs.inner.remove(name)
}

func (fs *recordingMailboxFS) close() error {
	fs.record("close")
	return fs.inner.close()
}

// memoryMailboxFS holds the events in memory and touches no filesystem at all.
// It is the honest test of the seam: an implementation that shares nothing with
// os.Root still has to satisfy the publisher, which is what the OCI helper read
// path will have to do.
type memoryMailboxFS struct {
	mu      sync.Mutex
	entries map[string][]byte
	// unreadable names exist in the listing but cannot be read, standing in
	// for an entry that is not a readable regular file.
	unreadable map[string]bool
	closed     bool
}

func newMemoryMailboxFS() *memoryMailboxFS {
	return &memoryMailboxFS{entries: map[string][]byte{}, unreadable: map[string]bool{}}
}

func (fs *memoryMailboxFS) put(name, content string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.entries[name] = []byte(content)
}

func (fs *memoryMailboxFS) remaining() []string {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	names := make([]string, 0, len(fs.entries))
	for name := range fs.entries {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func (fs *memoryMailboxFS) list(limit int) ([]string, bool, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	// Deliberately reverse-sorted: the publisher owns lexical order, not the
	// implementation, and this fails loudly if that ever stops being true.
	names := make([]string, 0, len(fs.entries))
	for name := range fs.entries {
		names = append(names, name)
	}
	slices.Sort(names)
	slices.Reverse(names)
	if len(names) > limit {
		return names[:limit], true, nil
	}
	return names, len(names) >= limit, nil
}

func (fs *memoryMailboxFS) read(name string, limit int) ([]byte, bool, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.unreadable[name] {
		return nil, false, fmt.Errorf("run mailbox path %q is not a regular file", name)
	}
	payload, ok := fs.entries[name]
	if !ok {
		return nil, false, os.ErrNotExist
	}
	if len(payload) > limit {
		return slices.Clone(payload[:limit]), true, nil
	}
	return slices.Clone(payload), false, nil
}

func (fs *memoryMailboxFS) remove(name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if _, ok := fs.entries[name]; !ok {
		return os.ErrNotExist
	}
	delete(fs.entries, name)
	return nil
}

func (fs *memoryMailboxFS) close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.closed = true
	return nil
}

const (
	mailboxAgnosticFirstEvent = "wefty-protocol: 1\nkind: envelope\nstep: alpha\nsummary: first\npayload: text\n--\nfirst payload\n"
	mailboxAgnosticLastEvent  = "wefty-protocol: 1\nkind: envelope\nstep: beta\nsummary: second\npayload: text\n--\nsecond payload\n"
)

// TestRunMailboxPublisherIsFilesystemAgnostic proves the publisher's rules --
// a complete listing, lexical publication order, one bounded read per event and
// retirement once the ledger holds it -- are the publisher's and not the
// os.Root implementation's. The OCI helper read path lands under this seam, so
// anything the publisher learned about the filesystem by other means would
// silently stop being true there.
func TestRunMailboxPublisherIsFilesystemAgnostic(t *testing.T) {
	t.Run("through a recording decorator", func(t *testing.T) {
		appender := newRecordingAppender("")
		mailbox, _, _ := newTestMailbox(t, appender, "")
		recorder := &recordingMailboxFS{inner: mailbox.fs}
		mailbox.fs = recorder

		// Write through the real directory the decorated implementation owns.
		writeMailboxEvent(t, mailbox.directory, "0002-beta", mailboxAgnosticLastEvent)
		writeMailboxEvent(t, mailbox.directory, "0001-alpha", mailboxAgnosticFirstEvent)

		if drained := mailbox.sweep(context.Background()); !drained {
			t.Fatal("sweep did not drain a complete, readable listing")
		}
		wantCalls := []string{"list", "read:0001-alpha", "remove:0001-alpha", "read:0002-beta", "remove:0002-beta"}
		if calls := recorder.snapshot(); !slices.Equal(calls, wantCalls) {
			t.Fatalf("publisher calls = %v, want %v", calls, wantCalls)
		}
		assertPublishedSteps(t, appender, "alpha", "beta")
	})

	t.Run("over an implementation that is not a filesystem", func(t *testing.T) {
		appender := newRecordingAppender("")
		mailbox, _, _ := newTestMailbox(t, appender, "")
		memory := newMemoryMailboxFS()
		memory.put("0002-beta", mailboxAgnosticLastEvent)
		memory.put("0001-alpha", mailboxAgnosticFirstEvent)
		memory.put("0003-junk", "this event has no separator")
		memory.unreadable["0004-unreadable"] = true
		memory.put("0004-unreadable", "")
		if err := mailbox.fs.close(); err != nil {
			t.Fatalf("close the prepared implementation: %v", err)
		}
		mailbox.fs = memory

		if drained := mailbox.sweep(context.Background()); !drained {
			t.Fatal("sweep did not drain a complete listing")
		}
		// Every entry is accounted for: two published, one reported as
		// malformed, and one unreadable entry discarded.
		if remaining := memory.remaining(); len(remaining) != 0 {
			t.Fatalf("events left behind = %v, want none", remaining)
		}
		// The two valid events publish first and in lexical order; the
		// rejection report for the malformed file follows them.
		assertPublishedSteps(t, appender, "alpha", "beta", "mailbox")

		mailbox.close()
		if !memory.closed {
			t.Fatal("closing the mailbox did not close its implementation")
		}
	})
}

// TestRunMailboxPublisherStopsWhenTheImplementationFails proves a failing
// implementation retains evidence rather than losing it, whatever the failure
// is underneath.
func TestRunMailboxPublisherStopsWhenTheImplementationFails(t *testing.T) {
	appender := newRecordingAppender("")
	mailbox, _, _ := newTestMailbox(t, appender, "")
	if err := mailbox.fs.close(); err != nil {
		t.Fatalf("close the prepared implementation: %v", err)
	}
	mailbox.fs = &failingMailboxFS{err: errors.New("mailbox transport is unavailable")}

	if drained := mailbox.sweep(context.Background()); drained {
		t.Fatal("a listing that failed must not report the mailbox drained")
	}
	if documents := appender.snapshot(); len(documents) != 0 {
		t.Fatalf("published %d documents from a failed listing, want none", len(documents))
	}
	if !mailbox.pending() {
		t.Fatal("a mailbox whose listing fails must remain pending so the handoff is retained")
	}
}

type failingMailboxFS struct{ err error }

func (fs *failingMailboxFS) list(int) ([]string, bool, error)       { return nil, false, fs.err }
func (fs *failingMailboxFS) read(string, int) ([]byte, bool, error) { return nil, false, fs.err }
func (fs *failingMailboxFS) remove(string) error                    { return fs.err }
func (fs *failingMailboxFS) close() error                           { return nil }

func assertPublishedSteps(t *testing.T, appender *recordingAppender, steps ...string) {
	t.Helper()
	documents := appender.snapshot()
	if len(documents) != len(steps) {
		t.Fatalf("published %d documents, want %d", len(documents), len(steps))
	}
	for index, document := range documents {
		envelope := decodeEnvelope(t, document.body)
		if envelope.StepID != steps[index] {
			t.Fatalf("document %d step = %q, want %q", index, envelope.StepID, steps[index])
		}
	}
}
