//go:build darwin || linux

package ocihelper

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The fake runtime holds live attempts while the production filesystem bodies
// serve their mailboxes. Refusals cross the server and wire before the client
// decides whether the helper session still holds authority.
type runMailboxFilesystemEngine struct {
	*fakeEngine
	runtimeRoot string
}

func (engine *runMailboxFilesystemEngine) ListRunMailbox(ctx context.Context, request ListRunMailboxRequest) (ListRunMailboxResponse, error) {
	return listRunMailbox(ctx, engine.runtimeRoot, request)
}

func (engine *runMailboxFilesystemEngine) ReadRunMailbox(ctx context.Context, request ReadRunMailboxRequest) (ReadRunMailboxResponse, error) {
	return readRunMailbox(ctx, engine.runtimeRoot, request)
}

func (engine *runMailboxFilesystemEngine) RemoveRunMailboxEntry(ctx context.Context, request RemoveRunMailboxEntryRequest) (RemoveRunMailboxEntryResponse, error) {
	return removeRunMailboxEntry(ctx, engine.runtimeRoot, request)
}

func TestRunMailboxFilesystemRefusalKeepsSessionAndNeighbourAttempt(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		operation Method
		symlink   bool
	}{
		{name: "list_symlinked_events", operation: MethodListRunMailbox, symlink: true},
		{name: "read_symlinked_events", operation: MethodReadRunMailbox, symlink: true},
		{name: "remove_symlinked_events", operation: MethodRemoveRunMailbox, symlink: true},
		{name: "remove_nonempty_directory", operation: MethodRemoveRunMailbox},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runtimeRoot, mailbox := newConfinementRoot(t)
			events := filepath.Join(mailbox, RunMailboxEventsDirectoryName)
			const name = "0001-event"
			const evidence = "retained evidence"
			var retained string
			if testCase.symlink {
				writeConfinementEvent(t, mailbox, name, evidence)
				retainedEvents := filepath.Join(mailbox, "retained-events")
				if err := os.Rename(events, retainedEvents); err != nil {
					t.Fatal(err)
				}
				retained = filepath.Join(retainedEvents, name)
				elsewhere := stableTempDir(t)
				if err := os.WriteFile(filepath.Join(elsewhere, name), []byte("outside evidence"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(elsewhere, events); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					payload, err := os.ReadFile(filepath.Join(elsewhere, name))
					if err != nil || string(payload) != "outside evidence" {
						t.Errorf("refusal changed evidence outside the mailbox: %q, %v", payload, err)
					}
				})
			} else {
				if err := os.Mkdir(filepath.Join(events, name), 0o700); err != nil {
					t.Fatal(err)
				}
				retained = filepath.Join(events, name, "child")
				if err := os.WriteFile(retained, []byte(evidence), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			engine := &runMailboxFilesystemEngine{fakeEngine: newFakeEngine(), runtimeRoot: runtimeRoot}
			session, authority := startRunMailboxSession(t, engine, func(authority AttemptAuthority) RunRequest {
				request := testRunMailboxRunRequest(authority)
				request.InitialDeadman = time.Minute
				return request
			})
			neighbour := authority
			neighbour.JobID = "neighbour-job"
			neighbour.AttemptID = "neighbour-attempt"
			if _, err := session.Run(t.Context(), testRunRequest(neighbour, time.Minute)); err != nil {
				t.Fatal(err)
			}
			generation := session.Handshake().SessionGeneration
			reference := mailboxReference(authority)
			var err error
			switch testCase.operation {
			case MethodListRunMailbox:
				_, err = session.ListRunMailbox(t.Context(), ListRunMailboxRequest{RunMailboxReference: reference})
			case MethodReadRunMailbox:
				response, readErr := session.ReadRunMailbox(t.Context(), ReadRunMailboxRequest{RunMailboxReference: reference, Name: name})
				err = readErr
				if response.Absent || response.Unusable || len(response.Payload) != 0 {
					t.Fatalf("unreadable evidence became a read result: %+v", response)
				}
			case MethodRemoveRunMailbox:
				response, removeErr := session.RemoveRunMailboxEntry(t.Context(), RemoveRunMailboxEntryRequest{RunMailboxReference: reference, Name: name})
				err = removeErr
				if response.Removed || response.Absent {
					t.Fatalf("refused removal became positive absence: %+v", response)
				}
			}
			if payload, err := os.ReadFile(retained); err != nil || string(payload) != evidence {
				t.Fatalf("refusal lost retained evidence: %q, %v", payload, err)
			}
			requireRefusalKeptSession(t, session, err, testCase.operation, generation, neighbour)
		})
	}
}

// A handoff-file read is the result.json read Adapter.ReadHandoffFile makes:
// ReadRunMailbox in the handoff_files scope, with no run-mailbox identity. The
// descent refuses a symlinked handoff volume. That refusal is scoped to the
// one attempt, like a refused event read, so the helper session and a
// neighbour attempt keep their authority (#848).
func TestHandoffFileReadRefusalKeepsSessionAndNeighbourAttempt(t *testing.T) {
	runtimeRoot, _ := newConfinementRoot(t)
	volumeName, err := DeterministicHandoffVolumeDirectory(mailboxTestOwnerKey)
	if err != nil {
		t.Fatal(err)
	}
	volume := filepath.Join(runtimeRoot, "handoffs", volumeName)
	const evidence = `{"ok":true}`
	if err := os.WriteFile(filepath.Join(volume, "result.json"), []byte(evidence), 0o600); err != nil {
		t.Fatal(err)
	}
	retainedVolume := filepath.Join(runtimeRoot, "handoffs", "retained-volume")
	if err := os.Rename(volume, retainedVolume); err != nil {
		t.Fatal(err)
	}
	elsewhere := stableTempDir(t)
	if err := os.WriteFile(filepath.Join(elsewhere, "result.json"), []byte("outside evidence"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, volume); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		payload, err := os.ReadFile(filepath.Join(elsewhere, "result.json"))
		if err != nil || string(payload) != "outside evidence" {
			t.Errorf("refusal changed evidence outside the handoff volume: %q, %v", payload, err)
		}
	})
	engine := &runMailboxFilesystemEngine{fakeEngine: newFakeEngine(), runtimeRoot: runtimeRoot}
	session, authority := startRunMailboxSession(t, engine, func(authority AttemptAuthority) RunRequest {
		request := testRunMailboxRunRequest(authority)
		request.Workload.RunMailbox = nil // handoff-only, as ReadHandoffFile reads it
		request.InitialDeadman = time.Minute
		return request
	})
	neighbour := authority
	neighbour.JobID = "neighbour-job"
	neighbour.AttemptID = "neighbour-attempt"
	if _, err := session.Run(t.Context(), testRunRequest(neighbour, time.Minute)); err != nil {
		t.Fatal(err)
	}
	generation := session.Handshake().SessionGeneration
	// The exact reference Adapter.ReadHandoffFile sends.
	reference := RunMailboxReference{Authority: authority, OwnerKey: mailboxTestOwnerKey, Scope: RunMailboxScopeHandoffFiles}
	response, err := session.ReadRunMailbox(t.Context(), ReadRunMailboxRequest{RunMailboxReference: reference, Name: "result.json"})
	if response.Absent || response.Unusable || len(response.Payload) != 0 || errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("refused handoff read became a read result: %+v, %v", response, err)
	}
	if payload, err := os.ReadFile(filepath.Join(retainedVolume, "result.json")); err != nil || string(payload) != evidence {
		t.Fatalf("refusal lost retained evidence: %q, %v", payload, err)
	}
	requireRefusalKeptSession(t, session, err, MethodReadRunMailbox, generation, neighbour)
}

// requireRefusalKeptSession is the outer half of an attempt-scoped refusal: the
// helper answered with an engine failure for the one operation, and the
// session, its generation and a neighbour attempt all keep their authority.
func requireRefusalKeptSession(t *testing.T, session *Session, err error, operation Method, generation uint64, neighbour AttemptAuthority) {
	t.Helper()
	var rpcErr *RPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != CodeEngineFailure || rpcErr.EngineFailure == nil ||
		rpcErr.EngineFailure.Operation != operation || rpcErr.EngineFailure.Reason != EngineFailureOperationFailed {
		t.Fatalf("filesystem refusal = %#v, %v, want %s engine failure", rpcErr, err, operation)
	}
	var runtimeLoss *RuntimeLossError
	if errors.As(err, &runtimeLoss) {
		t.Errorf("attempt-scoped mailbox refusal became runtime loss: %v", err)
	}
	if healthErr := session.HealthError(); healthErr != nil {
		t.Fatalf("mailbox refusal invalidated the helper session: %v", healthErr)
	}
	if err := session.flushHeartbeat(t.Context()); err != nil {
		t.Fatalf("mailbox refusal lost live helper authority: %v", err)
	}
	if got := session.Handshake().SessionGeneration; got != generation {
		t.Fatalf("session generation = %d after refusal, want %d", got, generation)
	}
	if err := session.Signal(t.Context(), SignalRequest{Authority: neighbour, Signal: SignalTERM}); err != nil {
		t.Fatalf("healthy neighbour lost attempt authority to a mailbox refusal: %v", err)
	}
}
