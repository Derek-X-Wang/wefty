//go:build darwin || linux

package agent

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// fakeRunMailboxRuntime stands in for the OCI adapter. It records the reference
// every call carries, so a test can prove the publisher never widens what it
// asks for, and it can be made to block so a fence can be shown to cancel an
// in-flight call rather than wait out its bound.
type fakeRunMailboxRuntime struct {
	mu         sync.Mutex
	entries    map[string][]byte
	references []workloadrunner.RunMailboxReference
	block      chan struct{}
	entered    chan struct{}
	failWith   error
	// readErr fails only the read, so a test can prove a transient read
	// failure never becomes a removal.
	readErr error
	removed []string
}

func newFakeRunMailboxRuntime() *fakeRunMailboxRuntime {
	return &fakeRunMailboxRuntime{entries: map[string][]byte{}, entered: make(chan struct{}, 8)}
}

func (runtime *fakeRunMailboxRuntime) put(name, content string) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.entries[name] = []byte(content)
}

func (runtime *fakeRunMailboxRuntime) remaining() []string {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	names := make([]string, 0, len(runtime.entries))
	for name := range runtime.entries {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func (runtime *fakeRunMailboxRuntime) seen() []workloadrunner.RunMailboxReference {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return slices.Clone(runtime.references)
}

func (runtime *fakeRunMailboxRuntime) enter(ctx context.Context, reference workloadrunner.RunMailboxReference) error {
	runtime.mu.Lock()
	runtime.references = append(runtime.references, reference)
	block, failWith := runtime.block, runtime.failWith
	runtime.mu.Unlock()
	select {
	case runtime.entered <- struct{}{}:
	default:
	}
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return failWith
}

func (runtime *fakeRunMailboxRuntime) ListRunMailbox(ctx context.Context, reference workloadrunner.RunMailboxReference, limit int) ([]string, bool, error) {
	if err := runtime.enter(ctx, reference); err != nil {
		return nil, false, err
	}
	names := runtime.remaining()
	if len(names) > limit {
		return names[:limit], true, nil
	}
	return names, len(names) >= limit, nil
}

func (runtime *fakeRunMailboxRuntime) ReadRunMailbox(ctx context.Context, reference workloadrunner.RunMailboxReference, name string, limit int) ([]byte, bool, error) {
	if err := runtime.enter(ctx, reference); err != nil {
		return nil, false, err
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.readErr != nil {
		return nil, false, runtime.readErr
	}
	payload, ok := runtime.entries[name]
	if !ok {
		return nil, false, fs.ErrNotExist
	}
	if len(payload) > limit {
		return slices.Clone(payload[:limit]), true, nil
	}
	return slices.Clone(payload), false, nil
}

func (runtime *fakeRunMailboxRuntime) RemoveRunMailboxEntry(ctx context.Context, reference workloadrunner.RunMailboxReference, name string) error {
	if err := runtime.enter(ctx, reference); err != nil {
		return err
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.removed = append(runtime.removed, name)
	if _, ok := runtime.entries[name]; !ok {
		return fs.ErrNotExist
	}
	delete(runtime.entries, name)
	return nil
}

func (runtime *fakeRunMailboxRuntime) removals() []string {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return slices.Clone(runtime.removed)
}

const (
	remoteMailboxRunID   = "run_remote"
	remoteMailboxAttempt = "attempt-remote"
	remoteMailboxOwner   = "run_remote"
)

func remoteMailboxClaim(params string) l1.Claim {
	labels := map[string]string{"run_id": remoteMailboxRunID}
	if params != "" {
		labels[l3.RunParamsLabel] = params
	}
	return l1.Claim{
		Job: l1.Job{JobID: "job-remote", Spec: contract.JobSpec{
			Kind:   contract.JobKindOCI,
			Class:  contract.JobClassOneShot,
			Labels: labels,
			Execution: contract.ExecutionSpec{
				OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "example.test/image:v1"}},
				Env: map[string]string{
					contract.EnvRunID:      remoteMailboxRunID,
					contract.EnvL3Endpoint: "http://127.0.0.1:9/",
				},
				SensitiveEnv: map[string]string{contract.EnvRunToken: mailboxTestToken},
			},
		}},
		Lease: l1.AttemptLease{AttemptID: remoteMailboxAttempt},
	}
}

func newRemoteTestMailbox(t *testing.T, appender runLedgerAppender, runtime workloadrunner.RunMailboxRuntime, params string) (*runMailbox, string) {
	t.Helper()
	stateRoot := t.TempDir()
	authority := workloadrunner.AttemptAuthority{
		NodeID: "node-1", BootSessionID: "boot-1", JobID: "job-remote",
		AttemptID: remoteMailboxAttempt, FencingToken: "fence-1",
		WorkloadClass: contract.JobClassOneShot, RemovalGeneration: "removal-1",
	}
	mailbox, err := prepareRemoteRunMailbox(remoteMailboxClaim(params), stateRoot, runtime, authority,
		appender, time.Hour, newManualClock(mailboxTestClockOrigin), t.Logf)
	if err != nil {
		t.Fatalf("prepare remote run mailbox: %v", err)
	}
	t.Cleanup(mailbox.close)
	return mailbox, stateRoot
}

// TestRemoteRunMailboxPublishesThroughTheRuntime is the OCI half of the
// publisher: exactly the same rules, over a mailbox this process never opens.
func TestRemoteRunMailboxPublishesThroughTheRuntime(t *testing.T) {
	appender := newRecordingAppender("")
	runtime := newFakeRunMailboxRuntime()
	runtime.put("0002-beta", mailboxAgnosticLastEvent)
	runtime.put("0001-alpha", mailboxAgnosticFirstEvent)
	mailbox, stateRoot := newRemoteTestMailbox(t, appender, runtime, "")

	if !mailbox.readsThroughRuntime() {
		t.Fatal("a mailbox served by a runtime must say so, or the final drain runs after the reap")
	}
	if drained := mailbox.sweep(context.Background()); !drained {
		t.Fatal("sweep did not drain the runtime's mailbox")
	}
	assertPublishedSteps(t, appender, "alpha", "beta")
	if remaining := runtime.remaining(); len(remaining) != 0 {
		t.Fatalf("events left behind = %v", remaining)
	}
	for _, reference := range runtime.seen() {
		if reference.OwnerKey != remoteMailboxOwner || reference.RunID != remoteMailboxRunID ||
			reference.Authority.AttemptID != remoteMailboxAttempt {
			t.Fatalf("the publisher asked for %+v", reference)
		}
	}
	// The bookkeeping is on the agent side of the boundary, never in the
	// volume the workload can write.
	state := filepath.Join(stateRoot, runMailboxStateDirectoryName, runMailboxStateComponent(remoteMailboxAttempt), runMailboxStateFileName)
	if _, err := os.Stat(state); err != nil {
		t.Fatalf("agent-local bookkeeping was not written: %v", err)
	}
	if err := discardRemoteRunMailboxState(stateRoot, remoteMailboxAttempt); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(state); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("drained bookkeeping survived: %v", err)
	}
}

// TestRemoteRunMailboxFenceCancelsAnInFlightCall proves authority loss does not
// wait out a helper that has stopped answering.
func TestRemoteRunMailboxFenceCancelsAnInFlightCall(t *testing.T) {
	appender := newRecordingAppender("")
	runtime := newFakeRunMailboxRuntime()
	runtime.block = make(chan struct{})
	runtime.put("0001-alpha", mailboxAgnosticFirstEvent)
	mailbox, _ := newRemoteTestMailbox(t, appender, runtime, "")

	swept := make(chan bool, 1)
	go func() { swept <- mailbox.sweep(context.Background()) }()
	waitMailboxSignal(t, runtime.entered)
	mailbox.fence(errors.New("attempt authority lost"))
	select {
	case drained := <-swept:
		if drained {
			t.Fatal("a fenced sweep reported the mailbox drained")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fencing did not cancel the in-flight runtime call")
	}
	if documents := appender.snapshot(); len(documents) != 0 {
		t.Fatalf("a fenced mailbox published %d documents", len(documents))
	}
	if !mailbox.pending() {
		t.Fatal("a fenced mailbox with unread evidence must stay pending so the volume is retained")
	}
	close(runtime.block)
}

// TestRemoteRunMailboxSeedCarriesOnlyDeliverableParams keeps the agent from
// asking the helper to write a document the contract says is not delivered.
func TestRemoteRunMailboxSeedCarriesOnlyDeliverableParams(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		params string
		want   string
	}{
		{name: "an object is delivered", params: `{"branch":"main"}`, want: `{"branch":"main"}`},
		{name: "no params deliver nothing", params: "", want: ""},
		{name: "an array is not delivered", params: `["not","an","object"]`, want: ""},
		{name: "an oversize document is not delivered", params: "{" + string(make([]byte, MaxRunMailboxParamsBytes)) + "}", want: ""},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			seed := runMailboxSeed(remoteMailboxClaim(testCase.params).Job.Spec)
			if seed.RunID != remoteMailboxRunID {
				t.Fatalf("seed run ID = %q", seed.RunID)
			}
			if string(seed.Params) != testCase.want {
				t.Fatalf("seed params = %q, want %q", seed.Params, testCase.want)
			}
		})
	}
}

// TestRemoteRunMailboxAvailabilityMatchesWhatCanBePublished keeps the OCI
// eligibility rule beside the process one it mirrors.
func TestRemoteRunMailboxAvailabilityMatchesWhatCanBePublished(t *testing.T) {
	base := remoteMailboxClaim("").Job.Spec
	if !remoteRunMailboxAvailable(base) {
		t.Fatal("a dispatched OCI one-shot with a run ID, an endpoint and a token must be eligible")
	}
	for _, testCase := range []struct {
		name   string
		mutate func(*contract.JobSpec)
	}{
		{name: "a process job", mutate: func(spec *contract.JobSpec) { spec.Kind = contract.JobKindProcess }},
		{name: "a service", mutate: func(spec *contract.JobSpec) { spec.Class = contract.JobClassService }},
		{name: "no run ID", mutate: func(spec *contract.JobSpec) { delete(spec.Execution.Env, contract.EnvRunID) }},
		{name: "a run ID that is not one component", mutate: func(spec *contract.JobSpec) {
			spec.Execution.Env[contract.EnvRunID] = "../escape"
		}},
		{name: "no ledger endpoint", mutate: func(spec *contract.JobSpec) { delete(spec.Execution.Env, contract.EnvL3Endpoint) }},
		{name: "no run token for the agent to publish with", mutate: func(spec *contract.JobSpec) {
			delete(spec.Execution.SensitiveEnv, contract.EnvRunToken)
		}},
		{name: "a Computer", mutate: func(spec *contract.JobSpec) {
			spec.Execution.OCI.Computer = &contract.OCIComputerSpec{}
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			spec := remoteMailboxClaim("").Job.Spec
			spec.Execution.Env = cloneEnvironment(spec.Execution.Env)
			spec.Execution.SensitiveEnv = cloneEnvironment(spec.Execution.SensitiveEnv)
			testCase.mutate(&spec)
			if remoteRunMailboxAvailable(spec) {
				t.Fatal("this claim must not receive a mailbox nothing would publish from")
			}
		})
	}
}

// TestRemoteRunMailboxKeepsAnEntryItCouldNotRead is the rule a transient helper
// failure must not break: a read that fails for any reason other than the
// helper positively classifying the entry says nothing about the entry, so the
// entry stays, nothing is removed, and the evidence survives to be retried.
// Deleting a run's only copy on the strength of a timeout is the one failure
// this publisher must not have.
func TestRemoteRunMailboxKeepsAnEntryItCouldNotRead(t *testing.T) {
	for _, testCase := range []struct {
		name string
		err  error
	}{
		{name: "an expired deadline", err: context.DeadlineExceeded},
		{name: "a transport failure", err: errors.New("helper session is not configured")},
		{name: "a refused authority", err: errors.New("attempt authority does not match a live attempt")},
		{name: "an unclassifiable I/O error", err: errors.New("input/output error")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			appender := newRecordingAppender("")
			runtime := newFakeRunMailboxRuntime()
			runtime.put("0001-alpha", mailboxAgnosticFirstEvent)
			runtime.readErr = testCase.err
			mailbox, _ := newRemoteTestMailbox(t, appender, runtime, "")

			if drained := mailbox.sweep(context.Background()); drained {
				t.Fatal("a sweep whose read failed reported the mailbox drained")
			}
			if removals := runtime.removals(); len(removals) != 0 {
				t.Fatalf("an unread entry was removed: %v", removals)
			}
			if remaining := runtime.remaining(); len(remaining) != 1 || remaining[0] != "0001-alpha" {
				t.Fatalf("the unread entry did not survive: %v", remaining)
			}
			if documents := appender.snapshot(); len(documents) != 0 {
				t.Fatalf("published %d documents from a failed read", len(documents))
			}
			if !mailbox.pending() {
				t.Fatal("an entry that could not be read must stay pending")
			}

			// Finalization retries and then reports incompleteness, which is
			// what retains the handoff volume instead of expiring it as a
			// clean success.
			mailbox.finalize(context.Background())
			if !mailbox.publicationIncomplete() {
				t.Fatal("a persistent read failure did not latch publicationIncomplete")
			}
			if removals := runtime.removals(); len(removals) != 0 {
				t.Fatalf("finalization removed an unread entry: %v", removals)
			}
		})
	}
}

// TestRemoteRunMailboxDiscardsOnlyPositivelyClassifiedJunk is the other half of
// the same rule: when the runtime does say the entry can never be an event, it
// is removed, because keeping it would hold the sweep undrained forever.
func TestRemoteRunMailboxDiscardsOnlyPositivelyClassifiedJunk(t *testing.T) {
	appender := newRecordingAppender("")
	runtime := newFakeRunMailboxRuntime()
	runtime.put("0001-junk", "")
	runtime.readErr = fmt.Errorf("%w: is not a regular file", workloadrunner.ErrRunMailboxEntryUnusable)
	mailbox, _ := newRemoteTestMailbox(t, appender, runtime, "")

	if drained := mailbox.sweep(context.Background()); !drained {
		t.Fatal("a mailbox holding only junk did not drain")
	}
	if removals := runtime.removals(); len(removals) != 1 || removals[0] != "0001-junk" {
		t.Fatalf("junk removals = %v, want exactly the junk entry", removals)
	}
	if documents := appender.snapshot(); len(documents) != 0 {
		t.Fatalf("junk published %d documents", len(documents))
	}
}

// TestRemoteRunMailboxSeedAcceptsParamsAtTheLedgersOwnBound keeps the agent
// from refusing, one byte at a time, a document L3 is willing to dispatch.
func TestRemoteRunMailboxSeedAcceptsParamsAtTheLedgersOwnBound(t *testing.T) {
	document := func(size int) string {
		// {"p":"<filler>"} padded to exactly size bytes.
		const prefix, suffix = `{"p":"`, `"}`
		filler := size - len(prefix) - len(suffix)
		if filler < 0 {
			t.Fatalf("cannot build a %d byte object", size)
		}
		return prefix + strings.Repeat("x", filler) + suffix
	}
	for _, testCase := range []struct {
		size      int
		delivered bool
	}{
		{size: MaxRunMailboxParamsBytes - 1, delivered: true},
		{size: MaxRunMailboxParamsBytes, delivered: true},
		{size: MaxRunMailboxParamsBytes + 1, delivered: false},
	} {
		t.Run(fmt.Sprintf("%d bytes", testCase.size), func(t *testing.T) {
			params := document(testCase.size)
			seed := runMailboxSeed(remoteMailboxClaim(params).Job.Spec)
			if !testCase.delivered {
				if len(seed.Params) != 0 {
					t.Fatalf("an oversize document was delivered as %d bytes", len(seed.Params))
				}
				return
			}
			if len(seed.Params) != testCase.size {
				t.Fatalf("delivered %d bytes, want the canonical document's %d", len(seed.Params), testCase.size)
			}
			// The helper applies the same bound to the wire, so a document the
			// agent delivers must be one the helper accepts.
			if len(seed.Params) > ocihelper.MaxRunMailboxParamsBytes {
				t.Fatalf("delivered %d bytes, past the helper's %d byte wire bound",
					len(seed.Params), ocihelper.MaxRunMailboxParamsBytes)
			}
		})
	}
}
