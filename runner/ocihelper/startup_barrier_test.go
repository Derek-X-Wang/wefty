package ocihelper

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/systemdpolicy"
)

// wedgedSweepEngine is a helper engine whose boot sweep fails the same way
// every time -- exactly the shape of #412, where one unreconcilable durable
// ownership record failed the startup sweep on every generation.
type wedgedSweepEngine struct {
	*fakeEngine
	fail bool
}

func (engine *wedgedSweepEngine) Sweep(ctx context.Context, request SweepRequest) (SweepResponse, error) {
	if engine.fail {
		return SweepResponse{}, errors.New("durable Attempt ownership record conflicts with fenced authority")
	}
	return engine.fakeEngine.Sweep(ctx, request)
}

// runHelperGeneration serves one helper process lifetime over its own listener
// and returns the error that ended it, the way the installed unit's ExecStart
// would observe it.
func runHelperGeneration(t *testing.T, engine Engine, stateDirectory string, bound int) error {
	t.Helper()
	return runHelperGenerationAt(t, engine, stateDirectory, bound, nil)
}

// runHelperGenerationAt serves one helper lifetime on an injected clock, so a
// streak's elapsed time is a decision the test makes rather than something it
// waits for.
func runHelperGenerationAt(t *testing.T, engine Engine, stateDirectory string, bound int, clock Clock) error {
	t.Helper()
	// macOS caps a unix socket path at 104 bytes; t.TempDir() names are long
	// enough to blow through it.
	directory, err := os.MkdirTemp("", "wefty-oci-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)
	listener, err := net.Listen("unix", filepath.Join(directory, "helper.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	server, err := NewServer(engine, ServerConfig{
		HelperVersion: "test", HelperChecksum: "checksum-test", AllowedUIDs: []uint32{uint32(os.Getuid())},
		StartupFailureStateDirectory: stateDirectory, StartupFailureBound: bound,
		StartupFailureWindow: testStartupFailureWindow, Clock: clock,
		Logf: func(string, ...any) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	select {
	case <-server.startupDone:
	case <-time.After(5 * time.Second):
		t.Fatal("helper startup barrier never completed")
	}
	server.sessionMu.Lock()
	startupErr := server.startupErr
	server.sessionMu.Unlock()
	cancel()
	_ = listener.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("helper server did not stop")
	}
	return startupErr
}

const testStartupFailureWindow = 30 * time.Second

// The #412 unit restarted 136 times in two minutes because a deterministic
// startup-sweep failure had no count bound. It must now surface as a failed
// unit with a typed reason after a bounded number of restarts -- but only once
// the streak has also outlasted the window, so a transient cannot wedge a
// healthy node.
func TestDeterministicStartupBarrierFailureWedgesWithinABoundedRestartCount(t *testing.T) {
	state := t.TempDir()
	engine := &wedgedSweepEngine{fakeEngine: newFakeEngine(), fail: true}
	clock := newManualClock(time.Unix(1_700_000_000, 0))

	for generation := 1; generation < 3; generation++ {
		err := runHelperGenerationAt(t, engine, state, 3, clock)
		if err == nil {
			t.Fatalf("generation %d served despite a failed startup barrier", generation)
		}
		var wedged *StartupWedgedError
		if errors.As(err, &wedged) {
			t.Fatalf("generation %d wedged before the bound: %v", generation, err)
		}
		clock.Advance(time.Second)
	}

	// The count is now satisfied but the streak is seconds old, not window old.
	err := runHelperGenerationAt(t, engine, state, 3, clock)
	var wedged *StartupWedgedError
	if errors.As(err, &wedged) {
		t.Fatalf("a streak shorter than the window wedged on count alone: %v", err)
	}

	clock.Advance(testStartupFailureWindow)
	err = runHelperGenerationAt(t, engine, state, 3, clock)
	if !errors.As(err, &wedged) {
		t.Fatalf("the helper kept restarting past its bound: %v", err)
	}
	if wedged.Consecutive < 3 || wedged.Bound != 3 || wedged.Phase != StartupBarrierSweep || wedged.Elapsed < testStartupFailureWindow {
		t.Fatalf("wedge = %+v, want a streak past both the count and the window", wedged)
	}
	if wedged.ExitStatus() != systemdpolicy.StartupWedgedExitStatus {
		t.Fatalf("wedge exit status = %d, want the status the unit names in RestartPreventExitStatus", wedged.ExitStatus())
	}
	// The typed cause survives to the journal line the operator reads.
	var barrier *StartupBarrierError
	if !errors.As(err, &barrier) || !strings.Contains(err.Error(), "conflicts with fenced authority") {
		t.Fatalf("the wedge discarded the underlying barrier failure: %v", err)
	}
}

// A barrier that succeeds clears the count, so a node that recovers is not one
// restart away from a wedge forever after.
func TestASuccessfulStartupBarrierClearsTheRestartBound(t *testing.T) {
	state := t.TempDir()
	engine := &wedgedSweepEngine{fakeEngine: newFakeEngine(), fail: true}
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	for generation := 0; generation < 3; generation++ {
		if err := runHelperGenerationAt(t, engine, state, 2, clock); err == nil {
			t.Fatal("a failed startup barrier served anyway")
		}
		clock.Advance(time.Second)
	}

	engine.fail = false
	if err := runHelperGenerationAt(t, engine, state, 2, clock); err != nil {
		t.Fatalf("a healthy startup barrier failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(state, startupFailureLedgerName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a verified barrier did not clear the failure ledger: %v", err)
	}

	// Time has moved well past the window, so only the cleared count stands
	// between this failure and a wedge.
	engine.fail = true
	clock.Advance(10 * testStartupFailureWindow)
	err := runHelperGenerationAt(t, engine, state, 2, clock)
	var wedged *StartupWedgedError
	if errors.As(err, &wedged) {
		t.Fatalf("the first failure after a healthy barrier wedged immediately: %v", err)
	}
}

// The rendered restart delays put five failures inside about 1.25 seconds, so a
// containerd restart that straddles helper activation would wedge a healthy
// node if the bound were a pure count.
func TestATransientBurstCannotConsumeTheWholeBound(t *testing.T) {
	state := t.TempDir()
	engine := &wedgedSweepEngine{fakeEngine: newFakeEngine(), fail: true}
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	for _, delay := range []time.Duration{250 * time.Millisecond, 370 * time.Millisecond, 550 * time.Millisecond, 820 * time.Millisecond, time.Second} {
		err := runHelperGenerationAt(t, engine, state, 3, clock)
		var wedged *StartupWedgedError
		if errors.As(err, &wedged) {
			t.Fatalf("a sub-second restart burst wedged the helper: %v", err)
		}
		clock.Advance(delay)
	}
	// The transient clears; the next healthy barrier resets everything.
	engine.fail = false
	if err := runHelperGenerationAt(t, engine, state, 3, clock); err != nil {
		t.Fatalf("the helper did not recover after the transient: %v", err)
	}
	if _, err := os.Stat(filepath.Join(state, startupFailureLedgerName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovery left the transient's ledger behind: %v", err)
	}
}

// A streak is a run of failures close together. A stale ledger from an
// unrelated incident last week must not count toward today's bound.
func TestAStaleLedgerStartsANewStreakInsteadOfContinuingTheOldOne(t *testing.T) {
	state := t.TempDir()
	engine := &wedgedSweepEngine{fakeEngine: newFakeEngine(), fail: true}
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	for generation := 0; generation < 4; generation++ {
		if err := runHelperGenerationAt(t, engine, state, 2, clock); err == nil {
			t.Fatalf("generation %d served despite a failed startup barrier", generation)
		}
		clock.Advance(time.Second)
	}

	// Nothing touched the helper for far longer than the window.
	clock.Advance(10 * testStartupFailureWindow)
	err := runHelperGenerationAt(t, engine, state, 2, clock)
	var wedged *StartupWedgedError
	if errors.As(err, &wedged) {
		t.Fatalf("a stale streak was resumed instead of restarted: %v", err)
	}
	payload, readErr := os.ReadFile(filepath.Join(state, startupFailureLedgerName))
	if readErr != nil {
		t.Fatal(readErr)
	}
	var ledger startupFailureLedger
	if err := json.Unmarshal(payload, &ledger); err != nil {
		t.Fatal(err)
	}
	if ledger.Consecutive != 1 || !ledger.FirstAt.Equal(ledger.UpdatedAt) {
		t.Fatalf("ledger = %+v, want a fresh one-failure streak", ledger)
	}
}

// A post-ready crash is not a startup-barrier failure and must not consume the
// bound: the injected-fault acceptance lane restarts the helper seven times on
// purpose, and every one of those barriers succeeds.
func TestPostReadyCrashesDoNotConsumeTheStartupBound(t *testing.T) {
	state := t.TempDir()
	engine := &wedgedSweepEngine{fakeEngine: newFakeEngine()}
	for generation := 0; generation < 7; generation++ {
		if err := runHelperGeneration(t, engine, state, 3); err != nil {
			t.Fatalf("generation %d: %v", generation, err)
		}
	}
	if _, err := os.Stat(filepath.Join(state, startupFailureLedgerName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("healthy restarts wrote a failure ledger: %v", err)
	}
}

// Losing the count must never manufacture a refusal to serve: an unusable
// ledger degrades to the old unbounded-but-honest behaviour, not to a wedge.
func TestAnUnusableLedgerReportsTheOrdinaryFailureInsteadOfWedging(t *testing.T) {
	engine := &wedgedSweepEngine{fakeEngine: newFakeEngine(), fail: true}
	// A state directory that cannot hold the ledger, because the ledger path
	// is occupied by a directory.
	state := t.TempDir()
	if err := os.MkdirAll(filepath.Join(state, startupFailureLedgerName), 0o700); err != nil {
		t.Fatal(err)
	}
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	for generation := 0; generation < 4; generation++ {
		err := runHelperGenerationAt(t, engine, state, 2, clock)
		var wedged *StartupWedgedError
		if err == nil || errors.As(err, &wedged) {
			t.Fatalf("generation %d with an unusable ledger = %v, want the ordinary barrier failure", generation, err)
		}
		clock.Advance(testStartupFailureWindow)
	}
}

// With no state directory configured the bound is simply off; nothing panics
// and nothing wedges.
func TestNoConfiguredStateDirectoryDisablesTheBound(t *testing.T) {
	engine := &wedgedSweepEngine{fakeEngine: newFakeEngine(), fail: true}
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	for generation := 0; generation < 4; generation++ {
		err := runHelperGenerationAt(t, engine, "", 2, clock)
		var wedged *StartupWedgedError
		if err == nil || errors.As(err, &wedged) {
			t.Fatalf("unconfigured bound = %v, want the ordinary barrier failure", err)
		}
		clock.Advance(testStartupFailureWindow)
	}
}
