package ocihelper

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/systemdpolicy"
)

// wedgedSweepEngine is a helper engine whose boot sweep fails the same way
// every time -- exactly the shape of #412, where one unreconcilable durable
// ownership record failed the startup sweep on every generation.
type wedgedSweepEngine struct {
	*fakeEngine
	fail    bool
	attempt atomic.Int64
	// clock and sweepDuration make a sweep take time on the injected clock.
	// A sweep that costs nothing hides every decision that compares the time a
	// failure is recorded against the time the attempt was scheduled for.
	clock         *manualClock
	sweepDuration time.Duration
}

func (engine *wedgedSweepEngine) Sweep(ctx context.Context, request SweepRequest) (SweepResponse, error) {
	engine.attempt.Add(1)
	if engine.clock != nil && engine.sweepDuration > 0 {
		engine.clock.Advance(engine.sweepDuration)
	}
	if engine.fail {
		return SweepResponse{}, errors.New("durable Attempt ownership record conflicts with fenced authority")
	}
	return engine.fakeEngine.Sweep(ctx, request)
}

// sweepAttempts counts every sweep the engine was asked for, denied ones
// included. The #419 loop is measured here: a tripped bound must add none.
func (engine *wedgedSweepEngine) sweepAttempts() int64 { return engine.attempt.Load() }

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

// testSweepDuration is what a real whole-namespace sweep costs before it is
// denied. The helper records the failure at the end of the sweep, not at the
// moment the attempt was scheduled for, and that difference decides whether a
// scheduled re-attempt reads as the same streak or a new one.
const testSweepDuration = 250 * time.Millisecond

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

// serveHelperGeneration starts one helper lifetime and leaves it running, the
// way a socket-activated relaunch leaves a live process holding the socket.
func serveHelperGeneration(t *testing.T, engine Engine, stateDirectory string, bound int, clock Clock) (*Client, *Server, func()) {
	t.Helper()
	// macOS caps a unix socket path at 104 bytes; t.TempDir() names are long
	// enough to blow through it.
	directory, err := os.MkdirTemp("", "wefty-oci-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "helper.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
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
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	select {
	case <-server.startupDone:
	case <-time.After(5 * time.Second):
		t.Fatal("helper startup never settled")
	}
	client := NewUnixClient(path, "checksum-test")
	client.disableHeartbeatPump = true
	return client, server, func() {
		cancel()
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("helper server did not stop")
		}
	}
}

// burnStartupBound drives the ledger to a tripped streak the way the unit
// does: repeated failing generations, the last one past both the count and the
// window, exiting with the status RestartPreventExitStatus names.
func burnStartupBound(t *testing.T, engine *wedgedSweepEngine, state string, bound int, clock *manualClock) {
	t.Helper()
	for generation := 1; generation < bound; generation++ {
		if err := runHelperGenerationAt(t, engine, state, bound, clock); err == nil {
			t.Fatalf("generation %d served despite a failed startup barrier", generation)
		}
		clock.Advance(time.Second)
	}
	// Land the last failure exactly one window after the one before it, so the
	// streak is both at the count and as old as the window without ever going
	// stale. The sweep itself costs time on this clock, and that time counts
	// against the gap.
	clock.Advance(testStartupFailureWindow - time.Second - engine.sweepDuration)
	err := runHelperGenerationAt(t, engine, state, bound, clock)
	var wedged *StartupWedgedError
	if !errors.As(err, &wedged) {
		t.Fatalf("the bound never tripped: %v", err)
	}
}

// readStartupFailureLedger returns the durable ledger a replacing generation
// would read.
func readStartupFailureLedger(t *testing.T, state string) startupFailureLedger {
	t.Helper()
	payload, err := os.ReadFile(filepath.Join(state, startupFailureLedgerName))
	if err != nil {
		t.Fatal(err)
	}
	var ledger startupFailureLedger
	if err := json.Unmarshal(payload, &ledger); err != nil {
		t.Fatal(err)
	}
	return ledger
}

// waitForSweepAttempts waits for the engine to reach exactly the expected
// number of sweep attempts, and fails if it overshoots.
func waitForSweepAttempts(t *testing.T, engine *wedgedSweepEngine, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := engine.sweepAttempts()
		if got == want {
			return
		}
		if got > want {
			t.Fatalf("sweep attempts = %d, want %d", got, want)
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("sweep attempts stalled at %d, want %d", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// requireNoFurtherSweep gives a re-arm that must not happen room to happen.
func requireNoFurtherSweep(t *testing.T, engine *wedgedSweepEngine, want int64) {
	t.Helper()
	time.Sleep(50 * time.Millisecond)
	if got := engine.sweepAttempts(); got != want {
		t.Fatalf("a sweep ran before its window elapsed: attempts = %d, want %d", got, want)
	}
}

// waitForRefusal waits until the server has published a tripped-bound refusal
// newer than the one already observed; nil accepts the first one.
//
// The sweep counter moves when a sweep starts, but a re-attempt only becomes a
// fact when its failure reaches the ledger, several steps later. Anything that
// depends on the recorded attempt -- the streak it grew, the schedule it wrote,
// what a replacing generation would read -- has to wait for the refusal that
// carries it, not for the sweep that preceded it.
func waitForRefusal(t *testing.T, server *Server, after *StartupBoundTrippedError) *StartupBoundTrippedError {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		server.sessionMu.Lock()
		startupErr := server.startupErr
		server.sessionMu.Unlock()
		var tripped *StartupBoundTrippedError
		if errors.As(startupErr, &tripped) && (after == nil || tripped.Facts.NextAttemptAt.After(after.Facts.NextAttemptAt)) {
			return tripped
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("the helper never published a tripped-bound refusal after %+v: %v", after, startupErr)
		}
		time.Sleep(time.Millisecond)
	}
}

// #419: RestartPreventExitStatus stops systemd's own restarts, but the helper
// is socket activated, so the agent's boot barrier relaunched a fresh
// generation on every connection -- 81 launches in 63 seconds on hardware,
// each one rerunning the denied whole-namespace sweep. A generation that
// inherits a fresh trip must sweep nothing, stay alive holding the socket so
// nothing else is activated, and answer every acquisition with the typed
// refusal.
func TestATrippedStartupBoundRefusesSocketActivatedRelaunchesWithoutSweeping(t *testing.T) {
	state := t.TempDir()
	engine := &wedgedSweepEngine{fakeEngine: newFakeEngine(), fail: true}
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	burnStartupBound(t, engine, state, 3, clock)
	sweepsAtTrip := engine.sweepAttempts()

	client, server, stop := serveHelperGeneration(t, engine, state, 3, clock)
	defer stop()

	if engine.sweepAttempts() != sweepsAtTrip {
		t.Fatalf("the relaunched generation reran the denied startup sweep: %d sweeps, want %d", engine.sweepAttempts(), sweepsAtTrip)
	}
	// Only a barrier that succeeds clears the ledger. A refusal that consumed
	// it would hand the bound to one process's memory, and on a native Linux
	// node nothing replaces that process.
	if _, err := os.Stat(filepath.Join(state, startupFailureLedgerName)); err != nil {
		t.Fatalf("the refusing generation consumed the tripped ledger: %v", err)
	}
	server.sessionMu.Lock()
	fatalErr := server.fatalErr
	startupErr := server.startupErr
	server.sessionMu.Unlock()
	if fatalErr != nil {
		t.Fatalf("the refusing generation failed its process instead of holding the socket: %v", fatalErr)
	}
	var tripped *StartupBoundTrippedError
	if !errors.As(startupErr, &tripped) || !tripped.Facts.Tripped || tripped.Facts.Bound != 3 ||
		tripped.Facts.Consecutive < 3 || tripped.Facts.Phase != StartupBarrierSweep || tripped.Facts.Elapsed < testStartupFailureWindow {
		t.Fatalf("startup error = %v, want a typed tripped-bound refusal naming the streak", startupErr)
	}
	if !tripped.Facts.NextAttemptAt.After(clock.Now()) {
		t.Fatalf("refusal = %+v, want a re-arm scheduled ahead of now", tripped.Facts)
	}

	// Two boot-barrier windows in a row. Each must be refused by the same live
	// process: a dial that still reaches a listener is the proof that nothing
	// exited and nothing was relaunched.
	for window := 1; window <= 2; window++ {
		barrier, err := NewBootBarrierWithConfig(client, testSessionRequest(), BootBarrierConfig{
			TakeoverTimeout: 2 * time.Second, TakeoverRetry: time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		ensureErr := barrier.Ensure(context.Background())
		if ensureErr == nil {
			t.Fatalf("window %d: the tripped helper admitted a session", window)
		}
		var rpcErr *RPCError
		if !errors.As(ensureErr, &rpcErr) || rpcErr.Code != CodeStartupBoundTripped {
			t.Fatalf("window %d: refusal = %v, want the typed %s code", window, ensureErr, CodeStartupBoundTripped)
		}
		// The repair path is the one a stalled handshake already drives; the
		// typed refusal reaches it at the first dial instead of after a whole
		// takeover window.
		var stalled *HelperHandshakeStalledError
		if !errors.As(ensureErr, &stalled) || stalled.DialAttempts != 1 {
			t.Fatalf("window %d: refusal = %v, want one dial reported as a stalled handshake", window, ensureErr)
		}
		if reason := barrier.CapabilityReasonCode(); reason != contract.CapabilityReasonHelperHandshakeStalled {
			t.Fatalf("window %d: capability reason = %q, want the unchanged bounded-repair reason", window, reason)
		}
		observation := barrier.StartupBound()
		if !observation.Facts.Tripped || observation.Facts.Bound != 3 || observation.Facts.Consecutive < 3 ||
			observation.Facts.Phase != StartupBarrierSweep || observation.ObservedAt.IsZero() {
			t.Fatalf("window %d: barrier bound observation = %+v, want the tripped bound the doctor names", window, observation)
		}
		if engine.sweepAttempts() != sweepsAtTrip {
			t.Fatalf("window %d: a refused acquisition ran a startup sweep: %d sweeps, want %d", window, engine.sweepAttempts(), sweepsAtTrip)
		}
	}
}

// The refusal must not be permanent. Nothing restarts the helper unit on a
// native Linux node -- there is no Lima repair there, and `wefty node oci
// start` reaches the same refusing process -- so the helper re-attempts its own
// barrier, at most once per window, for as long as it keeps failing.
func TestARefusingHelperReattemptsTheBarrierOncePerWindow(t *testing.T) {
	state := t.TempDir()
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	engine := &wedgedSweepEngine{fakeEngine: newFakeEngine(), fail: true, clock: clock, sweepDuration: testSweepDuration}
	burnStartupBound(t, engine, state, 2, clock)
	sweeps := engine.sweepAttempts()

	_, server, stop := serveHelperGeneration(t, engine, state, 2, clock)
	defer stop()
	first := waitForRefusal(t, server, nil)
	requireNoFurtherSweep(t, engine, sweeps)

	// Most of the window is not the window.
	clock.Advance(testStartupFailureWindow / 2)
	requireNoFurtherSweep(t, engine, sweeps)

	clock.Advance(testStartupFailureWindow / 2)
	sweeps++
	waitForSweepAttempts(t, engine, sweeps)
	requireNoFurtherSweep(t, engine, sweeps)
	second := waitForRefusal(t, server, first)
	if second.Facts.Consecutive <= first.Facts.Consecutive {
		t.Fatalf("a failed re-attempt did not extend the streak: %+v then %+v", first.Facts, second.Facts)
	}
	if !second.Facts.NextAttemptAt.After(first.Facts.NextAttemptAt) {
		t.Fatalf("a failed re-attempt did not schedule the next window: %+v then %+v", first.Facts, second.Facts)
	}
	// The durable record is what a replacing generation reads. A re-attempt is
	// scheduled a window out and the sweep costs more time on top, so the gap
	// that separates streaks always looks stale here: only an explicit
	// continuation keeps the trip on disk.
	ledger := readStartupFailureLedger(t, state)
	if !ledger.Tripped || ledger.Consecutive != second.Facts.Consecutive || ledger.Consecutive <= first.Facts.Consecutive {
		t.Fatalf("ledger after a failed re-attempt = %+v, want a still-tripped streak grown to %d", ledger, second.Facts.Consecutive)
	}

	clock.Advance(testStartupFailureWindow)
	sweeps++
	waitForSweepAttempts(t, engine, sweeps)
	requireNoFurtherSweep(t, engine, sweeps)
}

// A trip older than the window asks for its sweep at launch. It is still one
// sweep, and a denial that has not been fixed buys the next one a window later.
func TestAStaleTrippedLedgerSweepsOnceAtLaunchAndThenWaitsAWindow(t *testing.T) {
	state := t.TempDir()
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	engine := &wedgedSweepEngine{fakeEngine: newFakeEngine(), fail: true, clock: clock, sweepDuration: testSweepDuration}
	burnStartupBound(t, engine, state, 2, clock)
	sweeps := engine.sweepAttempts()

	clock.Advance(10 * testStartupFailureWindow)
	_, server, stop := serveHelperGeneration(t, engine, state, 2, clock)
	defer stop()
	sweeps++
	waitForSweepAttempts(t, engine, sweeps)
	waitForRefusal(t, server, nil)
	requireNoFurtherSweep(t, engine, sweeps)

	clock.Advance(testStartupFailureWindow)
	sweeps++
	waitForSweepAttempts(t, engine, sweeps)
	requireNoFurtherSweep(t, engine, sweeps)
}

// The bound clears on the path that repairs the helper: the denial goes away
// and the next re-attempt succeeds, with no restart at all.
func TestAReArmedBarrierThatSucceedsClearsTheBoundAndServes(t *testing.T) {
	state := t.TempDir()
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	engine := &wedgedSweepEngine{fakeEngine: newFakeEngine(), fail: true, clock: clock, sweepDuration: testSweepDuration}
	burnStartupBound(t, engine, state, 2, clock)
	sweeps := engine.sweepAttempts()

	client, server, stop := serveHelperGeneration(t, engine, state, 2, clock)
	defer stop()
	waitForRefusal(t, server, nil)

	// The operator clears the denial; nothing restarts the helper.
	engine.fail = false
	clock.Advance(testStartupFailureWindow)
	sweeps++
	waitForSweepAttempts(t, engine, sweeps)

	deadline := time.Now().Add(5 * time.Second)
	for {
		server.sessionMu.Lock()
		startupErr := server.startupErr
		server.sessionMu.Unlock()
		if startupErr == nil {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("a successful re-attempt did not lift the refusal: %v", startupErr)
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(state, startupFailureLedgerName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a successful re-attempt did not clear the ledger: %v", err)
	}
	barrier, err := NewBootBarrierWithConfig(client, testSessionRequest(), BootBarrierConfig{
		TakeoverTimeout: 2 * time.Second, TakeoverRetry: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := barrier.Ensure(context.Background()); err != nil {
		t.Fatalf("the recovered helper refused a session: %v", err)
	}
	defer barrier.Close()
	if observation := barrier.StartupBound(); observation.Facts.Tripped {
		t.Fatalf("a recovered helper still reported a tripped bound: %+v", observation)
	}
}

// A generation that starts long after a trip, against a denial that is already
// fixed, sweeps once and serves -- exactly the boot it would have had if the
// bound had never tripped.
func TestAStaleTrippedLedgerDoesNotRefuseAHealthyGeneration(t *testing.T) {
	state := t.TempDir()
	engine := &wedgedSweepEngine{fakeEngine: newFakeEngine(), fail: true}
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	burnStartupBound(t, engine, state, 2, clock)

	clock.Advance(10 * testStartupFailureWindow)
	engine.fail = false
	sweepsBefore := engine.sweepAttempts()
	if err := runHelperGenerationAt(t, engine, state, 2, clock); err != nil {
		t.Fatalf("a stale tripped ledger refused a healthy generation: %v", err)
	}
	if engine.sweepAttempts() != sweepsBefore+1 {
		t.Fatalf("a healthy generation ran %d sweeps, want exactly one", engine.sweepAttempts()-sweepsBefore)
	}
	if _, err := os.Stat(filepath.Join(state, startupFailureLedgerName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a verified barrier did not clear the tripped ledger: %v", err)
	}
}

// The refusal has to survive the process that serves it. A generation that
// replaces the refusing one -- crash, restart, reboot -- reads the same tripped
// ledger and refuses too. If a failed re-attempt had cleared the trip, this
// generation would run the ordinary barrier, fail its process, and hand the
// socket back to the #419 activation storm for a whole window.
func TestAGenerationThatReplacesARefusingOneStillRefuses(t *testing.T) {
	state := t.TempDir()
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	engine := &wedgedSweepEngine{fakeEngine: newFakeEngine(), fail: true, clock: clock, sweepDuration: testSweepDuration}
	burnStartupBound(t, engine, state, 2, clock)
	sweeps := engine.sweepAttempts()

	_, refusing, stopRefusing := serveHelperGeneration(t, engine, state, 2, clock)
	scheduled := waitForRefusal(t, refusing, nil)
	clock.Advance(testStartupFailureWindow)
	sweeps++
	waitForSweepAttempts(t, engine, sweeps)
	// Wait for the re-attempt's failure to reach the ledger, not merely for its
	// sweep to start: until it does, the durable schedule is still the one the
	// refusing generation inherited, and a replacement is genuinely entitled to
	// the attempt this one has not finished recording.
	waitForRefusal(t, refusing, scheduled)
	stopRefusing()

	// The replacement starts in the window the failed re-attempt just opened.
	_, replacement, stop := serveHelperGeneration(t, engine, state, 2, clock)
	defer stop()
	if engine.sweepAttempts() != sweeps {
		t.Fatalf("the replacing generation ran the ordinary barrier: %d sweeps, want %d", engine.sweepAttempts(), sweeps)
	}
	replacement.sessionMu.Lock()
	fatalErr := replacement.fatalErr
	startupErr := replacement.startupErr
	replacement.sessionMu.Unlock()
	if fatalErr != nil {
		t.Fatalf("the replacing generation failed its process: %v", fatalErr)
	}
	var tripped *StartupBoundTrippedError
	if !errors.As(startupErr, &tripped) {
		t.Fatalf("the replacing generation did not inherit the refusal: %v", startupErr)
	}
	if ledger := readStartupFailureLedger(t, state); !ledger.Tripped {
		t.Fatalf("ledger = %+v, want the trip to have survived the failed re-attempt", ledger)
	}
}

// The ledger carries absolute wall time. A clock stepped backwards -- a
// restored VM snapshot, an NTP correction -- must not buy the refusal that step
// plus a window, because the timer is monotonic from creation and a later
// correction cannot shorten it.
func TestABackwardClockStepCannotStretchTheReArmWindow(t *testing.T) {
	state := t.TempDir()
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	engine := &wedgedSweepEngine{fakeEngine: newFakeEngine(), fail: true, clock: clock, sweepDuration: testSweepDuration}
	burnStartupBound(t, engine, state, 2, clock)
	sweeps := engine.sweepAttempts()

	clock.Advance(-24 * time.Hour)
	steppedBack := clock.Now()
	_, server, stop := serveHelperGeneration(t, engine, state, 2, clock)
	defer stop()
	refusal := waitForRefusal(t, server, nil)
	if refusal.Facts.NextAttemptAt.After(steppedBack.Add(testStartupFailureWindow)) {
		t.Fatalf("re-arm scheduled at %s, want no later than one window after the stepped-back now %s",
			refusal.Facts.NextAttemptAt, steppedBack)
	}
	requireNoFurtherSweep(t, engine, sweeps)

	clock.Advance(testStartupFailureWindow)
	sweeps++
	waitForSweepAttempts(t, engine, sweeps)
}
