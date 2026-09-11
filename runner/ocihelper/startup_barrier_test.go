package ocihelper

import (
	"context"
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

// The #412 unit restarted 136 times in two minutes because a deterministic
// startup-sweep failure had no count bound. It must now surface as a failed
// unit with a typed reason after a bounded number of restarts.
func TestDeterministicStartupBarrierFailureWedgesWithinABoundedRestartCount(t *testing.T) {
	state := t.TempDir()
	engine := &wedgedSweepEngine{fakeEngine: newFakeEngine(), fail: true}

	for generation := 1; generation < 3; generation++ {
		err := runHelperGeneration(t, engine, state, 3)
		if err == nil {
			t.Fatalf("generation %d served despite a failed startup barrier", generation)
		}
		var wedged *StartupWedgedError
		if errors.As(err, &wedged) {
			t.Fatalf("generation %d wedged before the bound: %v", generation, err)
		}
	}

	err := runHelperGeneration(t, engine, state, 3)
	var wedged *StartupWedgedError
	if !errors.As(err, &wedged) {
		t.Fatalf("the helper kept restarting past its bound: %v", err)
	}
	if wedged.Consecutive != 3 || wedged.Bound != 3 || wedged.Phase != StartupBarrierSweep {
		t.Fatalf("wedge = %+v, want three consecutive startup_sweep failures", wedged)
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
	if err := runHelperGeneration(t, engine, state, 2); err == nil {
		t.Fatal("a failed startup barrier served anyway")
	}

	engine.fail = false
	if err := runHelperGeneration(t, engine, state, 2); err != nil {
		t.Fatalf("a healthy startup barrier failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(state, startupFailureLedgerName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a verified barrier did not clear the failure ledger: %v", err)
	}

	engine.fail = true
	err := runHelperGeneration(t, engine, state, 2)
	var wedged *StartupWedgedError
	if errors.As(err, &wedged) {
		t.Fatalf("the first failure after a healthy barrier wedged immediately: %v", err)
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
	for generation := 0; generation < 4; generation++ {
		err := runHelperGeneration(t, engine, state, 2)
		var wedged *StartupWedgedError
		if err == nil || errors.As(err, &wedged) {
			t.Fatalf("generation %d with an unusable ledger = %v, want the ordinary barrier failure", generation, err)
		}
	}
}

// With no state directory configured the bound is simply off; nothing panics
// and nothing wedges.
func TestNoConfiguredStateDirectoryDisablesTheBound(t *testing.T) {
	engine := &wedgedSweepEngine{fakeEngine: newFakeEngine(), fail: true}
	err := runHelperGeneration(t, engine, "", 2)
	var wedged *StartupWedgedError
	if err == nil || errors.As(err, &wedged) {
		t.Fatalf("unconfigured bound = %v, want the ordinary barrier failure", err)
	}
}
