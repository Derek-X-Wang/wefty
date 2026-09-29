package ocihelper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// errChildTerminated is what the reap saw in the Linux realtiming lane (#579):
// `systemctl stop` signals every process in the unit's control group, so the
// firewall-inventory child died under the reap that was already running.
var errChildTerminated = errors.New("inventory Computer firewall rules: signal: terminated")

// stopInterruptibleEngine parks the session reap and the boot sweep until the
// test releases them, then fails them with the error a stop signal leaves
// behind. The test decides whether a stop was requested before the failure.
type stopInterruptibleEngine struct {
	*fakeEngine
	reapEntered  chan struct{}
	releaseReap  chan struct{}
	sweepEntered chan struct{}
	releaseSweep chan struct{}
	failSweep    bool
}

func (engine *stopInterruptibleEngine) ReapSession(ctx context.Context, identity SessionIdentity) (SweepResponse, error) {
	if engine.reapEntered == nil {
		return engine.fakeEngine.ReapSession(ctx, identity)
	}
	close(engine.reapEntered)
	<-engine.releaseReap
	return SweepResponse{}, errChildTerminated
}

func (engine *stopInterruptibleEngine) Sweep(ctx context.Context, request SweepRequest) (SweepResponse, error) {
	if !engine.failSweep {
		return engine.fakeEngine.Sweep(ctx, request)
	}
	close(engine.sweepEntered)
	<-engine.releaseSweep
	return SweepResponse{}, errChildTerminated
}

type servedHelper struct {
	client *Client
	cancel context.CancelFunc
	done   chan error
	logMu  sync.Mutex
	logs   []string
}

func serveInterruptibleHelper(t *testing.T, engine Engine, stateDirectory string) *servedHelper {
	t.Helper()
	// macOS caps a unix socket path at 104 bytes.
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
	served := &servedHelper{done: make(chan error, 1)}
	server, err := NewServer(engine, ServerConfig{
		HelperChecksum: "checksum-test", AllowedUIDs: []uint32{uint32(os.Getuid())},
		HeartbeatTimeout: time.Minute, StartupFailureStateDirectory: stateDirectory,
		Logf: func(format string, args ...any) {
			served.logMu.Lock()
			defer served.logMu.Unlock()
			served.logs = append(served.logs, fmt.Sprintf(format, args...))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served.cancel = cancel
	go func() { served.done <- server.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
	})
	served.client = NewUnixClient(path, "checksum-test")
	served.client.disableHeartbeatPump = true
	return served
}

func (served *servedHelper) serveResult(t *testing.T) error {
	t.Helper()
	select {
	case err := <-served.done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return")
		return nil
	}
}

func (served *servedHelper) logged(fragment string) bool {
	served.logMu.Lock()
	defer served.logMu.Unlock()
	for _, line := range served.logs {
		if strings.Contains(line, fragment) {
			return true
		}
	}
	return false
}

func waitClosed(t *testing.T, channel <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s never happened", description)
	}
}

// A session reap that the stop signal interrupts is not a helper failure: Serve
// returns nil so the process exits 0 and systemd records a clean stop, not a
// failed unit (#579). The next helper start re-runs the boot Sweep+Verify
// barrier, so nothing the reap left behind can be admitted as absent.
func TestSessionReapInterruptedByHelperShutdownIsACleanStop(t *testing.T) {
	engine := &stopInterruptibleEngine{fakeEngine: newFakeEngine(), reapEntered: make(chan struct{}), releaseReap: make(chan struct{})}
	served := serveInterruptibleHelper(t, engine, "")
	session, err := served.client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	_ = session.Close()
	waitClosed(t, engine.reapEntered, "the session-control-EOF reap")

	served.cancel()
	close(engine.releaseReap)

	if err := served.serveResult(t); err != nil {
		t.Fatalf("a stop during an in-flight session reap failed the helper: %v", err)
	}
	if !served.logged(`OCI helper session reap interrupted by helper shutdown reason="session control EOF"`) {
		t.Fatalf("the interrupted reap was not logged as interrupted: %q", served.logs)
	}
	if !served.logged("signal: terminated") {
		t.Fatalf("the interrupted reap dropped its cause: %q", served.logs)
	}
	if served.logged("OCI helper session reap failed") {
		t.Fatalf("an interrupted reap was logged as a failure: %q", served.logs)
	}
}

// The same child failure with no stop requested is a real reap failure: the
// helper stops admitting sessions and exits non-zero for a fresh process.
func TestSessionReapFailureOutsideShutdownStillFailsTheHelper(t *testing.T) {
	engine := &stopInterruptibleEngine{fakeEngine: newFakeEngine(), reapEntered: make(chan struct{}), releaseReap: make(chan struct{})}
	served := serveInterruptibleHelper(t, engine, "")
	session, err := served.client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	_ = session.Close()
	waitClosed(t, engine.reapEntered, "the session-control-EOF reap")

	close(engine.releaseReap)

	err = served.serveResult(t)
	if err == nil || !strings.Contains(err.Error(), "reap OCI helper session after session control EOF") || !errors.Is(err, errChildTerminated) {
		t.Fatalf("a reap failure outside shutdown returned %v, want the fatal reap error", err)
	}
	if !served.logged("OCI helper session reap failed") || served.logged("interrupted by helper shutdown") {
		t.Fatalf("a real reap failure was not logged as a failure: %q", served.logs)
	}
}

// A boot sweep the stop signal interrupts is neither fatal nor a strike against
// the startup-failure bound: a deliberate stop is not the restart loop the
// bound exists to end, and counting it could wedge a healthy node.
func TestStartupBarrierInterruptedByHelperShutdownIsNotCountedOrFatal(t *testing.T) {
	for _, stop := range []bool{true, false} {
		t.Run(fmt.Sprintf("stop_requested=%t", stop), func(t *testing.T) {
			state := t.TempDir()
			engine := &stopInterruptibleEngine{fakeEngine: newFakeEngine(), failSweep: true, sweepEntered: make(chan struct{}), releaseSweep: make(chan struct{})}
			served := serveInterruptibleHelper(t, engine, state)
			waitClosed(t, engine.sweepEntered, "the boot sweep")
			if stop {
				served.cancel()
			}
			close(engine.releaseSweep)

			err := served.serveResult(t)
			_, ledgerErr := os.Stat(filepath.Join(state, startupFailureLedgerName))
			if stop {
				if err != nil {
					t.Fatalf("a stop during the boot sweep failed the helper: %v", err)
				}
				if !errors.Is(ledgerErr, os.ErrNotExist) {
					t.Fatalf("an interrupted boot sweep was counted toward the restart bound: %v", ledgerErr)
				}
				if !served.logged("OCI helper startup barrier interrupted by helper shutdown phase=startup_sweep") || served.logged("OCI helper startup barrier failed") {
					t.Fatalf("the interrupted boot sweep was not logged as interrupted: %q", served.logs)
				}
				return
			}
			var barrier *StartupBarrierError
			if !errors.As(err, &barrier) || !errors.Is(err, errChildTerminated) {
				t.Fatalf("a boot sweep failure outside shutdown returned %v, want the barrier error", err)
			}
			if ledgerErr != nil {
				t.Fatalf("a real boot sweep failure was not counted: %v", ledgerErr)
			}
		})
	}
}

// A generation serving a tripped bound re-attempts the barrier once per window.
// A stop that interrupts that re-attempt must not extend the streak either.
func TestTrippedBarrierReattemptInterruptedByHelperShutdownKeepsTheLedger(t *testing.T) {
	state := t.TempDir()
	path := filepath.Join(state, startupFailureLedgerName)
	past := time.Now().UTC().Add(-time.Hour)
	if err := writeStartupFailureLedger(path, startupFailureLedger{
		Version: startupFailureLedgerVersion, Consecutive: 5, Phase: StartupBarrierSweep,
		FirstAt: past, UpdatedAt: past, Tripped: true,
	}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	engine := &stopInterruptibleEngine{fakeEngine: newFakeEngine(), failSweep: true, sweepEntered: make(chan struct{}), releaseSweep: make(chan struct{})}
	served := serveInterruptibleHelper(t, engine, state)
	waitClosed(t, engine.sweepEntered, "the re-armed boot sweep")
	served.cancel()
	close(engine.releaseSweep)

	if err := served.serveResult(t); err != nil {
		t.Fatalf("a stop during the re-armed barrier failed the helper: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var ledger startupFailureLedger
	if err := json.Unmarshal(after, &ledger); err != nil || string(after) != string(before) {
		t.Fatalf("an interrupted re-attempt rewrote the ledger:\nbefore %s\nafter  %s", before, after)
	}
	if !served.logged("OCI helper startup barrier interrupted by helper shutdown") {
		t.Fatalf("the interrupted re-attempt was not logged as interrupted: %q", served.logs)
	}
}
