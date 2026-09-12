package ocihelper

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/systemdpolicy"
)

// StartupBarrierPhase names which half of the startup barrier failed. Both are
// closed values; neither carries privileged error text.
type StartupBarrierPhase string

const (
	StartupBarrierSweep  StartupBarrierPhase = "startup_sweep"
	StartupBarrierVerify StartupBarrierPhase = "startup_verify"
)

// StartupBarrierError marks a failure of the helper's boot Sweep+Verify
// barrier, as opposed to any later runtime failure. Only a barrier failure is
// counted against the wedge bound: a helper that reached ready and then crashed
// must keep restarting.
type StartupBarrierError struct {
	Phase StartupBarrierPhase
	Err   error
}

func (err *StartupBarrierError) Error() string { return err.Err.Error() }
func (err *StartupBarrierError) Unwrap() error { return err.Err }

// StartupWedgedError is returned once the helper has failed its startup barrier
// StartupFailureBound times in a row. Its exit status is named by the unit's
// RestartPreventExitStatus, so systemd stops the restart loop and leaves a
// failed unit carrying this typed reason.
type StartupWedgedError struct {
	Phase       StartupBarrierPhase
	Consecutive int
	Bound       int
	Elapsed     time.Duration
	Err         error
}

func (err *StartupWedgedError) Error() string {
	return fmt.Sprintf("OCI helper startup barrier failed %d consecutive times over %s (bound %d, phase %s); refusing to restart: %v",
		err.Consecutive, err.Elapsed, err.Bound, err.Phase, err.Err)
}

func (err *StartupWedgedError) Unwrap() error { return err.Err }

// ExitStatus is the distinguished process exit status the installed unit names
// in RestartPreventExitStatus.
func (err *StartupWedgedError) ExitStatus() int { return systemdpolicy.StartupWedgedExitStatus }

// StartupBoundTrippedError is the refusal a generation serves while the bound
// is tripped, in place of the startup sweep it did not run.
//
// RestartPreventExitStatus stops systemd's own restarts, but the helper is
// socket-activated: every fresh connection starts another generation, and on
// hardware that reran the denied sweep 81 times in 63 seconds (#419). A
// generation that inherits a tripped bound therefore keeps serving and refuses
// -- the live listener is what keeps systemd from activating another one --
// and re-attempts the barrier at most once per startup-failure window. One
// sweep per window per node is the whole post-trip budget, and a node whose
// denial is cleared recovers on its own, which matters most where nothing
// restarts the unit for us.
type StartupBoundTrippedError struct {
	Facts StartupBoundFacts
}

func (err *StartupBoundTrippedError) Error() string {
	return fmt.Sprintf("%s: OCI helper startup barrier failed %d consecutive times over %s (bound %d, phase %s); refusing until a barrier succeeds, next attempt at %s",
		CodeStartupBoundTripped, err.Facts.Consecutive, err.Facts.Elapsed, err.Facts.Bound, err.Facts.Phase, err.Facts.NextAttemptAt.Format(time.RFC3339))
}

// Code is the typed protocol refusal this failure becomes on the wire.
func (err *StartupBoundTrippedError) Code() ErrorCode { return CodeStartupBoundTripped }

const startupFailureLedgerVersion = 1

const startupFailureLedgerName = "startup-barrier-failures.json"

type startupFailureLedger struct {
	Version     int                 `json:"version"`
	Consecutive int                 `json:"consecutive"`
	Phase       StartupBarrierPhase `json:"phase"`
	// FirstAt anchors the streak. The wedge needs both a count and elapsed
	// time, so a short transient cannot burn the whole bound in one breath.
	FirstAt   time.Time `json:"first_at"`
	UpdatedAt time.Time `json:"updated_at"`
	// Tripped is set by the generation that declared the wedge and exited 78.
	// It is what a later generation reads to know it must not sweep: the count
	// and the window alone cannot tell the tripping generation apart from the
	// ones that follow it, and the tripping generation must still wedge so the
	// unit lands failed with its typed journal reason.
	Tripped bool `json:"tripped"`
}

// recordStartupBarrierFailure counts one startup-barrier failure and reports
// the wedge error once the streak has both reached the bound and lasted the
// window. A ledger that cannot be read or written is not converted into a
// wedge: the helper reports the ordinary failure and restarts, because losing
// the count must never manufacture a refusal to serve.
//
// A rearmed failure is one the refusing generation went looking for, a window
// after the last one. Its gap is therefore always at least the window, so the
// elapsed-gap test that separates a streak from an unrelated old incident
// would read every scheduled re-attempt as a brand new streak and clear the
// trip -- which would let the next generation that replaces this process run
// the ordinary barrier, fail its process, and restart the #419 storm. A
// re-attempt continues the streak it was scheduled by, and keeps it tripped.
func (server *Server) recordStartupBarrierFailure(err error, rearmed bool) error {
	var barrier *StartupBarrierError
	if !errors.As(err, &barrier) || server.config.StartupFailureStateDirectory == "" {
		return err
	}
	path := filepath.Join(server.config.StartupFailureStateDirectory, startupFailureLedgerName)
	now := server.config.Clock.Now().UTC()
	window := server.startupFailureWindow()
	ledger := startupFailureLedger{Version: startupFailureLedgerVersion, FirstAt: now}
	if payload, readErr := os.ReadFile(path); readErr == nil {
		var existing startupFailureLedger
		// A gap longer than the window is a new streak, not a continuation of
		// a stale one left behind by an unrelated incident -- unless this
		// failure is the scheduled re-attempt of that very streak.
		if json.Unmarshal(payload, &existing) == nil && existing.Version == startupFailureLedgerVersion &&
			existing.Consecutive > 0 && !existing.FirstAt.IsZero() && (rearmed || now.Sub(existing.UpdatedAt) <= window) {
			ledger.Consecutive = existing.Consecutive
			ledger.FirstAt = existing.FirstAt
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		server.config.Logf("OCI helper startup barrier ledger unreadable; the restart bound cannot be enforced this generation")
		return err
	}
	ledger.Consecutive++
	ledger.Phase = barrier.Phase
	ledger.UpdatedAt = now
	bound := server.startupFailureBound()
	elapsed := now.Sub(ledger.FirstAt)
	wedged := ledger.Consecutive >= bound && elapsed >= window
	// The trip is recorded in the same fsynced write as the failure that
	// caused it, so the generation that starts next cannot see the count
	// without seeing the verdict. A re-attempt that failed keeps the trip
	// whatever the arithmetic says: the bound was already spent, and the
	// durable record has to say so for a replacing generation to honour it.
	ledger.Tripped = wedged || rearmed
	if writeErr := writeStartupFailureLedger(path, ledger); writeErr != nil {
		server.config.Logf("OCI helper startup barrier ledger unwritable; the restart bound cannot be enforced this generation")
		return err
	}
	server.config.Logf("OCI helper startup barrier failed phase=%s consecutive=%d bound=%d elapsed=%s window=%s",
		barrier.Phase, ledger.Consecutive, bound, elapsed, window)
	if !wedged && !rearmed {
		return err
	}
	return &StartupWedgedError{Phase: barrier.Phase, Consecutive: ledger.Consecutive, Bound: bound, Elapsed: elapsed, Err: err}
}

// startupBoundTripped reports the tripped bound this generation inherits, or
// nil to run the ordinary startup barrier.
//
// The ledger is never consumed here: only a barrier that succeeds clears it.
// Consuming it would hand the bound to one process's memory, and on a native
// Linux node nothing restarts that process -- no Lima repair exists there and
// `wefty node oci start` reaches the same refusing helper -- so a node whose
// denial had been cleared would stay refused until a human restarted the unit.
// The next attempt is therefore scheduled from the last failure, and a ledger
// whose window has already elapsed asks for a sweep immediately.
//
// An unreadable or unparseable ledger is never converted into a refusal to
// serve, for the same reason an unwritable one never manufactures a wedge.
func (server *Server) startupBoundTripped() *StartupBoundTrippedError {
	if server.config.StartupFailureStateDirectory == "" {
		return nil
	}
	path := filepath.Join(server.config.StartupFailureStateDirectory, startupFailureLedgerName)
	payload, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var ledger startupFailureLedger
	if json.Unmarshal(payload, &ledger) != nil || ledger.Version != startupFailureLedgerVersion ||
		!ledger.Tripped || ledger.UpdatedAt.IsZero() {
		return nil
	}
	// The schedule is clamped to one window from now. The ledger carries
	// absolute wall time, so a clock stepped backwards -- a restored VM
	// snapshot, an NTP correction -- would otherwise refuse for that step plus
	// a window, and the timer, monotonic from creation, would not shorten when
	// the clock was corrected again.
	window := server.startupFailureWindow()
	now := server.config.Clock.Now().UTC()
	next := ledger.UpdatedAt.Add(window)
	if clamped := now.Add(window); clamped.Before(next) {
		next = clamped
	}
	return &StartupBoundTrippedError{Facts: StartupBoundFacts{
		Tripped: true, Phase: ledger.Phase, Consecutive: ledger.Consecutive,
		Bound: server.startupFailureBound(), Elapsed: ledger.UpdatedAt.Sub(ledger.FirstAt),
		NextAttemptAt: next,
	}}
}

// refusalAfterTrippedBarrier records a re-armed attempt that failed again and
// returns the refusal to publish until the next window. The count keeps growing
// and the ledger stays tripped, so a generation that is replaced mid-refusal
// inherits the same one-sweep-per-window budget instead of starting over.
func (server *Server) refusalAfterTrippedBarrier(previous StartupBoundFacts, err error) *StartupBoundTrippedError {
	facts := previous
	var wedged *StartupWedgedError
	if errors.As(server.recordStartupBarrierFailure(err, true), &wedged) {
		facts.Phase, facts.Bound, facts.Elapsed = wedged.Phase, wedged.Bound, wedged.Elapsed
		// The ledger is the durable authority, but an operator who deleted it
		// mid-refusal must not make the reported streak shrink below what this
		// process has actually watched fail.
		facts.Consecutive = max(facts.Consecutive, wedged.Consecutive)
	}
	// An unusable ledger loses the count, never the refusal or the re-arm: the
	// helper must not relaunch-loop, and it must not refuse forever either.
	facts.Tripped = true
	facts.NextAttemptAt = server.config.Clock.Now().UTC().Add(server.startupFailureWindow())
	return &StartupBoundTrippedError{Facts: facts}
}

// clearStartupBarrierFailures resets the count after a barrier that succeeded.
func (server *Server) clearStartupBarrierFailures() {
	if server.config.StartupFailureStateDirectory == "" {
		return
	}
	path := filepath.Join(server.config.StartupFailureStateDirectory, startupFailureLedgerName)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		server.config.Logf("OCI helper startup barrier ledger could not be cleared after a verified barrier")
	}
}

func (server *Server) startupFailureBound() int {
	if server.config.StartupFailureBound > 0 {
		return server.config.StartupFailureBound
	}
	return systemdpolicy.StartupFailureBound
}

func (server *Server) startupFailureWindow() time.Duration {
	if server.config.StartupFailureWindow > 0 {
		return server.config.StartupFailureWindow
	}
	return systemdpolicy.StartupFailureWindow
}

func writeStartupFailureLedger(path string, ledger startupFailureLedger) error {
	root := filepath.Dir(path)
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	payload, err := json.Marshal(ledger)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(root, ".startup-barrier.tmp-")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	writeErr := temporary.Chmod(0o600)
	if writeErr == nil {
		_, writeErr = temporary.Write(append(payload, '\n'))
	}
	if writeErr == nil {
		writeErr = temporary.Sync()
	}
	if writeErr = errors.Join(writeErr, temporary.Close()); writeErr != nil {
		return writeErr
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return err
	}
	directory, err := os.Open(root)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
