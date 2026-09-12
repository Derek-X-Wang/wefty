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

// StartupBoundTrippedError is the refusal a generation that starts while the
// bound is already tripped returns instead of running the startup sweep again.
//
// RestartPreventExitStatus stops systemd's own restarts, but the helper is
// socket-activated: every fresh connection starts another generation, and on
// hardware that reran the denied sweep 81 times in 63 seconds (#419). A
// generation that finds the bound tripped therefore sweeps nothing, keeps
// serving, and answers every connection with this typed refusal -- so the
// socket has a live listener and systemd activates nothing further.
type StartupBoundTrippedError struct {
	Facts StartupBoundFacts
}

func (err *StartupBoundTrippedError) Error() string {
	return fmt.Sprintf("%s: OCI helper startup barrier already failed %d consecutive times over %s (bound %d, phase %s); this generation refuses to sweep and serves refusals until the helper is repaired",
		CodeStartupBoundTripped, err.Facts.Consecutive, err.Facts.Elapsed, err.Facts.Bound, err.Facts.Phase)
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
func (server *Server) recordStartupBarrierFailure(err error) error {
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
		// a stale one left behind by an unrelated incident.
		if json.Unmarshal(payload, &existing) == nil && existing.Version == startupFailureLedgerVersion &&
			existing.Consecutive > 0 && !existing.FirstAt.IsZero() && now.Sub(existing.UpdatedAt) <= window {
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
	// without seeing the verdict.
	ledger.Tripped = wedged
	if writeErr := writeStartupFailureLedger(path, ledger); writeErr != nil {
		server.config.Logf("OCI helper startup barrier ledger unwritable; the restart bound cannot be enforced this generation")
		return err
	}
	server.config.Logf("OCI helper startup barrier failed phase=%s consecutive=%d bound=%d elapsed=%s window=%s",
		barrier.Phase, ledger.Consecutive, bound, elapsed, window)
	if !wedged {
		return err
	}
	return &StartupWedgedError{Phase: barrier.Phase, Consecutive: ledger.Consecutive, Bound: bound, Elapsed: elapsed, Err: err}
}

// startupBoundTripped reports the already-tripped bound this generation must
// honour without sweeping, or nil to run the ordinary startup barrier.
//
// The ledger must carry the trip the previous generation declared, and the
// streak must still be live: a last update older than the window is a stale
// wedge from an earlier incident, not today's. An unreadable or unparseable
// ledger is never converted into a refusal to serve, for the same reason an
// unwritable one never manufactures a wedge.
//
// A tripped ledger is consumed. This generation now holds the bound in memory
// for as long as it lives, and it lives until something restarts the unit --
// which socket activation cannot do while the listener is held, so only a
// repair can. Leaving the file behind would make that repair's generation
// refuse too.
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
	if json.Unmarshal(payload, &ledger) != nil || ledger.Version != startupFailureLedgerVersion || !ledger.Tripped {
		return nil
	}
	now := server.config.Clock.Now().UTC()
	window := server.startupFailureWindow()
	if ledger.UpdatedAt.IsZero() || now.Sub(ledger.UpdatedAt) > window {
		return nil
	}
	if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		server.config.Logf("OCI helper startup barrier ledger could not be consumed by the refusing generation")
	}
	return &StartupBoundTrippedError{Facts: StartupBoundFacts{
		Tripped: true, Phase: ledger.Phase, Consecutive: ledger.Consecutive,
		Bound: server.startupFailureBound(), Elapsed: ledger.UpdatedAt.Sub(ledger.FirstAt),
	}}
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
