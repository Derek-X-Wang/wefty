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
	Err         error
}

func (err *StartupWedgedError) Error() string {
	return fmt.Sprintf("OCI helper startup barrier failed %d consecutive times (bound %d, phase %s); refusing to restart: %v",
		err.Consecutive, err.Bound, err.Phase, err.Err)
}

func (err *StartupWedgedError) Unwrap() error { return err.Err }

// ExitStatus is the distinguished process exit status the installed unit names
// in RestartPreventExitStatus.
func (err *StartupWedgedError) ExitStatus() int { return systemdpolicy.StartupWedgedExitStatus }

const startupFailureLedgerVersion = 1

const startupFailureLedgerName = "startup-barrier-failures.json"

type startupFailureLedger struct {
	Version     int                 `json:"version"`
	Consecutive int                 `json:"consecutive"`
	Phase       StartupBarrierPhase `json:"phase"`
	UpdatedAt   time.Time           `json:"updated_at"`
}

// recordStartupBarrierFailure counts one consecutive startup-barrier failure
// and reports the wedge error once the bound is reached. A ledger that cannot
// be read or written is not converted into a wedge: the helper reports the
// ordinary failure and restarts, because losing the count must never
// manufacture a refusal to serve.
func (server *Server) recordStartupBarrierFailure(err error) error {
	var barrier *StartupBarrierError
	if !errors.As(err, &barrier) || server.config.StartupFailureStateDirectory == "" {
		return err
	}
	path := filepath.Join(server.config.StartupFailureStateDirectory, startupFailureLedgerName)
	ledger := startupFailureLedger{Version: startupFailureLedgerVersion}
	if payload, readErr := os.ReadFile(path); readErr == nil {
		var existing startupFailureLedger
		if json.Unmarshal(payload, &existing) == nil && existing.Version == startupFailureLedgerVersion && existing.Consecutive > 0 {
			ledger.Consecutive = existing.Consecutive
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		server.config.Logf("OCI helper startup barrier ledger unreadable; the restart bound cannot be enforced this generation")
		return err
	}
	ledger.Consecutive++
	ledger.Phase = barrier.Phase
	ledger.UpdatedAt = server.config.Clock.Now().UTC()
	if writeErr := writeStartupFailureLedger(path, ledger); writeErr != nil {
		server.config.Logf("OCI helper startup barrier ledger unwritable; the restart bound cannot be enforced this generation")
		return err
	}
	bound := server.startupFailureBound()
	server.config.Logf("OCI helper startup barrier failed phase=%s consecutive=%d bound=%d", barrier.Phase, ledger.Consecutive, bound)
	if ledger.Consecutive < bound {
		return err
	}
	return &StartupWedgedError{Phase: barrier.Phase, Consecutive: ledger.Consecutive, Bound: bound, Err: err}
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
