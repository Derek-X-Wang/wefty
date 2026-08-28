package lima

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

const MinimalDoctorFactsVersion = 1

type UnitState string

const (
	UnitStateUnmanaged      UnitState = "unmanaged"
	UnitStateLaunchedByUnit UnitState = "launched_by_unit"
)

func (state UnitState) Valid() bool {
	return state == UnitStateUnmanaged || state == UnitStateLaunchedByUnit
}

type HelperState string

const (
	HelperStateUnavailable HelperState = "unavailable"
	HelperStateReady       HelperState = "ready"
)

func (state HelperState) Valid() bool {
	return state == HelperStateUnavailable || state == HelperStateReady
}

type ProbeState string

const (
	ProbeStateNotRun ProbeState = "not_run"
	ProbeStateFailed ProbeState = "failed"
	ProbeStatePassed ProbeState = "passed"
)

func (state ProbeState) Valid() bool {
	return state == ProbeStateNotRun || state == ProbeStateFailed || state == ProbeStatePassed
}

type UnitFacts struct {
	Label string    `json:"label"`
	State UnitState `json:"state"`
}

type HelperFacts struct {
	State           HelperState `json:"state"`
	ProtocolVersion int         `json:"protocol_version"`
	Version         string      `json:"version,omitempty"`
	Checksum        string      `json:"checksum,omitempty"`
}

type ProbeFacts struct {
	State      ProbeState `json:"state"`
	ObservedAt *time.Time `json:"observed_at,omitempty"`
}

// MinimalDoctorFacts is the deliberately small #128 bootstrap surface. The
// later general doctor owns platform, cache, convergence, and human output.
type MinimalDoctorFacts struct {
	Version            int                           `json:"version"`
	ObservedAt         time.Time                     `json:"observed_at"`
	Unit               UnitFacts                     `json:"unit"`
	Lima               SupervisorFacts               `json:"lima"`
	Helper             HelperFacts                   `json:"helper"`
	Probe              ProbeFacts                    `json:"probe"`
	CapabilityRevision int64                         `json:"capability_revision"`
	ReasonCode         contract.CapabilityReasonCode `json:"reason_code,omitempty"`
}

func BuildMinimalDoctorFacts(unitState UnitState, lima SupervisorFacts, handshake *ocihelper.AcquireSessionResponse, observation contract.CapabilityObservation, lastProbe *contract.CapabilityObservation, now time.Time) MinimalDoctorFacts {
	observedAt := lima.ObservedAt
	if observation.ObservedAt.After(observedAt) {
		observedAt = observation.ObservedAt
	}
	if observedAt.IsZero() {
		observedAt = now
	}
	facts := MinimalDoctorFacts{
		Version: MinimalDoctorFactsVersion, ObservedAt: observedAt.UTC().Round(0),
		Unit: UnitFacts{Label: LaunchDaemonLabel, State: unitState}, Lima: lima,
		Helper: HelperFacts{State: HelperStateUnavailable}, Probe: ProbeFacts{State: ProbeStateNotRun},
		CapabilityRevision: observation.Revision, ReasonCode: observation.ReasonCode,
	}
	if lastProbe != nil && !lastProbe.ObservedAt.IsZero() {
		probeAt := lastProbe.ObservedAt.UTC().Round(0)
		facts.Probe.ObservedAt = &probeAt
		facts.Probe.State = ProbeStateFailed
		if lastProbe.Capabilities["kind:oci"] && lastProbe.ReasonCode == "" {
			facts.Probe.State = ProbeStatePassed
		}
	}
	if handshake != nil {
		facts.Helper = HelperFacts{
			State: HelperStateReady, ProtocolVersion: handshake.ProtocolVersion,
			Version: handshake.HelperVersion, Checksum: handshake.HelperChecksum,
		}
	}
	if !facts.ReasonCode.Valid() {
		facts.ReasonCode = lima.ReasonCode
	}
	return facts
}

// WriteMinimalDoctorFacts atomically replaces one operator-readable JSON
// snapshot. It never includes raw errors, command output, environment, or
// helper session capabilities.
func WriteMinimalDoctorFacts(path string, facts MinimalDoctorFacts) error {
	if !filepath.IsAbs(path) {
		return errors.New("minimal doctor facts path must be absolute")
	}
	if facts.Version != MinimalDoctorFactsVersion || facts.Unit.Label != LaunchDaemonLabel || !facts.Unit.State.Valid() ||
		!facts.Lima.State.Valid() || !facts.Helper.State.Valid() || !facts.Probe.State.Valid() ||
		!facts.ReasonCode.Valid() && facts.ReasonCode != "" {
		return errors.New("minimal doctor facts are invalid")
	}
	payload, err := json.MarshalIndent(facts, "", "  ")
	if err != nil {
		return fmt.Errorf("encode minimal doctor facts: %w", err)
	}
	payload = append(payload, '\n')
	if current, readErr := os.ReadFile(path); readErr == nil && bytes.Equal(current, payload) {
		if info, statErr := os.Stat(path); statErr == nil && info.Mode().Perm() == 0o600 {
			return nil
		}
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create minimal doctor directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".minimal-doctor-*")
	if err != nil {
		return fmt.Errorf("stage minimal doctor facts: %w", err)
	}
	temporaryPath := temporary.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(payload); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish minimal doctor facts: %w", err)
	}
	cleanup = false
	return nil
}
