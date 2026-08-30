package agent

import (
	"context"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

func TestStopOCIRuntimeSuppressesAndJoinsOnlyOCIResidents(t *testing.T) {
	capabilities := newCapabilityState(map[string]bool{
		"kind:process": true, "kind:oci": true,
	}, nil, systemClock{}, time.Second)
	session := newAgentSession(nil, contract.NodeRegistration{}, capabilities, time.Second, time.Second, systemClock{}, newLifecycleObserver(systemClock{}), nil, 1, 1)
	ociContext, cancelOCI := context.WithCancelCause(context.Background())
	processContext, cancelProcess := context.WithCancelCause(context.Background())
	defer cancelOCI(nil)
	defer cancelProcess(nil)
	ociDone := make(chan struct{})
	ociReaped := make(chan runtimeReapOutcome, 1)
	go func() {
		<-ociContext.Done()
		ociReaped <- runtimeReapOutcome{receipt: workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceOCIRuntimeSweep}}
		close(ociDone)
	}()
	session.resident["oci-job"] = &residentAttempt{kind: contract.JobKindOCI, class: contract.JobClassService, cancel: cancelOCI, done: ociDone, runtimeReaped: ociReaped}
	session.resident["process-job"] = &residentAttempt{kind: contract.JobKindProcess, class: contract.JobClassService, cancel: cancelProcess, done: make(chan struct{})}
	session.residentKind["oci-job"] = contract.JobKindOCI
	session.residentJobID["oci-job"] = struct{}{}
	if err := session.stopOCIRuntime(t.Context()); err != nil {
		t.Fatal(err)
	}
	if context.Cause(ociContext) != errOCIIntentDisabled {
		t.Fatalf("OCI cancel cause=%v", context.Cause(ociContext))
	}
	if processContext.Err() != nil {
		t.Fatalf("process resident was canceled: %v", context.Cause(processContext))
	}
	snapshot := capabilities.capabilitySnapshot()
	if snapshot.Capabilities[contract.JobKindOCI] || snapshot.Capabilities["kind:oci"] ||
		snapshot.ReasonCode != contract.CapabilityReasonOCIIntentDisabled {
		t.Fatalf("restrictive snapshot=%+v", snapshot)
	}
}

func TestOCIIntentStopPersistsAcrossSuccessfulProbesUntilOperatorRecovery(t *testing.T) {
	probe := capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
		return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil
	})
	capabilities := newCapabilityState(map[string]bool{"kind:process": true, "kind:oci": true}, probe, systemClock{}, time.Second)
	if err := capabilities.refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	session := newAgentSession(nil, contract.NodeRegistration{}, capabilities, time.Second, time.Second, systemClock{}, newLifecycleObserver(systemClock{}), nil, 1, 1)
	if err := session.stopOCIRuntime(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := capabilities.refresh(t.Context()); err != errOCIIntentDisabled {
		t.Fatalf("automatic probe after intent stop error=%v, want %v", err, errOCIIntentDisabled)
	}
	stopped := capabilities.capabilitySnapshot()
	if stopped.Capabilities["kind:oci"] || stopped.ReasonCode != contract.CapabilityReasonOCIIntentDisabled {
		t.Fatalf("automatic probe released operator intent stop: %+v", stopped)
	}

	capabilities.clearOCIIntent()
	if err := capabilities.refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	if recovered := capabilities.capabilitySnapshot(); !recovered.Capabilities["kind:oci"] {
		t.Fatalf("operator recovery did not release intent stop: %+v", recovered)
	}
}

func TestStopOCIRuntimeRejectsMissingReapProof(t *testing.T) {
	capabilities := newCapabilityState(map[string]bool{"kind:process": true, "kind:oci": true}, nil, systemClock{}, time.Second)
	session := newAgentSession(nil, contract.NodeRegistration{}, capabilities, time.Second, time.Second, systemClock{}, newLifecycleObserver(systemClock{}), nil, 1, 1)
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan struct{})
	go func() { <-ctx.Done(); close(done) }()
	session.resident["oci-job"] = &residentAttempt{kind: contract.JobKindOCI, cancel: cancel, done: done, runtimeReaped: make(chan runtimeReapOutcome, 1)}
	session.residentKind["oci-job"] = contract.JobKindOCI
	session.residentJobID["oci-job"] = struct{}{}
	if err := session.stopOCIRuntime(t.Context()); err == nil {
		t.Fatal("stop reported quiescence without a positive reap receipt")
	}
}

func TestStopOCIRuntimeJoinsClaimBeforeResidentPublication(t *testing.T) {
	capabilities := newCapabilityState(map[string]bool{"kind:process": true, "kind:oci": true}, nil, systemClock{}, time.Second)
	session := newAgentSession(nil, contract.NodeRegistration{}, capabilities, time.Second, time.Second, systemClock{}, newLifecycleObserver(systemClock{}), nil, 1, 1)
	session.residentKind["just-claimed"] = contract.JobKindOCI
	session.residentJobID["just-claimed"] = struct{}{}
	started := make(chan struct{})
	go func() {
		session.claimMu.Lock()
		ctx, cancel := context.WithCancelCause(context.Background())
		done := make(chan struct{})
		reaped := make(chan runtimeReapOutcome, 1)
		resident := &residentAttempt{kind: contract.JobKindOCI, cancel: cancel, done: done, runtimeReaped: reaped}
		session.resident["just-claimed"] = resident
		session.notifyResidentChangedLocked()
		session.claimMu.Unlock()
		close(started)
		<-ctx.Done()
		reaped <- runtimeReapOutcome{receipt: workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceOCIRuntimeSweep}}
		close(done)
	}()
	if err := session.stopOCIRuntime(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-started
}
