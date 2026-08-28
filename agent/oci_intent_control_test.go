package agent

import (
	"context"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
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
	go func() {
		<-ociContext.Done()
		close(ociDone)
	}()
	session.resident["oci-job"] = &residentAttempt{kind: contract.JobKindOCI, class: contract.JobClassService, cancel: cancelOCI, done: ociDone}
	session.resident["process-job"] = &residentAttempt{kind: contract.JobKindProcess, class: contract.JobClassService, cancel: cancelProcess, done: make(chan struct{})}
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
