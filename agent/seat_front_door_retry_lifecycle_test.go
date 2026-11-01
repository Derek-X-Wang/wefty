//go:build darwin || linux

package agent

import (
	"context"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestSeatFrontDoorAttemptReadsRecoveredDoor(t *testing.T) {
	node, _, clock, logs := newSeatListenRetryNode(t, 1)
	runtime := &computerNetworkClosedRuntime{}
	node.runtimes = workloadRuntimeSet{contract.JobKindOCI: runtime}
	node.managedResource = &preflightManagedResource{}
	// Construct the lifecycle before the door opens to catch a captured pointer.
	lifecycle := node.newAttemptLifecycle()
	lifecycle.dependencies.allowsStart = nil
	digest := preAdmissionImageDigest
	memory := int64(1 << 30)
	claim := l1.Claim{
		Job: l1.Job{JobID: "retry-job", Spec: contract.JobSpec{Kind: contract.JobKindOCI, Class: contract.JobClassService,
			Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
				Image:    contract.OCIImageSpec{Reference: preAdmissionImageReference, Digest: &digest},
				Computer: &contract.OCIComputerSpec{DiskBytes: 8 << 30}, Limits: &contract.OCILimits{MemoryBytes: &memory},
			}},
		}},
		Lease: l1.AttemptLease{AttemptID: "closed-attempt", FencingToken: "retry-fence", LeaseTTL: time.Second},
		ComputerStorage: &l1.ComputerStorageClaim{ComputerSettings: &l1.ComputerSettings{Network: l1.ComputerNetworkNone}, SeatRequestMade: true,
			ComputerID: "retry-computer", StorageID: "retry-storage", StorageGeneration: 1, IntentRevision: 1, DiskBytes: 8 << 30},
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); _ = node.Run(ctx) }()
	defer func() { cancel(); awaitSeatTestSignal(t, done) }()
	if got := awaitSeatLeaseSchedule(t, clock, done); got != time.Second {
		t.Fatalf("first delay=%v", got)
	}
	result, err := lifecycle.runWorkload(t.Context(), claim)
	if runtime.called || err == nil || result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailureRuntimeUnavailable {
		t.Fatalf("closed Attempt ran=%v result=%+v err=%v", runtime.called, result, err)
	}
	clock.Advance(time.Second)
	// The recovery log follows publication of the current door.
	for i := 0; i < 2; i++ {
		select {
		case <-logs:
		case <-time.After(testwait.Bound):
			t.Fatal("door did not recover")
		}
	}
	claim.Lease.AttemptID = "recovered-attempt"
	result, err = lifecycle.runWorkload(t.Context(), claim)
	if err != nil || result.SpawnError != nil || !runtime.called {
		t.Fatalf("recovered Attempt ran=%v result=%+v err=%v", runtime.called, result, err)
	}
}
