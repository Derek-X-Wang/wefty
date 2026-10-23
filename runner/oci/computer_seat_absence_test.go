package oci

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func TestSeatRoomAbsenceRejectsEvenBoundRetainedCgroups(t *testing.T) {
	a := ocihelper.AttemptAuthority{NodeID: "node", BootSessionID: "old-boot", JobID: "job", AttemptID: "old", FencingToken: "fence", Class: "service", RemovalGeneration: "0"}
	id, err := ocihelper.DeterministicResourceIdentity(a)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0).UTC()
	v := ocihelper.VerifyResponse{Absent: true, Inventory: ocihelper.ResourceInventory{Cgroups: []string{id.CgroupID}}, DurableRetained: ocihelper.ResourceInventory{Cgroups: []string{id.CgroupID}}, DurableRetentions: []ocihelper.DurableRetention{{Class: ocihelper.RemovalResourceCgroup, ID: id.CgroupID, Owner: ocihelper.DurableRetentionOwnerOCIHelper, Reason: ocihelper.DurableRetentionReasonCgroupReaping, AttemptID: a.AttemptID, State: ocihelper.DurableRetentionStatePopulated, Bound: time.Minute, RecordedAt: now, Deadline: now.Add(time.Minute)}}}
	if err := computerSeatAttemptAbsent(v, a, now); err == nil {
		t.Fatal("retained cgroup was mistaken for returned room")
	}
	if err := computerSeatAttemptAbsent(ocihelper.VerifyResponse{Absent: true}, a, now); err != nil {
		t.Fatal(err)
	}
}

func TestComputerMemoryOverlapHasNoRuntimeReapEvidence(t *testing.T) {
	engine := &adapterTestEngine{runErr: &ocihelper.RPCError{Code: ocihelper.CodeSeatOperationRefused, Detail: ocihelper.ErrorDetail(ocihelper.SeatRefusalAttemptOverlap)}, refuseDelete: true}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	if _, err := adapter.Run(t.Context(), request, nil); err == nil {
		t.Fatal("overlap started")
	}
	receipt, err := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority})
	if err != nil || !receipt.RuntimeQuiesced || receipt.Evidence != workloadrunner.ReapEvidenceNoRuntime || engine.runtimeDeletes != 0 {
		t.Fatalf("overlap cleanup: %+v err=%v deletes=%d", receipt, err, engine.runtimeDeletes)
	}
}

func TestSeatMemoryReapRequiresCgroupAbsenceOnEveryRetry(t *testing.T) {
	for _, computer := range []bool{false, true} {
		name := "uncertain-resize"
		if computer {
			name = "computer-before-first-resize"
		}
		t.Run(name, func(t *testing.T) {
			request := adapterTestRequest()
			if computer {
				memory := int64(2 << 30)
				request.Authority.WorkloadClass = contract.JobClassService
				request.Execution.Env = map[string]string{contract.EnvL3Endpoint: "http://127.0.0.1:43100/l3"}
				request.Execution.SensitiveEnv = map[string]string{contract.EnvComputerToken: "computer-pass"}
				request.AttemptEndpoints = []string{workloadrunner.AttemptEndpointView, workloadrunner.AttemptEndpointControl}
				request.AttemptEndpointReady = func(string, workloadrunner.AttemptEndpoint) error { return nil }
				request.HostBridgeFallbackActive = true
				request.HostBridgeDial = func(context.Context) (net.Conn, error) { return nil, context.Canceled }
				request.Execution.OCI.Computer = &contract.OCIComputerSpec{DiskBytes: 8 << 30}
				request.Execution.OCI.Limits = &contract.OCILimits{MemoryBytes: &memory}
				request.ManagedVolumes = []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeComputerDisk, ComputerStorage: &workloadrunner.ComputerStorage{ComputerID: "computer", StorageID: "storage", StorageGeneration: 1, IntentRevision: 1, DiskBytes: 8 << 30}}}
			}
			authority := HelperAuthority(request.Authority)
			identity, err := ocihelper.DeterministicResourceIdentity(authority)
			if err != nil {
				t.Fatal(err)
			}
			engine := &adapterTestEngine{watch: ocihelper.WatchResponse{ExitCode: intPointer(0)}, readOnlyVerifies: []ocihelper.VerifyResponse{
				{Absent: true, Inventory: ocihelper.ResourceInventory{Cgroups: []string{identity.CgroupID}}},
				{Absent: true, Inventory: ocihelper.ResourceInventory{Cgroups: []string{identity.CgroupID}}},
				{Absent: true},
			}}
			adapter, barrier, _, closeAdapter := startAdapterTestServerWithSnapshots(t, engine, ImagePolicy{})
			defer closeAdapter()
			if _, err := adapter.Run(t.Context(), request, nil); err != nil {
				t.Fatal(err)
			}
			if !computer {
				// Even an uncertain resize binds the strict room-release proof.
				// This fake refuses resizing without requiring a live Computer.
				_, _ = adapter.SetComputerMemoryLimit(t.Context(), ocihelper.SetComputerMemoryLimitRequest{Authority: authority, MemoryBytes: 1 << 30})
			}
			for range 2 {
				if err := barrier.Ensure(t.Context()); err != nil {
					t.Fatal(err)
				}
				receipt, err := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority})
				if err == nil || receipt.RuntimeQuiesced {
					t.Fatalf("remaining cgroup released Seat room: %+v %v", receipt, err)
				}
			}
			if err := barrier.Ensure(t.Context()); err != nil {
				t.Fatal(err)
			}
			receipt, err := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority})
			if err != nil || !receipt.RuntimeQuiesced {
				t.Fatalf("absent cgroup did not complete cleanup: %+v %v", receipt, err)
			}
		})
	}
}
