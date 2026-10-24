//go:build darwin || linux

package agent

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

// SL-1 crosses the production node-owned poller, native-dialog seam and L1.
// The disconnected CLI observer has no cancellation authority over the dialog.
func TestSeatRequestCutOffWaitKeepsOneDialogAndSeat(t *testing.T) {
	testSeatRequestDialog(t, false)
}

func TestSeatRequestNilIntentAfterDialog(t *testing.T) { testSeatRequestDialog(t, true) }

func testSeatRequestDialog(t *testing.T, nilIntent bool) {
	clock := newManualClock(time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC))
	client, store := approvalTestClient(t, clock)
	_, err := store.RegisterNode(t.Context(), fabric.Identity{NodeID: "dialog-node"}, contract.NodeRegistration{NodeID: "mac", BootSessionID: "boot", RootInstanceID: "root", OS: "linux", Architecture: "amd64", AgentVersion: "test", Capabilities: map[string]bool{"computer:seat-requests": true, "kind:oci": true, "computer": true, "cgroup_v2": true}, CapabilityRevision: 2, CapabilityObservedAt: clock.Now(), MissingCapabilities: []string{}}, l1.DefaultNodePolicy(contract.StableNodeTagPrefix+"mac"), true)
	if err != nil {
		t.Fatal(err)
	}
	memory := int64(1 << 30)
	pin := "sha256:" + strings.Repeat("a", 64)
	spec := contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: "cut-off-shared", Kind: contract.JobKindOCI, Class: contract.JobClassService, Restart: contract.RestartAlways, RoutingTags: []string{contract.StableNodeTagPrefix + "mac"}, Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "ghcr.io/example/desktop", Digest: &pin}, Limits: &contract.OCILimits{MemoryBytes: &memory}, Computer: &contract.OCIComputerSpec{Display: contract.OCIComputerDisplaySpec{Protocol: contract.ComputerDisplayProtocolRFBWebSocketV1}, DiskBytes: 8 << 30}}}}
	computer, _, err := store.CreateComputer(t.Context(), l1.CreateComputerRequest{Name: "shared", Actor: "test", Spec: spec, SeatRequestSettings: &l1.ComputerSettings{SeatLimit: 2, IdleAction: l1.ComputerIdleDelete, Network: l1.ComputerNetworkNone}})
	if err != nil {
		t.Fatal(err)
	}
	input := l1.SeatCallInput{SeatID: fmt.Sprintf("seat_%032x", 1), NodeID: "mac", Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}
	if _, err := store.CreateSeatCall(t.Context(), "cli", input); err != nil {
		t.Fatal(err)
	}
	shown := make(chan struct{}, 1)
	approve := make(chan struct{})
	var dialogs atomic.Int32
	dialog := SeatApprovalDialogFunc(func(ctx context.Context, _ SeatApprovalPrompt) (SeatApprovalChoice, error) {
		dialogs.Add(1)
		shown <- struct{}{}
		select {
		case <-approve:
			return SeatApprovalApprove, nil
		case <-ctx.Done():
			return SeatApprovalNoAnswer, ctx.Err()
		}
	})
	controller, err := NewSeatApprovalController(client, "mac", "boot", SeatApprovalSettings{}, dialog, clock)
	if err != nil {
		t.Fatal(err)
	}
	room := l1.DefaultComputerRoomConfig(3 << 30)
	node := &Agent{seatApprovals: controller, seatRoom: &room, clock: clock, registration: contract.NodeRegistration{NodeID: "mac", BootSessionID: "boot"}, session: &agentSession{client: client, ociIntent: func(context.Context) (OCIIntentObservation, error) {
		return OCIIntentObservation{Enabled: true, Revision: 1}, nil
	}}, capabilities: &capabilityState{current: contract.CapabilityObservation{Capabilities: map[string]bool{"kind:oci": true}}}}
	node.session.markRegistered()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); node.runSeatRequests(ctx) }()
	defer func() { cancel(); <-done }()
	clock.waitForDeadline(t, clock.Now())
	clock.Advance(0)
	select {
	case <-shown:
	case <-time.After(5 * time.Second):
		t.Fatal("node did not show approval")
	}
	observer, cutOff := context.WithCancel(t.Context())
	cutOff()
	if _, err := store.GetSeatCall(observer, "cli", input.SeatID); err == nil {
		t.Fatal("cut-off read unexpectedly succeeded")
	}
	if _, err := store.CreateSeatCall(t.Context(), "cli", input); err != nil {
		t.Fatal(err)
	}
	if nilIntent {
		node.session.ociIntent = nil
	}
	close(approve)
	// Real time bounds test execution only; all lifecycle time uses clock.
	deadline := time.Now().Add(5 * time.Second)
	var value l1.SeatCallResult
	for time.Now().Before(deadline) {
		value, err = store.GetSeatCall(t.Context(), "cli", input.SeatID)
		if err == nil && (value.State == l1.SeatStarting || nilIntent && value.State == l1.SeatRefused) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if nilIntent {
		if err != nil || value.State != l1.SeatRefused || value.Reason != "the host VM is off" || dialogs.Load() != 1 {
			t.Fatalf("nil intent: %+v %v dialogs=%d", value, err, dialogs.Load())
		}
		return
	}
	if err != nil || value.State != l1.SeatStarting || value.ComputerID != computer.ComputerID || dialogs.Load() != 1 {
		t.Fatalf("replayed request state=%s computer=%s dialogs=%d err=%v", value.State, value.ComputerID, dialogs.Load(), err)
	}
	// Fake the existing helper's fenced readiness receipt, after one membership.
	claim, err := store.ClaimJob(t.Context(), "dialog-node", "mac", "boot", contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim %v %v", claim, err)
	}
	auth := l1.ComputerSeatAttemptRequest{NodeID: "mac", BootSessionID: "boot", FencingToken: claim.Lease.FencingToken}
	list, err := store.GetComputerSeatListForAttempt(t.Context(), "dialog-node", computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil || len(list.Seats) != 1 {
		t.Fatalf("Seat membership: %+v %v", list, err)
	}
	if _, err := store.ReportComputerSeatObservations(t.Context(), "dialog-node", computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, l1.ComputerSeatObservationReport{NodeID: "mac", BootSessionID: "boot", FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1, Seats: []l1.ComputerSeatObservation{{SeatID: input.SeatID, Place: 1, State: l1.ComputerSeatReady}}}); err != nil {
		t.Fatal(err)
	}
	ready, err := store.GetSeatCall(t.Context(), "cli", input.SeatID)
	if err != nil || ready.State != l1.SeatLive || ready.Credential == "" || dialogs.Load() != 1 {
		t.Fatalf("ready replay state=%s dialogs=%d err=%v", ready.State, dialogs.Load(), err)
	}
}
