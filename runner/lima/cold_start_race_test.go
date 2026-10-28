package lima

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// HV-4: clocks control the idle wait and the elapsed cold start. Commands and
// helper transport are fakes; no host VM or container starts in this test.
func TestColdStartIdleStopRaceMatrix(t *testing.T) {
	for _, arrival := range []string{"before-cycle", "after-cycle", "before-shutdown", "during-shutdown", "after-shutdown"} {
		t.Run(arrival, func(t *testing.T) {
			now := time.Unix(1000, 0)
			items := []DemandItem(nil)
			intent := OCIIntent{Version: OCIIntentVersion, Revision: 1, Enabled: true, Position: OCIPositionOnDemand, UpdatedAt: now}
			runner := &supervisorRunner{states: []InstanceState{InstanceRunning}}
			reads := 0
			armed := false
			supervisor, err := NewSupervisor(SupervisorConfig{Instance: DefaultInstanceName, Intent: IntentSourceFunc(func(context.Context) (OCIIntent, error) { return intent, nil }), now: func() time.Time { return now }, run: runner.run, wait: func(context.Context, time.Duration) error { return nil }, Demand: DemandSourceFunc(func(context.Context) ([]DemandItem, error) {
				reads++
				if armed && arrival == "before-shutdown" && reads == 1 {
					items = []DemandItem{{Kind: "queued-oci"}}
				}
				return items, nil
			})})
			if err != nil {
				t.Fatal(err)
			}
			checksum := "sha256:" + strings.Repeat("a", 64)
			socket, _ := startReadyLimaHelper(t, checksum)
			helper, err := ocihelper.NewBootBarrier(ocihelper.NewUnixClient(socket, checksum), ocihelper.AcquireSessionRequest{NodeID: "node", BootSessionID: "boot"})
			if err != nil {
				t.Fatal(err)
			}
			barrier := &SupervisedBootBarrier{Supervisor: supervisor, Barrier: helper}
			if err := barrier.Ensure(t.Context()); err != nil {
				t.Fatal(err)
			}
			supervisor.IdleDue(t.Context())
			now = now.Add(10 * time.Minute)
			if !supervisor.IdleDue(t.Context()) {
				t.Fatal("idle wait not due")
			}
			reads = 0
			armed = true
			generation, _ := helper.Generation()
			shutdowns := 0
			runner.afterCommand = func(args []string) {
				if len(args) > 0 && args[0] == "stop" {
					shutdowns++
					if arrival == "during-shutdown" {
						items = []DemandItem{{Kind: "queued-oci"}}
					}
					now = now.Add(20 * time.Second)
				}
			}
			early := arrival == "before-cycle" || arrival == "after-cycle"
			if arrival == "before-cycle" {
				items = []DemandItem{{Kind: "queued-oci"}}
			}
			stopped, err := barrier.IdleStop(t.Context(), func() bool {
				// This is the node's final serialized demand read. The cycle's own
				// read below closes arrivals after that read and before shutdown.
				if arrival == "after-cycle" {
					items = []DemandItem{{Kind: "queued-oci"}}
				}
				return len(items) == 0
			})
			if err != nil {
				t.Fatal(err)
			}
			if early || arrival == "before-shutdown" {
				current, ready := helper.Generation()
				if stopped || shutdowns != 0 || !ready || current != generation {
					t.Fatalf("cancel replaced helper or shut down: stopped=%t shutdowns=%d ready=%t", stopped, shutdowns, ready)
				}
				return
			}
			if !stopped || shutdowns != 1 {
				t.Fatalf("started shutdown did not finish: stopped=%t shutdowns=%d", stopped, shutdowns)
			}
			if arrival == "after-shutdown" {
				items = []DemandItem{{Kind: "queued-oci"}}
			}
			runner.states = []InstanceState{InstanceStopped, InstanceRunning}
			if err := barrier.Ensure(t.Context()); err != nil {
				t.Fatal(err)
			}
			if !barrier.Ready() || now.Sub(time.Unix(1600, 0)) >= 3*time.Minute {
				t.Fatal("demand did not recover within approval budget")
			}
		})
	}
}

// HV-4 / RM-4: the Seat arrives after shutdown has started. The command,
// timeout and restart use one injected timeline; only helper transport is real.
func TestColdStartIdleStopStalledShutdownThenReadySeat(t *testing.T) {
	now := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{Clock: l1.ClockFunc(func() time.Time { return now }), LeaseDuration: time.Hour, NodeStaleAfter: time.Hour, NodeDeadAfter: 2 * time.Hour, ComputerDefaultImage: "ghcr.io/example/desktop@sha256:" + strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	caps := map[string]bool{l1.CapabilitySeatRequests: true}
	node, err := store.RegisterNode(t.Context(), fabric.Identity{NodeID: "fabric-cold"}, contract.NodeRegistration{NodeID: "cold", BootSessionID: "boot", RootInstanceID: "root", OS: "darwin", Architecture: "arm64", AgentVersion: "test", Capabilities: caps, CapabilityRevision: 1, CapabilityObservedAt: now, MissingCapabilities: []string{"kind:oci", "computer", "cgroup_v2"}, CapabilityReasonCode: contract.CapabilityReasonHostVMIdle}, l1.NodePolicy{Tags: []string{contract.StableNodeTagPrefix + "cold"}, MaxServiceSlots: 4}, true)
	if err != nil {
		t.Fatal(err)
	}
	id := "seat_00000000000000000000000000001147"
	poll := l1.SeatCallPoll{SeatNodeRequest: l1.SeatNodeRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID}, Room: l1.DefaultComputerRoomConfig(3 << 30), HostEnabled: true, AnswerWait: time.Minute}
	var seat l1.SeatRequest
	var approvedAt time.Time
	items := []DemandItem(nil)
	runner := &supervisorRunner{states: []InstanceState{InstanceRunning}}
	type budgetKey struct{}
	var gracefulBudget, forceBudget time.Duration
	var shutdowns, forces, restarts int
	supervisor, err := NewSupervisor(SupervisorConfig{Instance: DefaultInstanceName, Intent: IntentSourceFunc(func(context.Context) (OCIIntent, error) {
		return OCIIntent{Version: OCIIntentVersion, Revision: 1, Enabled: true, Position: OCIPositionOnDemand, UpdatedAt: now}, nil
	}), now: func() time.Time { return now }, Demand: DemandSourceFunc(func(context.Context) ([]DemandItem, error) { return items, nil }), withTimeout: func(ctx context.Context, budget time.Duration) (context.Context, context.CancelFunc) {
		return context.WithCancel(context.WithValue(ctx, budgetKey{}, budget))
	}, run: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		switch {
		case len(args) == 2 && args[0] == "stop":
			shutdowns++
			// Approval happens only after the graceful shutdown command starts.
			approvedAt = now
			if _, err := store.CreateSeatCall(ctx, "cli", l1.SeatCallInput{SeatID: id, NodeID: node.NodeID, Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
				return nil, err
			}
			if _, err := store.PollSeatCalls(ctx, "fabric-cold", poll); err != nil {
				return nil, err
			}
			if _, err := store.AnswerSeatApproval(ctx, "fabric-cold", id, l1.SeatApprovalAnswer{NodeID: node.NodeID, BootSessionID: node.BootSessionID, Approved: true}); err != nil {
				return nil, err
			}
			var err error
			seat, err = store.AdvanceSeatCall(ctx, "fabric-cold", id, poll)
			if err != nil {
				return nil, err
			}
			hint, err := store.ClaimJobPoll(ctx, "fabric-cold", node.NodeID, node.BootSessionID, contract.JobClassService)
			if err != nil {
				return nil, err
			}
			if !hint.QueuedOCIWork || hint.Claim != nil {
				return nil, fmt.Errorf("cold request did not produce a non-authoritative hint")
			}
			items = []DemandItem{{Kind: "queued-oci"}}
			gracefulBudget = ctx.Value(budgetKey{}).(time.Duration)
			now = now.Add(gracefulBudget)
			return nil, context.DeadlineExceeded
		case len(args) == 3 && args[0] == "stop" && args[1] == "--force":
			if err := ctx.Err(); err != nil {
				return nil, fmt.Errorf("force-stop inherited the canceled graceful context: %w", err)
			}
			forces++
			forceBudget = ctx.Value(budgetKey{}).(time.Duration)
			now = now.Add(5 * time.Second)
		case len(args) > 0 && args[0] == "start":
			restarts++
			// Simulated cold start, not a live timing measurement.
			now = now.Add(27300 * time.Millisecond)
		}
		return runner.run(ctx, name, args...)
	}})
	if err != nil {
		t.Fatal(err)
	}
	checksum := "sha256:" + strings.Repeat("a", 64)
	socket, _ := startReadyLimaHelper(t, checksum)
	helper, err := ocihelper.NewBootBarrier(ocihelper.NewUnixClient(socket, checksum), ocihelper.AcquireSessionRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID})
	if err != nil {
		t.Fatal(err)
	}
	barrier := &SupervisedBootBarrier{Supervisor: supervisor, Barrier: helper}
	if err := barrier.Ensure(t.Context()); err != nil {
		t.Fatal(err)
	}
	supervisor.IdleDue(t.Context())
	now = now.Add(10 * time.Minute)
	if !supervisor.IdleDue(t.Context()) {
		t.Fatal("idle stop not due")
	}
	stopped, err := barrier.IdleStop(t.Context(), func() bool {
		if err := barrier.MarkStopping(t.Context()); err != nil {
			t.Fatal(err)
		}
		return true
	})
	if err != nil || !stopped {
		t.Fatalf("stalled shutdown=%t %v", stopped, err)
	}
	if err := barrier.FinishStopping(); err != nil {
		t.Fatal(err)
	}
	if gracefulBudget != 90*time.Second || forceBudget != 30*time.Second || shutdowns != 1 || forces != 1 {
		t.Fatalf("shutdown budgets: gentle=%s force=%s calls=%d/%d", gracefulBudget, forceBudget, shutdowns, forces)
	}
	runner.states = []InstanceState{InstanceStopped, InstanceRunning}
	if err := barrier.Ensure(t.Context()); err != nil {
		t.Fatal(err)
	}
	readyCaps := map[string]bool{l1.CapabilitySeatRequests: true, "kind:oci": true, "computer": true, "cgroup_v2": true}
	if _, err := store.HeartbeatNodeWithCapabilityObservation(t.Context(), "fabric-cold", node.NodeID, node.BootSessionID, contract.CapabilityObservation{Revision: 2, Capabilities: readyCaps, ObservedAt: now, MissingCapabilities: []string{}}, l1.NodePolicy{Tags: []string{contract.StableNodeTagPrefix + node.NodeID}, MaxServiceSlots: 4}); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimJobPoll(t.Context(), "fabric-cold", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim.Claim == nil {
		t.Fatalf("restarted claim=%+v %v", claim, err)
	}
	auth := l1.ComputerSeatAttemptRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	list, err := store.GetComputerSeatListForAttempt(t.Context(), "fabric-cold", seat.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReportComputerSeatObservations(t.Context(), "fabric-cold", seat.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, l1.ComputerSeatObservationReport{NodeID: node.NodeID, BootSessionID: node.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1, Seats: []l1.ComputerSeatObservation{{SeatID: id, Place: seat.Place, State: l1.ComputerSeatReady}}}); err != nil {
		t.Fatal(err)
	}
	result, err := store.GetSeatCall(t.Context(), "cli", id)
	if err != nil || result.State != l1.SeatLive || result.SeatID != id || result.Credential == "" || restarts != 1 || !barrier.Ready() || now.Sub(approvedAt) >= 3*time.Minute {
		t.Fatalf("ready after stalled stop: state=%s elapsed=%s restarts=%d err=%v", result.State, now.Sub(approvedAt), restarts, err)
	}
	if elapsed := now.Sub(approvedAt); elapsed != 122300*time.Millisecond {
		t.Fatalf("injected elapsed=%s", elapsed)
	}
}
