package l1

import (
	"errors"
	"testing"
	"time"

	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/oci"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// Probe 4: a pending Seat-request Computer whose spec limit exceeds one share
// is charged only the share at reservation, then its start overcommits room.
func TestReviewProbe2PendingSpecOvercommit(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	spec := computerCapabilityJobSpec("bigpending")
	big := int64(3 << 30)
	spec.Execution.OCI.Limits.MemoryBytes = &big
	settings := ComputerSettings{SeatLimit: 2, IdleAction: ComputerIdleDelete, Network: ComputerNetworkNone}
	c, _, err := f.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "bigpending", Spec: spec, Actor: "test", SeatRequestSettings: &settings})
	if err != nil {
		t.Fatal(err)
	}
	room := DefaultComputerRoomConfig(2 << 30)
	id := f.request(t, 1, true)
	v := f.reserve(t, id, c, 1, room)
	t.Logf("reserve on pending 3 GiB Computer with 2 GiB room: %s %q", v.State, v.Reason)
	if v.State != SeatRefused {
		t.Fatalf("pending declaration exceeded room but was admitted: %+v", v)
	}
	reviewClaimAll(t, f)
	snap, err := f.store.GetComputerRoomSnapshot(t.Context(), f.node.NodeID, room)
	t.Logf("after claim: %+v %v", snap, err)
	if v.State == SeatApproved && snap.HeldMemoryBytes > room.MemoryBytes {
		t.Errorf("PROBE: reservation admitted, then start charges %d > room %d", snap.HeldMemoryBytes, room.MemoryBytes)
	}
}

func TestSeatRoomCompletionNeedsStrictAttemptAbsence(t *testing.T) {
	for _, evidence := range []RuntimeQuiescenceEvidence{RuntimeQuiescenceAttempt, RuntimeQuiescenceNoRuntime, RuntimeQuiescenceOCISweep, RuntimeQuiescencePriorBootOCISweep} {
		t.Run(string(evidence), func(t *testing.T) {
			f := newSeatLifecycleFixture(t)
			claim := reviewLiveSeat(t, f, 71)
			// The new process has no adapter runEntered state. Its old runtime
			// may be retained by the helper's otherwise successful boot sweep.
			f.reopen(t)
			exit := 1
			_, err := f.store.CompleteAttempt(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "weak-sweep-completion", RuntimeQuiescenceEvidence: evidence, Result: ProcessResult{ExitCode: &exit}})
			if err != nil {
				t.Fatal(err)
			}
			v := f.state(t, "seat_00000000000000000000000000000047", SeatEnding)
			if v.CleanupComplete {
				t.Fatal("ordinary completion became strict Seat cleanup")
			}
			room, err := f.store.GetComputerRoomSnapshot(t.Context(), f.node.NodeID, DefaultComputerRoomConfig(2<<30))
			if err != nil || room.HeldMemoryBytes < 1<<30 {
				t.Fatalf("weak completion returned room: %+v %v", room, err)
			}
			var absent bool
			if err := f.store.db.QueryRow(`SELECT first_attempt_absent FROM computer_seat_identities WHERE seat_id=?`, v.SeatID).Scan(&absent); err != nil || absent {
				t.Fatalf("weak completion proved Attempt absence: %v %v", absent, err)
			}
			if err := f.store.db.QueryRow(`SELECT memory_attempt_absent FROM computer_room_limits WHERE computer_id=?`, f.computer.ComputerID).Scan(&absent); err != nil || absent {
				t.Fatalf("weak completion freed the memory owner: %v %v", absent, err)
			}
		})
	}
}

// The boot sweep deliberately accepts a bounded, populated retention. A new
// adapter has no volatile Run history, and its receipt is weaker than absence.
type retainedSeatSweep struct {
	receipt ocihelper.VerifiedSweepReceipt
}

func (s *retainedSeatSweep) Session() (*ocihelper.Session, error) {
	return nil, errors.New("no live session in the prior-boot receipt")
}
func (s *retainedSeatSweep) ExecutionSnapshot() (*ocihelper.Session, ocihelper.VerifiedSweepReceipt, error) {
	return nil, s.receipt, errors.New("no live session in the prior-boot receipt")
}
func (s *retainedSeatSweep) SweepReceipt() (ocihelper.VerifiedSweepReceipt, bool) {
	return s.receipt, true
}

func TestSeatRoomRestartWithPopulatedRetentionKeepsCharge(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	claim := reviewLiveSeat(t, f, 72)
	a := ocihelper.AttemptAuthority{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, JobID: claim.Job.JobID, AttemptID: claim.Lease.AttemptID, FencingToken: claim.Lease.FencingToken, Class: "service", RemovalGeneration: "1"}
	identity, err := ocihelper.DeterministicResourceIdentity(a)
	if err != nil {
		t.Fatal(err)
	}
	now := f.clock.Now()
	retention := ocihelper.DurableRetention{Class: ocihelper.RemovalResourceCgroup, ID: identity.CgroupID, Owner: ocihelper.DurableRetentionOwnerOCIHelper, Reason: ocihelper.DurableRetentionReasonCgroupReaping, AttemptID: a.AttemptID, State: ocihelper.DurableRetentionStatePopulated, Bound: time.Minute, RecordedAt: now, Deadline: now.Add(time.Minute)}
	source := &retainedSeatSweep{receipt: ocihelper.VerifiedSweepReceipt{SweepEpoch: "sweep", HelperSession: ocihelper.HelperSession{HelperInstanceID: "helper", SessionGeneration: 2}, VerifiedAbsent: true,
		VerifiedInventory: ocihelper.ResourceInventory{Cgroups: []string{identity.CgroupID}}, VerifiedRetained: ocihelper.ResourceInventory{Cgroups: []string{identity.CgroupID}}, DurableRetentions: []ocihelper.DurableRetention{retention},
		Attempts: []ocihelper.SweptAttemptAuthority{{NodeID: a.NodeID, JobID: a.JobID, AttemptID: a.AttemptID, FencingToken: a.FencingToken, PriorBootSessionID: a.BootSessionID, Class: a.Class, RemovalGeneration: a.RemovalGeneration}},
	}}
	f.reopen(t)
	adapter := oci.NewAdapter(source)
	receipt, err := adapter.ReapPriorBoot(t.Context(), workloadrunner.PriorBootReapRequest{NodeID: a.NodeID, JobID: a.JobID, AttemptID: a.AttemptID, FencingToken: a.FencingToken, PriorBootSessionID: a.BootSessionID, CurrentBootSessionID: "replacement-boot", WorkloadClass: a.Class, RemovalGeneration: a.RemovalGeneration})
	if err != nil || !receipt.RuntimeQuiesced {
		t.Fatalf("ordinary retained sweep: %+v %v", receipt, err)
	}
	exit := 1
	if _, err := f.store.CompleteAttempt(t.Context(), "fabric-computer-node", a.JobID, a.AttemptID, CompletionRequest{FencingToken: a.FencingToken, IdempotencyKey: "retained-prior-boot", RuntimeQuiescenceEvidence: RuntimeQuiescenceEvidence(receipt.Evidence), Result: ProcessResult{ExitCode: &exit}}); err != nil {
		t.Fatal(err)
	}
	room, err := f.store.GetComputerRoomSnapshot(t.Context(), f.node.NodeID, DefaultComputerRoomConfig(2<<30))
	if err != nil || room.HeldMemoryBytes < 1<<30 {
		t.Fatalf("populated prior-boot cgroup returned room: %+v %v", room, err)
	}
	if v := f.state(t, "seat_00000000000000000000000000000048", SeatEnding); v.CleanupComplete {
		t.Fatal("retained cgroup cleaned the Seat")
	}
}
