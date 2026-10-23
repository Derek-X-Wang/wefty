package l1

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

type computerSeatEvidenceFixture struct {
	h        *integrationHarness
	computer Computer
	node     Node
	claim    *Claim
	seat     WantedComputerSeat
	auth     ComputerSeatAttemptRequest
}

func newComputerSeatEvidenceFixture(t *testing.T, modes ...string) *computerSeatEvidenceFixture {
	mode := "both"
	if len(modes) > 0 {
		mode = modes[0]
	}
	t.Helper()
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxServiceSlots: 2}})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true}, []string{contract.StableNodeTagPrefix + "computer-node"})
	settings := ComputerSettings{SeatLimit: 2, IdleAction: ComputerIdleDelete, Network: ComputerNetworkNone}
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "seat-retention", Spec: computerCapabilityJobSpec("seat-retention"), Actor: "test", SeatRequestSettings: &settings})
	if err != nil {
		t.Fatal(err)
	}
	seat := WantedComputerSeat{SeatID: "seat_" + strings.Repeat("d", 32), Place: 1}
	list, err := h.store.SetComputerWantedSeats(t.Context(), computer.ComputerID, 0, []WantedComputerSeat{seat})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.ClaimJob(t.Context(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim %+v %v", claim, err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	if mode != "report" {
		if _, err := h.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth); err != nil {
			t.Fatal(err)
		}
	}
	if mode != "list" {
		if _, err := h.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1, Seats: []ComputerSeatObservation{{SeatID: seat.SeatID, Place: 1, State: ComputerSeatReady}}}); err != nil {
			t.Fatal(err)
		}
	}
	return &computerSeatEvidenceFixture{h, computer, node, claim, seat, auth}
}
func TestComputerSeatEvidenceDoesNotBlockAttemptDeletion(t *testing.T) {
	f := newComputerSeatEvidenceFixture(t)
	if _, err := f.h.store.db.Exec(`UPDATE jobs SET current_attempt_id=NULL WHERE job_id=?`, f.claim.Job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.h.store.db.Exec(`DELETE FROM attempts WHERE attempt_id=?`, f.claim.Lease.AttemptID); err != nil {
		t.Fatal(err)
	}
	observations, err := f.h.store.GetComputerSeatObservations(t.Context(), f.computer.ComputerID)
	if err != nil || len(observations) != 0 {
		t.Fatalf("orphan observations %+v %v", observations, err)
	}
	var count int
	if err := f.h.store.db.QueryRow(`SELECT COUNT(*) FROM computer_seat_identities WHERE seat_id=?`, f.seat.SeatID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("identity tombstone lost: %d %v", count, err)
	}
}
func TestComputerSeatEvidenceDoesNotBlockSummaryPruning(t *testing.T) {
	f := newComputerSeatEvidenceFixture(t)
	// Make the reported attempt the oldest of 33 non-current terminal attempts.
	old := f.h.clock.Now().Add(-time.Hour).UnixNano()
	if _, err := f.h.store.db.Exec(`UPDATE attempts SET state=?,created_ns=?,updated_ns=?,lease_expires_ns=? WHERE attempt_id=?`, contract.AttemptFailed, old, old, old, f.claim.Lease.AttemptID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 33; i++ {
		stamp := old + int64(i+1)*int64(time.Second)
		if _, err := f.h.store.db.Exec(`INSERT INTO attempts(attempt_id,job_id,node_id,boot_session_id,state,fencing_token,lease_expires_ns,authority_generation,created_ns,updated_ns) VALUES(?,?,?,?,?,?,?,?,?,?)`, fmt.Sprintf("seat-summary-%02d", i), f.claim.Job.JobID, f.node.NodeID, f.node.BootSessionID, contract.AttemptFailed, fmt.Sprintf("fence-%d", i), stamp, f.node.AuthorityGeneration, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.h.store.db.Exec(`UPDATE jobs SET current_attempt_id='seat-summary-32' WHERE job_id=?`, f.claim.Job.JobID); err != nil {
		t.Fatal(err)
	}
	tx, err := f.h.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	pruned, err := f.h.store.pruneServiceAttemptSummaries(t.Context(), tx, f.claim.Job.JobID, f.h.clock.Now())
	if err != nil || pruned != 1 {
		t.Fatalf("prune %d %v", pruned, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	observations, err := f.h.store.GetComputerSeatObservations(t.Context(), f.computer.ComputerID)
	if err != nil || len(observations) != 0 {
		t.Fatalf("pruned evidence %+v %v", observations, err)
	}
	var count int
	if err := f.h.store.db.QueryRow(`SELECT COUNT(*) FROM attempts WHERE job_id=?`, f.claim.Job.JobID).Scan(&count); err != nil || count != 33 {
		t.Fatalf("summary floor %d %v", count, err)
	}
}
func TestComputerSeatEvidenceDoesNotBlockComputerDeletion(t *testing.T) {
	f := newComputerSeatEvidenceFixture(t)
	tx, err := f.h.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// The failed Custody-import path deletes these rows in this order.
	for _, statement := range []string{`DELETE FROM computer_job_projections WHERE computer_id=?`, `DELETE FROM computer_intent_history WHERE computer_id=?`, `DELETE FROM computer_storage_generations WHERE computer_id=?`, `DELETE FROM computers WHERE computer_id=?`} {
		if _, err := tx.ExecContext(t.Context(), statement, f.computer.ComputerID); err != nil {
			t.Fatal(err)
		}
	}
	if err := deleteServiceRows(t.Context(), tx, f.claim.Job.JobID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"computer_seat_lists", "computer_wanted_seats"} {
		var count int
		if err := f.h.store.db.QueryRow(`SELECT COUNT(*) FROM `+table+` WHERE computer_id=?`, f.computer.ComputerID).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s retained %d %v", table, count, err)
		}
	}
	var count int
	if err := f.h.store.db.QueryRow(`SELECT COUNT(*) FROM computer_seat_identities WHERE seat_id=?`, f.seat.SeatID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("identity lost %d %v", count, err)
	}
	settings := ComputerSettings{SeatLimit: 2, IdleAction: ComputerIdleDelete, Network: ComputerNetworkNone}
	other, _, err := f.h.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "after-deletion", Spec: computerCapabilityJobSpec("after-deletion"), Actor: "test", SeatRequestSettings: &settings})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.h.store.SetComputerWantedSeats(t.Context(), other.ComputerID, 0, []WantedComputerSeat{f.seat}); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("deleted Computer's Seat ID reused: %v", err)
	}
}

func TestComputerSeatRestartNeverReturnsEarlierAttemptIDs(t *testing.T) {
	for _, mode := range []string{"list", "report"} {
		t.Run(mode, func(t *testing.T) {
			f := newComputerSeatEvidenceFixture(t, mode)
			ctx := t.Context()
			s := f.h.store
			if _, err := s.ObserveAttemptImage(ctx, "fabric-computer-node", f.claim.Job.JobID, f.claim.Lease.AttemptID, testImageObservation(f.claim.Lease.FencingToken)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.StartAttempt(ctx, "fabric-computer-node", f.claim.Job.JobID, f.claim.Lease.AttemptID, StartedRequest{FencingToken: f.claim.Lease.FencingToken}); err != nil {
				t.Fatal(err)
			}
			exit := 1
			if _, err := s.CompleteAttempt(ctx, "fabric-computer-node", f.claim.Job.JobID, f.claim.Lease.AttemptID, CompletionRequest{FencingToken: f.claim.Lease.FencingToken, IdempotencyKey: "restart-seat", Result: ProcessResult{ExitCode: &exit}}); err != nil {
				t.Fatal(err)
			}
			f.h.clock.Advance(2 * time.Second)
			second, err := s.ClaimJob(ctx, "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
			if err != nil || second == nil || second.Lease.AttemptID == f.claim.Lease.AttemptID {
				t.Fatalf("restart claim %+v %v", second, err)
			}
			auth := f.auth
			auth.FencingToken = second.Lease.FencingToken
			list, err := s.GetComputerSeatListForAttempt(ctx, "fabric-computer-node", f.computer.ComputerID, second.Job.JobID, second.Lease.AttemptID, auth)
			if err != nil || len(list.Seats) != 0 || list.Revision != 2 {
				t.Fatalf("restart returned ended Seat: %+v %v", list, err)
			}
			again, err := s.GetComputerSeatListForAttempt(ctx, "fabric-computer-node", f.computer.ComputerID, second.Job.JobID, second.Lease.AttemptID, auth)
			if err != nil || again.Revision != list.Revision || len(again.Seats) != 0 {
				t.Fatalf("list replay %+v %v", again, err)
			}
			if _, err := s.SetComputerWantedSeats(ctx, f.computer.ComputerID, list.Revision, []WantedComputerSeat{f.seat}); errorCode(err) != contract.ErrorConflict {
				t.Fatalf("ended Seat re-added: %v", err)
			}
			if _, err := s.ReportComputerSeatObservations(ctx, "fabric-computer-node", f.computer.ComputerID, second.Job.JobID, second.Lease.AttemptID, ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1, Seats: []ComputerSeatObservation{{SeatID: f.seat.SeatID, Place: 1, State: ComputerSeatReady}}}); errorCode(err) != contract.ErrorConflict {
				t.Fatalf("new attempt reported earlier Seat: %v", err)
			}
			if _, err := s.db.Exec(`DELETE FROM attempts WHERE attempt_id=?`, f.claim.Lease.AttemptID); err != nil {
				t.Fatal(err)
			}
			if _, err := s.SetComputerWantedSeats(ctx, f.computer.ComputerID, list.Revision, []WantedComputerSeat{f.seat}); errorCode(err) != contract.ErrorConflict {
				t.Fatalf("pruning revived Seat ID: %v", err)
			}
			fresh := WantedComputerSeat{SeatID: "seat_" + strings.Repeat("e", 32), Place: 1}
			next, err := s.SetComputerWantedSeats(ctx, f.computer.ComputerID, list.Revision, []WantedComputerSeat{fresh})
			if err != nil {
				t.Fatal(err)
			}
			bound, err := s.GetComputerSeatListForAttempt(ctx, "fabric-computer-node", f.computer.ComputerID, second.Job.JobID, second.Lease.AttemptID, auth)
			if err != nil || len(bound.Seats) != 1 || bound.Seats[0] != fresh || bound.Revision != next.Revision {
				t.Fatalf("fresh Seat %+v %v", bound, err)
			}
		})
	}
}
