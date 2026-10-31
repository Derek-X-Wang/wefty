package l1

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// Exercise the same fenced readiness publication that delivers a live Seat.
func prepareSeatLogLive(t *testing.T, f *seatLifecycleFixture, id string) {
	t.Helper()
	f.request(t, 1, true)
	f.reserve(t, id, f.computer, 1, DefaultComputerRoomConfig(1<<30))
	if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ReadyWait: time.Minute}); err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim: %+v %v", claim, err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	report := ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1, Seats: []ComputerSeatObservation{{SeatID: id, Place: 1, State: ComputerSeatReady}}}
	if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, report); err != nil {
		t.Fatal(err)
	}
}

func TestSeatLogOutcomeReplyAcknowledgesItsHistory(t *testing.T) {
	for _, outcome := range []string{"requested", "approved", "starting", "live", "refused", "ended"} {
		t.Run(outcome, func(t *testing.T) {
			f := newSeatLifecycleFixture(t)
			id := "seat_00000000000000000000000000000001"
			first, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"})
			if err != nil {
				t.Fatal(err)
			}
			switch outcome {
			case "requested":
				if first.UnreadEvents != 0 {
					t.Fatalf("initial request repeats its own event: %+v", first)
				}
			case "approved", "starting":
				f.request(t, 1, true)
				if outcome == "starting" {
					f.reserve(t, id, f.computer, 1, DefaultComputerRoomConfig(1<<30))
					if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ReadyWait: time.Minute}); err != nil {
						t.Fatal(err)
					}
				}
			case "refused":
				f.request(t, 1, false)
				if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", id, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: false}); err != nil {
					t.Fatal(err)
				}
			case "live", "ended":
				prepareSeatLogLive(t, f, id)
				if outcome == "ended" {
					if _, err := f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Reason: "released"}); err != nil {
						t.Fatal(err)
					}
				}
			}
			// An event after the reported outcome must survive acknowledgement of the
			// outcome's history, including the first ready response and repeated waits.
			if err := f.store.AppendSeatEvent(t.Context(), SeatLogEvent{SeatID: id, EventID: "later-control", Kind: SeatEventControlTaken, Note: "owner testing"}); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				result, err := f.store.GetSeatCall(t.Context(), "cli", id)
				if err != nil || result.UnreadEvents != 1 {
					t.Fatalf("outcome=%s reply must report only later events: %+v %v", outcome, result, err)
				}
			}
			page, err := f.store.GetSeatLogs(t.Context(), "cli", id, "", "", 100)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.store.MarkSeatLogRead(t.Context(), "cli", id, "", page.NextCursor); err != nil {
				t.Fatal(err)
			}
			result, err := f.store.GetSeatCall(t.Context(), "cli", id)
			if err != nil || result.UnreadEvents != 0 {
				t.Fatalf("log read did not clear hint: %+v %v", result, err)
			}
		})
	}
}

func TestSeatLogRetainedIdentityCannotChangeCaller(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := "seat_00000000000000000000000000000001"
	input := SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", input); err != nil {
		t.Fatal(err)
	}
	// Model pruning of the public receipt before the retained log expires.
	if _, err := f.store.db.ExecContext(t.Context(), `DELETE FROM seat_request_calls WHERE seat_id=?`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.CreateSeatCall(t.Context(), "other-cli", input); errorCode(err) != contract.ErrorDispatchKeyConflict {
		t.Fatalf("retained Seat log was handed to a new caller: %v", err)
	}
	if _, err := f.store.GetSeatLogs(t.Context(), "cli", id, "", "", 100); err != nil {
		t.Fatalf("original reader lost its retained log: %v", err)
	}
	if _, err := f.store.GetSeatLogs(t.Context(), "other-cli", id, "", "", 100); errorCode(err) != contract.ErrorForbidden {
		t.Fatalf("replacement caller can read the log: %v", err)
	}
}

func TestSeatLogWriterRefusesMissingRequest(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	for i, kind := range []SeatEventKind{SeatEventRequested, SeatEventStarted, SeatEventControlTaken} {
		id := fmt.Sprintf("seat_%032x", i+10)
		if err := f.store.AppendSeatEvent(t.Context(), SeatLogEvent{SeatID: id, EventID: "orphan", Kind: kind}); errorCode(err) != contract.ErrorNotFound {
			t.Errorf("%s created a log without a request: %v", kind, err)
		}
	}
	id := f.request(t, 1, true)
	if err := f.store.AppendSeatEvent(t.Context(), SeatLogEvent{SeatID: id, EventID: "control", Kind: SeatEventControlTaken}); err != nil {
		t.Fatalf("approval request did not create its log: %v", err)
	}
}

func TestSeatLogUnresolvedRequestExpiresAndReportsPruning(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := "seat_00000000000000000000000000000001"
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(DefaultComputerTakeoverAuditRetentionAge)
	result, err := f.store.Reconcile(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if result.PrunedSeatLogs != 0 {
		t.Fatalf("unresolved request lost its log before refusal: %+v", result)
	}
	outcome, err := f.store.GetSeatCall(t.Context(), "cli", id)
	if err != nil || outcome.State != SeatRefused {
		t.Fatalf("old pending request cannot refuse: %+v %v", outcome, err)
	}
	// The refusal ends the log, which then keeps the full retention from
	// that end instead of expiring at once (#1239).
	result, err = f.store.Reconcile(t.Context())
	if err != nil || result.PrunedSeatLogs != 0 {
		t.Fatalf("late refusal lost its log at once: %+v %v", result, err)
	}
	if _, err := f.store.GetSeatLogs(t.Context(), "cli", id, "", "", 100); err != nil {
		t.Fatalf("late refusal log unreadable: %v", err)
	}
	f.clock.Advance(DefaultComputerTakeoverAuditRetentionAge)
	result, err = f.store.Reconcile(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var counters map[string]any
	if err := json.Unmarshal(data, &counters); err != nil {
		t.Fatal(err)
	}
	if counters["pruned_seat_logs"] != float64(1) {
		t.Fatalf("unresolved log not counted as pruned: %s", data)
	}
	if _, err := f.store.GetSeatLogs(t.Context(), "cli", id, "", "", 100); errorCode(err) != contract.ErrorNotFound {
		t.Fatalf("refused log survived the retention from its end: %v", err)
	}
	if err := f.store.AppendSeatEvent(t.Context(), SeatLogEvent{SeatID: id, EventID: "late", Kind: SeatEventControlTaken}); errorCode(err) != contract.ErrorNotFound {
		t.Fatalf("late event revived log: %v", err)
	}
}

func TestSeatLogUsesConfiguredAuditRetention(t *testing.T) {
	for _, operation := range []string{"read", "acknowledge", "sweep", "late-refusal"} {
		t.Run(operation, func(t *testing.T) {
			f := newSeatLifecycleFixture(t)
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			var err error
			f.store, err = OpenStore(f.path, StoreOptions{Clock: f.clock, ComputerTakeoverAuditRetentionAge: time.Hour, LeaseDuration: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			id := f.request(t, 1, true)
			// Give the direct approval fixture an original caller, as migration does.
			if _, err := f.store.db.ExecContext(t.Context(), `UPDATE seat_logs SET caller_identity='cli' WHERE seat_id=?`, id); err != nil {
				t.Fatal(err)
			}
			if operation == "late-refusal" {
				f.clock.Advance(time.Hour)
			}
			if _, err := f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Reason: "denied by the owner", Refused: true}); err != nil {
				t.Fatal(err)
			}
			if operation == "late-refusal" {
				// An ended log keeps the full retention from its end, even when
				// the Seat never went live (#1239). Read and sweep agree at the
				// exact boundary.
				readable := func(when string) {
					t.Helper()
					if _, err := f.store.GetSeatLogs(t.Context(), "cli", id, "", "", 100); err != nil {
						t.Fatalf("%s: late refusal lost its log: %v", when, err)
					}
					if result, err := f.store.Reconcile(t.Context()); err != nil || result.PrunedSeatLogs != 0 {
						t.Fatalf("%s: sweep pruned a log within retention: %+v %v", when, result, err)
					}
				}
				readable("at the refusal")
				f.clock.Advance(time.Hour - time.Nanosecond)
				readable("just before the end plus retention")
				f.clock.Advance(time.Nanosecond)
				if _, err := f.store.GetSeatLogs(t.Context(), "cli", id, "", "", 100); errorCode(err) != contract.ErrorNotFound {
					t.Fatalf("log outlived the retention from its end: %v", err)
				}
				if result, err := f.store.Reconcile(t.Context()); err != nil || result.PrunedSeatLogs != 1 {
					t.Fatalf("sweep did not prune at the retention boundary: %+v %v", result, err)
				}
				return
			}
			page, err := f.store.GetSeatLogs(t.Context(), "cli", id, "", "", 100)
			if err != nil {
				t.Fatal(err)
			}
			f.clock.Advance(time.Hour)
			switch operation {
			case "read":
				if _, err := f.store.GetSeatLogs(t.Context(), "cli", id, "", "", 100); errorCode(err) != contract.ErrorNotFound {
					t.Fatalf("read ignored configured retention: %v", err)
				}
			case "acknowledge":
				if err := f.store.MarkSeatLogRead(t.Context(), "cli", id, "", page.NextCursor); errorCode(err) != contract.ErrorNotFound {
					t.Fatalf("acknowledgement ignored configured retention: %v", err)
				}
			case "sweep":
				result, err := f.store.Reconcile(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				data, _ := json.Marshal(result)
				var counts map[string]any
				json.Unmarshal(data, &counts)
				if counts["pruned_seat_logs"] != float64(1) {
					t.Fatalf("configured retention did not prune/count log: %s", data)
				}
				if _, err := f.store.GetSeatLogs(t.Context(), "cli", id, "", "", 100); errorCode(err) != contract.ErrorNotFound {
					t.Fatalf("sweep retained log: %v", err)
				}
			}
		})
	}
}
