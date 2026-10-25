package l1

import (
	"testing"
	"time"
)

func TestSeatCopySendingResetsIdleAndHoldsLongTransfer(t *testing.T) {
	for _, tc := range []struct {
		name    string
		long    bool
		outcome string
	}{{"call", false, "sent"}, {"hold", true, "sent"}, {"failed", true, "failed"}} {
		t.Run(tc.name, func(t *testing.T) {
			long := tc.long
			f, id, secret, auth := liveSeatForCopy(t)
			f.clock.Advance(20 * time.Minute)
			r := SeatCopyReceipt{SeatCopyAuthorityInput: auth, CopyID: "node-copy", Direction: "out", Path: "report", Outcome: "sending"}
			if err := f.store.RecordSeatCopy(t.Context(), "fabric-computer-node", id, r); err != nil {
				t.Fatal(err)
			}
			page, err := f.store.GetSeatLogs(t.Context(), "cli", id, secret, "", 100)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, e := range page.Events {
				found = found || e.CopyID == r.CopyID && e.NodeOutcome == "sending" && *e.SizeBytes == 0
			}
			if !found {
				t.Fatal("no pre-stream sending event")
			}
			if long {
				// The pending event and running hold are durable across L1 restart.
				f.reopen(t)
				for range 62 {
					f.clock.Advance(30 * time.Second)
					if err := f.store.RecordSeatCopy(t.Context(), "fabric-computer-node", id, r); err != nil {
						t.Fatal(err)
					}
				}
				if err := f.store.CheckSeatLife(t.Context()); err != nil {
					t.Fatal(err)
				}
				v, err := f.store.GetSeatRequest(t.Context(), id)
				if err != nil || v.State != SeatLive {
					t.Fatalf("long copy expired: %+v %v", v, err)
				}
			}
			r.Outcome, r.SizeBytes = tc.outcome, 999
			if tc.outcome == "failed" {
				r.Reason = "copy_failed"
			}
			if err := f.store.RecordSeatCopy(t.Context(), "fabric-computer-node", id, r); err != nil {
				t.Fatal(err)
			}
			// A delayed retry of the pre-stream receipt cannot reacquire a hold.
			start := r
			start.Outcome, start.SizeBytes, start.Reason = "sending", 0, ""
			if err := f.store.RecordSeatCopy(t.Context(), "fabric-computer-node", id, start); err != nil {
				t.Fatal(err)
			}
			f.clock.Advance(29 * time.Minute)
			if err := f.store.CheckSeatLife(t.Context()); err != nil {
				t.Fatal(err)
			}
			v, err := f.store.GetSeatRequest(t.Context(), id)
			if err != nil || v.State != SeatLive {
				t.Fatalf("copy did not reset idle origin: %+v %v", v, err)
			}
			f.clock.Advance(time.Minute)
			if err := f.store.CheckSeatLife(t.Context()); err != nil {
				t.Fatal(err)
			}
			v, err = f.store.GetSeatRequest(t.Context(), id)
			if err != nil || v.State != SeatEnding || v.Reason != "expired" {
				t.Fatalf("finished copy retained its hold: %+v %v", v, err)
			}
		})
	}
}

func TestSeatCopyLostReceiptLeaseExpires(t *testing.T) {
	f, id, _, auth := liveSeatForCopy(t)
	r := SeatCopyReceipt{SeatCopyAuthorityInput: auth, CopyID: "lost-final", Direction: "out", Path: "file", Outcome: "sending"}
	if err := f.store.RecordSeatCopy(t.Context(), "fabric-computer-node", id, r); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	f.clock.Advance(30 * time.Minute)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	v, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || v.State != SeatLive {
		t.Fatalf("lease release did not restart idle wait: %+v %v", v, err)
	}
	f.clock.Advance(time.Minute)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	v, err = f.store.GetSeatRequest(t.Context(), id)
	if err != nil || v.State != SeatEnding {
		t.Fatalf("lost receipt held Seat forever: %+v %v", v, err)
	}
}
func TestSeatCopyHoldIsNotCommandAndEndsWithSeat(t *testing.T) {
	f, id, _, auth := liveSeatForCopy(t)
	r := SeatCopyReceipt{SeatCopyAuthorityInput: auth, CopyID: "copy-hold", Direction: "out", Path: "file", Outcome: "sending"}
	if err := f.store.RecordSeatCopy(t.Context(), "fabric-computer-node", id, r); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	commands, err := f.store.SeatCommandsToCancel(t.Context(), id)
	if err != nil || len(commands) != 0 {
		t.Fatalf("copy exposed as command: %v %v", commands, err)
	}
	if err := f.store.RecordSeatCopy(t.Context(), "fabric-computer-node", id, r); err == nil {
		t.Fatal("ended Seat renewed copy")
	}
}

func TestSeatCopyLeaseSurvivesSameBootRestartAndIgnoresCalendar(t *testing.T) {
	f, id, _, auth := liveSeatForCopy(t)
	clock := &seatBootTestClock{fakeClock: f.clock, boot: "same-boot", elapsed: time.Hour}
	f.store.clock = clock
	r := SeatCopyReceipt{SeatCopyAuthorityInput: auth, CopyID: "same-boot", Direction: "out", Path: "file", Outcome: "sending"}
	if err := f.store.RecordSeatCopy(t.Context(), "fabric-computer-node", id, r); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	f.store, err = OpenStore(f.path, StoreOptions{Clock: clock, LeaseDuration: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	// Calendar changes must not expire or extend the copy lease.
	f.clock.Advance(24 * time.Hour)
	clock.elapsed += 30 * time.Second
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	v, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || v.State != SeatLive {
		t.Fatalf("restart lost lease: %+v %v", v, err)
	}
	clock.elapsed += 30*time.Minute + 30*time.Second
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	v, err = f.store.GetSeatRequest(t.Context(), id)
	if err != nil || v.State != SeatEnding {
		t.Fatalf("calendar/restart extended lease: %+v %v", v, err)
	}
}

func TestSeatCopyAttemptEndClearsCopyHolds(t *testing.T) {
	f, id, _, auth := liveSeatForCopy(t)
	r := SeatCopyReceipt{SeatCopyAuthorityInput: auth, CopyID: "attempt-end", Direction: "out", Path: "file", Outcome: "sending"}
	if err := f.store.RecordSeatCopy(t.Context(), "fabric-computer-node", id, r); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ObserveAttemptImage(t.Context(), "fabric-computer-node", auth.JobID, auth.AttemptID, testImageObservation(auth.FencingToken)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.StartAttempt(t.Context(), "fabric-computer-node", auth.JobID, auth.AttemptID, StartedRequest{FencingToken: auth.FencingToken}); err != nil {
		t.Fatal(err)
	}
	exit := 0
	if _, err := f.store.CompleteAttempt(t.Context(), "fabric-computer-node", auth.JobID, auth.AttemptID, CompletionRequest{FencingToken: auth.FencingToken, IdempotencyKey: "copy-attempt-end", Result: ProcessResult{ExitCode: &exit}}); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := f.store.db.QueryRow(`SELECT COUNT(*) FROM seat_copy_holds WHERE attempt_id=?`, auth.AttemptID).Scan(&n); err != nil || n != 0 {
		t.Fatalf("ended Attempt retained copy holds: %d %v", n, err)
	}
}

func TestSeatCopyUpgradeBoundsLegacyCommandHold(t *testing.T) {
	f, id, _, auth := liveSeatForCopy(t)
	if _, err := f.store.db.Exec(`INSERT INTO seat_activity_holds(seat_id,kind,hold_id,active) VALUES(?,'command','copy:legacy',1)`, id); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	var n int
	if err := f.store.db.QueryRow(`SELECT COUNT(*) FROM seat_copy_holds WHERE seat_id=? AND attempt_id=?`, id, auth.AttemptID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("legacy hold was not leased: %d %v", n, err)
	}
	if err := f.store.db.QueryRow(`SELECT COUNT(*) FROM seat_activity_holds WHERE seat_id=? AND kind='command'`, id).Scan(&n); err != nil || n != 0 {
		t.Fatalf("legacy copy still a command: %d %v", n, err)
	}
	// Opening again must not extend the recovered lease.
	f.clock.Advance(30 * time.Second)
	f.reopen(t)
	f.clock.Advance(30*time.Minute + 30*time.Second)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	v, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || v.State != SeatEnding {
		t.Fatalf("restart extended migrated hold: %+v %v", v, err)
	}
}
