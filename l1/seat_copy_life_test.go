package l1

import (
	"github.com/Derek-X-Wang/wefty/contract"
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
				f.clock.Advance(31 * time.Minute)
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

func TestSeatCopyFrontDoorPublicationIsAttemptFenced(t *testing.T) {
	f, _, _, auth := liveSeatForCopy(t)
	ready := true
	endpoint := "ws://node:80" + contract.ComputerDisplayWebSocketPath
	request := PublicationRequest{FencingToken: auth.FencingToken, Ready: &ready, DisplayEndpoint: &endpoint}
	if _, err := f.store.SetAttemptPublication(t.Context(), "fabric-computer-node", auth.JobID, auth.AttemptID, request); err != nil {
		t.Fatal(err)
	}
	request.FencingToken = "stale"
	if _, err := f.store.SetAttemptPublication(t.Context(), "fabric-computer-node", auth.JobID, auth.AttemptID, request); err == nil {
		t.Fatal("stale publication accepted")
	}
	ready = false
	request = PublicationRequest{FencingToken: auth.FencingToken, Ready: &ready}
	if _, err := f.store.SetAttemptPublication(t.Context(), "fabric-computer-node", auth.JobID, auth.AttemptID, request); err != nil {
		t.Fatal(err)
	}
}
