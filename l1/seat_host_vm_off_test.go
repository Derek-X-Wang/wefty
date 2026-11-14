package l1

import (
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// The owner's off switch suppresses the Computer attempt's completion, so L1
// sees only the lease expiry. The Seat must name the off switch (#1671). With
// OCI on, a lease loss keeps the Seat until the replacement Attempt ends it as
// "the Computer restarted". The lease is shorter than the Seat idle wait, so
// idle expiry never decides the outcome.
func TestSeatEndsWithHostVMOffReasonAtLeaseExpiry(t *testing.T) {
	for _, tc := range []struct {
		name   string
		off    bool
		reason string
	}{
		{name: "off switch", off: true, reason: seatEndHostVMOff},
		{name: "lease loss with OCI on", reason: "the Computer restarted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, id, secret, old := liveSeatWithLease(t, 30*time.Second)
			revision := int64(1)
			heartbeat := func(ociOn bool) {
				t.Helper()
				revision++
				hb := heartbeatRequestForBoot(f.node, f.node.BootSessionID)
				hb.CapabilityRevision = revision
				hb.Capabilities = map[string]bool{CapabilitySeatRequests: true, "kind:oci": true, "cgroup_v2": true, "computer": true}
				hb.MissingCapabilities = []string{}
				hb.CapabilityReasonCode = ""
				if !ociOn {
					hb.Capabilities = map[string]bool{CapabilitySeatRequests: true}
					hb.MissingCapabilities = []string{"kind:oci", "computer", "cgroup_v2"}
					hb.CapabilityReasonCode = contract.CapabilityReasonOCIIntentDisabled
				}
				policy := NodePolicy{Tags: []string{contract.StableNodeTagPrefix + f.node.NodeID}, MaxServiceSlots: 4}
				if _, err := f.store.HeartbeatNodeWithCapabilityObservation(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, heartbeatCapabilityObservation(hb), policy); err != nil {
					t.Fatal(err)
				}
			}
			if tc.off {
				heartbeat(false)
			}
			f.clock.Advance(31 * time.Second)
			if result, err := f.store.Reconcile(t.Context()); err != nil || result.ExpiredAttempts != 1 {
				t.Fatalf("reconcile=%+v %v", result, err)
			}
			v, err := f.store.GetSeatRequest(t.Context(), id)
			wantAfterExpiry := SeatLive
			if tc.off {
				wantAfterExpiry = SeatEnding
			}
			if err != nil || v.State != wantAfterExpiry || (tc.off && v.Reason != tc.reason) {
				t.Fatalf("Seat after lease expiry=%+v %v", v, err)
			}

			// The owner turns OCI back on and a replacement Attempt starts.
			f.clock.Advance(2 * time.Minute)
			heartbeat(true)
			claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
			if err != nil || claim == nil || claim.Lease.AttemptID == old.AttemptID {
				t.Fatalf("replacement claim=%+v %v", claim, err)
			}
			replacement := "seat_00000000000000000000000000000002"
			if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: replacement, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
				t.Fatal(err)
			}
			f.request(t, 2, true)
			// The first Seat still holds its room until cleanup proof.
			if r := f.reserve(t, replacement, f.computer, 2, DefaultComputerRoomConfig(4<<30)); r.State != SeatApproved {
				t.Fatalf("replacement reservation=%+v", r)
			}
			if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", replacement, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ReadyWait: time.Minute}); err != nil {
				t.Fatal(err)
			}
			auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken}
			if _, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth); err != nil {
				t.Fatal(err)
			}

			// Late calls from the lost Attempt must not end anything again.
			if _, err := f.store.RenewLease(t.Context(), "fabric-computer-node", old.JobID, old.AttemptID, old.FencingToken); err == nil {
				t.Fatalf("lost Attempt renewal: %v", err)
			}
			exit := 0
			if _, err := f.store.CompleteAttempt(t.Context(), "fabric-computer-node", old.JobID, old.AttemptID, CompletionRequest{FencingToken: old.FencingToken, IdempotencyKey: "late-off", Result: ProcessResult{ExitCode: &exit}}); err == nil {
				t.Fatalf("lost Attempt completion: %v", err)
			}
			if _, err := f.store.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}

			v, err = f.store.GetSeatRequest(t.Context(), id)
			if err != nil || v.State != SeatEnding || v.Reason != tc.reason {
				t.Fatalf("first Seat=%+v %v", v, err)
			}
			page, err := f.store.GetSeatLogs(t.Context(), "cli", id, secret, "", 100)
			if err != nil {
				t.Fatal(err)
			}
			ends := 0
			for _, event := range page.Events {
				if event.Kind == SeatEventEnded {
					ends++
				}
			}
			if !page.Ended || page.Reason != tc.reason || ends != 1 {
				t.Fatalf("Seat log ended=%v reason=%q end events=%d", page.Ended, page.Reason, ends)
			}
			next, err := f.store.GetSeatRequest(t.Context(), replacement)
			if err != nil || next.State != SeatStarting {
				t.Fatalf("replacement Attempt's Seat=%+v %v", next, err)
			}
		})
	}
}
