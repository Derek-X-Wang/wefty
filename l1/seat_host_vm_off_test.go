package l1

import (
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// The owner's off switch suppresses the Computer attempt's completion, so L1
// sees only the lease expiry. The Seat must name the off switch (#1671);
// an ordinary lease loss leaves the Seat to the existing restart paths.
func TestSeatEndsWithHostVMOffReasonAtLeaseExpiry(t *testing.T) {
	// The fixture's one-hour lease outlives the Seat idle wait, so the
	// control case ends by idle expiry; it must not carry the off reason.
	for _, tc := range []struct {
		name string
		off  bool
	}{
		{name: "off switch", off: true},
		{name: "lease loss with OCI on"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, id, secret, _ := liveSeatForCopy(t)
			if tc.off {
				hb := heartbeatRequestForBoot(f.node, f.node.BootSessionID)
				hb.CapabilityRevision = 2
				hb.Capabilities = map[string]bool{CapabilitySeatRequests: true}
				hb.MissingCapabilities = []string{"kind:oci", "computer", "cgroup_v2"}
				hb.CapabilityReasonCode = contract.CapabilityReasonOCIIntentDisabled
				policy := NodePolicy{Tags: []string{contract.StableNodeTagPrefix + f.node.NodeID}, MaxServiceSlots: 4}
				if _, err := f.store.HeartbeatNodeWithCapabilityObservation(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, heartbeatCapabilityObservation(hb), policy); err != nil {
					t.Fatal(err)
				}
			}
			f.clock.Advance(time.Hour + time.Second)
			if result, err := f.store.Reconcile(t.Context()); err != nil || result.ExpiredAttempts != 1 {
				t.Fatalf("reconcile=%+v %v", result, err)
			}
			v, err := f.store.GetSeatRequest(t.Context(), id)
			if err != nil || v.State != SeatEnding || (v.Reason == seatEndHostVMOff) != tc.off {
				t.Fatalf("Seat after lease expiry=%+v %v", v, err)
			}
			page, err := f.store.GetSeatLogs(t.Context(), "cli", id, secret, "", 100)
			if err != nil {
				t.Fatal(err)
			}
			if !page.Ended || (page.Reason == seatEndHostVMOff) != tc.off {
				t.Fatalf("Seat log ended=%v reason=%q", page.Ended, page.Reason)
			}
		})
	}
}
