package l1

import (
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
)

// Pin the full bound decision, including the empty device-denial decision and
// the membership gate before administrator admission. The extraction must not
// let a device-restricted administrator fall back to a Computer grant.
func TestComputerPolicyAdministratorExtractionPreservesBoundDecision(t *testing.T) {
	until := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	none := ComputerPolicyDecision{Permission: ComputerGrantNone}
	closed := ComputerPolicyDecision{Permission: ComputerGrantNone, PolicyRevision: 7, FreshUntil: until}
	admin := ComputerPolicyDecision{Permission: ComputerGrantControl, Administrator: true, PolicyRevision: 7, FreshUntil: until}
	for _, tc := range []struct {
		name   string
		mutate func(*ComputerPolicySnapshot, *fabric.Identity)
		valid  bool
		bound  ComputerPolicyDecision
		local  bool
	}{
		{"administrator", nil, true, admin, true},
		{"non-administrator", func(s *ComputerPolicySnapshot, _ *fabric.Identity) { s.Admins = nil }, true, closed, false},
		{"unknown-identity", func(_ *ComputerPolicySnapshot, i *fabric.Identity) { *i = fabric.Identity{} }, true, none, false},
		{"foreign-fabric", func(_ *ComputerPolicySnapshot, i *fabric.Identity) { i.FabricID = "other-fabric" }, true, none, false},
		{"machine", func(_ *ComputerPolicySnapshot, i *fabric.Identity) { i.Kind = fabric.IdentityKindMachine }, true, none, false},
		{"missing-device", func(_ *ComputerPolicySnapshot, i *fabric.Identity) { i.DeviceID = "" }, true, none, false},
		{"wrong-device-with-grant", func(s *ComputerPolicySnapshot, _ *fabric.Identity) {
			s.Admins[0].DeviceID = "other-device"
			s.Computers[0].Grants = []ComputerGrant{{FabricID: "fabric", UserID: "person", Permission: ComputerGrantControl}}
		}, true, none, false},
		{"missing-enrolled-device", func(s *ComputerPolicySnapshot, _ *fabric.Identity) { s.Admins[0].DeviceID = "" }, true, none, false},
		{"legacy-device-policy", func(s *ComputerPolicySnapshot, _ *fabric.Identity) {
			s.AdminDeviceRequired = false
			s.Admins[0].DeviceID = ""
		}, true, admin, true},
		{"wrong-computer-membership", func(s *ComputerPolicySnapshot, _ *fabric.Identity) { s.Computers[0].ComputerID = "other" }, true, closed, true},
		{"no-computer-membership", func(s *ComputerPolicySnapshot, _ *fabric.Identity) { s.Computers = nil }, true, closed, true},
		{"stale-snapshot", nil, false, none, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := ComputerPolicySnapshot{IssuingFabricID: "fabric", PolicyRevision: 7, FreshUntil: until, AdminDeviceRequired: true, Admins: []ComputerPolicyAdmin{{FabricID: "fabric", UserID: "person", DeviceID: "device"}}, Computers: []ComputerPolicyComputer{{ComputerID: "computer"}}}
			identity := fabric.Identity{FabricID: "fabric", UserID: "person", DeviceID: "device"}
			if tc.mutate != nil {
				tc.mutate(&snapshot, &identity)
			}
			bound := EvaluateComputerPolicyPerson(snapshot, tc.valid, "computer", identity)
			local := EvaluateComputerPolicyAdministrator(snapshot, tc.valid, identity)
			if !reflect.DeepEqual(bound, tc.bound) || local.Administrator != tc.local {
				t.Fatalf("bound=%+v want=%+v local=%+v want administrator=%t", bound, tc.bound, local, tc.local)
			}
		})
	}
}
