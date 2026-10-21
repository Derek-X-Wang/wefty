package serviceplan

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
)

func TestControlPlaneProfileRequiresOwnNodeAdmission(t *testing.T) {
	p := connprofile.Profile{Version: connprofile.Version, Fabric: "tsnet", L1Address: "opaque-l1", L3Address: "opaque-l3", Identities: connprofile.Identities{Node: "machine-1"}, AutomaticNodeAdmission: true, Participants: map[string]connprofile.Participant{}}
	for _, role := range []string{"l1", "l3", "node-agent", "client"} {
		name := "wefty://client/machine-1"
		if role == "l1" {
			name = "wefty://control-plane"
		}
		if role == "l3" {
			name = "wefty://run-ledger"
		}
		if role == "node-agent" {
			name = "wefty://node/machine-1"
		}
		p.Participants[role] = connprofile.Participant{Name: name, NetworkName: "wefty-" + role, NodeID: "opaque-" + role, StateDirectory: filepath.Join("/state", role)}
	}
	p.NodeAdmission = []connprofile.NodeAdmission{{NodeID: "machine-1", Tags: []string{"wefty:node:machine-1"}, MaxOneshotSlots: 4, MaxServiceSlots: 2}}
	if err := ValidateProfile(ControlPlane, p); err != nil {
		t.Fatalf("complete control plane: %v", err)
	}
	for _, ownMissing := range [][]connprofile.NodeAdmission{nil, {{NodeID: "other-machine", MaxOneshotSlots: 4, MaxServiceSlots: 2}}} {
		p.NodeAdmission = ownMissing
		if err := ValidateProfile(ControlPlane, p); err == nil || !strings.Contains(err.Error(), "node_admission") {
			t.Fatalf("missing own admission accepted: %v", err)
		}
	}
}

func TestJoinProfileHasNoControlPlaneAuthority(t *testing.T) {
	p := connprofile.Profile{Version: 1, Fabric: "tsnet", L1Address: "opaque-l1", L3Address: "opaque-l3", Identities: connprofile.Identities{Node: "machine-1"}, Participants: map[string]connprofile.Participant{}}
	for _, role := range []string{"node-agent", "client"} {
		name := "wefty://client/machine-1"
		if role == "node-agent" {
			name = "wefty://node/machine-1"
		}
		p.Participants[role] = connprofile.Participant{Name: name, NodeID: "opaque-" + role, NetworkName: "wefty-" + role, StateDirectory: filepath.Join("/state", role)}
	}
	if err := ValidateProfile(Join, p); err != nil {
		t.Fatal(err)
	}
	p.Participants["l1"] = connprofile.Participant{Name: "wefty://control-plane", NodeID: "opaque-l1", NetworkName: "wefty-control-plane", StateDirectory: "/state/l1"}
	if err := ValidateProfile(Join, p); err == nil {
		t.Fatal("joining profile accepted L1 participant")
	}
	delete(p.Participants, "l1")
	p.NodeAdmission = []connprofile.NodeAdmission{{NodeID: "machine-1", MaxOneshotSlots: 4, MaxServiceSlots: 2}}
	if err := ValidateProfile(Join, p); err == nil {
		t.Fatal("joining profile accepted local admission policy")
	}
}
