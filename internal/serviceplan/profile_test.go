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
