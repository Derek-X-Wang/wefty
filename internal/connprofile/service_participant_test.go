package connprofile

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestServiceProjectionTracksOnlyItsParticipantInputs(t *testing.T) {
	p := Profile{Version: Version, Participants: map[string]Participant{
		ParticipantL1:        {Name: "wefty://control-plane", StateDirectory: "/state/l1", NodeID: "opaque-control"},
		ParticipantL3:        {Name: "wefty://run-ledger", StateDirectory: "/state/l3", NodeID: "opaque-ledger"},
		ParticipantNodeAgent: {Name: "wefty://node/test", StateDirectory: "/state/node-agent", NodeID: "opaque-agent"},
	}}
	projections := func() []any { return []any{p.L1(), p.L3(), p.NodeAgent()} }
	before := make([]string, 3)
	for i, projection := range projections() {
		before[i], _ = Digest(projection)
	}
	for i, role := range []string{ParticipantL1, ParticipantL3, ParticipantNodeAgent} {
		original := p.Participants[role]
		changed := original
		changed.StateDirectory += "-repaired"
		p.Participants[role] = changed
		for j, projection := range projections() {
			got, _ := Digest(projection)
			if (got != before[j]) != (i == j) {
				t.Errorf("changing %s state folder changed service %d digest=%v", role, j, got != before[j])
			}
		}
		p.Participants[role] = original
	}
	for _, tc := range []struct {
		value any
		peer  string
	}{{p.L1(), "opaque-ledger"}, {p.L3(), "opaque-control"}} {
		payload, _ := json.Marshal(tc.value)
		if !strings.Contains(string(payload), tc.peer) {
			t.Errorf("projection omits trusted peer %q: %s", tc.peer, payload)
		}
	}
}

func TestParticipantIdentityDigestsTrackTheirConsumers(t *testing.T) {
	p := Profile{Participants: map[string]Participant{
		ParticipantL1:        {Name: "wefty://control-plane", StateDirectory: "/state/l1", NodeID: "control"},
		ParticipantL3:        {Name: "wefty://run-ledger", StateDirectory: "/state/l3", NodeID: "ledger"},
		ParticipantNodeAgent: {Name: "wefty://node/test", StateDirectory: "/state/agent", NodeID: "agent"},
		ParticipantClient:    {Name: "wefty://client/test", StateDirectory: "/state/client", NodeID: "client"},
		ParticipantPerson:    {Name: "wefty://person/test", StateDirectory: "/state/person", NodeID: "person", UserID: "owner"},
	}}
	values := func() []any { return []any{p.L1(), p.L3(), p.NodeAgent(), p.ClientService()} }
	for _, tc := range []struct {
		role    string
		changed []bool
	}{
		{ParticipantL1, []bool{true, true, false, false}}, {ParticipantL3, []bool{true, true, false, false}},
		{ParticipantNodeAgent, []bool{false, false, true, false}}, {ParticipantClient, []bool{false, false, false, true}}, {ParticipantPerson, []bool{false, false, false, true}},
	} {
		before := make([]string, 4)
		for i, value := range values() {
			before[i], _ = Digest(value)
		}
		original := p.Participants[tc.role]
		changed := original
		changed.NodeID += "-repaired"
		p.Participants[tc.role] = changed
		for i, value := range values() {
			after, _ := Digest(value)
			if (after != before[i]) != tc.changed[i] {
				t.Errorf("%s identity changed digest %d=%v want=%v", tc.role, i, after != before[i], tc.changed[i])
			}
		}
		p.Participants[tc.role] = original
	}
	p = Profile{L1Address: "opaque-remote-control", L3Address: "opaque-remote-ledger", Participants: map[string]Participant{ParticipantClient: {NodeID: "client"}}}
	if p.PeerNodeID(ParticipantL1) != "" || p.PeerNodeID(ParticipantL3) != "" {
		t.Fatalf("dial endpoints became authorization IDs: %+v", p)
	}
}

func TestParticipantProfileTransitionInvalidatesEveryService(t *testing.T) {
	legacy := Profile{Version: Version, Fabric: "plain"}
	settings := func(p Profile) []any { return []any{p.L1(), p.L3(), p.NodeAgent(), p.ClientService()} }
	for _, role := range []string{ParticipantClient, ParticipantPerson, ParticipantNodeAgent, ParticipantL1, ParticipantL3} {
		strict := legacy
		strict.Participants = map[string]Participant{role: {NodeID: "saved"}}
		for i, before := range settings(legacy) {
			oldDigest, _ := Digest(before)
			after := settings(strict)[i]
			newDigest, _ := Digest(after)
			if oldDigest == newDigest {
				t.Errorf("adding only %s did not invalidate service %d", role, i)
			}
			payload, _ := json.Marshal(before)
			if strings.Contains(string(payload), "participant_profile") {
				t.Errorf("legacy service %d gained marker: %s", i, payload)
			}
			payload, _ = json.Marshal(after)
			if !strings.Contains(string(payload), `"participant_profile":true`) {
				t.Errorf("strict service %d omitted marker: %s", i, payload)
			}
		}
	}
}

func TestParticipantProfileRequiresExplicitFabricMode(t *testing.T) {
	profile := Profile{Version: Version, Participants: map[string]Participant{ParticipantClient: {Name: "wefty://client/test", StateDirectory: "/state/client", NodeID: "saved"}}}
	if err := profile.Validate(); err == nil {
		t.Fatal("participant profile accepted without an explicit Fabric mode")
	}
	for _, mode := range []string{"plain", "tsnet"} {
		profile.Fabric = mode
		if err := profile.Validate(); err != nil {
			t.Fatalf("explicit %s mode: %v", mode, err)
		}
	}
	legacy := Profile{Version: Version}
	if err := legacy.Validate(); err != nil {
		t.Fatalf("legacy default mode changed: %v", err)
	}
}
