package connprofile

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProfileNetworkNameRoundTripAndValidation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		valid bool
	}{
		{"", true}, {"wefty-studio-client", true}, {"wefty-studio-client-2", true},
		{"studio", true}, {strings.Repeat("a", 63), true},
		{"bad.name", false}, {"Bad", false}, {"-bad", false}, {"bad-", false},
		{"bad_name", false}, {"bad name", false}, {"bad\nname", false}, {strings.Repeat("a", 64), false},
	} {
		name := tc.name
		t.Run(name, func(t *testing.T) {
			profile := Profile{Version: Version, Fabric: "tsnet", Participants: map[string]Participant{ParticipantClient: {Name: "wefty://client/studio", StateDirectory: "/data/client", NodeID: "opaque", NetworkName: name}}}
			payload, err := Encode(profile)
			if !tc.valid {
				if err == nil {
					t.Fatal("invalid network name saved")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if name == "" && strings.Contains(string(payload), "network_name") {
				t.Fatal("legacy record gained network name")
			}
			path := filepath.Join(t.TempDir(), "profile.json")
			if err := os.WriteFile(path, payload, 0600); err != nil {
				t.Fatal(err)
			}
			read, err := Read(path)
			if err != nil || read.Participants[ParticipantClient].NetworkName != name {
				t.Fatalf("read=%+v err=%v", read, err)
			}
		})
	}
}

func TestNetworkNameChangesOnlyOwningServiceDigest(t *testing.T) {
	p := Profile{Participants: map[string]Participant{ParticipantL1: {NodeID: "l1"}, ParticipantL3: {NodeID: "l3"}, ParticipantNodeAgent: {NodeID: "agent"}, ParticipantClient: {NodeID: "client"}, ParticipantPerson: {NodeID: "person"}}}
	values := func() []any { return []any{p.L1(), p.L3(), p.NodeAgent(), p.ClientService()} }
	for _, tc := range []struct {
		role  string
		owner int
	}{{ParticipantL1, 0}, {ParticipantL3, 1}, {ParticipantNodeAgent, 2}, {ParticipantClient, 3}, {ParticipantPerson, 3}} {
		before := make([]string, 4)
		for i, value := range values() {
			before[i], _ = Digest(value)
			raw, _ := json.Marshal(value)
			if strings.Contains(string(raw), "network_name") {
				t.Fatal("legacy projection gained field")
			}
		}
		saved := p.Participants[tc.role]
		changed := saved
		changed.NetworkName = "wefty-studio-2"
		p.Participants[tc.role] = changed
		for i, value := range values() {
			after, _ := Digest(value)
			if (before[i] != after) != (i == tc.owner) {
				t.Errorf("changing %s changed service %d=%v", tc.role, i, before[i] != after)
			}
		}
		p.Participants[tc.role] = saved
	}
}
