package connprofile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestProfileReadsMachineParticipants(t *testing.T) {
	path := filepath.Join(t.TempDir(), "profile.json")
	payload := `{"version":1,"fabric":"tsnet","l1_address":"opaque-control-id","l3_address":"opaque-ledger-id","participants":{"client":{"name":"wefty://client/machine-1","state_directory":"/data/fabric/client","node_id":"opaque-client-id"},"person":{"name":"wefty://person/machine-1","state_directory":"/data/fabric/person","node_id":"opaque-person-id","user_id":"owner-id"}}}`
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	profile, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := Encode(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(path); err != nil {
		t.Fatal(err)
	}
}

func TestProfileRejectsInvalidParticipants(t *testing.T) {
	for _, tc := range []struct {
		name, role, logical, folder string
		shared                      bool
	}{
		{"wrong-role-prefix", ParticipantClient, "wefty://person/owner", "/data/client", false},
		{"service-suffix", ParticipantL1, "wefty://control-plane-x", "/data/l1", false},
		{"unknown-role", "other", "wefty://client/one", "/data/one", false},
		{"relative-folder", ParticipantClient, "wefty://client/one", "data/one", false},
		{"shared-folder", ParticipantClient, "wefty://client/one", "/data/one", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := Profile{Version: Version, Participants: map[string]Participant{tc.role: {Name: tc.logical, StateDirectory: tc.folder, NodeID: "opaque"}}}
			if tc.shared {
				p.Participants[ParticipantPerson] = Participant{Name: "wefty://person/one", StateDirectory: tc.folder, NodeID: "owner"}
			}
			if err := p.Validate(); err == nil {
				t.Fatal("accepted invalid participant")
			}
		})
	}
}
