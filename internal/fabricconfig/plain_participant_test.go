package fabricconfig

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestPlainParticipantVerifiesSavedIdentityBeforeOpening(t *testing.T) {
	for _, tc := range []struct {
		name    string
		actual  fabric.Identity
		refused bool
	}{
		{"matching", fabric.Identity{NodeID: "saved", Tags: []string{"tag:wefty-control"}}, false},
		{"replaced", fabric.Identity{NodeID: "other", Tags: []string{"tag:wefty-control"}}, true},
		{"wrong-tag", fabric.Identity{NodeID: "saved", Tags: []string{"tag:wefty-client"}}, true},
		{"extra-tag", fabric.Identity{NodeID: "saved", Tags: []string{"tag:wefty-control", "tag:wefty-client"}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := t.TempDir()
			payload, _ := json.Marshal(tc.actual)
			if err := os.WriteFile(filepath.Join(state, "plain-identity.json"), payload, 0o600); err != nil {
				t.Fatal(err)
			}
			want := fabric.Identity{NodeID: "saved", Tags: []string{"tag:wefty-control"}}
			_, closeBackend, err := Open(t.Context(), Config{Mode: "plain", Name: "wefty://control-plane", StateDirectory: state, Identity: want, ExpectedIdentity: &want})
			if closeBackend != nil {
				defer closeBackend()
			}
			var identity *fabric.ParticipantIdentityError
			if errors.As(err, &identity) != tc.refused || !tc.refused && err != nil {
				t.Fatalf("saved identity refusal=%v want=%v err=%v", identity, tc.refused, err)
			}
		})
	}
}
