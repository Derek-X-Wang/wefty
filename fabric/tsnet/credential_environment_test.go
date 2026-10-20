package tsnet

import (
	"os"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
)

// Bootstrap callers snapshot the environment once, then pass each embedded
// participant its explicit in-memory credential, even after New clears it.
func TestExplicitJoinCredentialSurvivesAmbientKeyClearing(t *testing.T) {
	t.Setenv("TS_AUTHKEY", "test-single-use-bootstrap-key")
	credential := fabric.Credential{Value: os.Getenv("TS_AUTHKEY")}
	for _, name := range []string{"wefty://node/bootstrap-one", "wefty://node/bootstrap-two"} {
		participant, err := New(Config{Name: name, StateDir: t.TempDir(), Credential: credential})
		if err != nil {
			t.Fatal(err)
		}
		if participant.server.AuthKey != "test-single-use-bootstrap-key" {
			t.Fatal("explicit bootstrap credential was lost")
		}
		if os.Getenv("TS_AUTHKEY") != "" {
			t.Fatal("ambient key remains available to later processes")
		}
	}
}
