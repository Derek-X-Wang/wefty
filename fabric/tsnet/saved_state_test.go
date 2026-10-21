package tsnet

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/participantstate"
)

func TestSavedParticipantRequiresStateWhileHoldingOwnership(t *testing.T) {
	state := t.TempDir()
	if err := os.Chmod(state, 0700); err != nil {
		t.Fatal(err)
	}
	backend, err := New(Config{Name: "wefty://client/test", StateDir: state, ExpectedIdentity: &fabric.Identity{NodeID: "saved", Tags: []string{"tag:wefty-client"}}})
	if backend != nil {
		defer backend.Close()
	}
	var refused *fabric.ParticipantIdentityError
	if !errors.As(err, &refused) {
		t.Fatalf("missing saved state constructed backend: %v", err)
	}
	release, err := participantstate.Acquire(state, "wefty://client/test")
	if err != nil {
		t.Fatalf("refused backend retained ownership: %v", err)
	}
	defer release()
	if participantstate.HasSavedState(state) {
		t.Fatal("refusal created saved backend state")
	}
}

func savedBackendState(t *testing.T) string {
	t.Helper()
	state := t.TempDir()
	if err := os.Chmod(state, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, "tailscaled.state"), []byte("saved test backend"), 0o600); err != nil {
		t.Fatal(err)
	}
	return state
}
