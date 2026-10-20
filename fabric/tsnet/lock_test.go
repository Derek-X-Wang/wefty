package tsnet

import (
	"context"
	"errors"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestEmbeddedOwnerLocksBeforeBackendStart(t *testing.T) {
	state := t.TempDir()
	for _, name := range []string{"wefty://client/test", "wefty://person/test", "wefty://control-plane", "wefty://run-ledger", "wefty://node/test"} {
		t.Run(name, func(t *testing.T) {
			first, err := New(Config{Name: name, StateDir: state})
			if err != nil {
				t.Fatal(err)
			}
			second, err := New(Config{Name: name, StateDir: state})
			if second != nil {
				_ = second.Close()
			}
			var inUse *fabric.ParticipantInUseError
			if !errors.As(err, &inUse) {
				t.Fatalf("second backend constructed: %v", err)
			}
			called := false
			enroller := Enroller{newNode: func(Config) (enrolNode, error) { called = true; return nil, errors.New("must not start") }}
			_, enrolErr := enroller.Enrol(context.Background(), fabric.EnrolSpec{Name: name, StateDirectory: state, Credential: fabric.Credential{Value: "fake"}})
			logoutErr := enroller.Logout(context.Background(), fabric.EnrolSpec{Name: name, StateDirectory: state})
			if called || !errors.As(enrolErr, &inUse) || !errors.As(logoutErr, &inUse) {
				t.Fatalf("enrol/logout started locked backend: called=%v enrol=%v logout=%v", called, enrolErr, logoutErr)
			}
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}
			next, err := New(Config{Name: name, StateDir: state})
			if err != nil {
				t.Fatal(err)
			}
			_ = next.Close()
		})
	}
}
