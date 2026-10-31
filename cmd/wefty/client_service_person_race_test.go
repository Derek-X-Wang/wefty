package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/local"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
)

func TestClientServiceKeepsClientWhenPersonStateDisappearsBeforeOpen(t *testing.T) {
	f := newServiceParticipantFixture(t, "wefty")
	personState := filepath.Join(f.home, "fabric", "person")
	if err := os.MkdirAll(personState, 0o700); err != nil {
		t.Fatal(err)
	}
	f.profile.Participants[connprofile.ParticipantPerson] = connprofile.Participant{Name: "wefty://person/stable-node", StateDirectory: personState, NodeID: "person-saved", UserID: "owner"}
	writeServicePlainIdentity(t, personState, f.profile.ServiceParticipant(connprofile.ParticipantPerson).ExpectedIdentity)
	f.write(t)
	options, _, err := parseGlobalOptions([]string{"--profile=" + f.path, "client-service"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	var raced atomic.Bool
	open := func(ctx context.Context, config fabricconfig.Config) (fabric.Fabric, fabricconfig.CloseFunc, error) {
		if config.ExpectedIdentity.UserID != "" {
			// SavedService has already observed saved state. Remove it precisely at
			// the production Open boundary, then use the real backend construction.
			if err := os.Remove(filepath.Join(personState, "plain-identity.json")); err != nil {
				return nil, nil, err
			}
			raced.Store(true)
		}
		return fabricconfig.Open(ctx, config)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	var serviceErr error
	go func() {
		serviceErr = executeClientServiceWithOpener(ctx, options, []string{"--socket=" + f.socket}, io.Discard, io.Discard, open)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(testwait.Bound):
			t.Error("client service did not stop")
		}
	})
	deadline := time.Now().Add(3 * time.Second)
	for {
		select {
		case <-done:
			t.Fatalf("optional-person race stopped client service: %v", serviceErr)
		default:
		}
		if _, err := os.Stat(f.socket); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("client service did not publish its socket")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !raced.Load() {
		t.Fatal("test did not exercise the saved-state/Open race")
	}
	client, err := local.New(f.socket, local.Client, "opaque-client")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Check(t.Context()); err != nil {
		t.Fatalf("client unavailable after optional-person race: %v", err)
	}
	person, err := local.New(f.socket, local.Person, "person-saved")
	if err != nil {
		t.Fatal(err)
	}
	var refused *fabric.ParticipantIdentityError
	if err := person.Check(t.Context()); !errors.As(err, &refused) || !refused.Person {
		t.Fatalf("person did not retain its identity refusal: %v", err)
	}
}
