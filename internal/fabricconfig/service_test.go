package fabricconfig

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/tsnet"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
)

func TestTrustedServicePeerRequiresEnrolledRecord(t *testing.T) {
	for _, peer := range []string{connprofile.ParticipantL3, connprofile.ParticipantL1} {
		for _, address := range []string{"run-ledger", "wefty://run-ledger", "127.0.0.1:2", "control-plane", "wefty://control-plane"} {
			t.Run(peer+"/"+address, func(t *testing.T) {
				profile := connprofile.Profile{Fabric: "plain", L1Address: address, L3Address: address, Participants: map[string]connprofile.Participant{connprofile.ParticipantClient: {NodeID: "client"}}}
				got, err := TrustedServicePeer(profile, peer, "legacy-default", false)
				var refused *fabric.ParticipantIdentityError
				if got != "" || !errors.As(err, &refused) {
					t.Fatalf("missing peer trusted address %q: ID=%q error=%v", address, got, err)
				}
			})
		}
	}
}

func TestSavedServicePassesRecordedNetworkNameToBackend(t *testing.T) {
	for _, networkName := range []string{"", "wefty-studio-2"} {
		t.Run(networkName, func(t *testing.T) {
			state := t.TempDir()
			if err := os.WriteFile(filepath.Join(state, "saved-state"), []byte("saved"), 0600); err != nil {
				t.Fatal(err)
			}
			p := connprofile.Profile{Fabric: "tsnet", Participants: map[string]connprofile.Participant{connprofile.ParticipantNodeAgent: {Name: "wefty://node/studio-0123abcd", NetworkName: networkName, StateDirectory: state, NodeID: "opaque"}}}
			config, err := SavedService(p, connprofile.ParticipantNodeAgent, Config{Mode: "tsnet"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, closeFabric, err := open(t.Context(), config, func(got tsnet.Config) (savedParticipant, error) {
				if got.NetworkName != networkName {
					t.Fatalf("backend network name=%q want=%q", got.NetworkName, networkName)
				}
				return &contextParticipant{verify: func(context.Context) error { return nil }}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := closeFabric(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
