package fabricconfig

import (
	"errors"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
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
