package fabricconfig

import (
	"context"
	"errors"

	"github.com/Derek-X-Wang/wefty/fabric"
)

// CheckFleetPeers uses opaque IDs and authenticated network evidence only.
// The returned vocabulary is bounded and safe for a durable setup failure record.
func CheckFleetPeers(ctx context.Context, peers fabric.PeerLookup, l1, l3 string) string {
	for _, peer := range []struct{ role, id, tag string }{{"l1", l1, "tag:wefty-control"}, {"l3", l3, "tag:wefty-client"}} {
		identity, err := peers.LookupPeer(ctx, peer.id)
		if errors.Is(err, fabric.ErrIdentityNotFound) {
			return peer.role + ":peer_not_found"
		}
		if err != nil {
			return peer.role + ":network_unavailable"
		}
		if identity.NodeID != peer.id || len(identity.Tags) != 1 || identity.Tags[0] != peer.tag {
			return peer.role + ":role_mismatch"
		}
	}
	return ""
}
