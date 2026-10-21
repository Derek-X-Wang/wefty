package tsnet

import (
	"context"
	"slices"

	"github.com/Derek-X-Wang/wefty/fabric"
)

// LookupPeer selects a remote opaque node ID from authenticated network state.
// It never resolves a name, consults DNS, or dials a transport endpoint.
func (f *Fabric) LookupPeer(ctx context.Context, nodeID string) (fabric.Identity, error) {
	if err := f.checkParticipant(ctx); err != nil {
		return fabric.Identity{}, err
	}
	status, err := f.runningStatus(ctx)
	if err != nil {
		return fabric.Identity{}, participantStatusError{cause: err}
	}
	if nodeID == "" || status.Self != nil && string(status.Self.ID) == nodeID {
		return fabric.Identity{}, fabric.ErrIdentityNotFound
	}
	var result fabric.Identity
	for _, peer := range status.Peer {
		if peer == nil || string(peer.ID) != nodeID || peer.Expired {
			continue
		}
		if result.NodeID != "" {
			return fabric.Identity{}, fabric.ErrIdentityIncomplete
		}
		result = fabric.Identity{NodeID: nodeID, DeviceID: nodeID}
		if peer.Tags != nil {
			result.Tags = slices.Clone(peer.Tags.AsSlice())
		}
	}
	if result.NodeID == "" {
		return result, fabric.ErrIdentityNotFound
	}
	return result, nil
}
