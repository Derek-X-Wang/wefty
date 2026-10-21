package fabricconfig

import (
	"context"
	"errors"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
)

type lookupPeers func(context.Context, string) (fabric.Identity, error)

func (f lookupPeers) LookupPeer(ctx context.Context, id string) (fabric.Identity, error) {
	return f(ctx, id)
}
func TestCheckFleetPeersRefusesWrongIdentityAndTags(t *testing.T) {
	for _, tc := range []struct {
		name, peer, want string
		tags             []string
		wrongID          bool
		err              error
	}{
		{name: "valid"},
		{name: "wrong control tag", peer: "l1", tags: []string{"tag:wefty-client"}, want: "l1:role_mismatch"},
		{name: "extra control tag", peer: "l1", tags: []string{"tag:wefty-control", "tag:wefty-client"}, want: "l1:role_mismatch"},
		{name: "extra ledger tags", peer: "l3", tags: []string{"tag:wefty-control", "tag:wefty-client", "tag:wefty-agent"}, want: "l3:role_mismatch"},
		{name: "duplicate ledger tag", peer: "l3", tags: []string{"tag:wefty-client", "tag:wefty-client"}, want: "l3:role_mismatch"},
		{name: "wrong ledger tag", peer: "l3", tags: []string{"tag:wefty-agent"}, want: "l3:role_mismatch"},
		{name: "wrong opaque ID", peer: "l1", wrongID: true, want: "l1:role_mismatch"},
		{name: "missing control", peer: "l1", err: fabric.ErrIdentityNotFound, want: "l1:peer_not_found"},
		{name: "missing ledger", peer: "l3", err: fabric.ErrIdentityNotFound, want: "l3:peer_not_found"},
		{name: "network unavailable", peer: "l3", err: errors.New("private error"), want: "l3:network_unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peers := lookupPeers(func(_ context.Context, id string) (fabric.Identity, error) {
				tags := []string{"tag:wefty-control"}
				if id == "l3" {
					tags = []string{"tag:wefty-client"}
				}
				actual := id
				if id == tc.peer {
					tags = tc.tags
					if tc.wrongID {
						actual = "another"
					}
					if tc.err != nil {
						return fabric.Identity{}, tc.err
					}
				}
				return fabric.Identity{NodeID: actual, Tags: tags}, nil
			})
			if got := CheckFleetPeers(t.Context(), peers, "l1", "l3"); got != tc.want {
				t.Fatalf("failure=%q want %q", got, tc.want)
			}
		})
	}
}
