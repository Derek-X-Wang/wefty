package tsnet

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
	"tailscale.com/client/local"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/types/key"
	"tailscale.com/types/views"
)

func TestAuthenticatedPeerLookupUsesOpaqueRemoteID(t *testing.T) {
	for _, tc := range []struct {
		name, id           string
		expired, duplicate bool
		want               error
	}{
		{name: "remote", id: "opaque-control"},
		{name: "name is not identity", id: "wefty-control-plane", want: fabric.ErrIdentityNotFound},
		{name: "self refused", id: "opaque-self", want: fabric.ErrIdentityNotFound},
		{name: "expired", id: "opaque-control", expired: true, want: fabric.ErrIdentityNotFound},
		{name: "ambiguous ID", id: "opaque-control", duplicate: true, want: fabric.ErrIdentityIncomplete},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status := ipnstate.Status{BackendState: "Running", Self: &ipnstate.PeerStatus{ID: "opaque-self"}, Peer: map[key.NodePublic]*ipnstate.PeerStatus{key.NewNode().Public(): {ID: "opaque-control", DNSName: "wefty-control-plane.network.test.", Expired: tc.expired, Tags: ptrTags([]string{"tag:wefty-control"})}}}
			if tc.duplicate {
				status.Peer[key.NewNode().Public()] = &ipnstate.PeerStatus{ID: "opaque-control"}
			}
			payload, _ := json.Marshal(status)
			f := &Fabric{client: &local.Client{OmitAuth: true, Transport: identityRoundTripper(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != "/localapi/v0/status" {
					t.Fatalf("unexpected API %s", r.URL.Path)
				}
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(string(payload)))}, nil
			})}}
			got, err := f.LookupPeer(t.Context(), tc.id)
			if !errors.Is(err, tc.want) {
				t.Fatalf("lookup: %+v %v want %v", got, err, tc.want)
			}
			if tc.want == nil && (got.NodeID != "opaque-control" || len(got.Tags) != 1 || got.Tags[0] != "tag:wefty-control") {
				t.Fatalf("identity: %+v", got)
			}
		})
	}
}
func ptrTags(tags []string) *views.Slice[string] { v := views.SliceOf(tags); return &v }
