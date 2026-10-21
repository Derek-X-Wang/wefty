package tsnet

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
	"tailscale.com/client/local"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/types/key"
)

func TestOpaqueNodeAddressesReachTheEnrolledPeer(t *testing.T) {
	status := ipnstate.Status{BackendState: "Running", Self: &ipnstate.PeerStatus{ID: "opaque-local"},
		Peer: map[key.NodePublic]*ipnstate.PeerStatus{
			key.NewNode().Public(): {ID: "opaque-l1", TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.1")}},
			key.NewNode().Public(): {ID: "opaque-l3", TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.2")}},
		}}
	payload, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	f := &Fabric{client: &local.Client{OmitAuth: true, Transport: identityRoundTripper(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/localapi/v0/status" {
			t.Fatalf("unexpected API: %s", r.URL.Path)
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(payload)))}, nil
	})}}
	f.dialPlan = func(_ context.Context, _, address string) (netip.AddrPort, bool, error) {
		ipp, err := netip.ParseAddrPort(address)
		return ipp, err == nil, err
	}
	var dialled, listened string
	stop := errors.New("test transport boundary")
	f.dialNode = func(_ context.Context, _, address string) (net.Conn, error) { dialled = address; return nil, stop }
	f.listenNode = func(_, address string) (net.Listener, error) { listened = address; return nil, stop }
	for _, peer := range []struct{ id, address string }{{"opaque-l1", "100.64.0.1:80"}, {"opaque-l3", "100.64.0.2:80"}} {
		_, err := f.Dial(t.Context(), "tcp", peer.id)
		if !errors.Is(err, stop) || dialled != peer.address {
			t.Fatalf("Dial(%q) address=%q err=%v", peer.id, dialled, err)
		}
	}
	if _, err := f.Dial(t.Context(), "tcp", "unknown-opaque"); !errors.Is(err, fabric.ErrIdentityNotFound) {
		t.Fatalf("unknown peer = %v", err)
	}
	if _, err := f.Listen("tcp", "opaque-local"); !errors.Is(err, stop) || listened != ":80" {
		t.Fatalf("local listen = %q %v", listened, err)
	}
	if _, err := f.Listen("tcp", "opaque-l1"); !errors.Is(err, fabric.ErrIdentityNotFound) {
		t.Fatalf("foreign listen = %v", err)
	}
}
