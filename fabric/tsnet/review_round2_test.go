package tsnet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/internal/naming"
	"tailscale.com/client/local"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
)

func TestDialRefusesSelfAndPeerServicesBeforePlan(t *testing.T) {
	status := destinationStatus()
	status.Self.TailscaleIPs = append(status.Self.TailscaleIPs, netip.MustParseAddr("fd7a:115c:a1e0::1"))
	status.Self.PeerAPIURL = []string{"http://100.64.0.1:5001", "http://100.64.0.2:5003"}
	for _, peer := range status.Peer {
		if peer.ID == "peer-id" {
			peer.PeerAPIURL = []string{"http://100.64.0.2:5002", "http://[fd7a:115c:a1e0::2]:5002"}
		}
	}
	for _, address := range []string{
		"100.64.0.1:443", "[fd7a:115c:a1e0::1]:443", "self:443", "SELF.NETWORK.TEST.:443", "self-id",
		"100.64.0.2:5002", "[fd7a:115c:a1e0::2]:5002", "peer:5002", "100.64.0.2:5003",
	} {
		t.Run(address, func(t *testing.T) {
			planned, dialled := false, false
			f := withDestinationStatus(t, &Fabric{dialPlan: func(_ context.Context, _, address string) (netip.AddrPort, bool, error) {
				planned = true
				return netip.MustParseAddrPort(address), true, nil
			}, dialNode: func(context.Context, string, string) (net.Conn, error) {
				dialled = true
				return nil, errors.New("transport reached")
			}}, status)
			_, err := f.Dial(t.Context(), "tcp", address)
			var refused *fabric.DestinationRefusedError
			if !errors.As(err, &refused) || planned || dialled {
				t.Fatalf("local/backend service escaped: plan=%v dial=%v err=%v", planned, dialled, err)
			}
		})
	}
}

func TestDialRefusesPlannedPeerService(t *testing.T) {
	status := destinationStatus()
	for _, peer := range status.Peer {
		if peer.ID == "peer-id" {
			peer.PeerAPIURL = []string{"http://100.64.0.2:5002"}
		}
	}
	for _, target := range []string{"100.64.0.1:80", "100.64.0.2:5002"} {
		t.Run(target, func(t *testing.T) {
			f := withDestinationStatus(t, &Fabric{dialPlan: func(context.Context, string, string) (netip.AddrPort, bool, error) {
				return netip.MustParseAddrPort(target), true, nil
			}, dialNode: func(context.Context, string, string) (net.Conn, error) {
				t.Error("backend service reached transport")
				return nil, nil
			}}, status)
			_, err := f.Dial(t.Context(), "tcp", "peer:8443")
			var refused *fabric.DestinationRefusedError
			if !errors.As(err, &refused) {
				t.Fatalf("backend service plan not refused: %v", err)
			}
		})
	}
}

func TestDialRefusesAmbiguousPeerNames(t *testing.T) {
	for _, sameID := range []bool{false, true} {
		t.Run(fmt.Sprint(sameID), func(t *testing.T) {
			status := destinationStatus()
			id := "other-id"
			if sameID {
				id = "peer-id"
			}
			status.Peer[key.NewNode().Public()] = &ipnstate.PeerStatus{ID: tailcfg.StableNodeID(id), DNSName: "PEER.NETWORK.TEST.", TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.4")}}
			for _, address := range []string{"peer:443", "peer.network.test:443"} {
				planned, dialled := false, false
				f := withDestinationStatus(t, &Fabric{dialPlan: func(_ context.Context, _, address string) (netip.AddrPort, bool, error) {
					planned = true
					return netip.MustParseAddrPort(address), true, nil
				}, dialNode: func(context.Context, string, string) (net.Conn, error) { dialled = true; return nil, nil }}, status)
				_, err := f.Dial(t.Context(), "tcp", address)
				var refused *fabric.DestinationRefusedError
				if !errors.As(err, &refused) || planned || dialled {
					t.Errorf("ambiguous name selected a node: plan=%v dial=%v err=%v", planned, dialled, err)
				}
			}
		})
	}
}

func TestDialStatusIdentityTransitionStaysIdentityRefusal(t *testing.T) {
	for _, person := range []bool{false, true} {
		for _, state := range []string{"NeedsLogin", "NeedsMachineAuth"} {
			t.Run(fmt.Sprintf("person=%v/%s", person, state), func(t *testing.T) {
				expected := &fabric.Identity{NodeID: "saved", Tags: []string{"tag:wefty-client"}}
				first := `{"BackendState":"Running","Self":{"ID":"saved","Tags":["tag:wefty-client"]}}`
				if person {
					expected = &fabric.Identity{NodeID: "saved", UserID: "42"}
					first = `{"BackendState":"Running","Self":{"ID":"saved","UserID":42}}`
				}
				reads := 0
				f := &Fabric{expectedIdentity: expected, client: &local.Client{OmitAuth: true, Transport: identityRoundTripper(func(r *http.Request) (*http.Response, error) {
					if r.URL.Path != "/localapi/v0/status" {
						t.Fatalf("unexpected API %s", r.URL.Path)
					}
					reads++
					body := first
					if reads > 1 {
						body = fmt.Sprintf(`{"BackendState":%q}`, state)
					}
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
				})}, dialPlan: func(context.Context, string, string) (netip.AddrPort, bool, error) {
					t.Error("identity refusal reached planner")
					return netip.AddrPort{}, false, nil
				}}
				_, err := f.Dial(t.Context(), "tcp", "100.64.0.2:443")
				var identity *fabric.ParticipantIdentityError
				var unreachable *fabric.ParticipantUnreachableError
				if reads != 2 || !errors.As(err, &identity) || identity.Person != person || errors.As(err, &unreachable) {
					t.Fatalf("identity refusal changed: reads=%d err=%T %v", reads, err, err)
				}
			})
		}
	}
}

func TestDialRefusesSelfLogicalNameBeforePlan(t *testing.T) {
	name, _, _ := naming.Parse("wefty://person/owner")
	status := destinationStatus()
	status.Self.DNSName = name.Hostname() + ".network.test."
	planned, dialled := false, false
	f := withDestinationStatus(t, &Fabric{dialPlan: func(_ context.Context, _, address string) (netip.AddrPort, bool, error) {
		planned = true
		return netip.MustParseAddrPort(address), true, nil
	}, dialNode: func(context.Context, string, string) (net.Conn, error) { dialled = true; return nil, nil }}, status)
	_, err := f.Dial(t.Context(), "tcp", "wefty://person/owner")
	var refused *fabric.DestinationRefusedError
	if !errors.As(err, &refused) || planned || dialled {
		t.Fatalf("own logical name escaped: plan=%v dial=%v err=%v", planned, dialled, err)
	}
}
