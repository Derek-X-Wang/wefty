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

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/internal/naming"
	"github.com/Derek-X-Wang/wefty/internal/takeover"
	"tailscale.com/client/local"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/net/tsdial"
	"tailscale.com/types/key"
)

func destinationStatus() *ipnstate.Status {
	return &ipnstate.Status{BackendState: "Running", CurrentTailnet: &ipnstate.TailnetStatus{MagicDNSSuffix: "network.test"},
		Self: &ipnstate.PeerStatus{ID: "self-id", DNSName: "self.network.test.", TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.1")}},
		Peer: map[key.NodePublic]*ipnstate.PeerStatus{
			key.NewNode().Public(): {ID: "peer-id", HostName: "untrusted-hostname", DNSName: "peer.network.test.", TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.2"), netip.MustParseAddr("fd7a:115c:a1e0::2")}},
			key.NewNode().Public(): {ID: "control-id", DNSName: "control-plane.network.test.", TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.3")}},
		}}
}

func withDestinationStatus(t *testing.T, f *Fabric, status *ipnstate.Status) *Fabric {
	t.Helper()
	payload, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	f.client = &local.Client{OmitAuth: true, Transport: identityRoundTripper(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/localapi/v0/status" {
			t.Fatalf("unexpected API: %s", r.URL.Path)
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(payload)))}, nil
	})}
	return f
}

func TestUnknownHostRefusedBeforeResolverOrPlan(t *testing.T) {
	for _, host := range []string{"unknown.example.test", "peer.attacker.test", "untrusted-hostname"} {
		t.Run(host, func(t *testing.T) {
			resolverCalled, planCalled, dialCalled := false, false, false
			planner := &tsdial.Dialer{UseNetstackForIP: func(netip.Addr) bool { return false }}
			// Capture the dependency's name-resolution entry point without sending DNS.
			planner.SetResolveMagicDNS(func(string, string) (netip.Addr, bool) {
				resolverCalled = true
				return netip.MustParseAddr("192.0.2.1"), true
			})
			defer planner.Close()
			f := withDestinationStatus(t, &Fabric{dialPlan: func(ctx context.Context, network, address string) (netip.AddrPort, bool, error) {
				planCalled = true
				return planner.UserDialPlan(ctx, network, address)
			}, dialNode: func(context.Context, string, string) (net.Conn, error) {
				dialCalled = true
				return nil, errors.New("transport reached")
			}}, destinationStatus())
			_, err := f.Dial(t.Context(), "tcp", net.JoinHostPort(host, "443"))
			var refused *fabric.DestinationRefusedError
			if !errors.As(err, &refused) || resolverCalled || planCalled || dialCalled {
				t.Fatalf("unknown name escaped policy: resolver=%v plan=%v dial=%v err=%v", resolverCalled, planCalled, dialCalled, err)
			}
		})
	}
}

func TestAuthenticatedNamesPinLiteralBeforePlanning(t *testing.T) {
	status := destinationStatus()
	for _, logical := range []string{"wefty://run-ledger", "wefty://node/runner-1", "wefty://client/laptop", "wefty://person/owner"} {
		name, _, _ := naming.Parse(logical)
		status.Peer[key.NewNode().Public()] = &ipnstate.PeerStatus{DNSName: name.Hostname() + ".network.test.", TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.3")}}
	}
	for _, tc := range []struct{ address, want, network string }{
		{"peer:443", "100.64.0.2:443", "tcp"},
		{"PEER.NETWORK.TEST.:8443", "100.64.0.2:8443", "tcp"},
		{"peer:8443", "[fd7a:115c:a1e0::2]:8443", "tcp6"},
		{"peer:443", "100.64.0.2:443", "tcp4"},
		{"peer:8443", "100.64.0.2:8443", "udp"},
		{"wefty://control-plane", "100.64.0.3:80", "tcp"},
		{"wefty://run-ledger", "100.64.0.3:80", "tcp"},
		{"wefty://node/runner-1", "100.64.0.3:80", "tcp"},
		{"wefty://client/laptop", "100.64.0.3:80", "tcp"},
		{"wefty://person/owner", "100.64.0.3:80", "tcp"},
		{"peer-id", "100.64.0.2:80", "tcp"},
	} {
		t.Run(tc.address+tc.network, func(t *testing.T) {
			planned, dialled := "", ""
			stop := errors.New("peer transport reached")
			planner := &tsdial.Dialer{UseNetstackForIP: func(netip.Addr) bool { return true }}
			planner.SetResolveMagicDNS(func(string, string) (netip.Addr, bool) {
				t.Error("name reached resolver")
				return netip.MustParseAddr("100.64.0.2"), true
			})
			defer planner.Close()
			f := withDestinationStatus(t, &Fabric{dialPlan: func(ctx context.Context, network, address string) (netip.AddrPort, bool, error) {
				planned = address
				return planner.UserDialPlan(ctx, network, address)
			}, dialNode: func(_ context.Context, _, address string) (net.Conn, error) { dialled = address; return nil, stop }}, status)
			_, err := f.Dial(t.Context(), tc.network, tc.address)
			if !errors.Is(err, stop) || planned != tc.want || dialled != tc.want {
				t.Fatalf("name was not pinned: plan=%q dial=%q err=%v", planned, dialled, err)
			}
		})
	}
}

// Exercise take-over's actual HTTP transport hook through the production
// participant policy, stopping at the injected embedded transport boundary.
func TestTakeoverEndpointReachesAuthenticatedPeer(t *testing.T) {
	name, _, _ := naming.Parse("wefty://node/runner-1")
	status := destinationStatus()
	status.Peer[key.NewNode().Public()] = &ipnstate.PeerStatus{DNSName: name.Hostname() + ".network.test.", TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.2")}}
	for _, host := range []string{name.Hostname(), "peer.network.test", "100.64.0.2", "fd7a:115c:a1e0::2"} {
		for _, action := range []string{"view", "take", "release"} {
			t.Run(host+"/"+action, func(t *testing.T) {
				stop := errors.New("embedded peer transport reached")
				dialled := ""
				f := withDestinationStatus(t, &Fabric{dialPlan: func(_ context.Context, _, address string) (netip.AddrPort, bool, error) {
					ip, err := netip.ParseAddrPort(address)
					return ip, true, err
				}, dialNode: func(_ context.Context, _, address string) (net.Conn, error) { dialled = address; return nil, stop }}, status)
				endpoint := "ws://" + net.JoinHostPort(host, "8443") + contract.ComputerDisplayWebSocketPath
				var err error
				if action == "view" {
					_, err = takeover.Open(t.Context(), f, endpoint)
				} else {
					_, err = takeover.Perform(t.Context(), f, endpoint, "test-token", action)
				}
				want := "100.64.0.2:8443"
				if host == "fd7a:115c:a1e0::2" {
					want = "[fd7a:115c:a1e0::2]:8443"
				}
				if !errors.Is(err, stop) || dialled != want {
					t.Fatalf("take-over endpoint blocked: dial=%q err=%v", dialled, err)
				}
			})
		}
	}
}

func TestDialCancellationDuringDestinationStatusStaysLocal(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f := &Fabric{client: &local.Client{OmitAuth: true, Transport: identityRoundTripper(func(r *http.Request) (*http.Response, error) {
		cancel()
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}}
	_, err := f.Dial(ctx, "tcp", "100.64.0.2:443")
	var unreachable *fabric.ParticipantUnreachableError
	if !errors.Is(err, context.Canceled) || errors.As(err, &unreachable) {
		t.Fatalf("local cancellation became an availability failure: %T %v", err, err)
	}
}
