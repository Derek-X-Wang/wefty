package tsnet

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/types/key"
)

func TestDialRequiresAuthenticatedPeerIP(t *testing.T) {
	for _, tc := range []struct {
		address string
		allowed bool
	}{
		{"100.100.100.100:53", false},
		{"[fd7a:115c:a1e0::53]:53", false},
		{"100.64.0.99:443", false},
		{"[fd7a:115c:a1e0::99]:443", false},
		{"100.64.0.2:8443", true},
		{"[fd7a:115c:a1e0::2]:8443", true},
		{"100.64.0.1:8443", false},
	} {
		t.Run(tc.address, func(t *testing.T) {
			planned, dialled := false, false
			stop := errors.New("transport reached")
			f := withDestinationStatus(t, &Fabric{dialPlan: func(_ context.Context, _, address string) (netip.AddrPort, bool, error) {
				planned = true
				return netip.MustParseAddrPort(address), true, nil
			}, dialNode: func(context.Context, string, string) (net.Conn, error) { dialled = true; return nil, stop }}, destinationStatus())
			_, err := f.Dial(t.Context(), "tcp", tc.address)
			if tc.allowed {
				if !errors.Is(err, stop) || !planned || !dialled {
					t.Fatalf("peer refused: plan=%v dial=%v err=%v", planned, dialled, err)
				}
			} else {
				var refused *fabric.DestinationRefusedError
				if !errors.As(err, &refused) || dialled || planned {
					t.Fatalf("non-peer escaped: plan=%v dial=%v err=%v", planned, dialled, err)
				}
			}
		})
	}
}

func TestServiceIPsRefusedEvenIfPresentInStatus(t *testing.T) {
	for _, ip := range []string{"100.100.100.100", "fd7a:115c:a1e0::53"} {
		t.Run(ip, func(t *testing.T) {
			status := destinationStatus()
			status.Peer[key.NewNode().Public()] = &ipnstate.PeerStatus{DNSName: "service.network.test.", TailscaleIPs: []netip.Addr{netip.MustParseAddr(ip)}}
			f := withDestinationStatus(t, &Fabric{dialPlan: func(context.Context, string, string) (netip.AddrPort, bool, error) {
				t.Error("service address reached planner")
				return netip.AddrPortFrom(netip.MustParseAddr(ip), 53), true, nil
			}, dialNode: func(context.Context, string, string) (net.Conn, error) {
				t.Error("service address reached transport")
				return nil, nil
			}}, status)
			for _, address := range []string{net.JoinHostPort(ip, "53"), "service:53"} {
				_, err := f.Dial(t.Context(), "tcp", address)
				var refused *fabric.DestinationRefusedError
				if !errors.As(err, &refused) {
					t.Fatalf("service address not refused: %v", err)
				}
			}
		})
	}
}

func TestDialRefusesPlanOutsideAuthenticatedPeer(t *testing.T) {
	f := withDestinationStatus(t, &Fabric{dialPlan: func(context.Context, string, string) (netip.AddrPort, bool, error) {
		return netip.MustParseAddrPort("100.64.0.99:443"), true, nil
	}, dialNode: func(context.Context, string, string) (net.Conn, error) {
		t.Error("non-peer plan reached transport")
		return nil, nil
	}}, destinationStatus())
	_, err := f.Dial(t.Context(), "tcp", "peer:443")
	var refused *fabric.DestinationRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("non-peer plan not refused: %v", err)
	}
}
