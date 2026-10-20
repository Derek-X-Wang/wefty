package tsnet

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
	"tailscale.com/net/tsdial"
)

func TestDialRefusesNonFabricAndPinsResolvedPeer(t *testing.T) {
	for _, tc := range []struct {
		address, resolved string
		allowed           bool
	}{
		{"example.com:443", "93.184.216.34:443", false},
		{"127.0.0.1:22", "127.0.0.1:22", false},
		{"169.254.169.254:80", "169.254.169.254:80", false},
		{"peer:443", "100.64.0.2:443", true},
		{"100.64.0.2:8443", "100.64.0.2:8443", true},
		{"wefty://control-plane", "100.64.0.3:80", true},
	} {
		t.Run(tc.address, func(t *testing.T) {
			planned, dialled := "", ""
			stop := errors.New("transport reached")
			planner := &tsdial.Dialer{UseNetstackForIP: func(ip netip.Addr) bool { return tc.allowed && ip == netip.MustParseAddrPort(tc.resolved).Addr() }}
			planner.SetResolveMagicDNS(func(string, string) (netip.Addr, bool) { return netip.MustParseAddrPort(tc.resolved).Addr(), true })
			defer planner.Close()
			f := &Fabric{dialPlan: func(ctx context.Context, network, address string) (netip.AddrPort, bool, error) {
				planned = address
				return planner.UserDialPlan(ctx, network, address)
			}, dialNode: func(_ context.Context, _, address string) (net.Conn, error) { dialled = address; return nil, stop }}
			_, err := f.Dial(t.Context(), "tcp", tc.address)
			if tc.allowed {
				if !errors.Is(err, stop) || dialled != tc.resolved || planned == "" {
					t.Fatalf("allowed dial planned=%q dialled=%q err=%v", planned, dialled, err)
				}
			} else {
				var refused *fabric.DestinationRefusedError
				if !errors.As(err, &refused) || dialled != "" {
					t.Fatalf("non-Fabric destination reached transport: dialled=%q err=%v", dialled, err)
				}
			}
		})
	}
}

func TestDialCannotFallBackToHostAfterPlanChanges(t *testing.T) {
	planned, peerDialled, hostDialled := false, false, false
	stop := errors.New("peer transport reached")
	planner := &tsdial.Dialer{UseNetstackForIP: func(netip.Addr) bool {
		if !planned {
			planned = true
			return true
		}
		return false
	}, NetstackDialTCP: func(context.Context, netip.AddrPort) (net.Conn, error) { peerDialled = true; return nil, stop }, NetstackDialUDP: func(context.Context, netip.AddrPort) (net.Conn, error) { t.Fatal("unexpected UDP"); return nil, nil }}
	planner.SetRoutes(nil, []netip.Prefix{netip.MustParsePrefix("100.64.0.0/10")})
	planner.SetSystemDialerForTest(func(context.Context, string, string) (net.Conn, error) {
		hostDialled = true
		return nil, errors.New("host transport reached")
	})
	defer planner.Close()
	f := &Fabric{outboundDialer: planner}
	_, err := f.Dial(t.Context(), "tcp", "100.64.0.2:443")
	if hostDialled || !peerDialled || !errors.Is(err, stop) {
		t.Fatalf("route change escaped Fabric: peer=%v host=%v err=%v", peerDialled, hostDialled, err)
	}
}

func TestDialCancelledBeforePlanningDoesNotReachPeer(t *testing.T) {
	reached := false
	planner := &tsdial.Dialer{UseNetstackForIP: func(netip.Addr) bool { return true }, NetstackDialTCP: func(context.Context, netip.AddrPort) (net.Conn, error) {
		reached = true
		return nil, errors.New("peer reached")
	}}
	defer planner.Close()
	f := &Fabric{outboundDialer: planner}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := f.Dial(ctx, "tcp", "100.64.0.2:443")
	if reached || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled caller reached transport: reached=%v err=%v", reached, err)
	}
}
