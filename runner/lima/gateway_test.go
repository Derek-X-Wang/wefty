package lima

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type gatewayEpoch struct{ generation uint64 }

func (epoch *gatewayEpoch) Generation() (ocihelper.HelperSession, bool) {
	return ocihelper.HelperSession{HelperInstanceID: "helper", SessionGeneration: epoch.generation}, true
}

// limaInspection fakes the two things Lima is asked about an instance: the
// virtual machine type it recorded, and the guest's own default route.
type limaInspection struct {
	instance     string
	vmType       string
	resolved     string
	defaultRoute string
	vmTypeErr    error
	routeErr     error
	resolveErr   error
	commands     []string
	resolutions  int
}

func (inspection *limaInspection) run(_ context.Context, name string, arguments ...string) ([]byte, error) {
	joined := strings.Join(append([]string{name}, arguments...), " ")
	inspection.commands = append(inspection.commands, joined)
	switch {
	case strings.Contains(joined, "list --json"):
		if inspection.vmTypeErr != nil {
			return nil, inspection.vmTypeErr
		}
		return fmt.Appendf(nil, "{\"name\":%q,\"vmType\":%q,\"status\":\"Running\"}\n", inspection.instance, inspection.vmType), nil
	case strings.Contains(joined, "getent ahostsv4"):
		inspection.resolutions++
		if inspection.resolveErr != nil {
			return nil, inspection.resolveErr
		}
		return fmt.Appendf(nil, "%s STREAM %s\n", inspection.resolved, HostGatewayName), nil
	case strings.Contains(joined, "ip -4 route show default"):
		if inspection.routeErr != nil {
			return nil, inspection.routeErr
		}
		return []byte(inspection.defaultRoute), nil
	}
	return nil, fmt.Errorf("unexpected Lima command %q", joined)
}

func (inspection *limaInspection) log() string { return strings.Join(inspection.commands, "\n") }

func vzInspection(resolved, gateway string) *limaInspection {
	return &limaInspection{
		instance:     "ticket-145",
		vmType:       "vz",
		resolved:     resolved,
		defaultRoute: fmt.Sprintf("default via %s dev eth0 proto dhcp src 198.18.0.15 metric 200\n", gateway),
	}
}

func vmnetInspection(resolved string) *limaInspection {
	return &limaInspection{instance: "ticket-145", vmType: "qemu", resolved: resolved}
}

func refuseRoute(t *testing.T) commandRunner {
	t.Helper()
	return func(context.Context, string, ...string) ([]byte, error) {
		t.Fatal("host route provenance consulted for a vz instance")
		return nil, nil
	}
}

// hostInterfaces fakes what the host itself holds, with no guest in the loop.
func hostInterfaces(addresses ...string) hostAddressLister {
	return func() ([]net.Addr, error) {
		held := make([]net.Addr, 0, len(addresses))
		for _, address := range addresses {
			parsed := net.ParseIP(address)
			mask := net.CIDRMask(64, 128)
			if parsed.To4() != nil {
				mask = net.CIDRMask(24, 32)
			}
			held = append(held, &net.IPNet{IP: parsed, Mask: mask})
		}
		return held, nil
	}
}

func refuseHostAddresses(t *testing.T) hostAddressLister {
	t.Helper()
	return func() ([]net.Addr, error) {
		t.Fatal("host interface addresses enumerated outside the vz proof")
		return nil, nil
	}
}

func refuseListen(t *testing.T) listenFunc {
	t.Helper()
	return func(string, string) (net.Listener, error) {
		t.Fatal("an unproven gateway reached the bind")
		return nil, nil
	}
}

func TestBridgeBinderUsesDiscoveredGatewayWithoutHardCoding(t *testing.T) {
	var listenAddress string
	inspection := vzInspection("198.18.0.2", "198.18.0.2")
	binder := NewBridgeBinder("ticket-145")
	binder.run = inspection.run
	binder.route = refuseRoute(t)
	binder.hostAddresses = hostInterfaces("192.168.100.7", "127.0.0.1", "::1")
	binder.listen = func(network, address string) (net.Listener, error) {
		listenAddress = address
		return net.Listen("tcp4", "127.0.0.1:0")
	}
	binding, err := binder.Bind(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer binding.Listener.Close()
	if binding.AdvertiseHost != HostGatewayName || binding.HostBridgeFallback {
		t.Fatalf("primary binding = %#v", binding)
	}
	if listenAddress != "198.18.0.2:0" {
		t.Fatalf("listen address = %q", listenAddress)
	}
	joined := inspection.log()
	forbiddenGateway := strings.Join([]string{"192", "168", "5", "2"}, ".")
	if !strings.Contains(joined, "getent ahostsv4 "+HostGatewayName) || strings.Contains(joined, forbiddenGateway) {
		t.Fatalf("discovery commands = %q", joined)
	}
}

// A vz instance's user-mode network lives inside Virtualization.framework, so
// the host owns no interface for the gateway: the proof is that the address is
// the one Lima configured for this instance, read from Lima rather than fixed
// in source.
func TestBridgeBinderProvesVZGatewayAgainstTheInstanceUserNetwork(t *testing.T) {
	inspection := vzInspection("198.18.0.2", "198.18.0.2")
	binder := NewBridgeBinder("ticket-145")
	binder.run = inspection.run
	binder.route = refuseRoute(t)
	binder.hostAddresses = hostInterfaces("192.168.100.7", "127.0.0.1", "::1")
	binder.listen = func(string, string) (net.Listener, error) { return net.Listen("tcp4", "127.0.0.1:0") }
	binding, err := binder.Bind(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer binding.Listener.Close()
	if !strings.Contains(inspection.log(), "list --json") {
		t.Fatalf("virtual machine type was not read from Lima: %q", inspection.log())
	}
}

func TestBridgeBinderRejectsVZGatewayOutsideTheInstanceUserNetwork(t *testing.T) {
	inspection := vzInspection("10.0.0.24", "198.18.0.2")
	binder := NewBridgeBinder("ticket-145")
	binder.run = inspection.run
	binder.route = refuseRoute(t)
	binder.hostAddresses = hostInterfaces("192.168.100.7", "127.0.0.1", "::1")
	binder.listen = refuseListen(t)
	_, err := binder.Bind(t.Context())
	if err == nil || !strings.Contains(err.Error(), "is not the vz user network gateway") {
		t.Fatalf("mismatched vz gateway error = %v", err)
	}
	if !strings.Contains(err.Error(), "10.0.0.24") || !strings.Contains(err.Error(), "198.18.0.2") {
		t.Fatalf("mismatch error omitted both addresses: %v", err)
	}
}

// A vmnet/socket_vmnet instance's gateway is a real host interface address, so
// the interface-name proof stays exactly as it was.
func TestBridgeBinderKeepsInterfaceProofForNonVZInstances(t *testing.T) {
	inspection := vmnetInspection("198.18.0.2")
	binder := NewBridgeBinder("ticket-145")
	binder.run = inspection.run
	binder.hostAddresses = refuseHostAddresses(t)
	routed := 0
	binder.route = func(context.Context, string, ...string) ([]byte, error) {
		routed++
		return []byte("interface: bridge100\n"), nil
	}
	binder.listen = func(string, string) (net.Listener, error) { return net.Listen("tcp4", "127.0.0.1:0") }
	binding, err := binder.Bind(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer binding.Listener.Close()
	if routed != 1 || binding.AdvertiseHost != HostGatewayName || binding.HostBridgeFallback {
		t.Fatalf("vmnet binding = %#v, route lookups=%d", binding, routed)
	}
	if strings.Contains(inspection.log(), "ip -4 route show default") {
		t.Fatal("guest route table consulted for a non-vz instance")
	}
}

func TestBridgeBinderRejectsPhysicalGatewayRoute(t *testing.T) {
	inspection := vmnetInspection("10.0.0.24")
	binder := NewBridgeBinder("ticket-145")
	binder.run = inspection.run
	binder.hostAddresses = refuseHostAddresses(t)
	binder.route = func(context.Context, string, ...string) ([]byte, error) { return []byte("interface: en0\n"), nil }
	binder.listen = refuseListen(t)
	if _, err := binder.Bind(t.Context()); err == nil || !strings.Contains(err.Error(), "physical interface") {
		t.Fatalf("physical route error = %v", err)
	}
}

func TestBridgeBinderRefusesGatewayLimaWillNotAccountFor(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		inspection *limaInspection
		wanted     string
	}{
		{
			name: "virtual machine type unavailable",
			inspection: func() *limaInspection {
				inspection := vzInspection("198.18.0.2", "198.18.0.2")
				inspection.vmTypeErr = errors.New("instance inventory unavailable")
				return inspection
			}(),
			wanted: "inspect the virtual machine type of Lima instance",
		},
		{
			name: "virtual machine type absent",
			inspection: func() *limaInspection {
				inspection := vzInspection("198.18.0.2", "198.18.0.2")
				inspection.vmType = ""
				return inspection
			}(),
			wanted: "reported no virtual machine type",
		},
		{
			name: "user network gateway unavailable",
			inspection: func() *limaInspection {
				inspection := vzInspection("198.18.0.2", "198.18.0.2")
				inspection.routeErr = errors.New("guest routing table unavailable")
				return inspection
			}(),
			wanted: "inspect the vz user network gateway of Lima instance",
		},
		{
			name: "user network gateway absent",
			inspection: func() *limaInspection {
				inspection := vzInspection("198.18.0.2", "198.18.0.2")
				inspection.defaultRoute = "198.18.0.0/24 dev eth0 proto kernel scope link src 198.18.0.15\n"
				return inspection
			}(),
			wanted: "reported no IPv4 default gateway",
		},
		{
			name: "conflicting user network gateways",
			inspection: func() *limaInspection {
				inspection := vzInspection("198.18.0.2", "198.18.0.2")
				inspection.defaultRoute = "default via 198.18.0.2 dev eth0\ndefault via 10.0.0.1 dev eth1\n"
				return inspection
			}(),
			wanted: "reported default gateways",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			binder := NewBridgeBinder("ticket-145")
			binder.run = testCase.inspection.run
			binder.route = refuseRoute(t)
			binder.hostAddresses = hostInterfaces("192.168.100.7")
			binder.listen = refuseListen(t)
			_, err := binder.Bind(t.Context())
			if err == nil || !strings.Contains(err.Error(), testCase.wanted) {
				t.Fatalf("error = %v, wanted %q", err, testCase.wanted)
			}
		})
	}
}

// The vz proof reads both halves from inside the instance, so a compromised
// guest can name one of the host's own addresses twice and pass the equality
// check. macOS owns that address, the bind would succeed, and the bridge would
// sit on the LAN. The host-side floor is what stops it, and it asks the guest
// nothing.
func TestBridgeBinderRefusesGuestClaimedHostOwnedGateway(t *testing.T) {
	lanAddress := "192.168.100.7"
	inspection := vzInspection(lanAddress, lanAddress)
	binder := NewBridgeBinder("ticket-145")
	binder.run = inspection.run
	binder.route = refuseRoute(t)
	binder.hostAddresses = hostInterfaces("127.0.0.1", lanAddress, "::1")
	binder.listen = refuseListen(t)
	_, err := binder.Bind(t.Context())
	if err == nil || !strings.Contains(err.Error(), "is assigned to a host interface") {
		t.Fatalf("host-owned gateway error = %v", err)
	}
	if !strings.Contains(err.Error(), lanAddress) {
		t.Fatalf("host-owned gateway error omitted the address: %v", err)
	}
}

func TestBridgeBinderRefusesGatewayWhenTheHostCannotBeEnumerated(t *testing.T) {
	inspection := vzInspection("198.18.0.2", "198.18.0.2")
	binder := NewBridgeBinder("ticket-145")
	binder.run = inspection.run
	binder.route = refuseRoute(t)
	binder.hostAddresses = func() ([]net.Addr, error) { return nil, errors.New("interface table unavailable") }
	binder.listen = refuseListen(t)
	_, err := binder.Bind(t.Context())
	if err == nil || !strings.Contains(err.Error(), "enumerate host interface addresses") {
		t.Fatalf("host enumeration failure error = %v", err)
	}
}

// The floor must not refuse the real thing: the gateway Lima hands a vz guest
// is precisely an address the host does not hold.
func TestBridgeBinderAcceptsVZGatewayTheHostDoesNotHold(t *testing.T) {
	gateway := strings.Join([]string{"192", "168", "5", "2"}, ".")
	var listenAddress string
	inspection := vzInspection(gateway, gateway)
	binder := NewBridgeBinder("ticket-145")
	binder.run = inspection.run
	binder.route = refuseRoute(t)
	binder.hostAddresses = hostInterfaces("127.0.0.1", "192.168.100.7", "::1")
	binder.listen = func(network, address string) (net.Listener, error) {
		listenAddress = address
		return net.Listen("tcp4", "127.0.0.1:0")
	}
	binding, err := binder.Bind(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer binding.Listener.Close()
	if listenAddress != gateway+":0" || binding.AdvertiseHost != HostGatewayName || binding.HostBridgeFallback {
		t.Fatalf("binding = %#v, listen address = %q", binding, listenAddress)
	}
}

func TestBridgeBinderUsesHelperFallbackOnlyAfterGatewayBindFailure(t *testing.T) {
	inspection := vzInspection("198.18.0.2", "198.18.0.2")
	binder := NewBridgeBinder("ticket-145")
	binder.run = inspection.run
	binder.route = refuseRoute(t)
	binder.hostAddresses = hostInterfaces("192.168.100.7", "127.0.0.1", "::1")
	calls := 0
	binder.listen = func(network, address string) (net.Listener, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("gateway surface cannot be bound")
		}
		if address != "127.0.0.1:0" {
			t.Fatalf("fallback address = %q", address)
		}
		return net.Listen("tcp4", address)
	}
	binding, err := binder.Bind(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer binding.Listener.Close()
	if !binding.HostBridgeFallback || binding.AdvertiseHost != "127.0.0.1" || calls != 2 {
		t.Fatalf("fallback binding = %#v, calls=%d", binding, calls)
	}
}

func TestBridgeBinderDoesNotTunnelAroundDiscoveryFailure(t *testing.T) {
	inspection := vzInspection("198.18.0.2", "198.18.0.2")
	inspection.resolveErr = errors.New("VM unavailable")
	binder := NewBridgeBinder("ticket-145")
	binder.run = inspection.run
	binder.route = refuseRoute(t)
	binder.hostAddresses = hostInterfaces("192.168.100.7", "127.0.0.1", "::1")
	binder.listen = refuseListen(t)
	if _, err := binder.Bind(t.Context()); err == nil {
		t.Fatal("discovery failure incorrectly selected the helper fallback")
	}
}

func TestBridgeBinderCachesDiscoveryOnlyForAuthoritativeHelperEpoch(t *testing.T) {
	epoch := &gatewayEpoch{generation: 1}
	inspection := vzInspection("198.18.0.2", "198.18.0.2")
	binder := NewBridgeBinder("ticket-145")
	binder.Epoch = epoch
	binder.run = inspection.run
	binder.route = refuseRoute(t)
	binder.hostAddresses = hostInterfaces("192.168.100.7", "127.0.0.1", "::1")
	binder.listen = func(string, string) (net.Listener, error) { return net.Listen("tcp4", "127.0.0.1:0") }
	for range 2 {
		binding, err := binder.Bind(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		_ = binding.Listener.Close()
	}
	if inspection.resolutions != 1 {
		t.Fatalf("same epoch discovery calls = %d", inspection.resolutions)
	}
	epoch.generation++
	binding, err := binder.Bind(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	_ = binding.Listener.Close()
	if inspection.resolutions != 2 {
		t.Fatalf("new epoch discovery calls = %d", inspection.resolutions)
	}
}
