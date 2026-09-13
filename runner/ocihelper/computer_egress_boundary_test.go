package ocihelper

import (
	"errors"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// forwardVerdict replays the ordered forward chain against one destination the
// way the kernel does: first matching rule wins. Only the egress direction is
// modelled, which is the direction a Computer's own traffic takes.
func forwardVerdict(t *testing.T, rules []computerFirewallRule, destination, protocol string, port int) string {
	t.Helper()
	address, err := netip.ParseAddr(destination)
	if err != nil {
		t.Fatalf("parse destination %q: %v", destination, err)
	}
	for _, rule := range rules {
		if rule.chain != computerFirewallForward {
			continue
		}
		arguments := rule.arguments
		if len(arguments) < 2 || arguments[0] != "-i" || arguments[1] != computerHostLinkPrefix+"+" {
			continue
		}
		if slices.Contains(arguments, "-o") {
			// Computer-to-Computer, handled by its own rule and not this path.
			continue
		}
		matched := true
		for index := 0; index+1 < len(arguments); index++ {
			switch arguments[index] {
			case "-d":
				prefix, parseErr := netip.ParsePrefix(arguments[index+1])
				if parseErr != nil {
					t.Fatalf("rule destination %q: %v", arguments[index+1], parseErr)
				}
				matched = matched && prefix.Contains(address)
			case "-p":
				matched = matched && arguments[index+1] == protocol
			case "--dport":
				matched = matched && arguments[index+1] == strconv.Itoa(port)
			}
		}
		if !matched {
			continue
		}
		target := arguments[len(arguments)-1]
		for index := 0; index+1 < len(arguments); index++ {
			if arguments[index] == "-j" {
				target = arguments[index+1]
			}
		}
		return target
	}
	return "NONE"
}

func testComputerEgressBoundary(t *testing.T, node, gateway string) computerEgressBoundary {
	t.Helper()
	boundary, err := newComputerEgressBoundary(
		[]netip.Addr{netip.MustParseAddr(node)},
		[]netip.Addr{netip.MustParseAddr(gateway)},
	)
	if err != nil {
		t.Fatal(err)
	}
	return boundary
}

// The run-1 destinations are the three a Computer on the Mac node reached:
// macOS sshd at the owner LAN address and at the vz gateway, and the Mac-side
// Computer view door behind that same gateway.
func TestComputerEgressRefusesTheMacHostBoundaryDestinations(t *testing.T) {
	boundary := testComputerEgressBoundary(t, "192.168.5.15", "192.168.5.2")
	rules := computerBaseFirewallRules("iptables", "icmp-port-unreachable", boundary.refusedIPv4, nil)
	for _, probe := range []struct {
		destination string
		port        int
		why         string
	}{
		{destination: "192.168.100.171", port: 22, why: "macOS sshd at the owner LAN address"},
		{destination: "192.168.5.2", port: 22, why: "macOS sshd through the vz gateway"},
		{destination: "192.168.5.2", port: 57814, why: "the Mac-side Computer view door"},
		{destination: "192.168.5.15", port: 22, why: "the Node's own address"},
		{destination: "169.254.169.254", port: 80, why: "a cloud metadata service"},
		{destination: "100.100.100.100", port: 443, why: "a carrier-grade-NAT peer"},
		{destination: "198.18.0.5", port: 5900, why: "another Computer's veth"},
		{destination: "10.1.2.3", port: 443, why: "an owner private network"},
		{destination: "172.20.0.1", port: 443, why: "an owner private network"},
		{destination: "127.0.0.1", port: 8080, why: "loopback named from outside the namespace"},
	} {
		if verdict := forwardVerdict(t, rules, probe.destination, "tcp", probe.port); verdict != "REJECT" {
			t.Errorf("forward verdict for %s:%d (%s) = %s; want REJECT", probe.destination, probe.port, probe.why, verdict)
		}
	}
}

func TestComputerEgressKeepsPublicInternetReachable(t *testing.T) {
	boundary := testComputerEgressBoundary(t, "192.168.5.15", "192.168.5.2")
	rules := computerBaseFirewallRules("iptables", "icmp-port-unreachable", boundary.refusedIPv4, nil)
	for _, destination := range []string{"172.66.147.243", "1.1.1.1", "93.184.216.34", "198.20.0.1"} {
		if verdict := forwardVerdict(t, rules, destination, "tcp", 443); verdict != "ACCEPT" {
			t.Errorf("forward verdict for public %s = %s; want ACCEPT", destination, verdict)
		}
	}
}

// The one destination-scoped hole is a routable Node resolver, and it is narrow:
// the same address on any other port stays refused.
func TestComputerEgressAllowsOnlyDNSToARoutableResolver(t *testing.T) {
	boundary := testComputerEgressBoundary(t, "192.168.5.15", "192.168.5.2")
	resolvers := computerRoutableResolvers([]string{"192.168.5.3", "192.168.5.3", "127.0.0.53", ""})
	if !slices.Equal(resolvers, []string{"192.168.5.3/32"}) {
		t.Fatalf("routable resolvers = %q; want the one non-loopback address once", resolvers)
	}
	rules := computerBaseFirewallRules("iptables", "icmp-port-unreachable", boundary.refusedIPv4, resolvers)
	for _, protocol := range []string{"udp", "tcp"} {
		if verdict := forwardVerdict(t, rules, "192.168.5.3", protocol, 53); verdict != "ACCEPT" {
			t.Errorf("forward verdict for the resolver over %s/53 = %s; want ACCEPT", protocol, verdict)
		}
	}
	if verdict := forwardVerdict(t, rules, "192.168.5.3", "tcp", 22); verdict != "REJECT" {
		t.Errorf("forward verdict for the resolver address on tcp/22 = %s; want REJECT", verdict)
	}
	if verdict := forwardVerdict(t, rules, "192.168.5.2", "udp", 53); verdict != "REJECT" {
		t.Errorf("forward verdict for DNS at the gateway = %s; want REJECT", verdict)
	}
}

// A Node whose own address is public is fenced by an explicit host rule, while
// an ordinary private Node adds none: the canonical body stays identical across
// DHCP leases.
func TestComputerEgressFoldsLearnedAddressesIntoTheReservedRanges(t *testing.T) {
	private := testComputerEgressBoundary(t, "192.168.5.15", "192.168.5.2")
	if !slices.Equal(private.refusedIPv4, computerReservedIPv4Destinations) {
		t.Fatalf("private Node refusals = %q; want exactly the reserved ranges", private.refusedIPv4)
	}
	public := testComputerEgressBoundary(t, "203.0.113.5", "203.0.113.1")
	if !slices.Contains(public.refusedIPv4, "203.0.113.5/32") || !slices.Contains(public.refusedIPv4, "203.0.113.1/32") {
		t.Fatalf("public Node refusals = %q; want the Node address and its next hop", public.refusedIPv4)
	}
	rules := computerBaseFirewallRules("iptables", "icmp-port-unreachable", public.refusedIPv4, nil)
	if verdict := forwardVerdict(t, rules, "203.0.113.1", "tcp", 22); verdict != "REJECT" {
		t.Errorf("forward verdict for a public next hop = %s; want REJECT", verdict)
	}
	if verdict := forwardVerdict(t, rules, "203.0.113.6", "tcp", 443); verdict != "ACCEPT" {
		t.Errorf("forward verdict for a neighbour of a public Node = %s; want ACCEPT", verdict)
	}
}

func TestComputerEgressBoundaryRefusesWhenTheNodeReportsNoAddress(t *testing.T) {
	_, err := newComputerEgressBoundary(nil, nil)
	var unproven *ComputerEgressBoundaryError
	if !errors.As(err, &unproven) {
		t.Fatalf("boundary without Node addresses = %v; want a typed refusal", err)
	}
	if reason := engineFailureReason(err); reason != EngineFailureEgressBoundary {
		t.Fatalf("engine failure reason = %q; want %q", reason, EngineFailureEgressBoundary)
	}
}

func TestComputerEgressRefusalsPrecedeTheForwardAccept(t *testing.T) {
	boundary := testComputerEgressBoundary(t, "192.168.5.15", "192.168.5.2")
	rules := computerBaseFirewallRules("iptables", "icmp-port-unreachable", boundary.refusedIPv4, []string{"192.168.5.3/32"})
	accept, lastRefusal, lastAllowance := -1, -1, -1
	for index, rule := range rules {
		if rule.chain != computerFirewallForward {
			continue
		}
		joined := strings.Join(rule.arguments, " ")
		switch {
		case joined == "-i "+computerHostLinkPrefix+"+ -j ACCEPT":
			accept = index
		case strings.Contains(joined, "-d ") && strings.HasSuffix(joined, "-j REJECT --reject-with icmp-port-unreachable"):
			lastRefusal = index
		case strings.Contains(joined, computerResolverRuleComment):
			lastAllowance = index
		}
	}
	if accept < 0 || lastRefusal < 0 || lastAllowance < 0 {
		t.Fatalf("forward chain is missing a verdict: accept=%d refusal=%d allowance=%d", accept, lastRefusal, lastAllowance)
	}
	if !(lastAllowance < lastRefusal && lastRefusal < accept) {
		t.Fatalf("forward order allowance=%d refusal=%d accept=%d; want allowance, then refusals, then the accept", lastAllowance, lastRefusal, accept)
	}
}

func TestComputerIPv6EgressMirrorsTheRefusals(t *testing.T) {
	boundary, err := newComputerEgressBoundary([]netip.Addr{netip.MustParseAddr("fd00::15")}, []netip.Addr{netip.MustParseAddr("fd00::2")})
	if err != nil {
		t.Fatal(err)
	}
	rules := computerIPv6BaseFirewallRules("ip6tables", boundary.refusedIPv6)
	for _, destination := range []string{"fd00::2", "fe80::1", "::1"} {
		if verdict := forwardVerdict(t, rules, destination, "tcp", 22); verdict != "REJECT" {
			t.Errorf("IPv6 forward verdict for %s = %s; want REJECT", destination, verdict)
		}
	}
	if verdict := forwardVerdict(t, rules, "2606:4700:4700::1111", "tcp", 443); verdict != "ACCEPT" {
		t.Errorf("IPv6 forward verdict for a public address = %s; want ACCEPT", verdict)
	}
}

// A second Computer on a Node can only start if the chain existence probe is
// listed numerically; reverse DNS on a chain that already carries a Computer's
// MASQUERADE rule outruns the helper's command deadline.
func TestComputerFirewallChainProbeIsListedNumerically(t *testing.T) {
	for _, probe := range []struct {
		table string
		want  []string
	}{
		{table: "", want: []string{"-n", "-L", computerFirewallForward}},
		{table: "filter", want: []string{"-n", "-L", computerFirewallForward}},
		{table: "nat", want: []string{"-t", "nat", "-n", "-L", computerFirewallForward}},
	} {
		arguments := computerFirewallChainProbeArguments(probe.table, computerFirewallForward)
		if !slices.Equal(arguments, probe.want) {
			t.Errorf("chain probe for table %q = %q; want %q", probe.table, arguments, probe.want)
		}
		if !slices.Contains(arguments, "-n") {
			t.Errorf("chain probe for table %q omits -n and would reverse-resolve every address in the chain", probe.table)
		}
	}
}
