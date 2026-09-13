package ocihelper

import (
	"net/netip"
	"slices"
	"strings"
)

const (
	computerFirewallInput   = "WEFTY-COMPUTER-IN"
	computerFirewallForward = "WEFTY-COMPUTER-FWD"
	computerFirewallNAT     = "WEFTY-COMPUTER-NAT"
	computerHostLinkPrefix  = "wftch"
	// computerResolverRuleComment marks the one destination-scoped hole in the
	// egress boundary so an operator reading the chain knows why it is there.
	computerResolverRuleComment = "WEFTY-COMPUTER-RESOLVER"
)

// computerReservedIPv4Destinations are the IPv4 destinations forwarded Computer
// egress always refuses. A Computer's authority is the public internet: every
// private, link-local, loopback, carrier-grade-NAT, multicast and reserved
// destination names something on the owner's side of the boundary — the Node,
// the virtual machine host, the owner's LAN, a cloud metadata service, or
// another Computer's veth — and none of those are the Computer's to reach.
// Ordered widest-family-first and then numerically so the canonical chain body
// is stable across reconciliations.
var computerReservedIPv4Destinations = []string{
	"0.0.0.0/8",
	"10.0.0.0/8",
	"100.64.0.0/10",
	"127.0.0.0/8",
	"169.254.0.0/16",
	"172.16.0.0/12",
	"192.168.0.0/16",
	"198.18.0.0/15",
	"224.0.0.0/4",
	"240.0.0.0/4",
}

// computerReservedIPv6Destinations mirror the IPv4 set for the ip6tables policy
// that stands ready if a future image re-enables IPv6 inside the namespace.
var computerReservedIPv6Destinations = []string{
	"::1/128",
	"fc00::/7",
	"fe80::/10",
	"ff00::/8",
}

// ComputerEgressBoundaryError is the typed refusal a Computer start carries when
// the helper cannot prove which destinations belong to the Node and its host.
// An unproven boundary is never a permissive one: a Computer that cannot be
// fenced off the host does not start with network egress.
type ComputerEgressBoundaryError struct {
	Cause string
}

func (failure *ComputerEgressBoundaryError) Error() string {
	return "Computer egress boundary unproven: " + failure.Cause
}

// EgressBoundaryUnproven types the refusal across the helper boundary.
func (*ComputerEgressBoundaryError) EgressBoundaryUnproven() bool { return true }

// computerEgressBoundary is the proven destination policy for forwarded
// Computer egress on this Node: the reserved ranges plus every address the Node
// itself holds and every next hop it routes through, the latter being how the
// virtual machine host appears from inside a guest.
type computerEgressBoundary struct {
	refusedIPv4 []string
	refusedIPv6 []string
}

// newComputerEgressBoundary folds the Node's own addresses and its route next
// hops into the reserved set. Addresses already inside a reserved range add no
// rule, so an ordinary private Node produces the same canonical chain body
// whatever address its DHCP lease happens to hold.
//
// Enumeration that fails is the refusal: the caller passes what the kernel
// reported, and a Node that reported no address of its own cannot be fenced.
// A Node with no route next hop is a determination rather than a failure — it
// has no path off itself to fence — and the reserved ranges still stand.
func newComputerEgressBoundary(nodeAddresses, gateways []netip.Addr) (computerEgressBoundary, error) {
	learned := make([]netip.Addr, 0, len(nodeAddresses)+len(gateways))
	for _, address := range slices.Concat(nodeAddresses, gateways) {
		address = address.Unmap()
		if !address.IsValid() || address.IsUnspecified() {
			continue
		}
		learned = append(learned, address)
	}
	if len(learned) == 0 {
		return computerEgressBoundary{}, &ComputerEgressBoundaryError{Cause: "the Node reported no address of its own"}
	}
	slices.SortFunc(learned, func(left, right netip.Addr) int { return left.Compare(right) })
	learned = slices.Compact(learned)
	boundary := computerEgressBoundary{
		refusedIPv4: slices.Clone(computerReservedIPv4Destinations),
		refusedIPv6: slices.Clone(computerReservedIPv6Destinations),
	}
	for _, address := range learned {
		refused := &boundary.refusedIPv4
		if address.Is6() {
			refused = &boundary.refusedIPv6
		}
		if computerDestinationRefused(*refused, address) {
			continue
		}
		*refused = append(*refused, netip.PrefixFrom(address, address.BitLen()).String())
	}
	return boundary, nil
}

// computerDestinationRefused reports whether the destinations already cover the
// address. Destinations are this package's own literals plus host prefixes
// derived from parsed addresses, so an unparseable entry simply covers nothing.
func computerDestinationRefused(destinations []string, address netip.Addr) bool {
	for _, destination := range destinations {
		prefix, err := netip.ParsePrefix(destination)
		if err != nil {
			continue
		}
		if prefix.Contains(address) {
			return true
		}
	}
	return false
}

// computerRoutableResolvers reports the one destination-scoped hole the boundary
// keeps: a Node resolver that is not on loopback is dialed by the Computer
// itself over the veth, so DNS to exactly that address survives the refusals.
// A loopback resolver is served by the helper's in-namespace proxy and needs no
// rule at all, which is why an ordinary Node carries none.
func computerRoutableResolvers(addresses []string) []string {
	var resolvers []string
	for _, address := range addresses {
		destination, ok := computerResolverDestination(address)
		if !ok || slices.Contains(resolvers, destination) {
			continue
		}
		resolvers = append(resolvers, destination)
	}
	slices.Sort(resolvers)
	return resolvers
}

// computerResolverDestination normalizes a routable Node resolver address into
// the host prefix iptables prints, so the canonical body compares equal to the
// installed rule instead of rebuilding the chain forever.
func computerResolverDestination(address string) (string, bool) {
	parsed, err := netip.ParseAddr(strings.TrimSpace(address))
	if err != nil || parsed.IsLoopback() || parsed.IsUnspecified() {
		return "", false
	}
	parsed = parsed.Unmap()
	return netip.PrefixFrom(parsed, parsed.BitLen()).String(), true
}

type computerFirewallRule struct {
	executable string
	table      string
	chain      string
	arguments  []string
	insert     bool
	first      bool
}

func (rule computerFirewallRule) prefix() []string {
	if rule.table == "" || rule.table == "filter" {
		return nil
	}
	return []string{"-t", rule.table}
}

// computerBaseFirewallRules is the Node-wide Computer firewall policy in
// canonical order. The forward chain reads as a sentence: no Computer reaches
// another Computer, replies to what a Computer started come back, a routable
// Node resolver is reachable for DNS and nothing else, every destination on the
// owner's side of the boundary is refused, and only what survives all of that
// is forwarded to the public internet.
//
// The refusals sit ahead of the blanket accept on purpose. Before #440 the
// accept was the only forward verdict for an off-Node destination, so on a Lima
// vz guest — where the macOS host is reached through the guest's default-route
// gateway rather than as a Node address — a Computer reached the host's sshd and
// its own view door across a boundary the input chain never saw.
func computerBaseFirewallRules(executable, rejectWith string, refused, resolvers []string) []computerFirewallRule {
	rules := []computerFirewallRule{
		{executable: executable, chain: computerFirewallInput, arguments: []string{"-i", computerHostLinkPrefix + "+", "-j", "REJECT", "--reject-with", rejectWith}},
		{executable: executable, chain: computerFirewallForward, arguments: []string{"-i", computerHostLinkPrefix + "+", "-o", computerHostLinkPrefix + "+", "-j", "REJECT", "--reject-with", rejectWith}},
		{executable: executable, chain: computerFirewallForward, arguments: []string{"-o", computerHostLinkPrefix + "+", "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT"}},
	}
	for _, resolver := range resolvers {
		for _, protocol := range []string{"udp", "tcp"} {
			rules = append(rules, computerFirewallRule{executable: executable, chain: computerFirewallForward, arguments: []string{
				"-i", computerHostLinkPrefix + "+", "-d", resolver, "-p", protocol, "-m", protocol, "--dport", "53",
				"-m", "comment", "--comment", computerResolverRuleComment, "-j", "ACCEPT",
			}})
		}
	}
	for _, destination := range refused {
		rules = append(rules, computerFirewallRule{executable: executable, chain: computerFirewallForward, arguments: []string{
			"-i", computerHostLinkPrefix + "+", "-d", destination, "-j", "REJECT", "--reject-with", rejectWith,
		}})
	}
	return append(rules,
		computerFirewallRule{executable: executable, chain: computerFirewallForward, arguments: []string{"-i", computerHostLinkPrefix + "+", "-j", "ACCEPT"}},
		computerFirewallRule{executable: executable, chain: computerFirewallForward, arguments: []string{"-o", computerHostLinkPrefix + "+", "-j", "REJECT", "--reject-with", rejectWith}},
	)
}

func computerIPv6BaseFirewallRules(executable string, refused []string) []computerFirewallRule {
	rules := computerBaseFirewallRules(executable, "icmp6-port-unreachable", refused, nil)
	return append([]computerFirewallRule{
		{executable: executable, chain: computerFirewallInput, arguments: []string{"-i", computerHostLinkPrefix + "+", "-p", "ipv6-icmp", "-m", "icmp6", "--icmpv6-type", "135", "-j", "ACCEPT"}, insert: true},
		{executable: executable, chain: computerFirewallInput, arguments: []string{"-i", computerHostLinkPrefix + "+", "-p", "tcp", "-j", "REJECT", "--reject-with", "tcp-reset"}, insert: true},
	}, rules...)
}
