package ocihelper

import (
	"errors"
	"testing"
)

func TestComputerNoneNetworkAcceptsOnlyInactiveFallbackTunnels(t *testing.T) {
	lo := computerNetworkLink{name: "lo", kind: "device", index: 1, up: true, loopback: true, addresses: 2}
	fallbacks := []computerNetworkLink{
		{name: "tunl0", kind: "ipip", index: 2},
		{name: "sit0", kind: "sit", index: 3},
		{name: "ip6tnl0", kind: "ip6tnl", index: 4},
		{name: "gre0", kind: "gre", index: 5},
		{name: "ip_vti0", kind: "vti", index: 6},
		{name: "ip6_vti0", kind: "vti6", index: 7},
	}
	for _, test := range []struct {
		name    string
		links   []computerNetworkLink
		refused bool
	}{
		{"loopback", []computerNetworkLink{lo}, false},
		{"fallbacks-before-loopback", append(append([]computerNetworkLink{}, fallbacks...), lo), false},
		{"fallback-up", []computerNetworkLink{lo, {name: "tunl0", kind: "ipip", up: true}}, true},
		{"fallback-addressed", []computerNetworkLink{lo, {name: "sit0", kind: "sit", addresses: 1}}, true},
		{"ethernet-down", []computerNetworkLink{lo, {name: "eth0", kind: "veth"}}, true},
		{"unknown-tunnel", []computerNetworkLink{lo, {name: "tunnel0", kind: "vxlan"}}, true},
		{"renamed-tunnel", []computerNetworkLink{lo, {name: "custom0", kind: "ipip"}}, true},
		{"spoofed-kind", []computerNetworkLink{lo, {name: "tunl0", kind: "dummy"}}, true},
		{"missing-loopback", fallbacks, true},
		{"loopback-down", []computerNetworkLink{{name: "lo", index: 1, loopback: true}}, true},
		{"loopback-flag-absent", []computerNetworkLink{{name: "lo", index: 1, up: true}}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			index, err := validateComputerNoneNetworkLinks(test.links)
			if !test.refused {
				if err != nil || index != 1 {
					t.Fatalf("valid namespace refused: index=%d error=%v", index, err)
				}
				return
			}
			var typed *RuntimeSpecRejectionError
			if !errors.As(err, &typed) {
				t.Fatalf("unsafe namespace lacked typed refusal: %v", err)
			}
		})
	}
}
