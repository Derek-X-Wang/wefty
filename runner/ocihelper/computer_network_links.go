package ocihelper

import "fmt"

type computerNetworkLink struct {
	name, kind   string
	index        int
	up, loopback bool
	addresses    int
}

func validateComputerNoneNetworkLinks(links []computerNetworkLink) (int, error) {
	loopbackIndex := 0
	for _, link := range links {
		if link.name == "lo" && link.loopback && link.up && link.index > 0 && loopbackIndex == 0 {
			loopbackIndex = link.index
			continue
		}
		if !link.up && link.addresses == 0 && computerFallbackTunnel(link.name, link.kind) {
			continue
		}
		return 0, &RuntimeSpecRejectionError{err: fmt.Errorf("Computer network none refuses link %q (kind=%s up=%t addresses=%d)", link.name, link.kind, link.up, link.addresses)}
	}
	if loopbackIndex == 0 {
		return 0, &RuntimeSpecRejectionError{err: fmt.Errorf("Computer network none requires an active loopback interface")}
	}
	return loopbackIndex, nil
}

func computerFallbackTunnel(name, kind string) bool {
	return map[string]string{
		"tunl0": "ipip", "sit0": "sit", "ip6tnl0": "ip6tnl", "gre0": "gre",
		"gretap0": "gretap", "erspan0": "erspan", "ip6gre0": "ip6gre",
		"ip6gretap0": "ip6gretap", "ip6erspan0": "ip6erspan",
	}[name] == kind && kind != ""
}
