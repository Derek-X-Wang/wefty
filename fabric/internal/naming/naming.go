// Package naming owns Wefty logical addresses and network-label derivation.
// Its representation stays behind the public Fabric helpers.
package naming

import (
	"crypto/sha256"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"
)

const servicePort = "80"

// Name is a parsed wefty logical address.
type Name struct {
	kind   string
	nodeID string
}

// Parse parses address when it uses the wefty scheme. The bool result is false
// for ordinary network addresses, which implementations should pass through.
func Parse(address string) (Name, bool, error) {
	if !strings.HasPrefix(strings.ToLower(address), "wefty:") {
		return Name{}, false, nil
	}
	u, err := url.Parse(address)
	if err != nil {
		return Name{}, true, err
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Port() != "" {
		return Name{}, true, fmt.Errorf("invalid wefty address %q", address)
	}

	switch strings.ToLower(u.Hostname()) {
	case "control-plane":
		if u.Path != "" && u.Path != "/" {
			return Name{}, true, fmt.Errorf("invalid control-plane address %q", address)
		}
		return Name{kind: "control-plane"}, true, nil
	case "run-ledger":
		if u.Path != "" && u.Path != "/" {
			return Name{}, true, fmt.Errorf("invalid run-ledger address %q", address)
		}
		return Name{kind: "run-ledger"}, true, nil
	case "node", "client", "person":
		escapedID := strings.TrimPrefix(u.EscapedPath(), "/")
		nodeID, err := url.PathUnescape(escapedID)
		if err != nil || nodeID == "" || strings.Contains(escapedID, "/") {
			return Name{}, true, fmt.Errorf("invalid node address %q", address)
		}
		return Name{kind: strings.ToLower(u.Hostname()), nodeID: nodeID}, true, nil
	default:
		return Name{}, true, fmt.Errorf("unknown wefty address %q", address)
	}
}

// String returns the canonical wefty address.
func (n Name) String() string {
	switch n.kind {
	case "control-plane", "run-ledger":
		return "wefty://" + n.kind
	}
	return "wefty://" + n.kind + "/" + url.PathEscape(n.nodeID)
}

// Hostname returns the private transport hostname for a logical address.
func (n Name) Hostname() string {
	switch n.kind {
	case "control-plane", "run-ledger":
		return n.kind
	}
	sum := sha256.Sum256([]byte(n.nodeID))
	return fmt.Sprintf("%s-%x", n.kind, sum[:8])
}

// AdvertisesNetworkName reports whether other machines reach this participant
// by its network name: only a node agent registers it as its connect host.
func (n Name) AdvertisesNetworkName() bool { return n.kind == "node" }

// TransportAddress returns the private transport target for a logical name.
func (n Name) TransportAddress() string {
	return net.JoinHostPort(n.Hostname(), servicePort)
}

// ReadableHostname requests a namespaced machine-first label for a new
// participant. Hostname deliberately remains the legacy saved-state mapping.
func (n Name) ReadableHostname() string {
	switch n.kind {
	case "control-plane", "run-ledger":
		return "wefty-" + n.kind
	}
	suffix := ""
	if n.kind != "node" {
		suffix = "-" + n.kind
	}
	machine := n.nodeID
	if len(machine) <= 41 && setupNodeID.MatchString(machine) {
		machine = machine[:len(machine)-9]
	}
	var slug strings.Builder
	for _, r := range strings.ToLower(machine) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			slug.WriteRune(r)
		} else if slug.Len() > 0 && !strings.HasSuffix(slug.String(), "-") {
			slug.WriteByte('-')
		}
	}
	base := strings.Trim(slug.String(), "-")
	if base == "" {
		base = "machine"
	}
	limit := 63 - len("wefty-") - len(suffix)
	reserved := base == "control-plane" || base == "run-ledger" || strings.HasSuffix(base, "-client") || strings.HasSuffix(base, "-person")
	if base != machine || len(base) > limit || reserved {
		sum := sha256.Sum256([]byte(n.nodeID))
		hash := fmt.Sprintf("-%x", sum[:4])
		if len(base) > limit-len(hash) {
			base = strings.TrimRight(base[:limit-len(hash)], "-")
		}
		base += hash
	}
	return "wefty-" + base + suffix
}

// Setup emits 1-32 lowercase alphanumeric characters with single internal
// hyphens, then a random eight-hex suffix. Other IDs retain their whole input.
var setupNodeID = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9]|-[a-z0-9]){0,31}-[0-9a-f]{8}$`)
