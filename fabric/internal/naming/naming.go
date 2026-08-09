// Package naming owns the implementation details of wefty logical addresses.
// It is internal so concrete network names never become part of the seam.
package naming

import (
	"crypto/sha256"
	"fmt"
	"net"
	"net/url"
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
	case "node":
		escapedID := strings.TrimPrefix(u.EscapedPath(), "/")
		nodeID, err := url.PathUnescape(escapedID)
		if err != nil || nodeID == "" || strings.Contains(escapedID, "/") {
			return Name{}, true, fmt.Errorf("invalid node address %q", address)
		}
		return Name{kind: "node", nodeID: nodeID}, true, nil
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
	return "wefty://node/" + url.PathEscape(n.nodeID)
}

// Hostname returns the private transport hostname for a logical address.
func (n Name) Hostname() string {
	switch n.kind {
	case "control-plane", "run-ledger":
		return n.kind
	}
	sum := sha256.Sum256([]byte(n.nodeID))
	return fmt.Sprintf("node-%x", sum[:8])
}

// TransportAddress returns the private transport target for a logical name.
func (n Name) TransportAddress() string {
	return net.JoinHostPort(n.Hostname(), servicePort)
}
