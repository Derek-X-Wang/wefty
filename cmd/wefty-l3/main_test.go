package main

import (
	"strings"
	"testing"
)

// TestRequireReachableControlPlaneRefusesAnUnresolvableAddress mirrors L1's
// -run-ledger guard (#604 item 7). On plain Fabric a separate L3 process can
// never dial the logical control-plane name, so it would accept runs and
// never dispatch one; the start is refused instead.
func TestRequireReachableControlPlaneRefusesAnUnresolvableAddress(t *testing.T) {
	for _, test := range []struct {
		name, fabricMode, address string
		wantRefusal               bool
	}{
		{name: "plain default logical name", fabricMode: "plain", address: "wefty://control-plane", wantRefusal: true},
		{name: "plain other logical name", fabricMode: "plain", address: "WEFTY://elsewhere", wantRefusal: true},
		{name: "plain empty", fabricMode: "plain", address: " ", wantRefusal: true},
		{name: "plain host:port", fabricMode: "plain", address: "127.0.0.1:42101"},
		{name: "tsnet logical name", fabricMode: "tsnet", address: "wefty://control-plane"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := requireReachableControlPlane(test.fabricMode, test.address)
			if test.wantRefusal != (err != nil) {
				t.Fatalf("requireReachableControlPlane(%q, %q) = %v, want refusal=%t", test.fabricMode, test.address, err, test.wantRefusal)
			}
			if err != nil && !strings.Contains(err.Error(), "host:port") {
				t.Fatalf("refusal does not say what to pass instead: %v", err)
			}
		})
	}
}
