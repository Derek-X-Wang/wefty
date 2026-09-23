package main

import (
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/l1"
)

func TestNodePolicyFlagsComposeIndependentCapacityAndTags(t *testing.T) {
	policies := make(map[string]l1.NodePolicy)
	if err := (nodeSlotsFlag{policies: policies}).Set("node-1=8"); err != nil {
		t.Fatal(err)
	}
	if err := (nodeTagsFlag{policies: policies}).Set("node-1=linux,arm64"); err != nil {
		t.Fatal(err)
	}
	if err := (nodeSlotsFlag{policies: policies, service: true}).Set("node-1=3"); err != nil {
		t.Fatal(err)
	}

	policy := policies["node-1"]
	if policy.MaxOneshotSlots != 8 || policy.MaxServiceSlots != 3 {
		t.Fatalf("capacity = %d/%d, want 8/3", policy.MaxOneshotSlots, policy.MaxServiceSlots)
	}
	if len(policy.Tags) != 2 || policy.Tags[0] != "linux" || policy.Tags[1] != "arm64" {
		t.Fatalf("tags = %v, want [linux arm64]", policy.Tags)
	}
}

func TestNodePolicyFlagsKeepDefaultForUnspecifiedClass(t *testing.T) {
	policies := make(map[string]l1.NodePolicy)
	if err := (nodeSlotsFlag{policies: policies, service: true}).Set("node-1=0"); err != nil {
		t.Fatal(err)
	}
	policy := policies["node-1"]
	if policy.MaxOneshotSlots != l1.DefaultMaxOneshotSlots || policy.MaxServiceSlots != 0 {
		t.Fatalf("capacity = %d/%d, want %d/0", policy.MaxOneshotSlots, policy.MaxServiceSlots, l1.DefaultMaxOneshotSlots)
	}
}

// TestRequireReachableRunLedgerRefusesAnUnreachableDefault pins the start-time
// guard from wefty #548. An L1 whose run-ledger address plain Fabric cannot
// resolve can never revoke a Computer's authority, so every stop, reset,
// reimage, restore, removal and attempt completion fails for the life of the
// deployment. The address is knowable at start; the refusal belongs there.
func TestRequireReachableRunLedgerRefusesAnUnreachableDefault(t *testing.T) {
	for _, test := range []struct {
		name, fabricMode, address string
		wantRefusal               bool
	}{
		{name: "plain default", fabricMode: "plain", address: "wefty://run-ledger", wantRefusal: true},
		{name: "plain mixed case", fabricMode: "Plain", address: "WEFTY://run-ledger", wantRefusal: true},
		{name: "plain host port", fabricMode: "plain", address: "127.0.0.1:42102"},
		{name: "plain no run ledger", fabricMode: "plain", address: ""},
		{name: "tsnet logical name", fabricMode: "tsnet", address: "wefty://run-ledger"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := requireReachableRunLedger(test.fabricMode, test.address)
			if test.wantRefusal != (err != nil) {
				t.Fatalf("requireReachableRunLedger(%q, %q) = %v, want refusal=%t", test.fabricMode, test.address, err, test.wantRefusal)
			}
			if err != nil && !strings.Contains(err.Error(), "host:port") {
				t.Fatalf("refusal does not say what to pass instead: %v", err)
			}
		})
	}
}
