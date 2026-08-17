package main

import (
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
