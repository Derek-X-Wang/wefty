package tsnet

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/tailscale/hujson"
)

func TestPrivateNetworkPolicy(t *testing.T) {
	policy := PrivateNetworkPolicy()
	standard, err := hujson.Standardize([]byte(policy.Snippet))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		TagOwners map[string][]string `json:"tagOwners"`
		ACLs      []struct {
			Action string   `json:"action"`
			Proto  string   `json:"proto"`
			Src    []string `json:"src"`
			Dst    []string `json:"dst"`
		} `json:"acls"`
	}
	if err := json.Unmarshal(standard, &doc); err != nil {
		t.Fatal(err)
	}
	owners := doc.TagOwners["tag:wefty-agent"]
	if len(owners) != 1 || owners[0] != "autogroup:admin" {
		t.Fatalf("agent tag owners: %v", owners)
	}
	allows := func(src, dst string) bool {
		for _, rule := range doc.ACLs {
			if rule.Action != "accept" || rule.Proto != "tcp" {
				continue
			}
			for _, source := range rule.Src {
				if source != src && source != "*" {
					continue
				}
				for _, target := range rule.Dst {
					if target == "*:*" || target == dst || strings.HasSuffix(target, ":*") && strings.HasPrefix(dst, strings.TrimSuffix(target, "*")) {
						return true
					}
				}
			}
		}
		return false
	}
	for _, source := range []string{"tag:wefty-client", "tag:wefty-agent"} {
		for _, target := range []string{"tag:wefty-control:80", "tag:wefty-client:80"} {
			if !allows(source, target) {
				t.Errorf("missing required allow %s → %s", source, target)
			}
		}
	}
	if !allows("tag:wefty-control", "tag:wefty-client:80") {
		t.Fatal("missing L1 → L3 allow")
	}
	if !allows("autogroup:owner", "tag:wefty-control:80") {
		t.Error("owner cannot reach L1 for person commands")
	}
	for _, port := range []string{"80", "443", "8080", "5432"} {
		target := "tag:wefty-agent:" + port
		if !allows("autogroup:owner", target) {
			t.Errorf("owner cannot reach %s", target)
		}
		for _, source := range []string{"tag:wefty-control", "tag:wefty-client", "tag:wefty-agent"} {
			if allows(source, target) {
				t.Errorf("machine may reach another agent's published port: %s → %s", source, target)
			}
		}
	}
	notes := strings.Join(policy.Notes, " ")
	for _, want := range []string{
		"Owners, Admins and Network admins implicitly own all tags",
		"tagOwners alone does not limit tagging to admins",
		"Keep the Owner person identity on the owner's laptop. Do not sign the control-plane machine's network app, or any machine where AI agents run as you, into the Owner account.",
	} {
		if !strings.Contains(notes, want) {
			t.Errorf("policy guidance missing %q", want)
		}
	}
	if len(policy.Requirements) != 5 || !strings.Contains(strings.Join(policy.Notes, " "), "overlapping") {
		t.Fatalf("missing policy guidance: %+v", policy)
	}
}
