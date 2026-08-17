package tsnet

import (
	"slices"
	"strings"
	"testing"

	"tailscale.com/client/tailscale/apitype"
	"tailscale.com/tailcfg"
)

func TestNewRequiresWeftyName(t *testing.T) {
	if _, err := New(Config{Name: "runner-1"}); err == nil {
		t.Fatal("New() accepted a transport-specific name")
	}
	if _, err := New(Config{Name: "wefty://node/runner-1"}); err != nil {
		t.Fatalf("New() rejected a wefty name: %v", err)
	}
}

func TestConnectHostProjectsPrivateTransportHostname(t *testing.T) {
	f, err := New(Config{Name: "wefty://node/runner-1"})
	if err != nil {
		t.Fatal(err)
	}
	got := f.ConnectHost()
	if got == "" || strings.Contains(got, "wefty://") || !strings.HasPrefix(got, "node-") {
		t.Fatalf("ConnectHost() = %q, want a non-logical private hostname", got)
	}
}

func TestIdentityTranslation(t *testing.T) {
	who := &apitype.WhoIsResponse{
		Node: &tailcfg.Node{
			StableID: tailcfg.StableNodeID("stable-node-id"),
			Name:     "internal-name.example.ts.net.",
			Tags:     []string{"tag:agent", "tag:linux"},
		},
		UserProfile: &tailcfg.UserProfile{LoginName: "agent@example.com"},
	}

	identity, err := identityFromWhoIs(who)
	if err != nil {
		t.Fatal(err)
	}
	if identity.NodeID != "stable-node-id" {
		t.Errorf("NodeID = %q, want stable-node-id", identity.NodeID)
	}
	if identity.User != "agent@example.com" {
		t.Errorf("User = %q, want agent@example.com", identity.User)
	}
	if !slices.Equal(identity.Tags, []string{"tag:agent", "tag:linux"}) {
		t.Errorf("Tags = %v", identity.Tags)
	}
}

func TestIdentityTranslationRejectsIncompleteResponse(t *testing.T) {
	if _, err := identityFromWhoIs(&apitype.WhoIsResponse{}); err == nil {
		t.Fatal("identityFromWhoIs() accepted an incomplete response")
	}
}
