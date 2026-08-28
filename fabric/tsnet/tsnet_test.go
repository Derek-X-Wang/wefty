package tsnet

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
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

func TestCoordinatorIdentityScopesFabricIdentity(t *testing.T) {
	first := coordinatorFabricID("https://issuer.invalid", "identity-domain-one")
	second := coordinatorFabricID("https://issuer.invalid", "identity-domain-two")
	if first == "" || second == "" || first == second || strings.Contains(first, "identity-domain-one") {
		t.Fatalf("Fabric identities = %q / %q", first, second)
	}
}

func TestIdentityTranslation(t *testing.T) {
	who := &apitype.WhoIsResponse{
		Node: &tailcfg.Node{
			StableID: tailcfg.StableNodeID("stable-node-id"),
			Name:     "internal-name.example.ts.net.",
			Tags:     []string{"tag:agent", "tag:linux"},
		},
		UserProfile: &tailcfg.UserProfile{ID: 42, LoginName: "agent@example.com", DisplayName: "Agent"},
	}

	identity, err := identityFromWhoIs(who, "fabric-one")
	if err != nil {
		t.Fatal(err)
	}
	if identity.NodeID != "stable-node-id" {
		t.Errorf("NodeID = %q, want stable-node-id", identity.NodeID)
	}
	if identity.UserID != "" || identity.DeviceID != "stable-node-id" || identity.FabricID != "fabric-one" {
		t.Errorf("machine identity = %#v", identity)
	}
	if identity.Kind != fabric.IdentityKindMachine {
		t.Errorf("Kind = %q, want machine", identity.Kind)
	}
	if identity.DisplayName != "" {
		t.Errorf("machine DisplayName = %q, want blank", identity.DisplayName)
	}
	if !slices.Equal(identity.Tags, []string{"tag:agent", "tag:linux"}) {
		t.Errorf("Tags = %v", identity.Tags)
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), who.UserProfile.LoginName) || strings.Contains(string(encoded), who.Node.Name) {
		t.Fatalf("translated identity leaked display login or network name: %s", encoded)
	}
}

func TestIdentityTranslationIgnoresLoginRenameAndDeviceName(t *testing.T) {
	base := &apitype.WhoIsResponse{
		Node:        &tailcfg.Node{StableID: "device-1", Name: "old-name.example.ts.net."},
		UserProfile: &tailcfg.UserProfile{ID: 42, LoginName: "old@example.test", DisplayName: "Old"},
	}
	renamed := &apitype.WhoIsResponse{
		Node:        &tailcfg.Node{StableID: "device-1", Name: "new-name.example.ts.net."},
		UserProfile: &tailcfg.UserProfile{ID: 42, LoginName: "new@example.test", DisplayName: "New"},
	}
	first, err := identityFromWhoIs(base, "fabric-one")
	if err != nil {
		t.Fatal(err)
	}
	second, err := identityFromWhoIs(renamed, "fabric-one")
	if err != nil {
		t.Fatal(err)
	}
	if first.UserID != second.UserID || first.DeviceID != second.DeviceID {
		t.Fatalf("rename changed stable identity: %#v -> %#v", first, second)
	}
	if first.DisplayName == second.DisplayName {
		t.Fatalf("fixture did not change optional display data: %#v -> %#v", first, second)
	}
}

func TestIdentityTranslationRejectsIncompleteResponse(t *testing.T) {
	fixtures := []*apitype.WhoIsResponse{
		{},
		{Node: &tailcfg.Node{StableID: "device"}, UserProfile: &tailcfg.UserProfile{}},
		{Node: &tailcfg.Node{}, UserProfile: &tailcfg.UserProfile{ID: 42}},
	}
	for _, fixture := range fixtures {
		if _, err := identityFromWhoIs(fixture, "fabric-one"); err == nil {
			t.Fatalf("identityFromWhoIs() accepted an incomplete response: %#v", fixture)
		}
	}
}
