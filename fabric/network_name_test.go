package fabric_test

import (
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestRequestedNetworkName(t *testing.T) {
	for _, tc := range []struct{ logical, want string }{
		{"wefty://control-plane", "wefty-control-plane"},
		{"wefty://run-ledger", "wefty-run-ledger"},
		{"wefty://node/studio-0123abcd", "wefty-studio"},
		{"wefty://client/studio-0123abcd", "wefty-studio-client"},
		{"wefty://person/studio-0123abcd", "wefty-studio-person"},
		{"wefty://node/studio", "wefty-studio"},
		{"wefty://client/a", "wefty-a-client"},
		{"wefty://person/a", "wefty-a-person"},
		{"wefty://node/studio-123", "wefty-studio-123"},
		{"wefty://node/" + strings.Repeat("a", 32) + "-0123abcd", "wefty-" + strings.Repeat("a", 32)},
	} {
		t.Run(tc.logical, func(t *testing.T) {
			got, err := fabric.RequestedNetworkName(tc.logical)
			if err != nil || got != tc.want {
				t.Fatalf("name=%q err=%v want=%q", got, err, tc.want)
			}
		})
	}
	label := regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
	for _, role := range []string{"node", "client", "person"} {
		seen := map[string]string{}
		for _, id := range []string{"Studio_One", "Studio.One", "studio-one", "---", "工作室", strings.Repeat("a", 100), strings.Repeat("a", 99) + "b", strings.Repeat("a", 32) + "-0123abcd", "studio--one-0123abcd", "studio-0123ABCD"} {
			name, err := fabric.RequestedNetworkName("wefty://" + role + "/" + url.PathEscape(id))
			if err != nil || len(name) > 63 || !label.MatchString(name) || !strings.HasPrefix(name, "wefty-") {
				t.Fatalf("%s/%s: name=%q err=%v", role, id, name, err)
			}
			if len(id) == 100 && len(name) != 63 {
				t.Fatalf("long ID label is %d bytes, want 63", len(name))
			}
			if role != "node" && !strings.HasSuffix(name, "-"+role) {
				t.Fatalf("lost role suffix: %q", name)
			}
			if old, ok := seen[name]; ok {
				t.Fatalf("%q and %q map to %q", old, id, name)
			}
			seen[name] = id
			again, _ := fabric.RequestedNetworkName("wefty://" + role + "/" + url.PathEscape(id))
			if again != name {
				t.Fatal("unstable name")
			}
		}
	}
	for _, bad := range []string{"studio", "wefty://node/", "wefty://unknown"} {
		if name, err := fabric.RequestedNetworkName(bad); err == nil {
			t.Fatalf("invalid logical name %q accepted as %q", bad, name)
		}
	}
}

func TestRequestedNetworkNameAvoidsServiceAndRoleSlugs(t *testing.T) {
	for _, tc := range []struct{ logical, want string }{
		{"wefty://node/control-plane", "wefty-control-plane-96826665"},
		{"wefty://client/control-plane", "wefty-control-plane-96826665-client"},
		{"wefty://person/control-plane", "wefty-control-plane-96826665-person"},
		{"wefty://node/run-ledger", "wefty-run-ledger-01df4aa5"},
		{"wefty://client/run-ledger", "wefty-run-ledger-01df4aa5-client"},
		{"wefty://person/run-ledger", "wefty-run-ledger-01df4aa5-person"},
		{"wefty://node/a-client", "wefty-a-client-65e80a6c"},
		{"wefty://client/a-client", "wefty-a-client-65e80a6c-client"},
		{"wefty://person/a-client", "wefty-a-client-65e80a6c-person"},
		{"wefty://node/a-person", "wefty-a-person-c910dc9d"},
		{"wefty://client/a-person", "wefty-a-person-c910dc9d-client"},
		{"wefty://person/a-person", "wefty-a-person-c910dc9d-person"},
		{"wefty://node/control-plane-0123abcd", "wefty-control-plane-5dafb69c"},
		{"wefty://client/control-plane-0123abcd", "wefty-control-plane-5dafb69c-client"},
		{"wefty://person/control-plane-0123abcd", "wefty-control-plane-5dafb69c-person"},
		{"wefty://node/a-client-0123abcd", "wefty-a-client-317dc9f3"},
		{"wefty://client/a-client-0123abcd", "wefty-a-client-317dc9f3-client"},
		{"wefty://person/a-client-0123abcd", "wefty-a-client-317dc9f3-person"},
	} {
		t.Run(tc.logical, func(t *testing.T) {
			got, err := fabric.RequestedNetworkName(tc.logical)
			if err != nil || got != tc.want {
				t.Fatalf("name=%q err=%v want=%q", got, err, tc.want)
			}
		})
	}
}
