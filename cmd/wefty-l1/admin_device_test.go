package main

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestOfflineAdminDeviceRebindFlags(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "wefty-l1")
	if out, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, out)
	}
	db := filepath.Join(t.TempDir(), "l1.sqlite")
	store, err := l1.OpenStore(db, l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	identity := fabric.Identity{FabricID: "issuer", UserID: "owner", DeviceID: "first-device"}
	challenge, err := store.InitiateAdminBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.BootstrapAdmin(t.Context(), identity, challenge.Nonce); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) (string, error) {
		t.Helper()
		out, err := exec.Command(binary, append([]string{"-db", db}, args...)...).CombinedOutput()
		return string(out), err
	}
	for _, args := range [][]string{
		{"-rebind-admin", "owner"}, {"-admin-device-id", "next-device"},
		{"-rebind-admin", "missing", "-admin-device-id", "next-device"},
		{"-rebind-admin", "owner", "-admin-device-id", "next-device", "-reset-admin-policy"},
		{"-rebind-admin", "owner", "-admin-device-id", "next-device", "-initiate-admin-bootstrap"},
		{"-rebind-admin", "owner", "-admin-device-id", "next-device", "-state-preflight"},
	} {
		if out, err := run(args...); err == nil {
			t.Fatalf("accepted invalid offline combination %v: %s", args, out)
		}
	}
	out, err := run("-rebind-admin", "owner", "-admin-device-id", "next-device")
	if err != nil || !strings.Contains(out, `"device_id":"next-device"`) || !strings.Contains(out, `"revision":2`) {
		t.Fatalf("rebind: %v %s", err, out)
	}
	store, err = l1.OpenStore(db, l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	audit, err := store.ListAdminPolicyAudit(t.Context(), identity, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(audit)
	if err != nil {
		t.Fatal(err)
	}
	if len(audit.Entries) != 2 || !strings.Contains(string(encoded), `"operation":"rebind"`) || !strings.Contains(string(encoded), `"actor_kind":"local_operator"`) {
		t.Fatalf("offline rebind audit: %s", encoded)
	}
}
