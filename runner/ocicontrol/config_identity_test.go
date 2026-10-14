package ocicontrol

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInstalledConfigRecordsNodeIdentityAndReadsOldInstalls(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.json")
	want := InstalledConfig{Version: InstalledConfigVersion, ControlSocket: "/tmp/control.sock", NodeID: "stable-node"}
	if err := WriteInstalledConfig(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := ReadInstalledConfig(path)
	if err != nil || got != want {
		t.Fatalf("installed configuration=%+v err=%v want=%+v", got, err, want)
	}
	// Socket-only commands must still be able to use an older installation.
	if err := os.WriteFile(path, []byte(`{"version":1,"control_socket":"/tmp/control.sock"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err = ReadInstalledConfig(path)
	if err != nil || got.NodeID != "" || got.ControlSocket != want.ControlSocket {
		t.Fatalf("old configuration=%+v err=%v", got, err)
	}
}

// A plain-process node installed by `wefty setup` has no OCI control socket.
// Its configuration is still the record of which node is this machine, so it
// must be valid with the stable node ID alone, and only then.
func TestInstalledConfigForAPlainNodeNeedsOnlyTheNodeID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.json")
	plainNode := InstalledConfig{Version: InstalledConfigVersion, NodeID: "plain-node"}
	if err := WriteInstalledConfig(path, plainNode); err != nil {
		t.Fatalf("write plain node configuration: %v", err)
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "{\"version\":1,\"node_id\":\"plain-node\"}\n" {
		t.Fatalf("plain node configuration = %s", payload)
	}
	got, err := ReadInstalledConfig(path)
	if err != nil || got != plainNode {
		t.Fatalf("plain node configuration=%+v err=%v", got, err)
	}
	for _, invalid := range []string{
		`{"version":1}`,
		`{"version":1,"node_id":"  "}`,
		`{"version":1,"control_socket":"relative.sock","node_id":"plain-node"}`,
		`{"version":2,"node_id":"plain-node"}`,
	} {
		if err := os.WriteFile(path, []byte(invalid), 0o600); err != nil {
			t.Fatal(err)
		}
		if got, err := ReadInstalledConfig(path); err == nil {
			t.Fatalf("configuration %s was accepted as %+v", invalid, got)
		}
	}
	if err := WriteInstalledConfig(path, InstalledConfig{Version: InstalledConfigVersion}); err == nil {
		t.Fatal("wrote a configuration that names neither a control socket nor a node")
	}
}
