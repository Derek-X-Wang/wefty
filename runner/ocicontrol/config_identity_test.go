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
