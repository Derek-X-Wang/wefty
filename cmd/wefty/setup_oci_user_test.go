package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
)

func TestSetupOCIUserServiceOwnsHostVMAndRetainsSettings(t *testing.T) {
	f := newSetupFixture(t)
	config := setupOCIAssetsFixture(t, f)
	f.setup(t, setupRequest{ociConfig: config})
	before := f.plist(t, setupNodeAgentLabel)
	for _, want := range []string{"--oci-user-install=", "--oci-helper-socket=", "--oci-intent-file=", "LIMA_HOME", "Interactive"} {
		if !strings.Contains(before, want) {
			t.Fatalf("node LaunchAgent lacks %s: %s", want, before)
		}
	}
	paths, err := resolveSetupPaths(f.home)
	if err != nil {
		t.Fatal(err)
	}
	intentPath := filepath.Join(paths.data, "oci-intent.json")
	f.setup(t, setupRequest{beforeWrites: func() error {
		_, err := limarunner.SetOCIIntent(t.Context(), intentPath, 1, false, time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC))
		return err
	}})
	intent, err := (limarunner.FileIntentSource{Path: intentPath}).ReadIntent(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if intent.Enabled || intent.Revision != 2 {
		t.Fatal("setup replaced an operator intent written during preparation")
	}
	f.setup(t, setupRequest{})
	if f.plist(t, setupNodeAgentLabel) != before {
		t.Fatal("rerun lost host VM supervision")
	}
	if len(f.launchd.mutating()) != 0 {
		t.Fatal("unchanged OCI setup restarted services")
	}
	stopped := false
	f.host.stopHostVM = func(context.Context, string, string) error { stopped = true; return nil }
	if _, err := runSetupRemove(t.Context(), f.host, false); err != nil {
		t.Fatal(err)
	}
	if !stopped {
		t.Fatal("removal did not gracefully stop the per-user host VM")
	}

}

func setupOCIAssetsFixture(t *testing.T, f *setupFixture) string {
	t.Helper()
	helper := filepath.Join(f.source, "linux-helper")
	archive := filepath.Join(f.source, "probe.tar")
	if err := os.WriteFile(helper, []byte("linux helper"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(archive, []byte("probe archive"), 0600); err != nil {
		t.Fatal(err)
	}
	checksum, err := fileDigest(helper)
	if err != nil {
		t.Fatal(err)
	}
	assets := limarunner.UserInstallAssets{LinuxHelper: helper, HelperVersion: "v1", HelperChecksum: checksum, ProbeArchive: archive, ProbeReference: "example/probe:v1", ProbeDigest: "sha256:" + strings.Repeat("a", 64)}
	payload, _ := json.Marshal(assets)
	config := filepath.Join(f.source, "assets.json")
	if err := os.WriteFile(config, payload, 0600); err != nil {
		t.Fatal(err)
	}
	return config
}

func TestRepeatedSetupPreservesExistingIntentPosition(t *testing.T) {
	for _, position := range []string{"legacy-enabled", "off", "on-demand", "always-on"} {
		t.Run(position, func(t *testing.T) {
			f := newSetupFixture(t)
			assets := setupOCIAssetsFixture(t, f)
			f.setup(t, setupRequest{ociConfig: assets})
			paths, err := resolveSetupPaths(f.home)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(paths.data, "oci-intent.json")
			var payload []byte
			if position == "legacy-enabled" {
				payload = []byte(`{"version":1,"revision":7,"enabled":true,"updated_at":"1970-01-01T00:00:01Z"}`)
			} else {
				payload, _ = json.Marshal(limarunner.OCIIntent{Version: 2, Revision: 7, Enabled: position != "off", Position: limarunner.OCIPosition(position), UpdatedAt: time.Unix(1, 0)})
			}
			if err := os.WriteFile(path, payload, 0600); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				f.setup(t, setupRequest{})
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(after) != string(payload) {
				t.Fatalf("setup rewrote %s marker", position)
			}
			if position == "legacy-enabled" {
				intent, err := (limarunner.FileIntentSource{Path: path}).ReadIntent(t.Context())
				if err != nil || intent.PositionValue() != limarunner.OCIPositionAlwaysOn {
					t.Fatalf("legacy upgrade=%+v %v", intent, err)
				}
			}
		})
	}
}
