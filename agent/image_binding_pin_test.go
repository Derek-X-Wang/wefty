package agent

import (
	"context"
	"strings"
	"testing"

	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

func TestOCIBindingPinLedgerSurvivesRestartAndReleasesExplicitly(t *testing.T) {
	directory := t.TempDir()
	spool := openTestLogSpool(t, directory, "cache-pin-node", 1024)
	pin := workloadrunner.OCIImageBindingPin{
		JobID: "service-1", Reference: "example.test/service:stable",
		Digest:     "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		PlatformOS: "linux", PlatformArchitecture: "amd64", Snapshotter: "overlayfs",
	}
	if _, created, err := spool.PutOCIImageBindingPin(context.Background(), pin); err != nil || !created {
		t.Fatal(err)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openTestLogSpool(t, directory, "cache-pin-node", 1024)
	defer reopened.Close()
	pins, err := reopened.ListOCIImageBindingPins(context.Background())
	if err != nil || len(pins) != 1 || pins[0] != pin {
		t.Fatalf("reopened binding pins = %#v err=%v, want %#v", pins, err, pin)
	}
	if err := reopened.DeleteOCIImageBindingPin(context.Background(), pin.JobID); err != nil {
		t.Fatal(err)
	}
	pins, err = reopened.ListOCIImageBindingPins(context.Background())
	if err != nil || len(pins) != 0 {
		t.Fatalf("released binding pins = %#v err=%v", pins, err)
	}
}

func TestOCIBindingPinLedgerPreservesFirstBindingIdentity(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "cache-pin-identity", 1024)
	defer spool.Close()
	pin := workloadrunner.OCIImageBindingPin{
		JobID: "service-1", Reference: "example.test/service:stable",
		Digest:     "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		PlatformOS: "linux", PlatformArchitecture: "amd64", Snapshotter: "overlayfs",
	}
	if stored, created, err := spool.PutOCIImageBindingPin(t.Context(), pin); err != nil || !created || stored != pin {
		t.Fatalf("first binding = (%+v, %t, %v)", stored, created, err)
	}
	if stored, created, err := spool.PutOCIImageBindingPin(t.Context(), pin); err != nil || created || stored != pin {
		t.Fatalf("idempotent binding = (%+v, %t, %v)", stored, created, err)
	}
	changed := pin
	changed.PlatformArchitecture = "arm64"
	if stored, created, err := spool.PutOCIImageBindingPin(t.Context(), changed); err == nil || created || stored != pin || !strings.Contains(err.Error(), "conflicts with its first binding") {
		t.Fatalf("changed binding = (%+v, %t, %v)", stored, created, err)
	}
}

// A binding pin persisted before the platform normalization landed spells arm64
// as "v8" (#408). The agent now probes that same hardware as plain "linux/arm64",
// and the row it writes must be recognised as the same first binding — a
// conflict here would refuse the service forever on a row that names the very
// hardware it is running on. A genuinely different platform is still a conflict.
func TestOCIBindingPinLedgerMatchesALegacyArm64VariantRow(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "cache-pin-arm64", 1024)
	defer spool.Close()
	legacy := workloadrunner.OCIImageBindingPin{
		JobID: "service-1", Reference: "example.test/service:stable",
		Digest:     "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		PlatformOS: "linux", PlatformArchitecture: "arm64", PlatformVariant: "v8", Snapshotter: "overlayfs",
	}
	if stored, created, err := spool.PutOCIImageBindingPin(t.Context(), legacy); err != nil || !created || stored != legacy {
		t.Fatalf("legacy binding = (%+v, %t, %v)", stored, created, err)
	}
	probed := legacy
	probed.PlatformVariant = ""
	stored, created, err := spool.PutOCIImageBindingPin(t.Context(), probed)
	if err != nil || created || stored != legacy {
		t.Fatalf("normal-form rebinding = (%+v, %t, %v), want the legacy row matched", stored, created, err)
	}
	other := probed
	other.PlatformVariant = "v9"
	if _, _, err := spool.PutOCIImageBindingPin(t.Context(), other); err == nil ||
		!strings.Contains(err.Error(), "conflicts with its first binding") {
		t.Fatalf("arm64 v9 rebinding = %v, want a conflict", err)
	}
}
