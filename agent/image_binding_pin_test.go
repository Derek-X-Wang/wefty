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
