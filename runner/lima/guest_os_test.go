package lima

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseOSReleaseNamesIDAndVersion(t *testing.T) {
	for _, tc := range []struct {
		payload string
		want    string
	}{
		{"NAME=\"Ubuntu\"\nVERSION_ID=\"26.04\"\nID=ubuntu\n", "ubuntu-26.04"},
		{"ID='debian'\nVERSION_ID='12'\n", "debian-12"},
		{"  ID=ubuntu  \nVERSION_ID=24.04\nVERSION=\"24.04.3 LTS\"\n", "ubuntu-24.04"},
	} {
		if got, err := ParseOSRelease([]byte(tc.payload)); err != nil || got != tc.want {
			t.Fatalf("ParseOSRelease(%q) = %q, %v", tc.payload, got, err)
		}
	}
	for _, payload := range []string{"", "ID=ubuntu\n", "VERSION_ID=26.04\n", "ID=Ubuntu Linux\nVERSION_ID=26.04\n", "ID=ubuntu\nVERSION_ID=\"26 04\"\n"} {
		if got, err := ParseOSRelease([]byte(payload)); err == nil {
			t.Fatalf("ParseOSRelease(%q) = %q, want refusal", payload, got)
		}
	}
}

func TestGuestOSObservationRoundTripsAndRefusesInvalidRecords(t *testing.T) {
	limaHome := t.TempDir()
	if err := os.Mkdir(filepath.Join(limaHome, "wefty-oci"), 0o700); err != nil {
		t.Fatal(err)
	}
	path, err := GuestOSObservationPath(limaHome, "wefty-oci")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadGuestOSObservation(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("never observed = %v, want not exist", err)
	}
	at := time.Date(2026, 11, 11, 12, 0, 0, 0, time.FixedZone("PST", -8*3600))
	written, err := RecordGuestOS(limaHome, "wefty-oci", []byte("ID=ubuntu\nVERSION_ID=\"24.04\"\n"), at)
	if err != nil {
		t.Fatal(err)
	}
	read, err := ReadGuestOSObservation(path)
	if err != nil || read != written || read.GuestOS != "ubuntu-24.04" || !read.ObservedAt.Equal(at) {
		t.Fatalf("read observation = %+v, %v; wrote %+v", read, err, written)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("observation mode = %v, %v", info, err)
	}
	for _, payload := range []string{
		`{"guest_os":"ubuntu-24.04"}`,
		`{"guest_os":"","observed_at":"2026-11-11T20:00:00Z"}`,
		`{"guest_os":"ubuntu-24.04","observed_at":"2026-11-11T20:00:00Z","kernel":"6.8"}`,
		`{"guest_os":"ubuntu-24.04","observed_at":"2026-11-11T20:00:00Z"}{}`,
	} {
		if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadGuestOSObservation(path); err == nil || errors.Is(err, os.ErrNotExist) {
			t.Fatalf("invalid observation %s = %v", payload, err)
		}
	}
	if _, err := GuestOSObservationPath("relative", "wefty-oci"); err == nil {
		t.Fatal("relative Lima home accepted")
	}
	if _, err := GuestOSObservationPath(limaHome, "../escape"); err == nil {
		t.Fatal("instance path escape accepted")
	}
}

func TestHostVMBaseNeverTreatsUnknownAsDifferent(t *testing.T) {
	if base := NewHostVMBase(GuestOSObservation{}, false); base.UpgradeAvailable || base.Current != "" || base.ObservedAt != nil || base.Desired != TemplateGuestOS {
		t.Fatalf("unobserved base = %+v", base)
	}
	at := time.Date(2026, 11, 11, 20, 0, 0, 0, time.UTC)
	if base := NewHostVMBase(GuestOSObservation{GuestOS: "ubuntu-24.04", ObservedAt: at}, true); !base.UpgradeAvailable || base.Current != "ubuntu-24.04" || base.ObservedAt == nil || !base.ObservedAt.Equal(at) {
		t.Fatalf("older base = %+v", base)
	}
	if base := NewHostVMBase(GuestOSObservation{GuestOS: TemplateGuestOS, ObservedAt: at}, true); base.UpgradeAvailable {
		t.Fatalf("current base = %+v", base)
	}
}

func TestTemplateBootsTheDesiredGuestOS(t *testing.T) {
	payload, err := RenderTemplate(TemplateConfig{Sizing: Sizing{Memory: "4GiB", CPUs: 4, Disk: "32GiB"}, HostAllowedMountRoot: "/Users/operator/wefty"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(payload), "  - template:_images/"+TemplateGuestOS+"\n") {
		t.Fatalf("template base does not name %s:\n%s", TemplateGuestOS, payload)
	}
}
