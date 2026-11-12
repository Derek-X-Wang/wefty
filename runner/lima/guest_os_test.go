package lima

import (
	"context"
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

// The base is advisory: a failed read, an unparsable release or a failed write
// leaves it unknown and lets preparation continue. Only cancellation stops it.
func TestRecordGuestOSAdvisoryNeverFailsPreparation(t *testing.T) {
	limaHome := t.TempDir()
	if err := os.Mkdir(filepath.Join(limaHome, "wefty-oci"), 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(limaHome, "wefty-oci", GuestOSObservationFile)
	now := time.Date(2026, 11, 11, 20, 0, 0, 0, time.UTC)
	for name, tc := range map[string]struct {
		instance string
		read     func() ([]byte, error)
	}{
		"read":  {"wefty-oci", func() ([]byte, error) { return nil, errors.New("ssh: connection refused") }},
		"parse": {"wefty-oci", func() ([]byte, error) { return []byte("NAME=nothing\n"), nil }},
		"write": {"absent-instance", func() ([]byte, error) { return []byte("ID=ubuntu\nVERSION_ID=26.04\n"), nil }},
	} {
		if err := recordGuestOSAdvisory(t.Context(), tc.read, limaHome, tc.instance, now); err != nil {
			t.Fatalf("%s failure stopped preparation: %v", name, err)
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s failure left a record: %v", name, err)
		}
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := recordGuestOSAdvisory(canceled, func() ([]byte, error) { return nil, canceled.Err() }, limaHome, "wefty-oci", now); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled observation = %v", err)
	}
	if err := recordGuestOSAdvisory(t.Context(), func() ([]byte, error) { return []byte("ID=ubuntu\nVERSION_ID=26.04\n"), nil }, limaHome, "wefty-oci", now); err != nil {
		t.Fatal(err)
	}
	if observation, err := ReadGuestOSObservation(path); err != nil || observation.GuestOS != "ubuntu-26.04" {
		t.Fatalf("recorded = %+v, %v", observation, err)
	}
}

func fakeLimactl(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "limactl")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+script), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestInspectGuestKernelReadsStdoutOnly(t *testing.T) {
	limactl := fakeLimactl(t, `[ "$1 $2 $3 $4 $5 $6" = "--tty=false shell --workdir=/ wefty-oci uname -r" ] || exit 9
echo "WARN[0000] Lima warning on stderr" >&2
echo "7.0.0-15-generic"
`)
	if release, err := InspectGuestKernel(t.Context(), "wefty-oci", limactl); err != nil || release != "7.0.0-15-generic" {
		t.Fatalf("kernel = %q, %v", release, err)
	}
}

func TestInspectGuestKernelIsBounded(t *testing.T) {
	limactl := fakeLimactl(t, "exec sleep 30\n")
	started := time.Now()
	if release, err := inspectGuestKernel(t.Context(), runUserInstallCommand, 200*time.Millisecond, "wefty-oci", limactl); err == nil {
		t.Fatalf("stuck guest read returned %q", release)
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("stuck guest read held the caller for %s", elapsed)
	}
}

// --recreate acts on the base, so its observation is strict: a failed read,
// an unparsable release or a hung guest refuses with the typed error and
// writes nothing; a good read replaces an older record (#1593).
func TestObserveGuestOSRefusesUnknownAndRecordsKnown(t *testing.T) {
	limaHome := t.TempDir()
	if err := os.Mkdir(filepath.Join(limaHome, "wefty-oci"), 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(limaHome, "wefty-oci", GuestOSObservationFile)
	now := time.Date(2026, 11, 11, 20, 0, 0, 0, time.UTC)
	for name, script := range map[string]string{
		"read":   "exit 255\n",
		"parse":  "echo NAME=nothing\n",
		"hung":   "exec sleep 30\n",
		"stderr": "echo ID=ubuntu >&2; echo VERSION_ID=26.04 >&2\n",
	} {
		started := time.Now()
		_, err := observeGuestOS(t.Context(), runUserInstallCommand, 200*time.Millisecond, limaHome, "wefty-oci", fakeLimactl(t, script), now)
		var unreadable *GuestOSUnreadableError
		if !errors.As(err, &unreadable) {
			t.Fatalf("%s: err = %v, want GuestOSUnreadableError", name, err)
		}
		if elapsed := time.Since(started); elapsed > 10*time.Second {
			t.Fatalf("%s held the caller for %s", name, elapsed)
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s left a record: %v", name, err)
		}
	}
	if _, err := RecordGuestOS(limaHome, "wefty-oci", []byte("ID=ubuntu\nVERSION_ID=24.04\n"), now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	limactl := fakeLimactl(t, `[ "$1 $2 $3 $4 $5 $6" = "--tty=false shell --workdir=/ wefty-oci cat /etc/os-release" ] || exit 9
printf 'ID=ubuntu\nVERSION_ID="26.04"\n'
`)
	observation, err := observeGuestOS(t.Context(), runUserInstallCommand, 5*time.Second, limaHome, "wefty-oci", limactl, now)
	if err != nil || observation.GuestOS != "ubuntu-26.04" || !observation.ObservedAt.Equal(now) {
		t.Fatalf("observation = %+v, %v", observation, err)
	}
	if recorded, err := ReadGuestOSObservation(path); err != nil || recorded != observation {
		t.Fatalf("recorded = %+v, %v", recorded, err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := observeGuestOS(canceled, runUserInstallCommand, 5*time.Second, limaHome, "wefty-oci", limactl, now); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled observation = %v", err)
	}
}
