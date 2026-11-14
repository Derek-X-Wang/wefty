package lima

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestPerUserInstallRefusesWrongHomeAndCreatesThroughNodeProcess(t *testing.T) {
	root := t.TempDir()
	helper := filepath.Join(root, "linux-helper")
	archive := filepath.Join(root, "probe.tar")
	for _, p := range []string{helper, archive} {
		if err := os.WriteFile(p, []byte("fixture"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	checksum, err := checksumFile(helper)
	if err != nil {
		t.Fatal(err)
	}
	c := UserInstallConfig{Instance: "wefty-proof", LimaHome: filepath.Join(root, "lima"), MountRoot: filepath.Join(root, "mount"), IntentFile: filepath.Join(root, "intent.json"), SetupState: filepath.Join(root, "setup.json"), TemplatePath: filepath.Join(root, "host.yaml"), Sizing: Sizing{Memory: "4GiB", CPUs: 4, Disk: "32GiB"}, Assets: UserInstallAssets{LinuxHelper: helper, HelperVersion: "test", HelperChecksum: checksum, ProbeArchive: archive, ProbeReference: "probe:test", ProbeDigest: "sha256:" + strings.Repeat("a", 64)}}
	if _, err := InitializeOCIIntent(c.IntentFile, time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	var commands [][]string
	installs := 0
	ops := userInstallOps{run: func(_ context.Context, name string, args ...string) ([]byte, error) {
		commands = append(commands, append([]string{name}, args...))
		if reflect.DeepEqual(args, []string{"list", "--json"}) {
			return nil, nil
		}
		if len(args) > 0 && args[len(args)-1] == "-un" {
			return []byte("operator\n"), nil
		}
		if len(args) > 0 && args[len(args)-1] == "-u" {
			return []byte("501\n"), nil
		}
		if len(args) > 0 && args[len(args)-1] == "--version" {
			return []byte("systemd 259 (259.1-1ubuntu1)\n+PAM +AUDIT\n"), nil
		}
		if len(args) > 0 && args[0] == "create" {
			return nil, os.MkdirAll(filepath.Join(c.LimaHome, c.Instance), 0o700)
		}
		if len(args) > 0 && args[len(args)-1] == "/etc/os-release" {
			return []byte("PRETTY_NAME=\"Ubuntu 26.04 LTS\"\nNAME=\"Ubuntu\"\nVERSION_ID=\"26.04\"\nID=ubuntu\nID_LIKE=debian\n"), nil
		}
		return nil, nil
	}, ensure: func(context.Context) error { commands = append(commands, []string{"supervisor.Ensure"}); return nil }, install: func(_ context.Context, h GuestHelperInstallConfig) error {
		installs++
		if h.GuestUID != 501 || h.GuestUser != "operator" || h.Instance != "wefty-proof" || h.MemoryCapacityBytes != 4<<30 || h.MemoryReserveBytes != 1<<30 || h.SystemdVersion != 259 {
			t.Fatalf("guest install=%+v", h)
		}
		return nil
	}}
	t.Setenv("LIMA_HOME", filepath.Join(root, "wrong"))
	if _, err := prepareUserInstall(t.Context(), c, "node", "boot", ops); err == nil || len(commands) != 0 {
		t.Fatalf("wrong Lima home reached commands=%v err=%v", commands, err)
	}
	t.Setenv("LIMA_HOME", c.LimaHome)
	// The returned version is the one the helper unit was rendered for, so
	// the node agent can record it in the durable setup state (#1670).
	if systemdVersion, err := prepareUserInstall(t.Context(), c, "node", "boot", ops); err != nil || systemdVersion != 259 {
		t.Fatalf("systemd version=%d err=%v", systemdVersion, err)
	}
	if installs != 1 {
		t.Fatalf("helper installs=%d", installs)
	}
	want := [][]string{{"limactl", "list", "--json"}, {"limactl", "create", "--tty=false", "--name=wefty-proof", c.TemplatePath}, {"supervisor.Ensure"}, {"limactl", "--tty=false", "shell", "--workdir=/", "wefty-proof", "id", "-un"}, {"limactl", "--tty=false", "shell", "--workdir=/", "wefty-proof", "id", "-u"}, {"limactl", "--tty=false", "shell", "--workdir=/", "wefty-proof", "systemctl", "--version"}, {"limactl", "--tty=false", "shell", "--workdir=/", "wefty-proof", "cat", "/etc/os-release"}}
	if !reflect.DeepEqual(commands, want) {
		t.Fatalf("commands=%v", commands)
	}
	observation, err := ReadGuestOSObservation(filepath.Join(c.LimaHome, c.Instance, GuestOSObservationFile))
	if err != nil || observation.GuestOS != "ubuntu-26.04" {
		t.Fatalf("recorded guest OS = %+v, %v", observation, err)
	}
	if _, err := os.Stat(filepath.Join(root, "user-install-timing.json")); err != nil {
		t.Fatal(err)
	}
}

// A hung guest OS read after a good helper install uses its own short budget:
// preparation continues and the base stays unknown (#1593).
func TestPerUserInstallBoundsHungGuestOSRead(t *testing.T) {
	root := t.TempDir()
	helper := filepath.Join(root, "linux-helper")
	archive := filepath.Join(root, "probe.tar")
	for _, p := range []string{helper, archive} {
		if err := os.WriteFile(p, []byte("fixture"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	checksum, err := checksumFile(helper)
	if err != nil {
		t.Fatal(err)
	}
	c := UserInstallConfig{Instance: "wefty-oci", LimaHome: filepath.Join(root, "lima"), MountRoot: filepath.Join(root, "mount"), IntentFile: filepath.Join(root, "intent.json"), SetupState: filepath.Join(root, "setup.json"), TemplatePath: filepath.Join(root, "host.yaml"), Sizing: Sizing{Memory: "4GiB", CPUs: 4, Disk: "32GiB"}, Assets: UserInstallAssets{LinuxHelper: helper, HelperVersion: "test", HelperChecksum: checksum, ProbeArchive: archive, ProbeReference: "probe:test", ProbeDigest: "sha256:" + strings.Repeat("a", 64)}}
	if _, err := InitializeOCIIntent(c.IntentFile, time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(c.LimaHome, c.Instance), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LIMA_HOME", c.LimaHome)
	hung := false
	ops := userInstallOps{run: func(ctx context.Context, _ string, args ...string) ([]byte, error) {
		switch {
		case reflect.DeepEqual(args, []string{"list", "--json"}):
			return []byte(`{"name":"wefty-oci","status":"Running"}`), nil
		case args[len(args)-1] == "-un":
			return []byte("operator\n"), nil
		case args[len(args)-1] == "-u":
			return []byte("501\n"), nil
		case args[len(args)-1] == "--version":
			return []byte("systemd 255 (255.4-1ubuntu8)\n"), nil
		case args[len(args)-1] == "/etc/os-release":
			hung = true
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return nil, nil
	}, ensure: func(context.Context) error { return nil }, install: func(context.Context, GuestHelperInstallConfig) error { return nil },
		guestReadBudget: 50 * time.Millisecond}
	if _, err := prepareUserInstall(t.Context(), c, "node", "boot", ops); err != nil || !hung {
		t.Fatalf("hung guest OS read: err=%v reached=%t", err, hung)
	}
	if t.Context().Err() != nil {
		t.Fatal("parent preparation context expired")
	}
	if _, err := os.Stat(filepath.Join(c.LimaHome, c.Instance, GuestOSObservationFile)); !os.IsNotExist(err) {
		t.Fatalf("hung read left a record: %v", err)
	}
}

// An unreadable guest systemd version stops preparation before the helper
// unit is rendered: no unit may be installed for a policy nobody can record.
func TestPerUserInstallRefusesUnparsedSystemdVersionBeforeInstall(t *testing.T) {
	root := t.TempDir()
	helper := filepath.Join(root, "linux-helper")
	archive := filepath.Join(root, "probe.tar")
	for _, p := range []string{helper, archive} {
		if err := os.WriteFile(p, []byte("fixture"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	checksum, err := checksumFile(helper)
	if err != nil {
		t.Fatal(err)
	}
	c := UserInstallConfig{Instance: "wefty-oci", LimaHome: filepath.Join(root, "lima"), MountRoot: filepath.Join(root, "mount"), IntentFile: filepath.Join(root, "intent.json"), SetupState: filepath.Join(root, "setup.json"), TemplatePath: filepath.Join(root, "host.yaml"), Sizing: Sizing{Memory: "4GiB", CPUs: 4, Disk: "32GiB"}, Assets: UserInstallAssets{LinuxHelper: helper, HelperVersion: "test", HelperChecksum: checksum, ProbeArchive: archive, ProbeReference: "probe:test", ProbeDigest: "sha256:" + strings.Repeat("a", 64)}}
	if _, err := InitializeOCIIntent(c.IntentFile, time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LIMA_HOME", c.LimaHome)
	installs := 0
	ops := userInstallOps{run: func(_ context.Context, _ string, args ...string) ([]byte, error) {
		switch {
		case reflect.DeepEqual(args, []string{"list", "--json"}):
			return []byte(`{"name":"wefty-oci","status":"Running"}`), nil
		case args[len(args)-1] == "-un":
			return []byte("operator\n"), nil
		case args[len(args)-1] == "-u":
			return []byte("501\n"), nil
		case args[len(args)-1] == "--version":
			return []byte("not systemd\n"), nil
		}
		return nil, nil
	}, ensure: func(context.Context) error { return nil }, install: func(context.Context, GuestHelperInstallConfig) error { installs++; return nil }}
	if version, err := prepareUserInstall(t.Context(), c, "node", "boot", ops); err == nil || version != 0 || installs != 0 {
		t.Fatalf("unparsed systemd version: version=%d err=%v installs=%d", version, err, installs)
	}
}
