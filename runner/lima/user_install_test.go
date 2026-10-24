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
		return nil, nil
	}, ensure: func(context.Context) error { commands = append(commands, []string{"supervisor.Ensure"}); return nil }, install: func(_ context.Context, h GuestHelperInstallConfig) error {
		installs++
		if h.GuestUID != 501 || h.GuestUser != "operator" || h.Instance != "wefty-proof" || h.MemoryCapacityBytes != 4<<30 || h.MemoryReserveBytes != 1<<30 {
			t.Fatalf("guest install=%+v", h)
		}
		return nil
	}}
	t.Setenv("LIMA_HOME", filepath.Join(root, "wrong"))
	if err := prepareUserInstall(t.Context(), c, "node", "boot", ops); err == nil || len(commands) != 0 {
		t.Fatalf("wrong Lima home reached commands=%v err=%v", commands, err)
	}
	t.Setenv("LIMA_HOME", c.LimaHome)
	if err := prepareUserInstall(t.Context(), c, "node", "boot", ops); err != nil {
		t.Fatal(err)
	}
	if installs != 1 {
		t.Fatalf("helper installs=%d", installs)
	}
	want := [][]string{{"limactl", "list", "--json"}, {"limactl", "create", "--tty=false", "--name=wefty-proof", c.TemplatePath}, {"supervisor.Ensure"}, {"limactl", "--tty=false", "shell", "--workdir=/", "wefty-proof", "id", "-un"}, {"limactl", "--tty=false", "shell", "--workdir=/", "wefty-proof", "id", "-u"}}
	if !reflect.DeepEqual(commands, want) {
		t.Fatalf("commands=%v", commands)
	}
	if _, err := os.Stat(filepath.Join(root, "user-install-timing.json")); err != nil {
		t.Fatal(err)
	}
}
