//go:build darwin

package lima

import (
	"context"
	"errors"
	"os/exec"
	"reflect"
	"strings"
	"testing"
)

// TestGuestHelperInstallRefusesUnmountedHostMountRootBeforeUnitMutation runs
// the bootstrap decision through the install path: the fake instance inventory
// mounts only /Users/operator/run-4, so a helper configured with the run-5
// root must be refused as host_mount_root_not_mounted before a single limactl
// mutation command, with the refusal naming both roots.
func TestGuestHelperInstallRefusesUnmountedHostMountRootBeforeUnitMutation(t *testing.T) {
	config := validGuestHelperInstallConfig(t)
	config.HostMountRoot = "/Users/operator/run-5-mounts"
	var commands [][]string
	installer := guestHelperInstaller{run: func(_ context.Context, name string, arguments ...string) ([]byte, error) {
		commands = append(commands, append([]string{name}, arguments...))
		return nil, nil
	}}
	installer.inventoryExecute = func(cmd *exec.Cmd) error {
		if !reflect.DeepEqual(cmd.Args, []string{"limactl", "list", "--json", DefaultInstanceName}) {
			t.Fatalf("inventory command = %v", cmd.Args)
		}
		_, err := cmd.Stdout.Write([]byte(`{"name":"` + DefaultInstanceName + `","config":{"mounts":[{"location":"/Users/operator/wefty-mounts","mountPoint":"/mnt/wefty-host"}]}}`))
		return err
	}
	err := installer.install(t.Context(), config)
	var refusal *HostMountRootNotMountedError
	if !errors.As(err, &refusal) {
		t.Fatalf("install = %v, want typed %s refusal", err, HostMountRootNotMountedReason)
	}
	if refusal.Root != config.HostMountRoot || len(refusal.MountedLocations) != 1 || refusal.MountedLocations[0] != "/Users/operator/wefty-mounts" {
		t.Fatalf("refusal = %+v", refusal)
	}
	if !strings.Contains(err.Error(), HostMountRootNotMountedReason) || !strings.Contains(err.Error(), "/Users/operator/run-5-mounts") {
		t.Fatalf("refusal message must name the reason and requested root: %v", err)
	}
	if len(commands) != 0 {
		t.Fatalf("refused install mutated Lima: %v", commands)
	}
}

// TestGuestHelperInstallAcceptsInstanceMountedRoot proves the same check does
// not carry over into refusals when the requested root is the instance's own
// configured mount: the install proceeds into its ordinary guest inspection.
func TestGuestHelperInstallProceedsWhenRootMatchesInstanceMount(t *testing.T) {
	config := validGuestHelperInstallConfig(t)
	installer := guestHelperInstaller{run: func(_ context.Context, name string, arguments ...string) ([]byte, error) {
		return nil, nil
	}}
	installer.inventoryExecute = func(cmd *exec.Cmd) error {
		_, err := cmd.Stdout.Write([]byte(`{"name":"` + DefaultInstanceName + `","config":{"mounts":[{"location":"/Users/operator/wefty-mounts","mountPoint":"/mnt/wefty-host"}]}}`))
		return err
	}
	if err := installer.install(t.Context(), config); err == nil {
		t.Fatal("fixture unexpectedly accepted the fake guest")
	}
}
