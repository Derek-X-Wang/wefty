package main

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

func TestSetupComputerFlagsReachInstalledServicesAndSurviveRerun(t *testing.T) {
	f := newSetupFixture(t)
	statePath := filepath.Join(f.home, "configured-host-vm.json")
	if err := ocicontrol.WriteSetupState(statePath, ocicontrol.SetupState{
		VMMemory: "8GiB", VMCPUs: 4, VMDisk: "32GiB", VMType: "vz", HostMountRoot: f.home,
		ProbeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}); err != nil {
		t.Fatal(err)
	}
	image := "ghcr.io/example/desktop:current"
	flags := flag.NewFlagSet("setup", flag.ContinueOnError)
	var values setupComputerOptions
	bindSetupComputerFlags(flags, &values)
	if err := flags.Parse([]string{"--computer-seat-memory-bytes=536870912", "--computer-dedicated-cap=2",
		"--computer-shared-seat-limit=3", "--computer-default-image=" + image, "--oci-setup-state=" + statePath}); err != nil {
		t.Fatal(err)
	}
	f.setup(t, setupRequest{computer: values})
	beforeL1 := f.plist(t, setupL1Label)
	beforeNode := f.plist(t, setupNodeAgentLabel)
	for _, flag := range []string{"--computer-seat-memory-bytes=536870912", "--computer-dedicated-cap=2", "--oci-setup-state=" + statePath} {
		if !strings.Contains(beforeNode, flag) {
			t.Errorf("node service lacks %s", flag)
		}
	}
	for _, flag := range []string{"--computer-default-image=" + image, "--computer-shared-seat-limit=3",
		"--node-max-service-slots=" + f.profile(t).Identities.Node + "=14"} {
		if !strings.Contains(beforeL1, flag) {
			t.Errorf("L1 service lacks %s", flag)
		}
	}
	image = "ghcr.io/example/desktop:next"
	f.setup(t, setupRequest{computer: setupComputerOptions{Image: &image}})
	if got := f.plist(t, setupNodeAgentLabel); got != beforeNode {
		t.Fatal("changing only the default image restarted the node service")
	}
	if got := f.plist(t, setupL1Label); !strings.Contains(got, "--computer-default-image="+image) ||
		!strings.Contains(got, "--computer-shared-seat-limit=3") || !strings.Contains(got, "=14") {
		t.Fatal("changing one cluster setting lost another setting or the service-Slot floor")
	}
	currentL1 := f.plist(t, setupL1Label)
	f.setup(t, setupRequest{})
	if f.plist(t, setupL1Label) != currentL1 || f.plist(t, setupNodeAgentLabel) != beforeNode {
		t.Fatal("a setup rerun erased Computer settings")
	}
	// Exercise the other installed-service format through the real Linux
	// setup path, with fake systemd and enrollment at the process boundary.
	lf, _, _, _ := newLinuxFleetFixture(t, "control-plane")
	lr := linuxSetupRequest(t, "control-plane")
	lr.computer = setupComputerOptions{Image: &image, SeatMemoryBytes: values.SeatMemoryBytes,
		DedicatedCap: values.DedicatedCap, SharedSeatLimit: values.SharedSeatLimit}
	if _, err := runSetup(t.Context(), lf.host, lr); err != nil {
		t.Fatal(err)
	}
	unit := func(label string) string {
		payload, err := os.ReadFile(filepath.Join(lf.home, ".config", "systemd", "user", label+".service"))
		if err != nil {
			t.Fatal(err)
		}
		return string(payload)
	}
	nodeUnit := unit(setupNodeAgentLabel)
	image = "ghcr.io/example/desktop:linux-next"
	lr.computer = setupComputerOptions{Image: &image}
	if _, err := runSetup(t.Context(), lf.host, lr); err != nil {
		t.Fatal(err)
	}
	if unit(setupNodeAgentLabel) != nodeUnit {
		t.Fatal("Linux cluster-only change restarted the node service")
	}
	if got := unit(setupL1Label); !strings.Contains(got, "--computer-default-image="+image) || !strings.Contains(got, "--computer-shared-seat-limit=3") {
		t.Fatal("Linux setup lost a retained cluster setting")
	}
}

func TestSetupRoomFloorUsesConfiguredPlanAndPreservesOtherNodePolicy(t *testing.T) {
	f := newSetupFixture(t)
	f.setup(t, setupRequest{})
	paths, err := resolveSetupPaths(f.home)
	if err != nil {
		t.Fatal(err)
	}
	profile := f.profile(t)
	profile.NodeAdmission = append(profile.NodeAdmission, connprofile.NodeAdmission{NodeID: "other-node",
		Tags: []string{"other-tag"}, MaxOneshotSlots: 9, MaxServiceSlots: 11})
	statePath := filepath.Join(f.home, "configured.json")
	if err := ocicontrol.WriteSetupState(statePath, ocicontrol.SetupState{VMMemory: "8GiB", VMCPUs: 4, VMDisk: "32GiB",
		VMType: "vz", HostMountRoot: f.home, ProbeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}); err != nil {
		t.Fatal(err)
	}
	services, err := applySetupComputerSettings(paths, profile, []launchagent.Agent{
		{Label: setupL1Label, Environment: map[string]string{}},
		{Label: setupNodeAgentLabel, Arguments: []string{"--oci-setup-state=" + statePath}, Environment: map[string]string{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(services[0].Arguments, " ")
	for _, want := range []string{"--node-max-service-slots=" + profile.Identities.Node + "=7",
		"--node-tags=other-node=other-tag", "--node-max-oneshot-slots=other-node=9", "--node-max-service-slots=other-node=11"} {
		if !strings.Contains(args, want) {
			t.Errorf("installed L1 arguments lack %s", want)
		}
	}
}
