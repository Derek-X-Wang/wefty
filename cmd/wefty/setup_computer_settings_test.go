package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

func TestSetupComputerFlagsReachInstalledServicesAndSurviveRerun(t *testing.T) {
	f := newSetupFixture(t)
	image := "ghcr.io/example/desktop:current@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	flags := flag.NewFlagSet("setup", flag.ContinueOnError)
	var values setupComputerOptions
	bindSetupComputerFlags(flags, &values)
	if err := flags.Parse([]string{"--computer-seat-memory-bytes=536870912", "--computer-dedicated-cap=2",
		"--computer-shared-seat-limit=3", "--computer-default-image=" + image}); err != nil {
		t.Fatal(err)
	}
	f.setup(t, setupRequest{computer: values})
	beforeL1 := f.plist(t, setupL1Label)
	beforeNode := f.plist(t, setupNodeAgentLabel)
	if strings.Contains(beforeNode, "WEFTY_SETUP_COMPUTER_SETTINGS") || strings.Contains(beforeL1, "WEFTY_SETUP_COMPUTER_SETTINGS") {
		t.Fatal("Computer settings leaked into service environments")
	}
	receipt := readSetupReceipt(t, f.home)
	data, _ := json.Marshal(receipt)
	if !strings.Contains(string(data), "seat_memory_bytes") {
		t.Fatal("Computer settings not stored in setup receipt")
	}
	for _, flag := range []string{"--computer-seat-memory-bytes=536870912", "--computer-dedicated-cap=2"} {
		if !strings.Contains(beforeNode, flag) {
			t.Errorf("node service lacks %s", flag)
		}
	}
	for _, flag := range []string{"--computer-default-image=" + image, "--computer-shared-seat-limit=3"} {
		if !strings.Contains(beforeL1, flag) {
			t.Errorf("L1 service lacks %s", flag)
		}
	}
	image = "ghcr.io/example/desktop:next@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	f.setup(t, setupRequest{computer: setupComputerOptions{Image: &image}})
	if got := f.plist(t, setupNodeAgentLabel); got != beforeNode {
		t.Fatal("changing only the default image restarted the node service")
	}
	if got := f.plist(t, setupL1Label); !strings.Contains(got, "--computer-default-image="+image) ||
		!strings.Contains(got, "--computer-shared-seat-limit=3") {
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
	image = "ghcr.io/example/desktop:linux-next@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
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
	profile.NodeAdmission = append([]connprofile.NodeAdmission{}, connprofile.NodeAdmission{NodeID: "other-node",
		Tags: []string{"other-tag"}, MaxOneshotSlots: 9, MaxServiceSlots: 11})
	statePath := filepath.Join(f.home, "configured.json")
	if err := ocicontrol.WriteSetupState(statePath, ocicontrol.SetupState{VMMemory: "8GiB", VMCPUs: 4, VMDisk: "32GiB",
		MemoryCapacityBytes: 8 << 30, MemoryReserveBytes: 1 << 30, VMType: "vz", HostMountRoot: f.home, ProbeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}); err != nil {
		t.Fatal(err)
	}
	services, err := applySetupComputerSettings(paths, profile, []launchagent.Agent{
		{Label: setupL1Label, Environment: map[string]string{}},
		{Label: setupNodeAgentLabel, Arguments: []string{"--oci-setup-state=" + statePath}, Environment: map[string]string{}},
	}, serviceplan.Legacy)
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

func TestSetupComputerSettingsValidateBeforeEnrolment(t *testing.T) {
	for _, tc := range []struct {
		role  string
		limit int
		want  string
	}{
		{"control-plane", 1, "shared Computer Seat limit must be at least 2"},
		{"join", 3, "cluster Computer settings require the control-plane installation"},
	} {
		t.Run(tc.role, func(t *testing.T) {
			f, e, _ := newFleetFixture(t)
			request := setupRequest{role: tc.role, nodeID: "studio-0123abcd", computer: setupComputerOptions{SharedSeatLimit: &tc.limit}}
			if tc.role == "join" {
				request.joinCode = joinCodeForTest(t, "opaque-l1", "opaque-l3", "")
			}
			_, err := runSetup(t.Context(), f.host, request)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("wrong preflight refusal: %v; want %q", err, tc.want)
			}
			if len(e.roles) != 0 {
				t.Fatalf("settings refused after enrolment: %v", e.roles)
			}
		})
	}
	t.Run("unreadable setup state", func(t *testing.T) {
		f, e, _ := newFleetFixture(t)
		paths, err := resolveSetupPaths(f.home)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(f.home, "setup.json")
		if err := ocicontrol.WriteSetupState(path, ocicontrol.SetupState{VMMemory: "4GiB", VMCPUs: 4, VMDisk: "32GiB", VMType: "vz", HostMountRoot: f.home, ProbeDigest: "sha256:" + strings.Repeat("a", 64)}); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0000); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(path, 0600) })
		if _, err := os.ReadFile(path); !errors.Is(err, os.ErrPermission) {
			if os.Geteuid() == 0 {
				t.Skip("root bypasses operator file permissions")
			}
			t.Fatalf("setup state not unreadable: %v", err)
		}
		plan, err := setupRolePlan(paths, serviceplan.ControlPlane)
		if err != nil {
			t.Fatal(err)
		}
		for i := range plan.Services {
			if plan.Services[i].Name == "node-agent" {
				plan.Services[i].Arguments = append(plan.Services[i].Arguments, "--oci-setup-state="+path)
			}
		}
		_, err = validateSetupComputerPlan(paths, plan, setupComputerOptions{})
		if !errors.Is(err, os.ErrPermission) || !strings.Contains(err.Error(), "read configured Computer room") {
			t.Fatalf("wrong state preflight refusal: %v", err)
		}
		if len(e.roles) != 0 {
			t.Fatalf("state read followed enrolment: %v", e.roles)
		}
	})
}

func TestSetupComputerSettingsRejectTinySeatFlag(t *testing.T) {
	flags := flag.NewFlagSet("setup", flag.ContinueOnError)
	var options setupComputerOptions
	bindSetupComputerFlags(flags, &options)
	if err := flags.Parse([]string{"--computer-seat-memory-bytes=268435455"}); err == nil {
		t.Fatal("tiny Seat memory accepted")
	}
}
func TestSetupDoesNotOwnOCISetupStateFlag(t *testing.T) {
	flags := flag.NewFlagSet("setup", flag.ContinueOnError)
	var options setupComputerOptions
	bindSetupComputerFlags(flags, &options)
	if flags.Lookup("oci-setup-state") != nil {
		t.Fatal("setup records a path owned by host VM setup")
	}
}
func TestSetupRoomPreservesExplicitSlotsAndHandlesNilPolicies(t *testing.T) {
	for _, explicit := range []bool{true, false} {
		t.Run(map[bool]string{true: "explicit", false: "nil"}[explicit], func(t *testing.T) {
			f := newSetupFixture(t)
			f.setup(t, setupRequest{})
			paths, _ := resolveSetupPaths(f.home)
			profile := f.profile(t)
			profile.NodeAdmission = nil
			if explicit {
				profile.NodeAdmission = []connprofile.NodeAdmission{{NodeID: profile.Identities.Node, MaxOneshotSlots: 4, MaxServiceSlots: 1}}
			}
			path := filepath.Join(f.home, "room.json")
			if err := ocicontrol.WriteSetupState(path, ocicontrol.SetupState{VMMemory: "8GiB", VMCPUs: 4, VMDisk: "32GiB", VMType: "vz", HostMountRoot: f.home, ProbeDigest: "sha256:" + strings.Repeat("a", 64), MemoryCapacityBytes: 8 << 30, MemoryReserveBytes: 1 << 30}); err != nil {
				t.Fatal(err)
			}
			services, err := applySetupComputerSettings(paths, profile, []launchagent.Agent{{Label: setupL1Label}, {Label: setupNodeAgentLabel, Arguments: []string{"--oci-setup-state=" + path}}}, serviceplan.Legacy)
			if err != nil {
				t.Fatal(err)
			}
			count := "7"
			if explicit {
				count = "1"
			}
			if got := strings.Join(services[0].Arguments, " "); !strings.Contains(got, "--node-max-service-slots="+profile.Identities.Node+"="+count) {
				t.Fatalf("wrong Slots: %s", got)
			}
		})
	}
}

func TestSetupRoomFloorAppliesToFreshSetupProfile(t *testing.T) {
	f := newSetupFixture(t)
	f.setup(t, setupRequest{})
	paths, err := resolveSetupPaths(f.home)
	if err != nil {
		t.Fatal(err)
	}
	profile := f.profile(t)
	if len(profile.NodeAdmission) != 1 || profile.NodeAdmission[0].MaxServiceSlots != 2 {
		t.Fatalf("fresh setup policy: %+v", profile.NodeAdmission)
	}
	path := filepath.Join(f.home, "room.json")
	if err := ocicontrol.WriteSetupState(path, ocicontrol.SetupState{VMMemory: "4GiB", VMCPUs: 4, VMDisk: "32GiB", VMType: "vz", HostMountRoot: f.home, ProbeDigest: "sha256:" + strings.Repeat("a", 64), MemoryCapacityBytes: 4 << 30, MemoryReserveBytes: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	prior := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(prior)
	for range 2 {
		services, err := applySetupComputerSettings(paths, profile, []launchagent.Agent{{Label: setupL1Label}, {Label: setupNodeAgentLabel, Arguments: []string{"--oci-setup-state=" + path}}}, serviceplan.Legacy)
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(services[0].Arguments, " "); !strings.Contains(got, "--node-max-service-slots="+profile.Identities.Node+"=3") {
			t.Fatalf("fresh setup room floor: %s", got)
		}
	}
	if strings.Contains(output.String(), "owner_override") {
		t.Fatalf("default reruns warn as owner override: %s", output.String())
	}
}

func breakComputerSettingsReceipt(t *testing.T, home, kind string) string {
	t.Helper()
	paths, err := resolveSetupPaths(home)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(paths.config, setupManifestName)
	switch kind {
	case "malformed":
		if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
			t.Fatal(err)
		}
	case "version":
		if err := os.WriteFile(path, []byte(`{"version":99,"computer":{"seat_memory_bytes":536870912,"dedicated_cap":99}}`), 0600); err != nil {
			t.Fatal(err)
		}
	case "unreadable":
		if err := os.Chmod(path, 0000); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(path, 0600) })
		if _, err := os.ReadFile(path); !errors.Is(err, os.ErrPermission) {
			if os.Geteuid() == 0 {
				t.Skip("root bypasses operator file permissions")
			}
			t.Fatalf("not unreadable: %v", err)
		}
	}
	return path
}
func TestSetupComputerSettingsReceiptRepair(t *testing.T) {
	for _, kind := range []string{"malformed", "version", "unreadable"} {
		t.Run(kind, func(t *testing.T) {
			f := newSetupFixture(t)
			f.setup(t, setupRequest{})
			breakComputerSettingsReceipt(t, f.home, kind)
			cap := 2
			report, err := runSetup(t.Context(), f.host, setupRequest{computer: setupComputerOptions{DedicatedCap: &cap}})
			if err != nil {
				t.Fatalf("plain setup did not repair receipt: %v", err)
			}
			if !strings.Contains(strings.Join(report.Notes, " "), "recorded Computer settings were not retained") {
				t.Fatalf("repair lacks note: %v", report.Notes)
			}
			receipt := readSetupReceipt(t, f.home)
			if receipt.Version != 1 || receipt.Computer == nil || receipt.Computer.SeatMemoryBytes != nil || receipt.Computer.DedicatedCap == nil || *receipt.Computer.DedicatedCap != 2 {
				t.Fatalf("repaired receipt: %+v", receipt)
			}
		})
	}
}
func TestUpgradeComputerSettingsReceiptRepair(t *testing.T) {
	for _, kind := range []string{"malformed", "version", "unreadable"} {
		t.Run(kind, func(t *testing.T) {
			f := newUpgradeFixture(t)
			breakComputerSettingsReceipt(t, f.home, kind)
			var output bytes.Buffer
			report, err := runUpgrade(t.Context(), f.host, true, &output)
			if err != nil {
				t.Fatalf("plain upgrade did not repair receipt: %v", err)
			}
			if !strings.Contains(output.String(), "recorded Computer settings were not retained") {
				t.Fatalf("repair lacks warning: %s", output.String())
			}
			receipt := readSetupReceipt(t, f.home)
			if receipt.Version != 1 || receipt.Computer != nil || receipt.InstalledVersion != report.Version {
				t.Fatalf("repaired receipt: %+v", receipt)
			}
		})
	}
}
func TestSetupComputerReceiptReadStaysInsideHome(t *testing.T) {
	f := newSetupFixture(t)
	f.setup(t, setupRequest{})
	paths, err := resolveSetupPaths(f.home)
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside.json")
	if err := os.WriteFile(outside, []byte(`{"version":1,"computer":{"dedicated_cap":99}}`), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(paths.config, setupManifestName)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, path); err != nil {
		t.Fatal(err)
	}
	if values, err := resolveSetupComputerSettings(paths); err == nil {
		t.Fatalf("receipt reader escaped home: %+v", values)
	}
}

func TestSetupTimerSettingsPersistAndInstall(t *testing.T) {
	f := newSetupFixture(t)
	flags := flag.NewFlagSet("setup", flag.ContinueOnError)
	var settings setupComputerOptions
	bindSetupComputerFlags(flags, &settings)
	if err := flags.Parse([]string{"--seat-start-wait=90s", "--seat-idle-wait=40m", "--seat-warning-wait=6m", "--computer-idle-wait=12m"}); err != nil {
		t.Fatal(err)
	}
	f.setup(t, setupRequest{computer: settings})
	node := f.plist(t, setupNodeAgentLabel)
	assertTimers := func(text string) {
		t.Helper()
		for _, want := range []string{"--seat-start-wait=1m30s", "--seat-idle-wait=40m0s", "--seat-warning-wait=6m0s", "--computer-idle-wait=12m0s"} {
			if !strings.Contains(text, want) {
				t.Errorf("installed L1 lacks %s", want)
			}
		}
	}
	assertTimers(f.plist(t, setupL1Label))
	flags = flag.NewFlagSet("setup", flag.ContinueOnError)
	settings = setupComputerOptions{}
	bindSetupComputerFlags(flags, &settings)
	if err := flags.Parse([]string{"--computer-idle-wait=15m"}); err != nil {
		t.Fatal(err)
	}
	f.setup(t, setupRequest{computer: settings})
	changed := f.plist(t, setupL1Label)
	for _, want := range []string{"--seat-start-wait=1m30s", "--seat-idle-wait=40m0s", "--seat-warning-wait=6m0s", "--computer-idle-wait=15m0s"} {
		if !strings.Contains(changed, want) {
			t.Errorf("changed settings lost %s", want)
		}
	}
	f.setup(t, setupRequest{})
	if f.plist(t, setupL1Label) != changed || f.plist(t, setupNodeAgentLabel) != node {
		t.Fatal("timer settings were not retained, or a cluster setting changed the node service")
	}
	receipt, _ := json.Marshal(readSetupReceipt(t, f.home))
	for _, key := range []string{"seat_start_wait", "seat_idle_wait", "seat_warning_wait", "computer_idle_wait"} {
		if !strings.Contains(string(receipt), key) {
			t.Errorf("receipt lacks %s", key)
		}
	}
	// systemd uses the same persisted cluster values, without node settings.
	lf, _, _, _ := newLinuxFleetFixture(t, "control-plane")
	request := linuxSetupRequest(t, "control-plane")
	paths, err := resolveSetupPaths(f.home)
	if err != nil {
		t.Fatal(err)
	}
	request.computer, err = readSetupComputerSettings(paths)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runSetup(t.Context(), lf.host, request); err != nil {
		t.Fatal(err)
	}
	unit, err := os.ReadFile(filepath.Join(lf.home, ".config", "systemd", "user", setupL1Label+".service"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"--seat-start-wait=1m30s", "--seat-idle-wait=40m0s", "--seat-warning-wait=6m0s", "--computer-idle-wait=15m0s"} {
		if !strings.Contains(string(unit), want) {
			t.Errorf("systemd unit lacks %s", want)
		}
	}
}

func TestSetupTimerSettingsValidateBeforeEnrolment(t *testing.T) {
	for _, tc := range []struct {
		role string
		args []string
		want string
	}{
		{"control-plane", []string{"--seat-idle-wait=4m"}, "Seat warning wait must be less"},
		{"control-plane", []string{"--seat-warning-wait=30m"}, "Seat warning wait must be less"},
		{"join", []string{"--computer-idle-wait=12m"}, "cluster Computer settings require"},
	} {
		t.Run(tc.role+strings.Join(tc.args, ""), func(t *testing.T) {
			flags := flag.NewFlagSet("setup", flag.ContinueOnError)
			var settings setupComputerOptions
			bindSetupComputerFlags(flags, &settings)
			if err := flags.Parse(tc.args); err != nil {
				t.Fatal(err)
			}
			f, enrol, _ := newFleetFixture(t)
			request := setupRequest{role: tc.role, nodeID: "studio-0123abcd", computer: settings}
			if tc.role == "join" {
				request.joinCode = joinCodeForTest(t, "opaque-l1", "opaque-l3", "")
			}
			_, err := runSetup(t.Context(), f.host, request)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("preflight error=%v want=%s", err, tc.want)
			}
			if len(enrol.roles) != 0 {
				t.Fatal("timer setting refused after enrolment")
			}
		})
	}
	for _, arg := range []string{"--seat-idle-wait=0", "--seat-warning-wait=-1s", "--computer-idle-wait=none"} {
		flags := flag.NewFlagSet("setup", flag.ContinueOnError)
		var settings setupComputerOptions
		bindSetupComputerFlags(flags, &settings)
		if err := flags.Parse([]string{arg}); err == nil {
			t.Errorf("accepted invalid timer %s", arg)
		}
	}
}
