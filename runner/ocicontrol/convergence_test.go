package ocicontrol

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConvergenceClassesAndAuthorization(t *testing.T) {
	base := SetupState{
		VMMemory: "4GiB", VMCPUs: 4, VMDisk: "32GiB", VMType: "vz",
		HostMountRoot: "/Users/operator/wefty", ProbeDigest: "sha256:old",
	}
	tests := []struct {
		name     string
		mutate   func(*SetupState)
		class    ConvergenceClass
		restart  bool
		recreate bool
		live     int
		wantErr  bool
	}{
		{name: "unchanged", mutate: func(*SetupState) {}, class: ConvergenceUnchanged},
		{name: "probe live-safe", mutate: func(state *SetupState) { state.ProbeDigest = "sha256:new" }, class: ConvergenceLiveSafe},
		{name: "memory restart blocked", mutate: func(state *SetupState) { state.VMMemory = "6GiB" }, class: ConvergenceRestartRequired, wantErr: true},
		{name: "memory restart authorized", mutate: func(state *SetupState) { state.VMMemory = "6GiB" }, class: ConvergenceRestartRequired, restart: true},
		{name: "mount recreate blocked", mutate: func(state *SetupState) { state.HostMountRoot = "/Users/operator/other" }, class: ConvergenceRecreateRequired, wantErr: true},
		{name: "mount recreate live attempt", mutate: func(state *SetupState) { state.HostMountRoot = "/Users/operator/other" }, class: ConvergenceRecreateRequired, recreate: true, live: 1, wantErr: true},
		{name: "mount recreate authorized", mutate: func(state *SetupState) { state.HostMountRoot = "/Users/operator/other" }, class: ConvergenceRecreateRequired, recreate: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			desired := base
			test.mutate(&desired)
			class := ClassifyConvergence(base, desired)
			if class != test.class {
				t.Fatalf("class=%s want=%s", class, test.class)
			}
			err := AuthorizeConvergence(class, test.restart, test.recreate, test.live)
			if (err != nil) != test.wantErr {
				t.Fatalf("authorization err=%v wantErr=%t", err, test.wantErr)
			}
		})
	}
}

func TestSetupStateFailsClosedOnIncompleteOrUnsafeState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "setup.json")
	for _, payload := range []string{
		`{"vm_memory":"4GiB","vm_cpus":4,"vm_disk":"32GiB","vm_type":"vz","host_mount_root":"relative","probe_digest":"sha256:a"}`,
		`{"vm_memory":"bad","vm_cpus":4,"vm_disk":"32GiB","vm_type":"vz","host_mount_root":"/srv/wefty","probe_digest":"sha256:a"}`,
		`{"vm_memory":"4GiB","vm_cpus":4,"vm_disk":"32GiB","vm_type":"","host_mount_root":"/srv/wefty","probe_digest":"sha256:a"}`,
		`{"vm_memory":"4GiB","vm_cpus":4,"vm_disk":"32GiB","vm_type":"vz","host_mount_root":"/srv/wefty","probe_digest":""}`,
		`{"vm_memory":"4GiB","vm_cpus":4,"vm_disk":"32GiB","vm_type":"vz","host_mount_root":"/srv/wefty","probe_digest":"sha256:a","systemd_version":252,"helper_restart_policy":"geometric_capped_1s"}`,
		`{"vm_memory":"4GiB","vm_cpus":4,"vm_disk":"32GiB","vm_type":"vz","host_mount_root":"/srv/wefty","probe_digest":"sha256:a","systemd_version":255,"helper_restart_policy":"legacy_fixed_1s"}`,
	} {
		if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadSetupState(path); err == nil {
			t.Fatalf("unsafe setup state was accepted: %s", payload)
		}
	}
}

func TestSetupStatePersistsSystemdRestartPolicyFacts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "setup.json")
	state := SetupState{VMMemory: "4GiB", VMCPUs: 4, VMDisk: "32GiB", VMType: "native", HostMountRoot: "/srv/wefty", ProbeDigest: "sha256:probe",
		SystemdVersion: 252, HelperRestartPolicy: "legacy_fixed_1s"}
	if err := WriteSetupState(path, state); err != nil {
		t.Fatal(err)
	}
	got, err := ReadSetupState(path)
	if err != nil || got != state {
		t.Fatalf("setup state=%+v err=%v", got, err)
	}
	desired := state
	desired.SystemdVersion = 255
	desired.HelperRestartPolicy = "geometric_capped_1s"
	if class := ClassifyConvergence(state, desired); class != ConvergenceRestartRequired {
		t.Fatalf("policy drift class=%s", class)
	}
}

func TestSetupStateAcceptsConservativeUnknownSystemdPolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "setup.json")
	state := SetupState{VMMemory: "4GiB", VMCPUs: 4, VMDisk: "32GiB", VMType: "native", HostMountRoot: "/srv/wefty", ProbeDigest: "sha256:probe",
		HelperRestartPolicy: "conservative_fixed_1s"}
	if err := WriteSetupState(path, state); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadSetupState(path); err != nil || got != state {
		t.Fatalf("setup state=%+v err=%v", got, err)
	}
}
