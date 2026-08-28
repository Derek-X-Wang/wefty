package ocicontrol

import "testing"

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
