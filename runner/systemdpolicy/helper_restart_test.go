package systemdpolicy

import (
	"maps"
	"testing"
)

func TestVersionedHelperRestartPolicy(t *testing.T) {
	for _, test := range []struct {
		name       string
		version    int
		directives map[string]string
		policyName string
	}{
		{name: "unknown", version: 0, directives: map[string]string{"RestartSec": "1s"}, policyName: "conservative_fixed_1s"},
		{name: "systemd 252", version: 252, directives: map[string]string{"RestartSec": "1s"}, policyName: "legacy_fixed_1s"},
		{name: "systemd 254", version: 254, directives: map[string]string{"RestartSec": "250ms", "RestartSteps": "6", "RestartMaxDelaySec": "1s"}, policyName: "geometric_capped_1s"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := Directives(test.version); !maps.Equal(got, test.directives) {
				t.Fatalf("directives = %#v, want %#v", got, test.directives)
			}
			if got := Name(test.version); got != test.policyName {
				t.Fatalf("name = %q, want %q", got, test.policyName)
			}
			wantUnit := map[string]string{"Unit.StartLimitIntervalSec": "0", "Service.Restart": "on-failure"}
			for key, value := range test.directives {
				wantUnit["Service."+key] = value
			}
			if got := UnitPolicy(test.version); !maps.Equal(got, wantUnit) {
				t.Fatalf("unit policy = %#v, want %#v", got, wantUnit)
			}
		})
	}
}

func TestHelperRestartPolicyResultsAreIndependent(t *testing.T) {
	directives := Directives(254)
	directives["RestartSec"] = "30s"
	policy := UnitPolicy(254)
	policy["Service.RestartSec"] = "30s"
	if Directives(254)["RestartSec"] != "250ms" || UnitPolicy(254)["Service.RestartSec"] != "250ms" {
		t.Fatal("caller mutation changed the shared restart-policy authority")
	}
}
