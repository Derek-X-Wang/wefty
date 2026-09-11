package systemdpolicy

import (
	"maps"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestVersionedHelperRestartPolicy(t *testing.T) {
	for _, test := range []struct {
		name       string
		version    int
		directives map[string]string
		policyName string
	}{
		{name: "unknown", version: 0, directives: map[string]string{"RestartSec": "1s", "RestartPreventExitStatus": "78"}, policyName: "conservative_fixed_1s"},
		{name: "systemd 252", version: 252, directives: map[string]string{"RestartSec": "1s", "RestartPreventExitStatus": "78"}, policyName: "legacy_fixed_1s"},
		{name: "systemd 254", version: 254, directives: map[string]string{"RestartSec": "250ms", "RestartSteps": "6", "RestartMaxDelaySec": "1s", "RestartPreventExitStatus": "78"}, policyName: "geometric_capped_1s"},
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

// A rate bound is not a count bound. StartLimitIntervalSec stays 0 so the
// triggering socket is never failed with service-start-limit-hit; the count
// bound lives in the helper and lands through RestartPreventExitStatus.
func TestDeterministicStartupFailureIsBoundedWithoutFailingTheSocket(t *testing.T) {
	for _, version := range []int{0, 252, 254, 255} {
		policy := UnitPolicy(version)
		if policy["Unit.StartLimitIntervalSec"] != "0" {
			t.Fatalf("systemd %d bounded the start limiter and would fail the triggering socket: %#v", version, policy)
		}
		if policy["Service.Restart"] != "on-failure" {
			t.Fatalf("systemd %d lost socket-activated restart: %#v", version, policy)
		}
		if policy["Service.RestartPreventExitStatus"] != strconv.Itoa(StartupWedgedExitStatus) {
			t.Fatalf("systemd %d does not stop restarting on the wedged exit status: %#v", version, policy)
		}
		if !strings.Contains(Render(version), "RestartPreventExitStatus="+strconv.Itoa(StartupWedgedExitStatus)+"\n") {
			t.Fatalf("systemd %d rendered policy omits the wedged exit status: %q", version, Render(version))
		}
	}
	if StartupWedgedExitStatus <= 0 || StartupWedgedExitStatus >= 200 {
		t.Fatalf("wedged exit status %d collides with systemd's reserved range", StartupWedgedExitStatus)
	}
	if StartupFailureBound < 2 {
		t.Fatalf("startup failure bound %d gives a transient boot fault no room at all", StartupFailureBound)
	}
	// The rendered delays burn StartupFailureBound restarts in about 1.25
	// seconds, so a pure count would wedge on a containerd restart that
	// straddles helper activation. The window is what makes the bound safe.
	burst := InitialDelay
	saturated := time.Duration(0)
	for step := 0; step < StartupFailureBound; step++ {
		saturated += min(burst, MaximumDelay)
		burst *= 2
	}
	if StartupFailureWindow <= saturated {
		t.Fatalf("startup failure window %s does not outlast the %s it takes to burn the bound", StartupFailureWindow, saturated)
	}
}
