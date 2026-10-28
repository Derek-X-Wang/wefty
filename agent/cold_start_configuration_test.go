package agent

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestColdStartIdleRestoresConfiguredCapabilitiesWithoutProbe(t *testing.T) {
	for _, room := range []bool{false, true} {
		t.Run(map[bool]string{false: "no-room", true: "room"}[room], func(t *testing.T) {
			configured := map[string]bool{"kind:process": true, "kind:oci": true, "runtime_handler:custom": true, "runtime_platform:linux/arm64": true, l1.CapabilitySeatRequests: room}
			calls := 0
			state := newCapabilityState(configured, capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
				calls++
				return CapabilityProbeResult{}, errors.New("unexpected probe")
			}), newManualClock(time.Unix(1000, 0)), 0, nil)
			state.suppressOCI(contract.CapabilityReasonHostVMIdle, errors.New("stopped on demand"))
			want := []string{"cgroup_v2", "kind:oci", "runtime_handler:custom", "runtime_platform:linux/arm64"}
			if room {
				want = []string{"cgroup_v2", "computer", "kind:oci", "runtime_handler:custom", "runtime_platform:linux/arm64"}
			}
			for pass := 0; pass < 2; pass++ {
				got := state.snapshot()
				if !reflect.DeepEqual(got.MissingCapabilities, want) || got.Capabilities["kind:oci"] || calls != 0 {
					t.Fatalf("room=%v idle=%+v calls=%d want=%v", room, got, calls, want)
				}
				state.suppressOCI(contract.CapabilityReasonHostVMIdle, errors.New("still idle"))
			}
		})
	}
}

func TestColdStartIdleRetainsLastPositiveProbeAfterFailure(t *testing.T) {
	clock := newManualClock(time.Unix(1000, 0))
	fails := false
	state := newCapabilityState(map[string]bool{"kind:oci": true}, capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
		if fails {
			return CapabilityProbeResult{}, errors.New("helper unavailable")
		}
		return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true, "runtime_handler:earned": true, "computer": true}}, nil
	}), clock, 0, nil)
	if err := state.refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	fails = true
	if err := state.refresh(t.Context()); err == nil {
		t.Fatal("failed probe succeeded")
	}
	state.suppressOCI(contract.CapabilityReasonHostVMIdle, errors.New("idle"))
	got := state.snapshot()
	if !reflect.DeepEqual(got.MissingCapabilities, []string{"cgroup_v2", "kind:oci", "runtime_handler:earned"}) {
		t.Fatalf("lost configured/positive facts: %+v", got)
	}
}
