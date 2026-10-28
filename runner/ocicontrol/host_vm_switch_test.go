package ocicontrol

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/lima"
)

func TestOnDemandSwitchDoesNotStartHostVM(t *testing.T) {
	path := filepath.Join(t.TempDir(), "intent.json")
	if _, err := lima.InitializeOCIIntent(path, time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	runtime := &controlTestRuntime{intentPath: path}
	controller, err := NewController(ControllerConfig{IntentPath: path, Runtime: runtime})
	if err != nil {
		t.Fatal(err)
	}
	response, err := controller.Start(t.Context(), IntentMutationRequest{ExpectedRevision: 1, Position: lima.OCIPositionOnDemand})
	if err != nil {
		t.Fatal(err)
	}
	if response.Intent.PositionValue() != lima.OCIPositionOnDemand || runtime.recovered != 0 || runtime.onDemandReason != "host_vm_idle" || !response.CapabilityPublished {
		t.Fatalf("on-demand switch started host VM: %+v recoveries=%d", response, runtime.recovered)
	}
}

type reportingHostVMRuntime struct{ controlTestRuntime }

func (*reportingHostVMRuntime) OCIWork() []lima.DemandItem {
	return []lima.DemandItem{{Kind: "oci", ID: "job-running"}}
}

func TestOffSwitchReportsEndedWorkAfterClosingAdmission(t *testing.T) {
	path := filepath.Join(t.TempDir(), "intent.json")
	if _, err := lima.InitializeOCIIntent(path, time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	runtime := &reportingHostVMRuntime{controlTestRuntime: controlTestRuntime{intentPath: path, live: true}}
	controller, err := NewController(ControllerConfig{IntentPath: path, Runtime: runtime})
	if err != nil {
		t.Fatal(err)
	}
	response, err := controller.Stop(t.Context(), IntentMutationRequest{ExpectedRevision: 1})
	if err != nil || !response.RuntimeQuiesced || runtime.stopped != 1 || !runtime.fenced || len(response.EndedWork) != 1 || response.EndedWork[0].ID != "job-running" {
		t.Fatalf("off response=%+v stopped=%d fenced=%t err=%v", response, runtime.stopped, runtime.fenced, err)
	}
	if response.Intent.PositionValue() != lima.OCIPositionOff {
		t.Fatalf("off marker=%+v", response.Intent)
	}
}

type loadDemandRuntime struct {
	controlTestRuntime
	loads int
}

func (runtime *loadDemandRuntime) BeginHostVMLoadImage() func() {
	runtime.loads++
	return func() { runtime.loads-- }
}

func TestLoadImageKeepsHealthyHostVMAndEndsDemand(t *testing.T) {
	for _, live := range []bool{true, false} {
		t.Run(fmt.Sprint(live), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "intent.json")
			if _, err := lima.InitializeOCIIntent(path, time.Unix(1, 0)); err != nil {
				t.Fatal(err)
			}
			runtime := &loadDemandRuntime{controlTestRuntime: controlTestRuntime{intentPath: path, live: live}}
			images := &controlTestImages{}
			controller, err := NewController(ControllerConfig{IntentPath: path, Runtime: runtime, Images: images})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := controller.LoadImage(t.Context(), LoadImageRequest{}, strings.NewReader("image")); err != nil {
				t.Fatal(err)
			}
			wantRecovery := 1
			if live {
				wantRecovery = 0
			}
			if runtime.recovered != wantRecovery || runtime.loads != 0 || images.archive != "image" {
				t.Fatalf("live=%t recoveries=%d remaining demand=%d archive=%q", live, runtime.recovered, runtime.loads, images.archive)
			}
		})
	}
}
