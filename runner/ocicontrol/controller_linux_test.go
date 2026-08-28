//go:build linux

package ocicontrol

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/lima"
)

func TestLinuxStopWithoutLimaCycleStillQuiescesRuntime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "intent.json")
	clock := &controlTestClock{now: time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)}
	if _, err := lima.InitializeOCIIntent(path, clock.Now()); err != nil {
		t.Fatal(err)
	}
	runtime := &controlTestRuntime{intentPath: path}
	controller, err := NewController(ControllerConfig{IntentPath: path, Runtime: runtime, StopCycle: nil, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	response, err := controller.Stop(t.Context(), IntentMutationRequest{ExpectedRevision: 1})
	if err != nil || !response.RuntimeQuiesced || response.Intent.Enabled || runtime.stopped != 1 {
		t.Fatalf("Linux stop response=%+v stopped=%d err=%v", response, runtime.stopped, err)
	}
}
