package lima

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestColdStartRestartClearsStoppingMarkAfterProbeFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "intent.json")
	if _, err := InitializeOCIIntent(path, time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	config := SupervisorConfig{Instance: DefaultInstanceName, Intent: FileIntentSource{Path: path}, run: func(context.Context, string, ...string) ([]byte, error) { return nil, errors.New("list failed") }}
	first, err := NewSupervisor(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := (&SupervisedBootBarrier{Supervisor: first}).MarkStopping(t.Context()); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewSupervisor(config)
	if err != nil {
		t.Fatal(err)
	}
	if !restarted.recoveryNeeded(t.Context(), false) {
		t.Fatal("failed list must enter normal recovery")
	}
	if facts := restarted.Facts(); facts.Stopping || facts.ReasonCode != "probe_failed" {
		t.Fatalf("facts=%+v", facts)
	}
	if _, err := os.Stat(path + ".stopping"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale mark: %v", err)
	}
}

func TestColdStartStoppingDoesNotHideOffSwitch(t *testing.T) {
	intent := newMutableIntent(false)
	supervisor := newTestSupervisor(t, intent, &supervisorRunner{})
	supervisor.facts.Stopping = true
	if err := (&SupervisedBootBarrier{Supervisor: supervisor}).OCIRecoverySkip(t.Context()); err != nil {
		t.Fatalf("off switch hidden by stopping mark: %v", err)
	}
}
