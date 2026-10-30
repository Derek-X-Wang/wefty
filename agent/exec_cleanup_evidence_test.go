//go:build darwin || linux

package agent

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestCapturedExecSnapshotStorageCarriesCleanupEvidence(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	path := filepath.Join(harness.root, "run_cleanup")
	document := []byte(`{"exit_code":0,"stopped_processes":[{"pid":4321,"name":"sleep","cwd":"/tmp/capture/work"}]}`)
	lifecycle := uploadingLifecycle(t, harness.manager, &resultUploadRecorder{}, writingRun(path, "result.json", document))
	claim := retentionClaim(t, "run_cleanup", path)
	claim.SubmittedByRunLedger = true
	claim.Job.Spec.Labels[contract.LabelRunParams] = `{"snapshot_storage":"/tmp/capture"}`
	result, err := lifecycle.runWorkload(t.Context(), claim)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.StoppedProcesses) != 1 || result.StoppedProcesses[0].PID != 4321 {
		t.Fatalf("Attempt result omitted cleanup evidence for snapshot_storage: %+v", result)
	}
}
