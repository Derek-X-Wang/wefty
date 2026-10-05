//go:build service_acceptance_realtiming && linux

package serviceacceptance

import (
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Direct app path: real L1 and node binaries, the real OCI helper/containerd,
// job-owned handoff and no L3. Startup arbitration is controlled in the fake
// engine races; this row proves delivery and reap of a real container payload.
func TestDirectOCIOneShotCancellationRealEngine(t *testing.T) {
	reference, digest := os.Getenv("WEFTY_OCI_PROBE_REFERENCE"), os.Getenv("WEFTY_OCI_PROBE_DIGEST")
	if reference == "" || digest == "" {
		t.Skip("OCI probe image is not published for this lane")
	}
	for _, tc := range []struct {
		name, trap string
		forced     bool
	}{
		{name: "TERM_handled", trap: `trap 'exit 0' TERM`},
		{name: "TERM_ignored", trap: `trap '' TERM`, forced: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newAcceptanceHarnessWithOptions(t, acceptanceHarnessOptions{leaseDuration: 10 * time.Second, agentArguments: ociAgentArguments(t)})
			script := tc.trap + `
printf '{"cancel_evidence":true}' > "$WEFTY_HANDOFF_DIR/result.json"
printf 'oci-cancel-ready\n'
while :; do sleep 0.1; done
`
			spec := contract.JobSpec{SchemaVersion: 1, DispatchKey: "direct-oci-cancel-" + tc.name, Kind: contract.JobKindOCI, Class: contract.JobClassOneShot,
				RoutingTags: []string{"service-acceptance"}, RuntimeHandler: "io.containerd.runc.v2",
				Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: reference, Digest: &digest}, Argv: []string{"/bin/sh", "-c", script}}}}
			var job l1.Job
			status, body := h.doJSON(t, http.MethodPost, "/v1/jobs", spec, &job)
			if status != http.StatusCreated {
				t.Fatalf("direct submit=%d %s", status, body)
			}
			deadline := time.Now().Add(2 * time.Minute)
			ready := false
			for time.Now().Before(deadline) {
				var logs l1.LogPage
				status, body = h.doJSON(t, http.MethodGet, "/v1/jobs/"+job.JobID+"/logs", nil, &logs)
				if status != http.StatusOK {
					t.Fatalf("logs=%d %s", status, body)
				}
				for _, event := range logs.Events {
					if strings.Contains(string(event.Bytes), "oci-cancel-ready") {
						ready = true
					}
				}
				if ready {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if !ready {
				t.Fatalf("container not ready: %s", h.agent.outputString())
			}
			accepted := time.Now()
			status, body = h.doJSON(t, http.MethodPost, "/v1/jobs/"+job.JobID+"/cancel", nil, &job)
			if status != http.StatusOK || job.Outcome != "canceled" {
				t.Fatalf("cancel=%d %s", status, body)
			}
			done := h.waitForJobState(t, job.JobID, contract.JobClassOneShot, contract.JobFailed, 30*time.Second)
			if done.Outcome != "canceled" || len(done.Attempts) != 1 || done.Attempts[0].Result == nil {
				t.Fatalf("canceled without completion evidence=%+v", done)
			}
			result := done.Attempts[0].Result
			if tc.forced {
				if result.Signal != "killed" || result.TerminationCause != contract.TerminationCauseAgent || time.Since(accepted) < 5*time.Second {
					t.Fatalf("forced delivery=%+v elapsed=%s", result, time.Since(accepted))
				}
			} else if result.ExitCode == nil || *result.ExitCode != 0 {
				t.Fatalf("TERM handler evidence=%+v", result)
			}
			var uploaded l1.JobResult
			deadline = time.Now().Add(30 * time.Second)
			for time.Now().Before(deadline) {
				status, body = h.doJSON(t, http.MethodGet, "/v1/jobs/"+job.JobID+"/result", nil, &uploaded)
				if status == http.StatusOK {
					break
				}
				if status != http.StatusNotFound {
					t.Fatalf("result=%d %s", status, body)
				}
				time.Sleep(100 * time.Millisecond)
			}
			if status != http.StatusOK || string(uploaded.Document) != `{"cancel_evidence":true}` || uploaded.AttemptID != done.Attempts[0].AttemptID {
				t.Fatalf("captured before reap and uploaded=%d %s", status, body)
			}
			t.Logf("direct_oci_cancel_real_engine=true forced=%t elapsed=%s result_uploaded=true attempt=%s", tc.forced, time.Since(accepted), uploaded.AttemptID)
		})
	}
}
