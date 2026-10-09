package l1

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

var jobProjectionCases = []string{"claimed", "running", "stopped", "restart-pending", "capability-missing", "capacity", "policy-stop", "never-interruption", "latch", "removal", "tombstone"}

func jobProjectionFixture(t *testing.T, state string) (*integrationHarness, *http.Client, Job, Claim) {
	t.Helper()
	h, client, agent, node, job, claim := neverFixtureWith(t, "process", func(spec *contract.JobSpec) {
		if state == "restart-pending" {
			spec.Restart = contract.RestartAlways
		}
	})
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := h.store.db.Exec(query, args...); err != nil {
			t.Fatal(err)
		}
	}
	switch state {
	case "running":
		neverStarted(t, h, agent, job, claim)
	case "stopped":
		exec("UPDATE jobs SET state='stopped' WHERE job_id=?", job.JobID)
		exec("UPDATE service_jobs SET desired_state='stopped' WHERE job_id=?", job.JobID)
	case "restart-pending":
		exec("UPDATE jobs SET state='queued' WHERE job_id=?", job.JobID)
		exec("UPDATE service_jobs SET next_restart_at=? WHERE job_id=?", h.clock.Now().Add(time.Minute).UnixNano(), job.JobID)
	case "capability-missing":
		exec("UPDATE jobs SET state='queued' WHERE job_id=?", job.JobID)
		exec("UPDATE nodes SET capabilities_json='{}' WHERE node_id=?", node.NodeID)
	case "capacity":
		exec("UPDATE jobs SET state='queued' WHERE job_id=?", job.JobID)
		exec("UPDATE service_jobs SET bound_node_id=NULL WHERE job_id=?", job.JobID)
		exec("UPDATE nodes SET max_service_slots=0 WHERE node_id=?", node.NodeID)
	case "policy-stop":
		neverStarted(t, h, agent, job, claim)
		exit := 0
		neverComplete(t, h, agent, job, claim, CompletionRequest{Result: ProcessResult{ExitCode: &exit}})
	case "never-interruption":
		neverStarted(t, h, agent, job, claim)
		neverComplete(t, h, agent, job, claim, CompletionRequest{Result: ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}})
	case "latch":
		neverStarted(t, h, agent, job, claim)
		neverComplete(t, h, agent, job, claim, CompletionRequest{Result: ProcessResult{OutputError: "durable output failed"}})
	case "removal", "tombstone":
		if state == "tombstone" {
			exec("UPDATE service_jobs SET bound_node_id=NULL WHERE job_id=?", job.JobID)
		}
		if _, err := h.store.RemoveService(t.Context(), job.JobID); err != nil {
			t.Fatal(err)
		}
	}
	return h, client, job, claim
}
func normalizedJobProjection(t *testing.T, raw []byte, job Job, claim Claim) []byte {
	t.Helper()
	text := strings.NewReplacer(job.JobID, "JOB", claim.Lease.AttemptID, "ATTEMPT").Replace(string(raw))
	var decoded any
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		t.Fatal(err)
	}
	normalized, err := json.MarshalIndent(decoded, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return append(normalized, '\n')
}

// These complete response fixtures are recorded against original main 8d24686,
// then consumed unchanged by both the legacy views and the shared projector.
func TestJobProjectorPublicResponseFixtures(t *testing.T) {
	for _, state := range jobProjectionCases {
		t.Run(state, func(t *testing.T) {
			h, client, job, claim := jobProjectionFixture(t, state)
			status, _, raw := h.do(client, http.MethodGet, "/v1/jobs/"+job.JobID+"?class=service", nil)
			if status != http.StatusOK {
				t.Fatalf("response=%d %s", status, raw)
			}
			got := normalizedJobProjection(t, raw, job, claim)
			path := filepath.Join("testdata", "job-projection-"+state+".json")
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != string(want) {
				t.Fatalf("public projection changed\ngot:\n%s\nwant:\n%s", got, want)
			}
		})
	}
}
