//go:build service_acceptance

package l1

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestServiceAcceptanceStateMetadataAndSlotLifecycle(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{
		"service-node": {Tags: []string{"service"}, MaxOneshotSlots: 4, MaxServiceSlots: 2},
	})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "service-agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "service-node")

	port := 8080
	serviceSpec := validJobSpec("service-state-acceptance", []string{"service"})
	serviceSpec.Class = contract.JobClassService
	serviceSpec.Execution.HandoffDirectory = ""
	serviceSpec.Restart = contract.RestartAlways
	serviceSpec.PublishedPort = &port
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", serviceSpec)
	if status != http.StatusCreated {
		t.Fatalf("create service status = %d body=%s", status, body)
	}
	var service Job
	if err := json.Unmarshal(body, &service); err != nil {
		t.Fatal(err)
	}
	if service.ServiceJob == nil || service.DesiredState != contract.ServiceDesiredRunning {
		t.Fatalf("created service metadata = %#v", service.ServiceJob)
	}
	if service.PublishedPort == nil || *service.PublishedPort != port {
		t.Fatalf("created service published_port = %v, want %d", service.PublishedPort, port)
	}

	oneshot := h.submit(client, "one-shot-without-service-state", []string{"one-shot-only"})
	status, _, body = h.do(client, http.MethodGet, "/v1/jobs/"+oneshot.JobID, nil)
	if status != http.StatusOK {
		t.Fatalf("get one-shot status = %d body=%s", status, body)
	}
	for _, field := range []string{
		"desired_state", "bound_node_id", "restart_streak", "lifetime_restart_count",
		"next_restart_at", "published_port", "last_failure", "healthy_since_at", "published_attempt_id",
	} {
		if bytes.Contains(body, []byte(field)) {
			t.Fatalf("one-shot response carries service metadata %q: %s", field, body)
		}
	}
	var serviceRows int
	if err := h.store.db.QueryRow("SELECT COUNT(*) FROM service_jobs").Scan(&serviceRows); err != nil {
		t.Fatal(err)
	}
	if serviceRows != 1 {
		t.Fatalf("service_jobs rows = %d, want exactly the service row", serviceRows)
	}
	assertNonUniqueIndex(t, h.store, "service_jobs", "service_jobs_bound_desired", []string{"bound_node_id", "desired_state"})

	claim, err := h.store.ClaimJob(context.Background(), "service-agent", "service-node", node.BootSessionID, contract.JobClassService)
	if err != nil {
		t.Fatal(err)
	}
	if claim == nil || claim.Job.JobID != service.JobID {
		t.Fatalf("claim = %#v, want service job %s", claim, service.JobID)
	}
	if _, err := h.store.db.Exec("UPDATE service_jobs SET bound_node_id=? WHERE job_id=?", node.NodeID, service.JobID); err != nil {
		t.Fatal(err)
	}

	claimed := getAcceptanceServiceJob(t, h, service.JobID)
	if claimed.NodeID != node.NodeID || claimed.BoundNodeID != node.NodeID {
		t.Fatalf("attempt node/binding = %q/%q, want %q/%q", claimed.NodeID, claimed.BoundNodeID, node.NodeID, node.NodeID)
	}
	if !claimed.HoldsSlot(claimed.State) {
		t.Fatal("claimed service binding must hold a slot")
	}

	transitionAcceptanceService(t, h, service.JobID, contract.ServiceDesiredStopped, contract.JobStopping)
	stopping := getAcceptanceServiceJob(t, h, service.JobID)
	if !stopping.HoldsSlot(stopping.State) {
		t.Fatal("stopping service must hold its slot until positive reap")
	}
	if stopping.CurrentAttemptID != claim.Lease.AttemptID {
		t.Fatalf("stopping current attempt = %q, want %q", stopping.CurrentAttemptID, claim.Lease.AttemptID)
	}
	if _, err := h.store.db.Exec("UPDATE attempts SET state=? WHERE attempt_id=?", contract.AttemptFailed, claim.Lease.AttemptID); err != nil {
		t.Fatal(err)
	}
	transitionAcceptanceService(t, h, service.JobID, contract.ServiceDesiredStopped, contract.JobStopped)
	stopped := getAcceptanceServiceJob(t, h, service.JobID)
	if stopped.HoldsSlot(stopped.State) {
		t.Fatal("stopped service must release its slot")
	}
	if stopped.BoundNodeID != node.NodeID {
		t.Fatalf("stopped binding = %q, want retained node %q", stopped.BoundNodeID, node.NodeID)
	}
	if stopped.CurrentAttemptID != claim.Lease.AttemptID {
		t.Fatalf("stopped current attempt = %q, want replayable %q", stopped.CurrentAttemptID, claim.Lease.AttemptID)
	}

	transitionAcceptanceService(t, h, service.JobID, contract.ServiceDesiredRunning, contract.JobQueued)
	nextRestart := h.clock.Now().Add(time.Second)
	if _, err := h.store.db.Exec("UPDATE service_jobs SET next_restart_at=? WHERE job_id=?", nextRestart.UnixNano(), service.JobID); err != nil {
		t.Fatal(err)
	}
	backingOff := getAcceptanceServiceJob(t, h, service.JobID)
	if !backingOff.RestartPending(backingOff.State, h.clock.Now()) || !backingOff.HoldsSlot(backingOff.State) {
		t.Fatalf("restart backoff projection/slot = %t/%t, want true/true", backingOff.RestartPending(backingOff.State, h.clock.Now()), backingOff.HoldsSlot(backingOff.State))
	}
	var persistedState string
	if err := h.store.db.QueryRow("SELECT state FROM jobs WHERE job_id=?", service.JobID).Scan(&persistedState); err != nil {
		t.Fatal(err)
	}
	if persistedState != string(contract.JobQueued) {
		t.Fatalf("persisted restart state = %q, want queued; restart-pending must be computed", persistedState)
	}

	transitionAcceptanceService(t, h, service.JobID, contract.ServiceDesiredRunning, contract.JobFailed)
	failed := getAcceptanceServiceJob(t, h, service.JobID)
	if failed.HoldsSlot(failed.State) {
		t.Fatal("latched failed service must release its slot")
	}
}

func transitionAcceptanceService(t *testing.T, h *integrationHarness, jobID string, desired contract.ServiceDesiredState, next contract.JobState) {
	t.Helper()
	tx, err := h.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := transitionServiceJob(context.Background(), tx, jobID, desired, next, h.clock.Now()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func getAcceptanceServiceJob(t *testing.T, h *integrationHarness, jobID string) Job {
	t.Helper()
	job, err := h.store.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatal(err)
	}
	if job.ServiceJob == nil {
		t.Fatal("service projection is missing service metadata")
	}
	return job
}
