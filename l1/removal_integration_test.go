package l1

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestServiceRemovalControllerTransactionAndAttestation(t *testing.T) {
	assertServiceRemovalControllerTransactionAndAttestation(t)
}

func assertServiceRemovalControllerTransactionAndAttestation(t *testing.T) {
	t.Helper()
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{
		"node-1": {Tags: []string{"service"}, MaxOneshotSlots: DefaultMaxOneshotSlots, MaxServiceSlots: 1},
	})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "fabric-agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "node-1")
	spec := removalServiceSpec("remove-attested", []string{"service"})
	spec.Execution.SensitiveEnv = map[string]string{"SECRET_TOKEN": "spec-secret-value"}
	job := submitRemovalService(t, h, client, spec)
	claim := claimRestartService(t, h, agent, node)

	if _, err := h.store.AppendLogs(context.Background(), "fabric-agent", job.JobID, claim.Lease.AttemptID, AppendLogsRequest{
		FencingToken: claim.Lease.FencingToken,
		Events: []contract.LogEvent{{
			AttemptID: claim.Lease.AttemptID, Stream: contract.LogStdout, Sequence: 0,
			Timestamp: h.clock.Now(), Bytes: []byte("controller-log-secret"),
		}},
	}); err != nil {
		t.Fatal(err)
	}

	status, _, body := h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/remove", nil)
	if status != http.StatusAccepted {
		t.Fatalf("remove status = %d body=%s", status, body)
	}
	var pending Job
	if err := json.Unmarshal(body, &pending); err != nil {
		t.Fatal(err)
	}
	if pending.State != contract.JobRemovalPending || pending.Removal == nil ||
		pending.Removal.RemovalDesiredState != contract.ServiceDesiredRemoved ||
		pending.Removal.RemovalGeneration != 1 || pending.Removal.RemovalBoundNodeID != node.NodeID {
		t.Fatalf("pending removal = %#v", pending)
	}
	if bytes.Contains(body, []byte("spec-secret-value")) || bytes.Contains(body, []byte("controller-log-secret")) {
		t.Fatalf("remove response retained secret-bearing bytes: %s", body)
	}

	var storedSpec []byte
	var state contract.JobState
	var fenceCounter int64
	if err := h.store.db.QueryRow(`SELECT spec_json, state, fence_counter FROM jobs WHERE job_id=?`, job.JobID).
		Scan(&storedSpec, &state, &fenceCounter); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(storedSpec, []byte("spec-secret-value")) || bytes.Contains(storedSpec, []byte("sensitive_env")) {
		t.Fatalf("scrubbed spec retained SensitiveEnv: %s", storedSpec)
	}
	if state != contract.JobRemovalPending || fenceCounter != 2 {
		t.Fatalf("removal state/fence = %q/%d, want removal_pending/2", state, fenceCounter)
	}
	var attemptState contract.AttemptState
	if err := h.store.db.QueryRow(`SELECT state FROM attempts WHERE attempt_id=?`, claim.Lease.AttemptID).Scan(&attemptState); err != nil {
		t.Fatal(err)
	}
	if attemptState != contract.AttemptLost {
		t.Fatalf("attempt state = %q, want lost", attemptState)
	}
	for table, query := range map[string]string{
		"log_events":              `SELECT COUNT(*) FROM log_events WHERE job_id=?`,
		"service_log_truncations": `SELECT COUNT(*) FROM service_log_truncations WHERE job_id=?`,
	} {
		var count int
		if err := h.store.db.QueryRow(query, job.JobID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s rows after remove = %d, want 0", table, count)
		}
	}
	var jsonl []byte
	if err := h.store.db.QueryRow(`SELECT jsonl FROM job_log_jsonl WHERE job_id=?`, job.JobID).Scan(&jsonl); err != nil {
		t.Fatal(err)
	}
	if len(jsonl) != 0 {
		t.Fatalf("authoritative JSONL after remove = %q, want empty", jsonl)
	}
	if _, err := h.store.AppendLogs(context.Background(), "fabric-agent", job.JobID, claim.Lease.AttemptID, AppendLogsRequest{
		FencingToken: claim.Lease.FencingToken,
		Events: []contract.LogEvent{{
			AttemptID: claim.Lease.AttemptID, Stream: contract.LogStdout, Sequence: 1,
			Timestamp: h.clock.Now(), Bytes: []byte("late secret"),
		}},
	}); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("post-remove log append error = %v, want conflict", err)
	}

	directives, err := h.store.ListNodeRemovalDirectives(context.Background(), "fabric-agent", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("removal directives = %#v, %v", directives, err)
	}
	directive := directives[0]
	if directive.JobID != job.JobID || directive.RemovalGeneration != 1 ||
		directive.RootInstanceID != node.RootInstanceID || directive.CleanupFence == "" {
		t.Fatalf("removal directive = %#v", directive)
	}
	status, _, repeatedBody := h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/remove", nil)
	if status != http.StatusAccepted {
		t.Fatalf("repeated remove status = %d body=%s", status, repeatedBody)
	}
	repeatedDirectives, err := h.store.ListNodeRemovalDirectives(context.Background(), "fabric-agent", node.NodeID, node.BootSessionID)
	if err != nil || len(repeatedDirectives) != 1 || repeatedDirectives[0] != directive {
		t.Fatalf("repeated directive = %#v, %v; want %#v", repeatedDirectives, err, directive)
	}
	waiting := submitRemovalService(t, h, client, removalServiceSpec("waits-for-removal-slot", []string{"service"}))
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{
		NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: contract.JobClassService,
	})
	if status != http.StatusNoContent {
		t.Fatalf("removal_pending released service slot early: status=%d body=%s", status, body)
	}

	replacement := node.NodeRegistration
	replacement.BootSessionID = "boot-replacement"
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/register", replacement)
	if status != http.StatusOK {
		t.Fatalf("replacement register status = %d body=%s", status, body)
	}
	if _, err := h.store.ListNodeRemovalDirectives(context.Background(), "fabric-agent", node.NodeID, node.BootSessionID); errorCode(err) != contract.ErrorNodeSessionReplaced {
		t.Fatalf("old boot directive read error = %v, want node_session_replaced", err)
	}
	directives, err = h.store.ListNodeRemovalDirectives(context.Background(), "fabric-agent", node.NodeID, replacement.BootSessionID)
	if err != nil || len(directives) != 1 || directives[0] != directive {
		t.Fatalf("replacement boot directives = %#v, %v", directives, err)
	}
	recreated := replacement
	recreated.BootSessionID = "boot-recreated-root"
	recreated.RootInstanceID = "different-root-instance"
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/register", recreated)
	if status != http.StatusOK {
		t.Fatalf("recreated-root registration status = %d body=%s", status, body)
	}
	recreatedAck := RemovalAcknowledgementRequest{
		NodeID: node.NodeID, BootSessionID: recreated.BootSessionID,
		RemovalGeneration: directive.RemovalGeneration, CleanupFence: directive.CleanupFence,
		RootInstanceID: directive.RootInstanceID, IdempotencyKey: "wrong-root-cleanup",
	}
	if _, err := h.store.AcknowledgeServiceRemoval(context.Background(), "fabric-agent", job.JobID, recreatedAck); errorCode(err) != contract.ErrorStaleFence {
		t.Fatalf("recreated root acknowledgement error = %v, want stale_fence", err)
	}
	resumed := recreated
	resumed.BootSessionID = "boot-resumed-root"
	resumed.RootInstanceID = node.RootInstanceID
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/register", resumed)
	if status != http.StatusOK {
		t.Fatalf("resumed-root registration status = %d body=%s", status, body)
	}

	ack := RemovalAcknowledgementRequest{
		NodeID: node.NodeID, BootSessionID: node.BootSessionID,
		RemovalGeneration: directive.RemovalGeneration, CleanupFence: directive.CleanupFence,
		RootInstanceID: directive.RootInstanceID, IdempotencyKey: "cleanup-ack-1",
	}
	ackPath := "/v1/agent/jobs/" + job.JobID + "/removal-acknowledgement"
	status, _, body = h.do(agent, http.MethodPost, ackPath, ack)
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorNodeSessionReplaced)
	ack.BootSessionID = resumed.BootSessionID
	acknowledged, err := h.store.AcknowledgeServiceRemoval(context.Background(), "fabric-agent", job.JobID, ack)
	if err != nil || acknowledged.State != contract.JobAgentCleaned {
		t.Fatalf("direct cleanup acknowledgement = %#v, %v", acknowledged, err)
	}
	if replayed, err := h.store.AcknowledgeServiceRemoval(context.Background(), "fabric-agent", job.JobID, ack); err != nil || replayed.State != contract.JobAgentCleaned {
		t.Fatalf("identical mutable acknowledgement replay = %#v, %v", replayed, err)
	}
	conflictingAck := ack
	conflictingAck.IdempotencyKey = "conflicting-before-finalization"
	if _, err := h.store.AcknowledgeServiceRemoval(context.Background(), "fabric-agent", job.JobID, conflictingAck); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("conflicting mutable acknowledgement error = %v, want conflict", err)
	}
	status, _, body = h.do(agent, http.MethodPost, ackPath, ack)
	if status != http.StatusOK {
		t.Fatalf("cleanup acknowledgement status = %d body=%s", status, body)
	}
	var removed Job
	if err := json.Unmarshal(body, &removed); err != nil {
		t.Fatal(err)
	}
	if removed.State != contract.JobRemovedVerified || removed.Removal == nil ||
		removed.Removal.RemovalOutcome != ServiceRemovalVerified || removed.Removal.CleanupAcknowledgedAt == nil {
		t.Fatalf("removed projection = %#v", removed)
	}
	assertRemovedServiceRows(t, h, job.JobID)
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{
		NodeID: node.NodeID, BootSessionID: resumed.BootSessionID, Class: contract.JobClassService,
	})
	if status != http.StatusOK {
		t.Fatalf("verified removal did not release service slot: status=%d body=%s", status, body)
	}
	var next Claim
	if err := json.Unmarshal(body, &next); err != nil {
		t.Fatal(err)
	}
	if next.Job.JobID != waiting.JobID {
		t.Fatalf("post-removal claim = %q, want waiting service %q", next.Job.JobID, waiting.JobID)
	}
	var tombstoneDispatchHash, tombstoneRequestHash, tombstoneOutcome, tombstoneNode, tombstoneRoot string
	var tombstoneCreated, tombstoneRequested, tombstoneRemoved, tombstoneGeneration, tombstoneAcknowledged int64
	if err := h.store.db.QueryRow(`SELECT dispatch_key_hash, request_hash, created_ns, removal_requested_ns,
		removed_ns, outcome, last_bound_node_id, removal_generation, root_instance_id, cleanup_acknowledged_ns
		FROM service_tombstones WHERE job_id=?`, job.JobID).Scan(&tombstoneDispatchHash, &tombstoneRequestHash,
		&tombstoneCreated, &tombstoneRequested, &tombstoneRemoved, &tombstoneOutcome, &tombstoneNode,
		&tombstoneGeneration, &tombstoneRoot, &tombstoneAcknowledged); err != nil {
		t.Fatal(err)
	}
	if tombstoneDispatchHash != hashDispatchKey(spec.DispatchKey) || len(tombstoneRequestHash) != 64 ||
		tombstoneCreated != job.CreatedAt.UnixNano() || tombstoneRequested > tombstoneRemoved ||
		tombstoneOutcome != string(ServiceRemovalVerified) || tombstoneNode != node.NodeID ||
		tombstoneGeneration != 1 || tombstoneRoot != node.RootInstanceID || tombstoneAcknowledged == 0 {
		t.Fatalf("tombstone fields = hash:%q request:%q times:%d/%d/%d outcome:%q node:%q gen:%d root:%q ack:%d",
			tombstoneDispatchHash, tombstoneRequestHash, tombstoneCreated, tombstoneRequested, tombstoneRemoved,
			tombstoneOutcome, tombstoneNode, tombstoneGeneration, tombstoneRoot, tombstoneAcknowledged)
	}
	status, _, body = h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/remove", nil)
	if status != http.StatusAccepted {
		t.Fatalf("already-removed replay = %d body=%s", status, body)
	}

	// After finalization there is no mutable state for an idempotency key to
	// protect. The tombstone's retained authority fields make this a constant
	// success without resurrecting or changing anything.
	ack.IdempotencyKey = "different-after-finalization"
	ack.CleanupFence = "not-retained-after-finalization"
	status, _, body = h.do(agent, http.MethodPost, ackPath, ack)
	if status != http.StatusOK {
		t.Fatalf("post-finalization acknowledgement replay = %d body=%s", status, body)
	}

	status, headers, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusOK || headers.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("tombstone create replay = %d/%q body=%s", status, headers.Get("Idempotency-Replayed"), body)
	}
	if bytes.Contains(body, []byte(`"spec"`)) || bytes.Contains(body, []byte("spec-secret-value")) {
		t.Fatalf("tombstone replay retained specification bytes: %s", body)
	}
	var replay Job
	if err := json.Unmarshal(body, &replay); err != nil {
		t.Fatal(err)
	}
	if replay.JobID != job.JobID || replay.State != contract.JobRemovedVerified || !reflect.DeepEqual(replay.Spec, contract.JobSpec{}) {
		t.Fatalf("tombstone create replay = %#v", replay)
	}
	var activeJobs int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE job_id=?`, job.JobID).Scan(&activeJobs); err != nil {
		t.Fatal(err)
	}
	if activeJobs != 0 {
		t.Fatalf("create replay resurrected %d active jobs", activeJobs)
	}
	spec.Labels = map[string]string{"changed": "request"}
	status, _, body = h.do(client, http.MethodPost, "/v1/jobs", spec)
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorDispatchKeyConflict)
}

func TestServiceRemovalForceForgetLeavesDirectiveStanding(t *testing.T) {
	assertServiceRemovalForceForgetLeavesDirectiveStanding(t)
}

func TestReconcileFinalizesAcknowledgedServiceRemoval(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": nil})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "fabric-agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "node-1")
	job := submitRemovalService(t, h, client, removalServiceSpec("reconcile-removal", nil))
	claimRestartService(t, h, agent, node)
	if _, err := h.store.RemoveService(context.Background(), job.JobID); err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeRemovalDirectives(context.Background(), "fabric-agent", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("directives = %#v, %v", directives, err)
	}
	directive := directives[0]
	if _, err := h.store.AcknowledgeServiceRemoval(context.Background(), "fabric-agent", job.JobID, RemovalAcknowledgementRequest{
		NodeID: node.NodeID, BootSessionID: node.BootSessionID, RemovalGeneration: directive.RemovalGeneration,
		CleanupFence: directive.CleanupFence, RootInstanceID: directive.RootInstanceID, IdempotencyKey: "reconcile-ack",
	}); err != nil {
		t.Fatal(err)
	}
	result, err := h.store.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.FinalizedRemovals != 1 {
		t.Fatalf("finalized removals = %d, want 1", result.FinalizedRemovals)
	}
	removed, err := h.store.GetJob(context.Background(), job.JobID)
	if err != nil || removed.State != contract.JobRemovedVerified {
		t.Fatalf("reconciled removal = %#v, %v", removed, err)
	}
	assertRemovedServiceRows(t, h, job.JobID)
}

func assertServiceRemovalForceForgetLeavesDirectiveStanding(t *testing.T) {
	t.Helper()
	h := newIntegrationHarness(t, map[string][]string{"node-1": nil})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "fabric-agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "node-1")
	job := submitRemovalService(t, h, client, removalServiceSpec("force-forget", nil))
	claimRestartService(t, h, agent, node)

	status, _, body := h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/forget", ForceForgetRequest{Force: true})
	if status != http.StatusOK {
		t.Fatalf("force forget status = %d body=%s", status, body)
	}
	var forgotten Job
	if err := json.Unmarshal(body, &forgotten); err != nil {
		t.Fatal(err)
	}
	if forgotten.State != contract.JobForgottenCleanupUnverified || forgotten.Removal == nil ||
		forgotten.Removal.RemovalOutcome != ServiceRemovalForgotten {
		t.Fatalf("force-forgotten projection = %#v", forgotten)
	}
	var activeRows int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE job_id=?`, job.JobID).Scan(&activeRows); err != nil {
		t.Fatal(err)
	}
	if activeRows != 1 {
		t.Fatalf("force forget deleted active directive owner; jobs=%d", activeRows)
	}
	directives, err := h.store.ListNodeRemovalDirectives(context.Background(), "fabric-agent", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("force-forgotten directives = %#v, %v", directives, err)
	}
	directive := directives[0]
	ack := RemovalAcknowledgementRequest{
		NodeID: node.NodeID, BootSessionID: node.BootSessionID, RemovalGeneration: directive.RemovalGeneration,
		CleanupFence: directive.CleanupFence, RootInstanceID: directive.RootInstanceID, IdempotencyKey: "late-cleanup",
	}
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/"+job.JobID+"/removal-acknowledgement", ack)
	if status != http.StatusOK {
		t.Fatalf("late cleanup acknowledgement = %d body=%s", status, body)
	}
	if err := json.Unmarshal(body, &forgotten); err != nil {
		t.Fatal(err)
	}
	if forgotten.State != contract.JobForgottenCleanupUnverified || forgotten.Removal.RemovalOutcome != ServiceRemovalForgotten ||
		forgotten.Removal.CleanupAcknowledgedAt == nil {
		t.Fatalf("late cleanup upgraded force-forgotten outcome: %#v", forgotten)
	}
	assertRemovedServiceRows(t, h, job.JobID)
}

func TestUnboundServiceRemovalFinalizesWithoutAgentAttestation(t *testing.T) {
	assertUnboundServiceRemovalFinalizesWithoutAgentAttestation(t)
}

func TestServiceRemovalAcceptsEveryBoundServiceState(t *testing.T) {
	assertServiceRemovalAcceptsEveryBoundServiceState(t)
}

func assertServiceRemovalAcceptsEveryBoundServiceState(t *testing.T) {
	t.Helper()
	tests := []struct {
		state        contract.JobState
		attemptState contract.AttemptState
		desired      contract.ServiceDesiredState
	}{
		{state: contract.JobClaimed, attemptState: contract.AttemptClaimed, desired: contract.ServiceDesiredRunning},
		{state: contract.JobRunning, attemptState: contract.AttemptRunning, desired: contract.ServiceDesiredRunning},
		{state: contract.JobStopping, attemptState: contract.AttemptRunning, desired: contract.ServiceDesiredStopped},
		{state: contract.JobStopped, attemptState: contract.AttemptSucceeded, desired: contract.ServiceDesiredStopped},
		{state: contract.JobFailed, attemptState: contract.AttemptFailed, desired: contract.ServiceDesiredRunning},
	}
	for _, test := range tests {
		t.Run(string(test.state), func(t *testing.T) {
			h := newIntegrationHarness(t, map[string][]string{"node-1": nil})
			client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
			agent := h.client(fabric.Identity{NodeID: "fabric-agent", Tags: []string{DefaultAgentPrincipalTag}})
			node := h.register(agent, "node-1")
			job := submitRemovalService(t, h, client, removalServiceSpec("remove-state-"+string(test.state), nil))
			claim := claimRestartService(t, h, agent, node)
			if _, err := h.store.db.Exec(`UPDATE attempts SET state=? WHERE attempt_id=?`, test.attemptState, claim.Lease.AttemptID); err != nil {
				t.Fatal(err)
			}
			if _, err := h.store.db.Exec(`UPDATE jobs SET state=? WHERE job_id=?`, test.state, job.JobID); err != nil {
				t.Fatal(err)
			}
			if _, err := h.store.db.Exec(`UPDATE service_jobs SET desired_state=? WHERE job_id=?`, test.desired, job.JobID); err != nil {
				t.Fatal(err)
			}
			status, _, body := h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/remove", nil)
			if status != http.StatusAccepted {
				t.Fatalf("remove from %s = %d body=%s", test.state, status, body)
			}
			var pending Job
			if err := json.Unmarshal(body, &pending); err != nil {
				t.Fatal(err)
			}
			if pending.State != contract.JobRemovalPending || pending.Removal == nil {
				t.Fatalf("remove from %s projected %#v", test.state, pending)
			}
		})
	}
}

func assertUnboundServiceRemovalFinalizesWithoutAgentAttestation(t *testing.T) {
	t.Helper()
	h := newIntegrationHarness(t, nil)
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	job := submitRemovalService(t, h, client, removalServiceSpec("remove-unbound", nil))
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/remove", nil)
	if status != http.StatusAccepted {
		t.Fatalf("unbound remove status = %d body=%s", status, body)
	}
	var removed Job
	if err := json.Unmarshal(body, &removed); err != nil {
		t.Fatal(err)
	}
	if removed.State != contract.JobRemovedVerified || removed.Removal == nil ||
		removed.Removal.RemovalOutcome != ServiceRemovalVerified || removed.Removal.CleanupAcknowledgedAt != nil ||
		removed.Removal.RemovalBoundNodeID != "" {
		t.Fatalf("unbound removal projection = %#v", removed)
	}
	assertRemovedServiceRows(t, h, job.JobID)
	var busy, logFrames, checkpointed int
	if err := h.store.db.QueryRow(`PRAGMA wal_checkpoint(PASSIVE)`).Scan(&busy, &logFrames, &checkpointed); err != nil {
		t.Fatal(err)
	}
	if busy != 0 || logFrames != 0 || checkpointed != 0 {
		t.Fatalf("post-removal WAL = busy:%d log:%d checkpointed:%d, want truncated", busy, logFrames, checkpointed)
	}
}

func removalServiceSpec(dispatchKey string, tags []string) contract.JobSpec {
	spec := validJobSpec(dispatchKey, tags)
	spec.Class = contract.JobClassService
	spec.Execution.HandoffDirectory = ""
	spec.Restart = contract.RestartAlways
	return spec
}

func submitRemovalService(t *testing.T, h *integrationHarness, client *http.Client, spec contract.JobSpec) Job {
	t.Helper()
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusCreated {
		t.Fatalf("submit removal service status = %d body=%s", status, body)
	}
	var job Job
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	return job
}

func assertRemovedServiceRows(t *testing.T, h *integrationHarness, jobID string) {
	t.Helper()
	for table, query := range map[string]string{
		"jobs":             `SELECT COUNT(*) FROM jobs WHERE job_id=?`,
		"service_jobs":     `SELECT COUNT(*) FROM service_jobs WHERE job_id=?`,
		"attempts":         `SELECT COUNT(*) FROM attempts WHERE job_id=?`,
		"service_removals": `SELECT COUNT(*) FROM service_removals WHERE job_id=?`,
	} {
		var count int
		if err := h.store.db.QueryRow(query, jobID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s rows after finalization = %d, want 0", table, count)
		}
	}
	var tombstones int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM service_tombstones WHERE job_id=?`, jobID).Scan(&tombstones); err != nil {
		t.Fatal(err)
	}
	if tombstones != 1 {
		t.Fatalf("service tombstones = %d, want 1", tombstones)
	}
}

func TestServiceRemovalWALCheckpointRetriesBlockedReaders(t *testing.T) {
	// A reader that predates deletion can keep old WAL frames visible. Removal
	// must wait and retry TRUNCATE rather than treating SQLITE_BUSY as success.
	h := newIntegrationHarness(t, nil)
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	job := submitRemovalService(t, h, client, removalServiceSpec("wal-reader", nil))
	conn, err := h.store.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tx, err := conn.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	var count int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM jobs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	released := make(chan struct{})
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = tx.Rollback()
		_ = conn.Close()
		close(released)
	}()
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/remove", nil)
	if status != http.StatusAccepted {
		t.Fatalf("remove with blocked reader = %d body=%s", status, body)
	}
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("reader was not released")
	}
}
