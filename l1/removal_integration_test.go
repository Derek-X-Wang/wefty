package l1

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
)

func TestServiceRemovalControllerTransactionAndAttestation(t *testing.T) {
	assertServiceRemovalControllerTransactionAndAttestation(t)
}

func TestLegacyFinalizedRemovalAcceptsBareAcknowledgementAfterMigration(t *testing.T) {
	harness := newRemovalStallHarness(t)
	ack := RemovalAcknowledgementRequest{
		NodeID: harness.node.NodeID, BootSessionID: harness.node.BootSessionID,
		RemovalGeneration: harness.directive.RemovalGeneration, CleanupFence: harness.directive.CleanupFence,
		RootInstanceID: harness.directive.RootInstanceID, IdempotencyKey: "legacy-positive-cleanup",
	}
	if _, err := harness.h.store.AcknowledgeServiceRemoval(t.Context(), "fabric-agent", harness.job.JobID, ack); err != nil {
		t.Fatal(err)
	}
	finalizeOrObserveRemoval(t, harness.h.store, harness.job.JobID, func(job Job) bool {
		return job.State == contract.JobRemovedVerified
	})
	var sequence int
	var name, databasePath string
	if err := harness.h.store.db.QueryRow(`PRAGMA database_list`).Scan(&sequence, &name, &databasePath); err != nil {
		t.Fatal(err)
	}
	if err := harness.h.store.Close(); err != nil {
		t.Fatal(err)
	}
	legacy, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"cleanup_acknowledgement_key", "cleanup_acknowledgement_hash"} {
		if _, err := legacy.Exec(`ALTER TABLE service_tombstones DROP COLUMN ` + column); err != nil {
			t.Fatal(err)
		}
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	migrated, err := OpenStore(databasePath, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()
	var key, hash sql.NullString
	if err := migrated.db.QueryRow(`SELECT cleanup_acknowledgement_key, cleanup_acknowledgement_hash
		FROM service_tombstones WHERE job_id=?`, harness.job.JobID).Scan(&key, &hash); err != nil {
		t.Fatal(err)
	}
	if key.Valid || hash.Valid {
		t.Fatal("migration unexpectedly invented boot-specific replay identity")
	}
	ack.BootSessionID = "returning-boot"
	ack.IdempotencyKey = "returning-positive-cleanup"
	ack.CleanupFence = "returning-cleanup-fence"
	job, err := migrated.AcknowledgeServiceRemoval(t.Context(), "fabric-agent", harness.job.JobID, ack)
	if err != nil || job.State != contract.JobRemovedVerified {
		t.Fatalf("legacy finalized bare acknowledgement = %+v, %v", job, err)
	}
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

	status, _, body := h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/remove?class=service", nil)
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
	if directive.JobID != job.JobID || directive.Kind != contract.JobKindProcess || directive.RemovalGeneration != 1 ||
		directive.RootInstanceID != node.RootInstanceID || directive.CleanupFence == "" {
		t.Fatalf("removal directive = %#v", directive)
	}
	status, _, repeatedBody := h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/remove?class=service", nil)
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
	status, _, body = h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/remove?class=service", nil)
	if status != http.StatusAccepted {
		t.Fatalf("already-removed replay = %d body=%s", status, body)
	}

	// A finalized bare acknowledgement is the positive-cleanup shape already
	// accepted. A returning boot may regenerate its key and fence after L1
	// committed but before the old agent cleared its local record.
	status, _, body = h.do(agent, http.MethodPost, ackPath, ack)
	if status != http.StatusOK {
		t.Fatalf("post-finalization acknowledgement replay = %d body=%s", status, body)
	}
	changedAfterFinalization := ack
	changedAfterFinalization.BootSessionID = "returning-after-finalization"
	changedAfterFinalization.IdempotencyKey = "different-after-finalization"
	changedAfterFinalization.CleanupFence = "not-retained-after-finalization"
	status, _, body = h.do(agent, http.MethodPost, ackPath, changedAfterFinalization)
	if status != http.StatusOK {
		t.Fatalf("different-boot finalized acknowledgement replay = %d body=%s", status, body)
	}
	changedAfterFinalization.CleanupStall = &ServiceRemovalStallEvidence{
		Kind: ServiceRemovalStallEvidenceKind,
	}
	status, _, body = h.do(agent, http.MethodPost, ackPath, changedAfterFinalization)
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorIdempotencyConflict)

	status, headers, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusOK || headers.Get("Idempotent-Replay") != "true" {
		t.Fatalf("tombstone create replay = %d/%q body=%s", status, headers.Get("Idempotent-Replay"), body)
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

	status, _, body := h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/forget?class=service", ForceForgetRequest{Force: true})
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
			status, _, body := h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/remove?class=service", nil)
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
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/remove?class=service", nil)
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
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/remove?class=service", nil)
	if status != http.StatusAccepted {
		t.Fatalf("remove with blocked reader = %d body=%s", status, body)
	}
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("reader was not released")
	}
}

// Serve only the real HTTP handler: this test owns every reconciliation step.
// In particular no background tick can finalize between acknowledgement and GET.
func TestForceForgottenAcknowledgementPrecedesTombstoneFinalization(t *testing.T) {
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	clock := &fakeClock{now: time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)}
	store, err := OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), StoreOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server, err := NewServer(serverFabric, store, ServerConfig{NodePolicies: map[string]NodePolicy{"node-1": DefaultNodePolicy()}})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: server.Handler()}
	served := make(chan error, 1)
	go func() { served <- httpServer.Serve(listener) }()
	h := &integrationHarness{t: t, network: network, store: store, server: server, clock: clock}
	defer func() {
		for _, client := range h.clients {
			client.CloseIdleConnections()
		}
		_ = httpServer.Close()
		if err := <-served; !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve L1 handler: %v", err)
		}
	}()
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "fabric-agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "node-1")
	job := submitRemovalService(t, h, client, removalServiceSpec("forgotten-finalization-phase", nil))
	claimRestartService(t, h, agent, node)
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/forget?class=service", ForceForgetRequest{Force: true})
	if status != http.StatusOK {
		t.Fatalf("force forget = %d body=%s", status, body)
	}
	directives, err := store.ListNodeRemovalDirectives(t.Context(), "fabric-agent", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("directives=%#v err=%v", directives, err)
	}
	directive := directives[0]
	ack := RemovalAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
		RemovalGeneration: directive.RemovalGeneration, CleanupFence: directive.CleanupFence,
		RootInstanceID: directive.RootInstanceID, IdempotencyKey: "phase-ack"}
	// Stage the same committed store operation used by the HTTP ACK handler,
	// leaving its separate finalization operation under test control.
	if _, err := store.AcknowledgeServiceRemoval(t.Context(), "fabric-agent", job.JobID, ack); err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(client, http.MethodGet, "/v1/jobs/"+job.JobID+"?class=service", nil)
	var projected Job
	if status != http.StatusOK {
		t.Fatalf("GET acknowledged job=%d body=%s", status, body)
	}
	if err := json.Unmarshal(body, &projected); err != nil {
		t.Fatal(err)
	}
	if projected.State != contract.JobForgottenCleanupUnverified || projected.Removal == nil ||
		projected.Removal.RemovalOutcome != ServiceRemovalForgotten || projected.Removal.CleanupAcknowledgedAt == nil ||
		projected.Removal.RemovalGeneration != directive.RemovalGeneration || projected.Removal.RemovalBoundNodeID != directive.BoundNodeID {
		t.Fatalf("acknowledged GET projection=%#v", projected)
	}
	var before sql.NullInt64
	if err := store.db.QueryRow(`SELECT cleanup_acknowledged_ns FROM service_tombstones WHERE job_id=?`, job.JobID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if before.Valid {
		t.Fatalf("tombstone finalized before explicit phase: %#v", before)
	}
	t.Log("real acknowledged GET satisfies original waiter predicate while tombstone cleanup_acknowledged_ns is NULL")
	// Guard the old predicate/reader mismatch using the real unfinalized row.
	var originalReader int64
	if err := store.db.QueryRow(`SELECT cleanup_acknowledged_ns FROM service_tombstones WHERE job_id=?`, job.JobID).Scan(&originalReader); err == nil {
		t.Fatal("original int64 reader unexpectedly accepted the unfinalized tombstone")
	}
	finalized, changed, err := store.FinalizeServiceRemoval(t.Context(), job.JobID)
	if err != nil || !changed {
		t.Fatalf("finalize=%#v changed=%v err=%v", finalized, changed, err)
	}
	if finalized.State != contract.JobForgottenCleanupUnverified || finalized.Removal.RemovalOutcome != ServiceRemovalForgotten {
		t.Fatalf("finalization upgraded forgotten outcome: %#v", finalized)
	}
	var after int64
	if err := store.db.QueryRow(`SELECT cleanup_acknowledged_ns FROM service_tombstones WHERE job_id=?`, job.JobID).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != projected.Removal.CleanupAcknowledgedAt.UnixNano() {
		t.Fatalf("finalized ack=%d want=%d", after, projected.Removal.CleanupAcknowledgedAt.UnixNano())
	}

	var generation uint64
	var boundNode, rootInstance string
	if err := store.db.QueryRow(`SELECT removal_generation, last_bound_node_id, root_instance_id FROM service_tombstones WHERE job_id=?`, job.JobID).Scan(&generation, &boundNode, &rootInstance); err != nil {
		t.Fatal(err)
	}
	if generation != directive.RemovalGeneration || boundNode != directive.BoundNodeID || rootInstance != directive.RootInstanceID {
		t.Fatalf("finalized identity=%d/%q/%q want=%d/%q/%q", generation, boundNode, rootInstance, directive.RemovalGeneration, directive.BoundNodeID, directive.RootInstanceID)
	}
	t.Log("explicit finalization records non-NULL acknowledgement without upgrading forgotten outcome")
	assertRemovedServiceRows(t, h, job.JobID)
}

// finalizeOrObserveRemoval finalizes a service removal's cleanup phase and
// returns the resulting Job, accepting either winner of the race between this
// call and the harness server's own background reconcile pass: the
// immediate recovery pass in Serve and its 1s ticker (l1/server.go ~168,181)
// finalize any agent_cleaned+acknowledged removal on their own
// (l1/recovery.go ~185-209), so on a loaded runner they can land between a
// fixture's acknowledgement and its own FinalizeServiceRemoval call. When
// that happens FinalizeServiceRemoval reports changed=false, but it still
// returns the removal's current Job -- finalization is idempotent, so that
// Job carries the same outcome this call would have produced. want is
// evaluated against that Job regardless of which call actually finalized it,
// so the assertion is about the outcome reached, never about who won.
func finalizeOrObserveRemoval(t *testing.T, store *Store, jobID string, want func(Job) bool) Job {
	t.Helper()
	job, changed, err := store.FinalizeServiceRemoval(context.Background(), jobID)
	if err != nil {
		t.Fatalf("finalize removal %s: err=%v", jobID, err)
	}
	if !want(job) {
		t.Fatalf("finalize removal %s changed=%t did not reach the expected outcome: %#v", jobID, changed, job)
	}
	return job
}

// TestLateCleanupNeverUpgradesAWaivedRemoval holds the waiver terminal for
// both service kinds. An operator who force-forgets accepts an outcome nobody
// proved; when a returning node finally acknowledges cleanup, that
// acknowledgement is evidence of what the node did, not the proof the operator
// waived. The ordinary-service branch has always refused the upgrade by
// leaving the force-forgotten tombstone's outcome alone. The Computer branch
// runs first in finalization and wrote removed_verified unconditionally, so a
// waived Computer would have ended up claiming a proof nobody made.
//
// The Computer half drives the waiver at the store level on purpose: today
// ForceForgetService refuses a Computer-mapped Job, so the only way to pin the
// invariant before Computers gain an operator waiver is to write the waived
// row the waiver will write. It deliberately leaves removed_ns NULL, because
// removed_ns is the Computer branch's "already finalized" marker and setting
// it would make the test pass through an unrelated guard.
func TestLateCleanupNeverUpgradesAWaivedRemoval(t *testing.T) {
	computer := finalizeWaivedComputerRemoval(t)
	ordinary := finalizeWaivedOrdinaryRemoval(t)
	if computer.State != contract.JobForgottenCleanupUnverified || ordinary.State != contract.JobForgottenCleanupUnverified {
		t.Fatalf("waived states = Computer %q, ordinary %q, want forgotten_cleanup_unverified for both",
			computer.State, ordinary.State)
	}
	if computer.State != ordinary.State {
		t.Fatalf("the two kinds disagree about a waived removal: Computer %q, ordinary %q",
			computer.State, ordinary.State)
	}
	if computer.Removal.RemovalOutcome != ServiceRemovalForgotten ||
		ordinary.Removal.RemovalOutcome != ServiceRemovalForgotten {
		t.Fatalf("waived outcomes = Computer %q, ordinary %q, want force_forgotten for both",
			computer.Removal.RemovalOutcome, ordinary.Removal.RemovalOutcome)
	}
	if computer.Removal.CleanupAcknowledgedAt == nil || ordinary.Removal.CleanupAcknowledgedAt == nil {
		t.Fatalf("the late acknowledgement was not recorded: Computer %#v, ordinary %#v",
			computer.Removal, ordinary.Removal)
	}
}

// finalizeWaivedComputerRemoval waives proof on a Computer-projecting removal,
// lets the bound node acknowledge cleanup afterwards, and finalizes.
func finalizeWaivedComputerRemoval(t *testing.T) Job {
	t.Helper()
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"computer-node": {
			Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1,
		},
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	computer, _, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "waived", Spec: computerCapabilityJobSpec("computer:waived-removal"), Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	if claim, err := h.store.ClaimJob(context.Background(), "fabric-computer-node", node.NodeID,
		node.BootSessionID, contract.JobClassService); err != nil || claim == nil {
		t.Fatalf("claim = %#v err=%v", claim, err)
	}
	if computer, err = h.store.GetComputer(context.Background(), computer.ComputerID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.RemoveComputer(context.Background(), computer.ComputerID, ComputerRemoveRequest{
		ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
	}); err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeRemovalDirectives(context.Background(), "fabric-computer-node",
		node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("Computer removal directives = %#v, %v", directives, err)
	}
	directive := directives[0]
	waiveComputerRemoval(t, h.store, computer.CurrentJobID)

	completion := RemovalAcknowledgementRequest{
		NodeID: node.NodeID, BootSessionID: node.BootSessionID,
		RemovalGeneration: directive.RemovalGeneration, CleanupFence: directive.CleanupFence,
		RootInstanceID: directive.RootInstanceID, IdempotencyKey: "removal:computer-waived-late",
	}
	acknowledged, err := h.store.AcknowledgeServiceRemoval(context.Background(), "fabric-computer-node",
		computer.CurrentJobID, completion)
	if err != nil {
		t.Fatalf("late cleanup acknowledgement on a waived Computer: %v", err)
	}
	if acknowledged.State != contract.JobForgottenCleanupUnverified {
		t.Fatalf("acknowledgement state = %q, want the waiver to stand", acknowledged.State)
	}
	finalized := finalizeOrObserveRemoval(t, h.store, computer.CurrentJobID, func(job Job) bool {
		return job.State == contract.JobForgottenCleanupUnverified && job.Removal != nil
	})
	// The durable row, not just the projection, must still say unverified.
	var status contract.JobState
	if err := h.store.db.QueryRow(`SELECT status FROM service_removals WHERE job_id=?`,
		computer.CurrentJobID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	var state contract.JobState
	if err := h.store.db.QueryRow(`SELECT state FROM jobs WHERE job_id=?`, computer.CurrentJobID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if status != contract.JobForgottenCleanupUnverified || state != contract.JobForgottenCleanupUnverified {
		t.Fatalf("finalization upgraded the waiver: service_removals.status=%q jobs.state=%q", status, state)
	}
	// The Computer's Storage-custody outcome is a separate claim about secret
	// absence and is still earned by the same positive acknowledgement, exactly
	// as it is for a removal that was declared stalled.
	var custody string
	if err := h.store.db.QueryRow(`SELECT removal_outcome FROM computers WHERE computer_id=?`,
		computer.ComputerID).Scan(&custody); err != nil {
		t.Fatal(err)
	}
	if custody != "removed_verified" {
		t.Fatalf("Computer custody outcome = %q, want the acknowledgement to still earn removed_verified", custody)
	}
	if finalized.Removal == nil {
		t.Fatalf("finalized waived Computer has no removal projection: %#v", finalized)
	}
	return finalized
}

// waiveComputerRemoval writes the waiver an operator waiver for Computers will
// write. It asserts its own fixture: a removal that is not actually waived, or
// one that already carries the Computer branch's finalized marker, would let
// the test pass without exercising the guard at all.
func waiveComputerRemoval(t *testing.T, store *Store, jobID string) {
	t.Helper()
	if _, err := store.db.Exec(`UPDATE service_removals SET status=? WHERE job_id=?`,
		contract.JobForgottenCleanupUnverified, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE jobs SET state=? WHERE job_id=?`,
		contract.JobForgottenCleanupUnverified, jobID); err != nil {
		t.Fatal(err)
	}
	var status contract.JobState
	var removedNS sql.NullInt64
	if err := store.db.QueryRow(`SELECT status, removed_ns FROM service_removals WHERE job_id=?`, jobID).
		Scan(&status, &removedNS); err != nil {
		t.Fatal(err)
	}
	if status != contract.JobForgottenCleanupUnverified {
		t.Fatalf("waiver fixture left status=%q; the guard would never be reached", status)
	}
	if removedNS.Valid {
		t.Fatalf("waiver fixture set removed_ns=%d; finalization would stop at the already-finalized guard",
			removedNS.Int64)
	}
	var projections int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM computer_job_projections WHERE job_id=?`, jobID).
		Scan(&projections); err != nil {
		t.Fatal(err)
	}
	if projections != 1 {
		t.Fatalf("waiver fixture job has %d Computer projections; finalization would take the ordinary branch",
			projections)
	}
}

// finalizeWaivedOrdinaryRemoval is the same story through the operator path
// that exists today, so the two branches can be compared on one run.
func finalizeWaivedOrdinaryRemoval(t *testing.T) Job {
	t.Helper()
	harness := newRemovalStallHarness(t)
	waived, err := harness.h.store.ForceForgetService(context.Background(), harness.job.JobID)
	if err != nil || waived.State != contract.JobForgottenCleanupUnverified {
		t.Fatalf("force forget = %#v, %v", waived, err)
	}
	completion := RemovalAcknowledgementRequest{
		NodeID: harness.node.NodeID, BootSessionID: harness.node.BootSessionID,
		RemovalGeneration: harness.directive.RemovalGeneration, CleanupFence: harness.directive.CleanupFence,
		RootInstanceID: harness.directive.RootInstanceID, IdempotencyKey: "removal:ordinary-waived-late",
	}
	acknowledged, err := harness.declare(t, completion)
	if err != nil {
		t.Fatalf("late cleanup acknowledgement on a waived service: %v", err)
	}
	if acknowledged.State != contract.JobForgottenCleanupUnverified {
		t.Fatalf("acknowledgement state = %q, want the waiver to stand", acknowledged.State)
	}
	finalized := finalizeOrObserveRemoval(t, harness.h.store, harness.job.JobID, func(job Job) bool {
		return job.State == contract.JobForgottenCleanupUnverified && job.Removal != nil
	})
	var outcome string
	if err := harness.h.store.db.QueryRow(`SELECT outcome FROM service_tombstones WHERE job_id=?`,
		harness.job.JobID).Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if ServiceRemovalOutcome(outcome) != ServiceRemovalForgotten {
		t.Fatalf("tombstone outcome = %q, want force_forgotten", outcome)
	}
	if finalized.Removal == nil {
		t.Fatalf("finalized waived service has no removal projection: %#v", finalized)
	}
	return finalized
}
