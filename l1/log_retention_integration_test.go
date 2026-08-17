package l1

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestServiceLogByteRetentionAndDerivedJSONL(t *testing.T) {
	assertServiceLogByteRetentionAndDerivedJSONL(t)
}

func assertServiceLogByteRetentionAndDerivedJSONL(t *testing.T) {
	t.Helper()
	h := newIntegrationHarnessWithOptions(t, StoreOptions{ServiceLogRetentionBytes: 10}, map[string]NodePolicy{
		"service-node": DefaultNodePolicy("service"),
	})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "service-node")
	job := submitRestartService(t, h, client, "service-byte-retention", []string{"service"}, nil)
	claim := claimRestartService(t, h, agent, node)
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", job.JobID, claim.Lease.AttemptID)

	appendRetentionLogs(t, h, agent, path, claim.Lease.FencingToken, []contract.LogEvent{
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 0, []byte("aaaaaa")),
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 1, []byte("bbbbbb")),
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 2, []byte("cccccc")),
	})
	assertRetainedRawBytes(t, h, job.JobID, 6)
	appendRetentionLogs(t, h, agent, path, claim.Lease.FencingToken, []contract.LogEvent{
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 3, []byte("dddddd")),
	})
	assertRetainedRawBytes(t, h, job.JobID, 6)

	page := getRetentionPage(t, h, client, job.JobID)
	if len(page.Events) != 1 || page.Events[0].Sequence != 3 || string(page.Events[0].Bytes) != "dddddd" {
		t.Fatalf("retained service events = %#v, want only sequence 3", page.Events)
	}
	if page.Truncation == nil || page.Truncation.BoundKind != ServiceLogRetentionBytes ||
		page.Truncation.EvictedEventCount != 3 || page.Truncation.EvictedByteCount != 18 {
		t.Fatalf("aggregate byte truncation = %#v", page.Truncation)
	}
	if page.Truncation.EarliestRetainedAt == nil || !page.Truncation.EarliestRetainedAt.Equal(page.Events[0].Timestamp) {
		t.Fatalf("earliest retained timestamp = %v, want %s", page.Truncation.EarliestRetainedAt, page.Events[0].Timestamp)
	}
	var markerRows int
	if err := h.store.db.QueryRow("SELECT COUNT(*) FROM service_log_truncations WHERE job_id=?", job.JobID).Scan(&markerRows); err != nil {
		t.Fatal(err)
	}
	if markerRows != 1 {
		t.Fatalf("aggregate truncation marker rows = %d, want 1", markerRows)
	}
	if got := getRestartService(t, h, job.JobID).State; got != contract.JobRunning {
		t.Fatalf("service state after byte eviction = %q, want running", got)
	}

	raw, err := h.store.RawJobLogJSONL(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(page.Events[0])
	if err != nil {
		t.Fatal(err)
	}
	wantRaw := append(encoded, '\n')
	if !bytes.Equal(raw, wantRaw) {
		t.Fatalf("derived JSONL = %s, want %s", raw, wantRaw)
	}
	var legacyBlobBytes int
	if err := h.store.db.QueryRow("SELECT LENGTH(jsonl) FROM job_log_jsonl WHERE job_id=?", job.JobID).Scan(&legacyBlobBytes); err != nil {
		t.Fatal(err)
	}
	if legacyBlobBytes != 0 {
		t.Fatalf("independent JSONL blob grew to %d bytes", legacyBlobBytes)
	}

	oneshot := h.submit(client, "one-shot-byte-exemption", nil)
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{
		NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: contract.JobClassOneShot,
	})
	if status != http.StatusOK {
		t.Fatalf("one-shot claim status = %d body=%s", status, body)
	}
	var oneshotClaim Claim
	if err := json.Unmarshal(body, &oneshotClaim); err != nil {
		t.Fatal(err)
	}
	if oneshotClaim.Job.JobID != oneshot.JobID {
		t.Fatalf("one-shot claim job = %q, want %q", oneshotClaim.Job.JobID, oneshot.JobID)
	}
	oneshotPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", oneshot.JobID, oneshotClaim.Lease.AttemptID)
	oneshotEvents := []contract.LogEvent{
		logEvent(oneshotClaim.Lease.AttemptID, contract.LogStdout, 0, []byte("111111")),
		logEvent(oneshotClaim.Lease.AttemptID, contract.LogStdout, 1, []byte("222222")),
		logEvent(oneshotClaim.Lease.AttemptID, contract.LogStdout, 2, []byte("333333")),
	}
	appendRetentionLogs(t, h, agent, oneshotPath, oneshotClaim.Lease.FencingToken, oneshotEvents)
	assertRetainedRawBytes(t, h, oneshot.JobID, 18)
	oneshotPage, err := h.store.GetJobLogs(context.Background(), oneshot.JobID, "", MaxLogPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(oneshotPage.Events) != 3 || oneshotPage.Truncation != nil {
		t.Fatalf("one-shot retention changed = %#v", oneshotPage)
	}
}

func TestServiceLogWatermarksPreserveStrictPerStreamContinuity(t *testing.T) {
	assertServiceLogWatermarksPreserveStrictPerStreamContinuity(t)
}

func assertServiceLogWatermarksPreserveStrictPerStreamContinuity(t *testing.T) {
	t.Helper()
	h := newIntegrationHarnessWithOptions(t, StoreOptions{ServiceLogRetentionBytes: 6}, map[string]NodePolicy{
		"service-node": DefaultNodePolicy("service"),
	})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "service-node")
	job := submitRestartService(t, h, client, "service-stream-watermarks", []string{"service"}, nil)
	claim := claimRestartService(t, h, agent, node)
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", job.JobID, claim.Lease.AttemptID)

	appendRetentionLogs(t, h, agent, path, claim.Lease.FencingToken, []contract.LogEvent{
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 0, []byte("aaa")),
		logEvent(claim.Lease.AttemptID, contract.LogStderr, 0, []byte("bbb")),
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 1, []byte("ccc")),
	})
	appendRetentionLogs(t, h, agent, path, claim.Lease.FencingToken, []contract.LogEvent{
		logEvent(claim.Lease.AttemptID, contract.LogStderr, 1, []byte("ddd")),
	})

	page := getRetentionPage(t, h, client, job.JobID)
	if len(page.Events) != 2 || page.Events[0].Stream != contract.LogStdout || page.Events[0].Sequence != 1 ||
		page.Events[1].Stream != contract.LogStderr || page.Events[1].Sequence != 1 {
		t.Fatalf("retained per-stream watermarks = %#v", page.Events)
	}
	assertRetainedRawBytes(t, h, job.JobID, 6)
}

func TestServiceLogAgeRetentionRunsFromReconcile(t *testing.T) {
	assertServiceLogAgeRetentionRunsFromReconcile(t)
}

func assertServiceLogAgeRetentionRunsFromReconcile(t *testing.T) {
	t.Helper()
	h := newIntegrationHarnessWithOptions(t, StoreOptions{
		ServiceLogRetentionBytes: 1 << 20,
		ServiceLogRetentionAge:   time.Hour,
	}, map[string]NodePolicy{"service-node": DefaultNodePolicy("service")})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "service-node")
	job := submitRestartService(t, h, client, "service-age-retention", []string{"service"}, nil)
	claim := claimRestartService(t, h, agent, node)
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", job.JobID, claim.Lease.AttemptID)
	events := []contract.LogEvent{
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 0, []byte("old-a")),
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 1, []byte("old-b")),
	}
	events[0].Timestamp = h.clock.Now().Add(-2 * time.Hour)
	events[1].Timestamp = h.clock.Now().Add(-90 * time.Minute)
	appendRetentionLogs(t, h, agent, path, claim.Lease.FencingToken, events)
	assertRetainedRawBytes(t, h, job.JobID, 10) // ingest enforces bytes only

	result, err := h.store.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.EvictedLogEvents != 1 || result.EvictedLogBytes != 5 {
		t.Fatalf("active age sweep result = %#v, want one evicted event and one retained watermark", result)
	}
	if got := getRestartService(t, h, job.JobID).State; got != contract.JobRunning {
		t.Fatalf("service state after active age eviction = %q, want running", got)
	}

	exitCode := 1
	completionPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
	status, _, body := h.do(agent, http.MethodPost, completionPath, CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "finish-age-retention", Result: ProcessResult{ExitCode: &exitCode},
	})
	if status != http.StatusOK {
		t.Fatalf("completion status = %d body=%s", status, body)
	}
	h.clock.Advance(2 * time.Hour)
	waitForRetainedEventCount(t, h, job.JobID, 0)
	page := getRetentionPage(t, h, client, job.JobID)
	if len(page.Events) != 0 || page.Truncation == nil || page.Truncation.BoundKind != ServiceLogRetentionAge ||
		page.Truncation.EvictedEventCount != 2 || page.Truncation.EvictedByteCount != 10 || page.Truncation.EarliestRetainedAt != nil {
		t.Fatalf("final age retention page = %#v", page)
	}
	if got := getRestartService(t, h, job.JobID).State; got != contract.JobQueued {
		t.Fatalf("age eviction changed service state to %q, want queued restart", got)
	}
}

func TestServiceAttemptSummariesAreBounded(t *testing.T) {
	assertServiceAttemptSummariesAreBounded(t)
}

func assertServiceAttemptSummariesAreBounded(t *testing.T) {
	t.Helper()
	h := newIntegrationHarnessWithOptions(t, StoreOptions{}, map[string]NodePolicy{
		"service-node": DefaultNodePolicy("service"),
	})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "service-node")
	job := submitRestartService(t, h, client, "service-attempt-summary-bound", []string{"service"}, nil)
	claim := claimRestartService(t, h, agent, node)
	if _, err := h.store.db.Exec("UPDATE service_jobs SET lifetime_restart_count=87 WHERE job_id=?", job.JobID); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 35; index++ {
		created := h.clock.Now().Add(time.Duration(index-35) * time.Minute).UnixNano()
		_, err := h.store.db.Exec(`INSERT INTO attempts(
			attempt_id, job_id, node_id, boot_session_id, state, fencing_token,
			lease_expires_ns, authority_generation, created_ns, updated_ns
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, fmt.Sprintf("summary-%02d", index), job.JobID,
			node.NodeID, node.BootSessionID, contract.AttemptFailed, fmt.Sprintf("summary-fence-%02d", index),
			created, node.AuthorityGeneration, created, created)
		if err != nil {
			t.Fatal(err)
		}
	}
	result, err := h.store.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.PrunedAttempts != 3 {
		t.Fatalf("pruned attempt summaries = %d, want 3", result.PrunedAttempts)
	}
	var count int
	if err := h.store.db.QueryRow("SELECT COUNT(*) FROM attempts WHERE job_id=?", job.JobID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1+DefaultServiceAttemptSummaries {
		t.Fatalf("retained attempt summaries = %d, want current + %d", count, DefaultServiceAttemptSummaries)
	}
	for _, attemptID := range []string{"summary-00", "summary-01", "summary-02"} {
		var found int
		if err := h.store.db.QueryRow("SELECT COUNT(*) FROM attempts WHERE attempt_id=?", attemptID).Scan(&found); err != nil {
			t.Fatal(err)
		}
		if found != 0 {
			t.Fatalf("old attempt summary %q was not pruned", attemptID)
		}
	}
	stored := getRestartService(t, h, job.JobID)
	if stored.CurrentAttemptID != claim.Lease.AttemptID || stored.LifetimeRestartCount != 87 {
		t.Fatalf("bounded restart evidence lost current/lifetime facts: %#v", stored)
	}
}

func TestAttemptSummaryPruningNeverCascadesRetainedLogs(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{}, map[string]NodePolicy{
		"service-node": DefaultNodePolicy("service"),
	})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "service-node")
	job := submitRestartService(t, h, client, "service-attempt-log-floor", []string{"service"}, nil)
	_ = claimRestartService(t, h, agent, node)
	created := h.clock.Now().Add(-time.Hour).UnixNano()
	if _, err := h.store.db.Exec(`INSERT INTO attempts(
		attempt_id, job_id, node_id, boot_session_id, state, fencing_token,
		lease_expires_ns, authority_generation, created_ns, updated_ns
	) VALUES('retained-old-attempt', ?, ?, ?, ?, 'old-fence', ?, ?, ?, ?)`, job.JobID, node.NodeID,
		node.BootSessionID, contract.AttemptFailed, created, node.AuthorityGeneration, created, created); err != nil {
		t.Fatal(err)
	}
	event := logEvent("retained-old-attempt", contract.LogStdout, 0, []byte("retained"))
	eventJSON, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`INSERT INTO log_events(job_id, attempt_id, stream, sequence, sequence_end, timestamp_ns, bytes, event_json)
		VALUES(?, 'retained-old-attempt', ?, 0, 0, ?, ?, ?)`, job.JobID, contract.LogStdout, h.clock.Now().UnixNano(), event.Bytes, eventJSON); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < DefaultServiceAttemptSummaries+1; index++ {
		stamp := h.clock.Now().Add(time.Duration(index) * time.Second).UnixNano()
		if _, err := h.store.db.Exec(`INSERT INTO attempts(
			attempt_id, job_id, node_id, boot_session_id, state, fencing_token,
			lease_expires_ns, authority_generation, created_ns, updated_ns
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, fmt.Sprintf("newer-empty-%02d", index), job.JobID, node.NodeID,
			node.BootSessionID, contract.AttemptFailed, fmt.Sprintf("newer-fence-%02d", index), stamp,
			node.AuthorityGeneration, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.store.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	var retainedAttempt, retainedEvent int
	if err := h.store.db.QueryRow("SELECT COUNT(*) FROM attempts WHERE attempt_id='retained-old-attempt'").Scan(&retainedAttempt); err != nil {
		t.Fatal(err)
	}
	if err := h.store.db.QueryRow("SELECT COUNT(*) FROM log_events WHERE attempt_id='retained-old-attempt'").Scan(&retainedEvent); err != nil {
		t.Fatal(err)
	}
	if retainedAttempt != 1 || retainedEvent != 1 {
		t.Fatalf("attempt/log retained after summary pruning = %d/%d, want 1/1", retainedAttempt, retainedEvent)
	}
}

func appendRetentionLogs(t *testing.T, h *integrationHarness, agent *http.Client, path, fence string, events []contract.LogEvent) {
	t.Helper()
	status, _, body := h.do(agent, http.MethodPost, path, AppendLogsRequest{FencingToken: fence, Events: events})
	if status != http.StatusOK {
		t.Fatalf("append retained logs status = %d body=%s", status, body)
	}
}

func getRetentionPage(t *testing.T, h *integrationHarness, client *http.Client, jobID string) LogPage {
	t.Helper()
	status, _, body := h.do(client, http.MethodGet, "/v1/jobs/"+jobID+"/logs?class=service&limit=1000", nil)
	if status != http.StatusOK {
		t.Fatalf("get retained logs status = %d body=%s", status, body)
	}
	var page LogPage
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	return page
}

func assertRetainedRawBytes(t *testing.T, h *integrationHarness, jobID string, want int64) {
	t.Helper()
	var got sql.NullInt64
	if err := h.store.db.QueryRow("SELECT SUM(LENGTH(bytes)) FROM log_events WHERE job_id=?", jobID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	value := int64(0)
	if got.Valid {
		value = got.Int64
	}
	if value != want {
		t.Fatalf("retained raw bytes = %d, want %d", value, want)
	}
}

func waitForRetainedEventCount(t *testing.T, h *integrationHarness, jobID string, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var got int
		if err := h.store.db.QueryRow("SELECT COUNT(*) FROM log_events WHERE job_id=?", jobID).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("retained event count = %d, want %d before periodic sweep deadline", got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
