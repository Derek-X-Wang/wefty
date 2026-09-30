package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// One-shot logs were kept forever (#52). They now carry the per-job byte cap
// at ingest, exactly as a service's do, and a trimmed job says so.
func TestOneshotLogByteRetentionAtAppendCarriesMarker(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{OneshotLogRetentionBytes: 10}, map[string]NodePolicy{
		"worker": DefaultNodePolicy("worker"),
	})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "worker")
	job := h.submit(client, "oneshot-byte-retention", nil)
	claim := claimOneshot(t, h, agent, node, job.JobID)
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", job.JobID, claim.Lease.AttemptID)

	appendRetentionLogs(t, h, agent, path, claim.Lease.FencingToken, []contract.LogEvent{
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 0, []byte("aaaaaa")),
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 1, []byte("bbbbbb")),
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 2, []byte("cccccc")),
	})
	// Enforced in the append transaction, before any sweep could run.
	assertRetainedRawBytes(t, h, job.JobID, 6)
	assertUsageCountersMatchLogEvents(t, h.store)

	page := getJobLogPage(t, h, client, job.JobID)
	if len(page.Events) != 1 || page.Events[0].Sequence != 2 {
		t.Fatalf("retained one-shot events = %#v, want only sequence 2", page.Events)
	}
	if page.Truncation == nil || page.Truncation.BoundKind != LogRetentionBytes ||
		page.Truncation.EvictedEventCount != 2 || page.Truncation.EvictedByteCount != 12 {
		t.Fatalf("one-shot byte truncation marker = %#v", page.Truncation)
	}
	if page.Truncation.EarliestRetainedAt == nil || !page.Truncation.EarliestRetainedAt.Equal(page.Events[0].Timestamp) {
		t.Fatalf("earliest retained = %v, want %s", page.Truncation.EarliestRetainedAt, page.Events[0].Timestamp)
	}
}

// Age is measured by each event's own timestamp and applied by the sweep. The
// newest row per stream of a live attempt survives until the attempt ends.
func TestOneshotLogAgeRetentionRunsFromReconcile(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{OneshotLogRetentionAge: time.Hour}, map[string]NodePolicy{
		"worker": DefaultNodePolicy("worker"),
	})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "worker")
	job := h.submit(client, "oneshot-age-retention", nil)
	claim := claimOneshot(t, h, agent, node, job.JobID)
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", job.JobID, claim.Lease.AttemptID)
	now := h.clock.Now()
	events := []contract.LogEvent{
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 0, []byte("old-a")),
		logEvent(claim.Lease.AttemptID, contract.LogStderr, 0, []byte("old-err")),
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 1, []byte("old-b")),
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 2, []byte("fresh")),
	}
	events[0].Timestamp = now.Add(-3 * time.Hour)
	events[1].Timestamp = now.Add(-150 * time.Minute)
	events[2].Timestamp = now.Add(-2 * time.Hour)
	events[3].Timestamp = now.Add(-time.Minute)
	appendRetentionLogs(t, h, agent, path, claim.Lease.FencingToken, events)
	assertRetainedRawBytes(t, h, job.JobID, 22) // ingest enforces bytes only

	// A service's logs of the same age are under the service's own 7-day
	// bound, not the one-shot bound.
	serviceJob := submitRestartService(t, h, client, "service-beside-oneshot-age", nil, nil)
	serviceClaim := claimRestartService(t, h, agent, node)
	servicePath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", serviceJob.JobID, serviceClaim.Lease.AttemptID)
	serviceEvents := []contract.LogEvent{
		logEvent(serviceClaim.Lease.AttemptID, contract.LogStdout, 0, []byte("svc-old")),
		logEvent(serviceClaim.Lease.AttemptID, contract.LogStdout, 1, []byte("svc-new")),
	}
	serviceEvents[0].Timestamp = now.Add(-3 * time.Hour)
	serviceEvents[1].Timestamp = now.Add(-time.Minute)
	appendRetentionLogs(t, h, agent, servicePath, serviceClaim.Lease.FencingToken, serviceEvents)

	if _, err := h.store.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	page := getJobLogPage(t, h, client, job.JobID)
	if len(page.Events) != 1 || string(page.Events[0].Bytes) != "fresh" {
		t.Fatalf("one-shot events after live age sweep = %#v, want only the fresh stdout event", page.Events)
	}
	if page.Truncation == nil || page.Truncation.BoundKind != LogRetentionAge ||
		page.Truncation.EvictedEventCount != 3 || page.Truncation.EvictedByteCount != 17 {
		t.Fatalf("one-shot age truncation marker = %#v", page.Truncation)
	}
	if page.Truncation.EarliestRetainedAt == nil || !page.Truncation.EarliestRetainedAt.Equal(events[3].Timestamp) {
		t.Fatalf("earliest retained = %v, want %s", page.Truncation.EarliestRetainedAt, events[3].Timestamp)
	}
	// The live stderr stream lost its only row, and still continues at 1.
	nextErr := logEvent(claim.Lease.AttemptID, contract.LogStderr, 1, []byte("err-1"))
	nextErr.Timestamp = now
	appendRetentionLogs(t, h, agent, path, claim.Lease.FencingToken, []contract.LogEvent{nextErr})
	assertRetainedRawBytes(t, h, serviceJob.JobID, 14)

	exitCode := 0
	completionPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
	status, _, body := h.do(agent, http.MethodPost, completionPath, CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "finish-oneshot-age", Result: ProcessResult{ExitCode: &exitCode},
	})
	if status != http.StatusOK {
		t.Fatalf("completion status = %d body=%s", status, body)
	}
	if _, err := h.store.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	page = getJobLogPage(t, h, client, job.JobID)
	if len(page.Events) != 2 || string(page.Events[0].Bytes) != "fresh" || page.Truncation.EvictedEventCount != 3 {
		t.Fatalf("terminal one-shot after age sweep = %#v, truncation %#v", page.Events, page.Truncation)
	}
	if got, err := h.store.GetJob(context.Background(), job.JobID); err != nil || got.State != contract.JobSucceeded {
		t.Fatalf("age eviction changed the job record: %#v, %v", got, err)
	}
	h.clock.Advance(2 * time.Hour)
	if _, err := h.store.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	page = getJobLogPage(t, h, client, job.JobID)
	if len(page.Events) != 0 || page.Truncation == nil || page.Truncation.EvictedEventCount != 5 ||
		page.Truncation.EvictedByteCount != 27 || page.Truncation.EarliestRetainedAt != nil {
		t.Fatalf("a fully aged-out one-shot must still say it was trimmed: %#v", page)
	}
	assertUsageCountersMatchLogEvents(t, h.store)
}

// The cluster-wide ceiling spans one-shots and services and evicts oldest
// first by event time, whichever job the event belongs to.
func TestLogRetentionTotalCeilingEvictsOldestAcrossJobs(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LogRetentionTotalBytes: 20}, map[string]NodePolicy{
		"worker": DefaultNodePolicy("worker"),
	})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "worker")
	now := h.clock.Now()
	at := func(event contract.LogEvent, age time.Duration) contract.LogEvent {
		event.Timestamp = now.Add(-age)
		return event
	}

	first := h.submit(client, "oneshot-total-a", nil)
	firstClaim := claimOneshot(t, h, agent, node, first.JobID)
	appendRetentionLogs(t, h, agent, fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", first.JobID, firstClaim.Lease.AttemptID),
		firstClaim.Lease.FencingToken, []contract.LogEvent{
			at(logEvent(firstClaim.Lease.AttemptID, contract.LogStdout, 0, []byte("a0a0a0")), 50*time.Minute),
			at(logEvent(firstClaim.Lease.AttemptID, contract.LogStdout, 1, []byte("a1a1a1")), 20*time.Minute),
			at(logEvent(firstClaim.Lease.AttemptID, contract.LogStdout, 2, []byte("a2a2a2")), 2*time.Minute),
		})
	second := h.submit(client, "oneshot-total-b", nil)
	secondClaim := claimOneshot(t, h, agent, node, second.JobID)
	appendRetentionLogs(t, h, agent, fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", second.JobID, secondClaim.Lease.AttemptID),
		secondClaim.Lease.FencingToken, []contract.LogEvent{
			at(logEvent(secondClaim.Lease.AttemptID, contract.LogStdout, 0, []byte("b0b0b0")), 40*time.Minute),
			at(logEvent(secondClaim.Lease.AttemptID, contract.LogStdout, 1, []byte("b1b1b1")), 5*time.Minute),
		})
	service := submitRestartService(t, h, client, "service-total", nil, nil)
	serviceClaim := claimRestartService(t, h, agent, node)
	appendRetentionLogs(t, h, agent, fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", service.JobID, serviceClaim.Lease.AttemptID),
		serviceClaim.Lease.FencingToken, []contract.LogEvent{
			at(logEvent(serviceClaim.Lease.AttemptID, contract.LogStdout, 0, []byte("s0s0s0")), 30*time.Minute),
			at(logEvent(serviceClaim.Lease.AttemptID, contract.LogStdout, 1, []byte("s1s1s1")), time.Minute),
		})

	if _, err := h.store.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 42 bytes against a 20-byte ceiling: a0 (-50m), b0 (-40m), s0 (-30m)
	// and a1 (-20m) go, oldest first; b1 (-5m) is younger than a1 and stays.
	assertRetainedPayloads(t, h, first.JobID, "a2a2a2")
	assertRetainedPayloads(t, h, second.JobID, "b1b1b1")
	assertRetainedPayloads(t, h, service.JobID, "s1s1s1")
	for _, want := range []struct {
		jobID          string
		events, nbytes int64
	}{{first.JobID, 2, 12}, {second.JobID, 1, 6}, {service.JobID, 1, 6}} {
		var page LogPage
		if want.jobID == service.JobID {
			page = getRetentionPage(t, h, client, want.jobID)
		} else {
			page = getJobLogPage(t, h, client, want.jobID)
		}
		if page.Truncation == nil || page.Truncation.BoundKind != LogRetentionTotal ||
			page.Truncation.EvictedEventCount != want.events || page.Truncation.EvictedByteCount != want.nbytes {
			t.Fatalf("job %s total-ceiling marker = %#v, want %d events/%d bytes", want.jobID, page.Truncation, want.events, want.nbytes)
		}
	}
	var total int64
	if err := h.store.db.QueryRow("SELECT retained_bytes FROM log_usage_total").Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 18 {
		t.Fatalf("retained total after ceiling sweep = %d, want 18", total)
	}
	assertUsageCountersMatchLogEvents(t, h.store)
}

// Any row may go, even a live attempt's newest per stream: upload continuity
// is a durable record, not the retained rows.
func TestLogRetentionTotalCeilingEvictsAnyRowWithoutBreakingContinuity(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LogRetentionTotalBytes: 1}, map[string]NodePolicy{
		"worker": DefaultNodePolicy("worker"),
	})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "worker")
	job := h.submit(client, "oneshot-total-continuity", nil)
	claim := claimOneshot(t, h, agent, node, job.JobID)
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", job.JobID, claim.Lease.AttemptID)
	batch := []contract.LogEvent{
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 0, []byte("out-0")),
		logEvent(claim.Lease.AttemptID, contract.LogStderr, 0, []byte("err-0")),
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 1, []byte("out-1")),
	}
	appendRetentionLogs(t, h, agent, path, claim.Lease.FencingToken, batch)
	if _, err := h.store.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertRetainedPayloads(t, h, job.JobID)
	response := appendLogsExpectingOK(t, h, agent, path, claim.Lease.FencingToken, batch)
	if response.Acknowledged[contract.LogStdout] != 1 || response.Acknowledged[contract.LogStderr] != 0 {
		t.Fatalf("replay of an evicted batch acknowledged %#v, want stdout 1 and stderr 0", response.Acknowledged)
	}
	appendRetentionLogs(t, h, agent, path, claim.Lease.FencingToken, []contract.LogEvent{
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 2, []byte("out-2")),
		logEvent(claim.Lease.AttemptID, contract.LogStderr, 1, []byte("err-1")),
	})
	assertRetainedPayloads(t, h, job.JobID, "out-2", "err-1")
}

// Review repro (#589): a per-job cap that evicts inside the append
// transaction must not turn an identical retry of that batch, whose response
// was lost, into a conflict. Both classes carry the cap at ingest.
func TestAppendEvictionKeepsIdenticalRetryIdempotent(t *testing.T) {
	for _, class := range []string{contract.JobClassOneShot, contract.JobClassService} {
		t.Run(class, func(t *testing.T) {
			h := newIntegrationHarnessWithOptions(t, StoreOptions{
				OneshotLogRetentionBytes: 10, ServiceLogRetentionBytes: 10,
			}, map[string]NodePolicy{"worker": DefaultNodePolicy("worker")})
			client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
			agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
			node := h.register(agent, "worker")
			var jobID string
			var claim Claim
			if class == contract.JobClassService {
				jobID = submitRestartService(t, h, client, "retry-after-append-eviction", nil, nil).JobID
				claim = claimRestartService(t, h, agent, node)
			} else {
				jobID = h.submit(client, "retry-after-append-eviction", nil).JobID
				claim = claimOneshot(t, h, agent, node, jobID)
			}
			path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", jobID, claim.Lease.AttemptID)
			batch := []contract.LogEvent{
				logEvent(claim.Lease.AttemptID, contract.LogStdout, 0, []byte("aaaaaa")),
				logEvent(claim.Lease.AttemptID, contract.LogStdout, 1, []byte("bbbbbb")),
				logEvent(claim.Lease.AttemptID, contract.LogStdout, 2, []byte("cccccc")),
			}
			appendRetentionLogs(t, h, agent, path, claim.Lease.FencingToken, batch)
			assertRetainedPayloads(t, h, jobID, "cccccc")
			response := appendLogsExpectingOK(t, h, agent, path, claim.Lease.FencingToken, batch)
			if response.Acknowledged[contract.LogStdout] != 2 {
				t.Fatalf("identical retry acknowledged %#v, want stdout 2", response.Acknowledged)
			}
			// A retained row is still compared: the same sequence with other
			// bytes is refused.
			status, _, body := h.do(agent, http.MethodPost, path, AppendLogsRequest{
				FencingToken: claim.Lease.FencingToken,
				Events:       []contract.LogEvent{logEvent(claim.Lease.AttemptID, contract.LogStdout, 2, []byte("zzzzzz"))},
			})
			if status != http.StatusConflict {
				t.Fatalf("conflicting replay of a retained event status = %d body=%s", status, body)
			}
			assertRetainedPayloads(t, h, jobID, "cccccc")
		})
	}
}

// Review repro (#589): a lost attempt keeps its late-evidence window. Evicting
// its last retained row must not reset the stream, or its next valid upload
// inside the window is refused.
func TestLostAttemptLateEvidenceSurvivesEvictedRows(t *testing.T) {
	for _, class := range []string{contract.JobClassOneShot, contract.JobClassService} {
		t.Run(class, func(t *testing.T) {
			h := newIntegrationHarnessWithOptions(t, StoreOptions{LogRetentionTotalBytes: 1}, map[string]NodePolicy{
				"worker": DefaultNodePolicy("worker"),
			})
			client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
			agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
			node := h.register(agent, "worker")
			var jobID string
			var claim Claim
			if class == contract.JobClassService {
				jobID = submitRestartService(t, h, client, "late-evidence-after-eviction", nil, nil).JobID
				claim = claimRestartService(t, h, agent, node)
			} else {
				jobID = h.submit(client, "late-evidence-after-eviction", nil).JobID
				claim = claimOneshot(t, h, agent, node, jobID)
			}
			path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", jobID, claim.Lease.AttemptID)
			appendRetentionLogs(t, h, agent, path, claim.Lease.FencingToken, []contract.LogEvent{
				logEvent(claim.Lease.AttemptID, contract.LogStdout, 0, []byte("before-loss")),
			})
			h.clock.Advance(time.Minute) // past the 30s lease: the attempt is lost
			if _, err := h.store.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			assertAttemptState(t, h, claim.Lease.AttemptID, contract.AttemptLost)
			assertRetainedPayloads(t, h, jobID)
			late := logEvent(claim.Lease.AttemptID, contract.LogStdout, 1, []byte("late"))
			late.Timestamp = h.clock.Now()
			response := appendLogsExpectingOK(t, h, agent, path, claim.Lease.FencingToken, []contract.LogEvent{late})
			if response.Acknowledged[contract.LogStdout] != 1 || response.AttemptState != contract.AttemptLost {
				t.Fatalf("late upload after eviction = %#v", response)
			}
		})
	}
}

// Review P2 (#589): a backlog far past the cap (an upgrade, a lowered cap) is
// worked off in bounded pieces. The append transaction evicts at most its own
// bound, each sweep pass at most the sweep budget, and a renewal issued while
// a pass runs completes.
func TestRetentionBacklogIsEvictedInBoundedPasses(t *testing.T) {
	// The background loop stays out of the way so each count is one
	// transaction's; the passes below are driven explicitly.
	h := newIntegrationHarnessWithReconcileInterval(t, StoreOptions{OneshotLogRetentionBytes: 10}, map[string]NodePolicy{
		"worker": DefaultNodePolicy("worker"),
	}, true, time.Hour)
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "worker")
	job := h.submit(client, "retention-backlog", nil)
	claim := claimOneshot(t, h, agent, node, job.JobID)
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", job.JobID, claim.Lease.AttemptID)
	appendRetentionLogs(t, h, agent, path, claim.Lease.FencingToken, []contract.LogEvent{
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 0, []byte("x")),
	})

	// 10,000 rows accepted under a looser cap, as an upgraded database has.
	const backlog = 10_000
	tx, err := h.store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for sequence := 1; sequence <= backlog; sequence++ {
		if _, err := tx.Exec(`INSERT INTO log_events(job_id, attempt_id, stream, sequence, sequence_end, timestamp_ns, bytes, event_json)
			VALUES(?, ?, 'stdout', ?, ?, ?, X'78', X'7B7D')`, job.JobID, claim.Lease.AttemptID, sequence, sequence,
			h.clock.Now().UnixNano()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(`UPDATE log_stream_continuity SET accepted_through=? WHERE attempt_id=? AND stream='stdout'`,
		backlog, claim.Lease.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	retained := func() int {
		var count int
		if err := h.store.db.QueryRow("SELECT COUNT(*) FROM log_events WHERE job_id=?", job.JobID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}

	before := retained()
	appendRetentionLogs(t, h, agent, path, claim.Lease.FencingToken, []contract.LogEvent{
		logEvent(claim.Lease.AttemptID, contract.LogStdout, backlog+1, []byte("y")),
	})
	if evicted := before + 1 - retained(); evicted != appendEvictionEvents {
		t.Fatalf("append transaction evicted %d rows, want exactly its bound %d", evicted, appendEvictionEvents)
	}

	renewPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/lease", job.JobID, claim.Lease.AttemptID)
	passes := 0
	for retained() > 10 {
		passes++
		if passes > 10 {
			t.Fatalf("backlog not drained after %d passes: %d rows retained", passes-1, retained())
		}
		renewed := make(chan int, 1)
		go func() {
			status, _, _ := h.do(agent, http.MethodPost, renewPath, RenewalRequest{FencingToken: claim.Lease.FencingToken})
			renewed <- status
		}()
		result, err := h.store.Reconcile(context.Background())
		select {
		case status := <-renewed:
			if status != http.StatusOK {
				t.Fatalf("renewal during pass %d status = %d", passes, status)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("renewal during pass %d did not complete", passes)
		}
		if err != nil {
			t.Fatal(err)
		}
		if result.EvictedLogEvents > sweepEvictionEvents {
			t.Fatalf("pass %d evicted %d rows, over the %d-row sweep budget", passes, result.EvictedLogEvents, sweepEvictionEvents)
		}
	}
	if passes < 2 {
		t.Fatalf("a %d-row backlog drained in %d pass, so passes are not bounded", backlog, passes)
	}
	assertUsageCountersMatchLogEvents(t, h.store)
	// Continuity survived the whole backlog's eviction.
	appendRetentionLogs(t, h, agent, path, claim.Lease.FencingToken, []contract.LogEvent{
		logEvent(claim.Lease.AttemptID, contract.LogStdout, backlog+2, []byte("z")),
	})
}

// A database created by the #49 schema carries its service markers into the
// job-generic table and gets exact usage counters on first open.
func TestStoreUpgradesServiceLogTruncationsAndSeedsUsage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pre-52.sqlite")
	store, err := OpenStore(path, StoreOptions{Clock: &fakeClock{now: time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)}})
	if err != nil {
		t.Fatal(err)
	}
	spec := validJobSpec("pre-52-service", nil)
	spec.Class = contract.JobClassService
	spec.Execution.HandoffDirectory = ""
	spec.Restart = contract.RestartAlways
	service, err := submitDirect(store, spec)
	if err != nil {
		t.Fatal(err)
	}
	oneshot, err := submitDirect(store, validJobSpec("pre-52-oneshot", nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Rewind the file to #49's shape: no generic marker, no counters, no
	// triggers, and a service-only marker table holding one row.
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.Exec(`PRAGMA foreign_keys=OFF;
		DROP TRIGGER log_events_usage_insert;
		DROP TRIGGER log_events_usage_delete;
		DROP TRIGGER log_events_usage_update;
		DROP TABLE job_log_truncations;
		DROP TABLE job_log_usage;
		DROP TABLE log_usage_total;
		DROP INDEX log_events_age_order;
		DROP TABLE log_stream_continuity;
		DROP TABLE l1_data_migrations;
		CREATE TABLE service_log_truncations (
		  job_id TEXT PRIMARY KEY REFERENCES service_jobs(job_id) ON DELETE CASCADE,
		  bound_kind TEXT NOT NULL CHECK(bound_kind IN ('bytes', 'age')),
		  evicted_event_count INTEGER NOT NULL CHECK(evicted_event_count >= 0),
		  evicted_byte_count INTEGER NOT NULL CHECK(evicted_byte_count >= 0),
		  evicted_through_ordinal INTEGER NOT NULL CHECK(evicted_through_ordinal >= 0),
		  earliest_retained_ns INTEGER,
		  updated_ns INTEGER NOT NULL
		);
		INSERT INTO attempts(attempt_id, job_id, node_id, boot_session_id, state, fencing_token,
			lease_expires_ns, authority_generation, created_ns, updated_ns)
		VALUES('legacy-service-attempt', '` + service.JobID + `', 'n', 'b', 'failed', 'f1', 1, 1, 1, 1),
			('legacy-oneshot-attempt', '` + oneshot.JobID + `', 'n', 'b', 'failed', 'f2', 1, 1, 1, 1);
		INSERT INTO log_events(job_id, attempt_id, stream, sequence, sequence_end, timestamp_ns, bytes, event_json)
		VALUES('` + service.JobID + `', 'legacy-service-attempt', 'stdout', 7, 7, 1000, X'616263', X'7B7D'),
			('` + oneshot.JobID + `', 'legacy-oneshot-attempt', 'stdout', 0, 0, 2000, X'6465666768', X'7B7D');
		INSERT INTO service_log_truncations VALUES('` + service.JobID + `', 'age', 7, 70, 41, 1000, 5000);
		PRAGMA foreign_keys=ON;`)
	closeErr := database.Close()
	if err != nil || closeErr != nil {
		t.Fatal(errors.Join(err, closeErr))
	}

	store, err = OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var legacy int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name='service_log_truncations'`).Scan(&legacy); err != nil {
		t.Fatal(err)
	}
	if legacy != 0 {
		t.Fatal("the service-only marker table survived the upgrade")
	}
	truncation, err := readLogTruncation(context.Background(), store.db, service.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if truncation == nil || truncation.BoundKind != LogRetentionAge || truncation.EvictedEventCount != 7 ||
		truncation.EvictedByteCount != 70 || truncation.EvictedThroughOrdinal != 41 ||
		truncation.EarliestRetainedAt == nil || truncation.EarliestRetainedAt.UnixNano() != 1000 {
		t.Fatalf("migrated service marker = %#v", truncation)
	}
	assertUsageCountersMatchLogEvents(t, store)
	for attemptID, want := range map[string]int64{"legacy-service-attempt": 7, "legacy-oneshot-attempt": 0} {
		through, err := readLogContinuity(context.Background(), store.db, attemptID, contract.LogStdout)
		if err != nil {
			t.Fatal(err)
		}
		if through != want {
			t.Fatalf("seeded continuity for %s = %d, want %d", attemptID, through, want)
		}
	}
	var total int64
	if err := store.db.QueryRow("SELECT retained_bytes FROM log_usage_total").Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 8 {
		t.Fatalf("seeded total = %d, want 8", total)
	}
	// A one-shot can now carry a marker, and the widened bound is admitted.
	if _, err := store.db.Exec(`INSERT INTO job_log_truncations VALUES(?, 'total', 1, 1, 1, NULL, 1)`, oneshot.JobID); err != nil {
		t.Fatalf("upgraded marker table refuses a one-shot total-ceiling marker: %v", err)
	}
	// Counters stay exact through a cascade from the job row.
	if _, err := store.db.Exec(`DELETE FROM jobs WHERE job_id=?`, oneshot.JobID); err != nil {
		t.Fatal(err)
	}
	assertUsageCountersMatchLogEvents(t, store)
	if err := store.db.QueryRow("SELECT retained_bytes FROM log_usage_total").Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 3 {
		t.Fatalf("total after the one-shot's cascade = %d, want 3", total)
	}
}

// Ingest is the mandatory byte site; the sweep re-trims a one-shot left over
// a cap that was lowered across a restart, finding it through the usage
// index rather than by walking every one-shot ever run.
func TestOneshotLogByteRetentionSweepAppliesALoweredCap(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "lowered.sqlite"), StoreOptions{
		Clock:                    &fakeClock{now: time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)},
		OneshotLogRetentionBytes: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	job, err := submitDirect(store, validJobSpec("lowered-cap", nil))
	if err != nil {
		t.Fatal(err)
	}
	// The rows stand in for logs accepted under the earlier, looser cap; the
	// attempt's node is immaterial to retention, so its reference is not kept.
	conn, err := store.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), "PRAGMA foreign_keys=OFF"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(), `INSERT INTO attempts(attempt_id, job_id, node_id, boot_session_id, state, fencing_token,
			lease_expires_ns, authority_generation, created_ns, updated_ns)
		VALUES('earlier-attempt', ?, 'n', 'b', 'succeeded', 'f', 1, 1, 1, 1)`, job.JobID); err != nil {
		t.Fatal(err)
	}
	for sequence, payload := range []string{"aaaaaa", "bbbbbb", "cccccc"} {
		if _, err := conn.ExecContext(context.Background(), `INSERT INTO log_events(job_id, attempt_id, stream, sequence, sequence_end, timestamp_ns, bytes, event_json)
			VALUES(?, 'earlier-attempt', 'stdout', ?, ?, ?, ?, X'7B7D')`, job.JobID, sequence, sequence,
			store.clock.Now().UnixNano(), []byte(payload)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := conn.ExecContext(context.Background(), "PRAGMA foreign_keys=ON"); err != nil {
		t.Fatal(err)
	}
	result, err := store.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.EvictedLogEvents != 2 || result.EvictedLogBytes != 12 {
		t.Fatalf("sweep result = %#v, want two events and 12 bytes evicted", result)
	}
	truncation, err := readLogTruncation(context.Background(), store.db, job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if truncation == nil || truncation.BoundKind != LogRetentionBytes || truncation.EvictedEventCount != 2 {
		t.Fatalf("lowered-cap marker = %#v", truncation)
	}
	assertUsageCountersMatchLogEvents(t, store)
}

func appendLogsExpectingOK(t *testing.T, h *integrationHarness, agent *http.Client, path, fence string, events []contract.LogEvent) AppendLogsResponse {
	t.Helper()
	status, _, body := h.do(agent, http.MethodPost, path, AppendLogsRequest{FencingToken: fence, Events: events})
	if status != http.StatusOK {
		t.Fatalf("append status = %d body=%s", status, body)
	}
	var response AppendLogsResponse
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func submitDirect(store *Store, spec contract.JobSpec) (Job, error) {
	job, _, err := store.CreateJob(context.Background(), spec)
	return job, err
}

func claimOneshot(t *testing.T, h *integrationHarness, agent *http.Client, node Node, wantJobID string) Claim {
	t.Helper()
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{
		NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: contract.JobClassOneShot,
	})
	if status != http.StatusOK {
		t.Fatalf("one-shot claim status = %d body=%s", status, body)
	}
	var claim Claim
	if err := json.Unmarshal(body, &claim); err != nil {
		t.Fatal(err)
	}
	if claim.Job.JobID != wantJobID {
		t.Fatalf("one-shot claim job = %q, want %q", claim.Job.JobID, wantJobID)
	}
	return claim
}

func getJobLogPage(t *testing.T, h *integrationHarness, client *http.Client, jobID string) LogPage {
	t.Helper()
	status, _, body := h.do(client, http.MethodGet, "/v1/jobs/"+jobID+"/logs?limit=1000", nil)
	if status != http.StatusOK {
		t.Fatalf("get job logs status = %d body=%s", status, body)
	}
	var page LogPage
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	return page
}

func assertRetainedPayloads(t *testing.T, h *integrationHarness, jobID string, want ...string) {
	t.Helper()
	rows, err := h.store.db.Query("SELECT bytes FROM log_events WHERE job_id=? ORDER BY ordinal", jobID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			t.Fatal(err)
		}
		got = append(got, string(payload))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("job %s retained payloads = %q, want %q", jobID, got, want)
	}
}

func assertUsageCountersMatchLogEvents(t *testing.T, store *Store) {
	t.Helper()
	var mismatched int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM (
		SELECT jobs.job_id, COALESCE(u.retained_bytes, 0) AS counted,
			(SELECT COALESCE(SUM(LENGTH(bytes)), 0) FROM log_events e WHERE e.job_id=jobs.job_id) AS actual
		FROM jobs LEFT JOIN job_log_usage u ON u.job_id=jobs.job_id
	) WHERE counted<>actual`).Scan(&mismatched); err != nil {
		t.Fatal(err)
	}
	if mismatched != 0 {
		t.Fatalf("%d jobs have a retained-byte counter that disagrees with log_events", mismatched)
	}
	var counted, actual int64
	if err := store.db.QueryRow(`SELECT retained_bytes, (SELECT COALESCE(SUM(LENGTH(bytes)), 0) FROM log_events)
		FROM log_usage_total WHERE singleton=1`).Scan(&counted, &actual); err != nil {
		t.Fatal(err)
	}
	if counted != actual {
		t.Fatalf("total retained-byte counter = %d, log_events hold %d", counted, actual)
	}
}
