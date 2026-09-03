package l1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestTailRunningJobWithOpaqueCursorAndPerStreamOrder(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agentClient := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agentClient, "node-1")
	job := h.submit(client, "dispatch-live-logs", []string{"linux"})
	claim := claimForLogs(t, h, agentClient, job.JobID)

	events := []contract.LogEvent{
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 0, []byte("out-0\n")),
		logEvent(claim.Lease.AttemptID, contract.LogStderr, 0, []byte("err-0\n")),
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 1, []byte("out-1\n")),
	}
	appendPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", job.JobID, claim.Lease.AttemptID)
	status, _, body := h.do(agentClient, http.MethodPost, appendPath, AppendLogsRequest{FencingToken: claim.Lease.FencingToken, Events: events})
	if status != http.StatusOK {
		t.Fatalf("append status = %d body=%s", status, body)
	}
	var acknowledgement AppendLogsResponse
	if err := json.Unmarshal(body, &acknowledgement); err != nil {
		t.Fatal(err)
	}
	if acknowledgement.Acknowledged[contract.LogStdout] != 1 || acknowledgement.Acknowledged[contract.LogStderr] != 0 {
		t.Fatalf("acknowledged = %#v", acknowledgement.Acknowledged)
	}
	if acknowledgement.AttemptState != contract.AttemptRunning {
		t.Fatalf("append attempt state = %q, want %q", acknowledgement.AttemptState, contract.AttemptRunning)
	}

	storedJob, err := h.store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if storedJob.State != contract.JobRunning {
		t.Fatalf("job state while tailing = %q, want %q", storedJob.State, contract.JobRunning)
	}

	status, _, body = h.do(client, http.MethodGet, "/v1/jobs/"+job.JobID+"/logs?limit=2", nil)
	if status != http.StatusOK {
		t.Fatalf("first poll status = %d body=%s", status, body)
	}
	var first LogPage
	if err := json.Unmarshal(body, &first); err != nil {
		t.Fatal(err)
	}
	if len(first.Events) != 2 || first.NextCursor == "" {
		t.Fatalf("first page = %#v", first)
	}
	// Treat the cursor only as an opaque token returned by the protocol. The
	// test deliberately makes no assertion about SQLite row identifiers.
	status, _, body = h.do(client, http.MethodGet, "/v1/jobs/"+job.JobID+"/logs?limit=2&cursor="+first.NextCursor, nil)
	if status != http.StatusOK {
		t.Fatalf("second poll status = %d body=%s", status, body)
	}
	var second LogPage
	if err := json.Unmarshal(body, &second); err != nil {
		t.Fatal(err)
	}
	all := append(append([]contract.LogEvent(nil), first.Events...), second.Events...)
	assertPerStreamEvents(t, all)
	if got := string(joinLogStream(all, contract.LogStdout)); got != "out-0\nout-1\n" {
		t.Fatalf("stdout = %q", got)
	}
	if got := string(joinLogStream(all, contract.LogStderr)); got != "err-0\n" {
		t.Fatalf("stderr = %q", got)
	}
}

func TestLogReplayIsIdempotentAndRawJSONLMatchesRows(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": nil})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agentClient := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agentClient, "node-1")
	job := h.submit(client, "dispatch-log-replay", nil)
	claim := claimForLogs(t, h, agentClient, job.JobID)
	request := AppendLogsRequest{FencingToken: claim.Lease.FencingToken, Events: []contract.LogEvent{
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 0, []byte("first\n")),
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 1, []byte("second\n")),
	}}
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", job.JobID, claim.Lease.AttemptID)
	for upload := 0; upload < 2; upload++ {
		status, _, body := h.do(agentClient, http.MethodPost, path, request)
		if status != http.StatusOK {
			t.Fatalf("upload %d status = %d body=%s", upload, status, body)
		}
	}
	var count int
	if err := h.store.db.QueryRow("SELECT count(*) FROM log_events WHERE job_id=?", job.JobID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != len(request.Events) {
		t.Fatalf("stored log rows = %d, want %d", count, len(request.Events))
	}

	exitCode := 0
	completionPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
	status, _, body := h.do(agentClient, http.MethodPost, completionPath, CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "complete-logs", Result: ProcessResult{ExitCode: &exitCode},
	})
	if status != http.StatusOK {
		t.Fatalf("complete status = %d body=%s", status, body)
	}

	raw, err := h.store.RawJobLogJSONL(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	page, err := h.store.GetJobLogs(context.Background(), job.JobID, "", MaxLogPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	var rows bytes.Buffer
	for _, event := range page.Events {
		encoded, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		rows.Write(encoded)
		rows.WriteByte('\n')
	}
	if !bytes.Equal(raw, rows.Bytes()) {
		t.Fatalf("authoritative JSONL does not match stored rows\njsonl=%s\nrows=%s", raw, rows.Bytes())
	}
}

func TestAgentLogGapReasonsPassL1Validation(t *testing.T) {
	reasons := contract.LogGapReasons()
	for _, reason := range reasons {
		t.Run(string(reason), func(t *testing.T) {
			gap := &contract.LogGap{
				ThroughSequence: 0,
				LostEventCount:  1,
				LostByteCount:   1,
				Reason:          reason,
			}
			if reason == contract.LogGapLateEvidenceWindowExpired {
				gap.SourceEventSHA256 = strings.Repeat("a", 64)
			}
			event := contract.LogEvent{
				AttemptID: "attempt-gap-reasons",
				Stream:    contract.LogStdout,
				Sequence:  0,
				Timestamp: time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC),
				Gap:       gap,
			}
			if err := validateLogEvent(event.AttemptID, event); err != nil {
				t.Fatalf("L1 rejected agent log gap reason %q: %v", reason, err)
			}
		})
	}

	unknown := contract.LogEvent{
		AttemptID: "attempt-gap-reasons",
		Stream:    contract.LogStdout,
		Sequence:  0,
		Timestamp: time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC),
		Gap: &contract.LogGap{
			ThroughSequence: 0,
			LostEventCount:  1,
			LostByteCount:   1,
			Reason:          contract.LogGapReason("future_unknown_reason"),
		},
	}
	err := validateLogEvent(unknown.AttemptID, unknown)
	var protocolErr *Error
	if !errors.As(err, &protocolErr) || protocolErr.Code != contract.ErrorInvalidRequest {
		t.Fatalf("unknown log gap reason = %v, want invalid_request", err)
	}
}

func TestLogBytesPreserveLongPartialAndInvalidUTF8Data(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": nil})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agentClient := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agentClient, "node-1")
	job := h.submit(client, "dispatch-raw-log-bytes", nil)
	claim := claimForLogs(t, h, agentClient, job.JobID)
	longLine := []byte(strings.Repeat("x", 70*1024))
	partial := []byte("partial-without-newline")
	invalidUTF8 := []byte{0xff, 0xfe, 0x00, '\n'}
	events := []contract.LogEvent{
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 0, longLine),
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 1, partial),
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 2, invalidUTF8),
	}
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", job.JobID, claim.Lease.AttemptID)
	status, _, body := h.do(agentClient, http.MethodPost, path, AppendLogsRequest{FencingToken: claim.Lease.FencingToken, Events: events})
	if status != http.StatusOK {
		t.Fatalf("append status = %d body=%s", status, body)
	}
	page, err := h.store.GetJobLogs(context.Background(), job.JobID, "", MaxLogPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Join([][]byte{longLine, partial, invalidUTF8}, nil)
	if got := joinLogStream(page.Events, contract.LogStdout); !bytes.Equal(got, want) {
		t.Fatalf("round-tripped bytes differ: got %d bytes, want %d", len(got), len(want))
	}
}

func TestStaleFenceCannotAppendLogs(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": nil})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agentClient := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agentClient, "node-1")
	job := h.submit(client, "dispatch-stale-log-fence", nil)
	claim := claimForLogs(t, h, agentClient, job.JobID)
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", job.JobID, claim.Lease.AttemptID)
	status, _, body := h.do(agentClient, http.MethodPost, path, AppendLogsRequest{
		FencingToken: claim.Lease.FencingToken + "-stale",
		Events:       []contract.LogEvent{logEvent(claim.Lease.AttemptID, contract.LogStdout, 0, []byte("stale-fence-rejected"))},
	})
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorStaleFence)
	var count int
	if err := h.store.db.QueryRow("SELECT count(*) FROM log_events WHERE job_id=?", job.JobID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("stale-fenced append stored %d rows", count)
	}
}

func claimForLogs(t *testing.T, h *integrationHarness, agentClient *http.Client, jobID string) Claim {
	t.Helper()
	status, _, body := h.do(agentClient, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-1", BootSessionID: "boot-node-1", Class: contract.JobClassOneShot})
	if status != http.StatusOK {
		t.Fatalf("claim status = %d body=%s", status, body)
	}
	var claim Claim
	if err := json.Unmarshal(body, &claim); err != nil {
		t.Fatal(err)
	}
	if claim.Job.JobID != jobID {
		t.Fatalf("claimed job %q, want %q", claim.Job.JobID, jobID)
	}
	return claim
}

func logEvent(attemptID string, stream contract.LogStream, sequence uint64, payload []byte) contract.LogEvent {
	return contract.LogEvent{
		AttemptID: attemptID,
		Stream:    stream,
		Sequence:  sequence,
		Timestamp: time.Date(2026, 8, 9, 10, 0, int(sequence), 0, time.UTC),
		Bytes:     bytes.Clone(payload),
	}
}

func assertPerStreamEvents(t *testing.T, events []contract.LogEvent) {
	t.Helper()
	next := map[contract.LogStream]uint64{}
	for _, event := range events {
		if event.Sequence != next[event.Stream] {
			t.Fatalf("%s sequence = %d, want %d", event.Stream, event.Sequence, next[event.Stream])
		}
		next[event.Stream]++
	}
}

func joinLogStream(events []contract.LogEvent, stream contract.LogStream) []byte {
	var joined []byte
	for _, event := range events {
		if event.Stream == stream {
			joined = append(joined, event.Bytes...)
		}
	}
	return joined
}
