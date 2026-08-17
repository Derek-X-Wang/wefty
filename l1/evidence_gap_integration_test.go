package l1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestEvidenceGapContract(t *testing.T) {
	assertEvidenceGapContract(t)
}

func assertEvidenceGapContract(t *testing.T) {
	t.Helper()

	t.Run("gap advances only its own stream", func(t *testing.T) {
		h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
		client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
		agent := h.client(fabric.Identity{NodeID: "fabric-node-1", Tags: []string{DefaultAgentPrincipalTag}})
		h.register(agent, "node-1")
		job := h.submit(client, "gap-per-stream", []string{"linux"})
		claim := claimJob(t, h, agent, "node-1")
		path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", job.JobID, claim.Lease.AttemptID)

		status, _, body := h.do(agent, http.MethodPost, path, AppendLogsRequest{
			FencingToken: claim.Lease.FencingToken,
			Events: []contract.LogEvent{{
				AttemptID: claim.Lease.AttemptID,
				Stream:    contract.LogStdout,
				Sequence:  0,
				Timestamp: h.clock.Now(),
				Gap: &contract.LogGap{
					ThroughSequence: 2,
					LostEventCount:  3,
					LostByteCount:   4096,
					Reason:          contract.LogGapSpoolEviction,
				},
			}},
		})
		if status != http.StatusOK {
			t.Fatalf("gap append status = %d body=%s", status, body)
		}

		status, _, body = h.do(agent, http.MethodPost, path, AppendLogsRequest{
			FencingToken: claim.Lease.FencingToken,
			Events: []contract.LogEvent{
				logEvent(claim.Lease.AttemptID, contract.LogStdout, 3, []byte("stdout-after-gap\n")),
				logEvent(claim.Lease.AttemptID, contract.LogStderr, 0, []byte("stderr-remains-independent\n")),
			},
		})
		if status != http.StatusOK {
			t.Fatalf("post-gap append status = %d body=%s", status, body)
		}
		var response AppendLogsResponse
		if err := json.Unmarshal(body, &response); err != nil {
			t.Fatal(err)
		}
		if response.Acknowledged[contract.LogStdout] != 3 || response.Acknowledged[contract.LogStderr] != 0 {
			t.Fatalf("gap acknowledgements = %#v, want stdout=3 stderr=0", response.Acknowledged)
		}

		status, _, body = h.do(agent, http.MethodPost, path, AppendLogsRequest{
			FencingToken: claim.Lease.FencingToken,
			Events: []contract.LogEvent{{
				AttemptID: claim.Lease.AttemptID,
				Stream:    contract.LogStderr,
				Sequence:  1,
				Timestamp: h.clock.Now(),
				Gap: &contract.LogGap{
					ThroughSequence: 1,
					LostEventCount:  1,
					LostByteCount:   32<<20 + 1,
					Reason:          contract.LogGapOversizedEvent,
				},
			}},
		})
		if status != http.StatusOK {
			t.Fatalf("oversized-event gap status = %d body=%s", status, body)
		}
	})

	t.Run("observation window expires before retention and records a gap", func(t *testing.T) {
		const defaultServiceLogRetentionAge = 7 * 24 * time.Hour
		if DefaultLateEvidenceWindow >= defaultServiceLogRetentionAge {
			t.Fatalf("late evidence window = %s, must bind before %s retention", DefaultLateEvidenceWindow, defaultServiceLogRetentionAge)
		}

		h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
		client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
		agent := h.client(fabric.Identity{NodeID: "fabric-node-1", Tags: []string{DefaultAgentPrincipalTag}})
		h.register(agent, "node-1")
		job := h.submit(client, "late-window-gap", []string{"linux"})
		claim := claimJob(t, h, agent, "node-1")
		path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", job.JobID, claim.Lease.AttemptID)
		initial := AppendLogsRequest{FencingToken: claim.Lease.FencingToken, Events: []contract.LogEvent{
			logEvent(claim.Lease.AttemptID, contract.LogStdout, 0, []byte("accepted-before-expiry\n")),
		}}
		status, _, body := h.do(agent, http.MethodPost, path, initial)
		if status != http.StatusOK {
			t.Fatalf("initial append status = %d body=%s", status, body)
		}
		h.clock.Advance(DefaultLeaseDuration)
		if _, err := h.store.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		h.clock.Advance(DefaultLateEvidenceWindow + time.Nanosecond)
		status, _, body = h.do(agent, http.MethodPost, path, initial)
		if status != http.StatusOK {
			t.Fatalf("accepted raw replay after observation window status = %d body=%s", status, body)
		}

		late := AppendLogsRequest{FencingToken: claim.Lease.FencingToken, Events: []contract.LogEvent{
			logEvent(claim.Lease.AttemptID, contract.LogStdout, 1, []byte("discarded-outside-window\n")),
		}}
		status, _, body = h.do(agent, http.MethodPost, path, late)
		if status != http.StatusOK {
			t.Fatalf("out-of-window append status = %d body=%s", status, body)
		}
		status, _, body = h.do(agent, http.MethodPost, path, late)
		if status != http.StatusOK {
			t.Fatalf("out-of-window replay status = %d body=%s", status, body)
		}
		conflict := late
		conflict.Events = []contract.LogEvent{
			logEvent(claim.Lease.AttemptID, contract.LogStdout, 1, []byte("conflicting-outside-window\n")),
		}
		status, _, body = h.do(agent, http.MethodPost, path, conflict)
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorIdempotencyConflict)

		status, _, body = h.do(client, http.MethodGet, fmt.Sprintf("/v1/jobs/%s/logs", job.JobID), nil)
		if status != http.StatusOK {
			t.Fatalf("read logs status = %d body=%s", status, body)
		}
		var page LogPage
		if err := json.Unmarshal(body, &page); err != nil {
			t.Fatal(err)
		}
		if len(page.Events) != 2 {
			t.Fatalf("retained events = %d, want raw event plus window gap", len(page.Events))
		}
		gap := page.Events[1]
		if gap.Gap == nil || gap.Gap.Reason != contract.LogGapLateEvidenceWindowExpired ||
			gap.Gap.ThroughSequence != 1 || gap.Gap.LostEventCount != 1 ||
			gap.Gap.LostByteCount != uint64(len(late.Events[0].Bytes)) || len(gap.Bytes) != 0 {
			t.Fatalf("out-of-window gap = %#v", gap)
		}

		exitCode := 0
		completionPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
		completion := CompletionRequest{
			FencingToken:   claim.Lease.FencingToken,
			IdempotencyKey: "outside-window-completion",
			Result:         ProcessResult{ExitCode: &exitCode},
		}
		status, _, body = h.do(agent, http.MethodPost, completionPath, completion)
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorLeaseExpired)
		var lateResultJSON []byte
		if err := h.store.db.QueryRow(`SELECT late_result_json FROM attempts WHERE attempt_id=?`, claim.Lease.AttemptID).
			Scan(&lateResultJSON); err != nil {
			t.Fatal(err)
		}
		var lateResult LateResultEvidence
		if err := json.Unmarshal(lateResultJSON, &lateResult); err != nil {
			t.Fatalf("decode completion gap %q: %v", lateResultJSON, err)
		}
		if lateResult.Kind != LateResultGapKind || !lateResult.Late || lateResult.Result != nil || lateResult.Gap == nil ||
			lateResult.Gap.Reason != LateResultGapObservationWindowExpired ||
			!lateResult.ObservedAt.Equal(h.clock.Now()) ||
			!lateResult.AuthorityLostAt.Equal(h.clock.Now().Add(-DefaultLateEvidenceWindow-time.Nanosecond)) {
			t.Fatalf("completion gap = %#v", lateResult)
		}
		assertJobAndAttemptState(t, h.store, job.JobID, claim.Lease.AttemptID, contract.JobFailed, contract.AttemptLost)
	})
}
