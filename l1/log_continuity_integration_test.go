package l1

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// insertDetachedLogRows writes an attempt and its log rows directly, as an
// older database or an earlier, looser bound left them. The attempt's node is
// immaterial to retention, so its reference is not kept.
func insertDetachedLogRows(t *testing.T, store *Store, jobID, attemptID string, state contract.AttemptState, payloads ...string) {
	t.Helper()
	conn, err := store.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys=OFF"); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(ctx, "PRAGMA foreign_keys=ON")
	if _, err := conn.ExecContext(ctx, `INSERT INTO attempts(attempt_id, job_id, node_id, boot_session_id, state, fencing_token,
			lease_expires_ns, authority_generation, created_ns, updated_ns)
		VALUES(?, ?, 'n', 'b', ?, 'f', 1, 1, 1, 1)`, attemptID, jobID, state); err != nil {
		t.Fatal(err)
	}
	for sequence, payload := range payloads {
		if _, err := conn.ExecContext(ctx, `INSERT INTO log_events(job_id, attempt_id, stream, sequence, sequence_end, timestamp_ns, bytes, event_json)
			VALUES(?, ?, 'stdout', ?, ?, ?, ?, X'7B7D')`, jobID, attemptID, sequence, sequence, int64(sequence+1), []byte(payload)); err != nil {
			t.Fatal(err)
		}
	}
}

// Review round 2 (#589): the continuity seed must be atomic with its marker.
// A crash after the table exists but before the seed commits must seed on the
// next open, not skip it for good.
func TestLogContinuitySeedRunsAgainAfterACrashBeforeItCommitted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crash-window.sqlite")
	store, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	job, err := submitDirect(store, validJobSpec("crash-window", nil))
	if err != nil {
		t.Fatal(err)
	}
	insertDetachedLogRows(t, store, job.JobID, "pre-upgrade-attempt", contract.AttemptRunning, "a", "b", "c", "d", "e")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	// The crash window: the table exists, its seed rows and marker do not.
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.Exec(`DELETE FROM log_stream_continuity; DELETE FROM l1_data_migrations WHERE name='log_stream_continuity_seed'`)
	closeErr := database.Close()
	if err != nil || closeErr != nil {
		t.Fatal(errors.Join(err, closeErr))
	}

	store, err = OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	through, err := readLogContinuity(context.Background(), store.db, "pre-upgrade-attempt", contract.LogStdout)
	if err != nil {
		t.Fatal(err)
	}
	if through != 4 {
		t.Fatalf("continuity after reopening the crash window = %d, want 4", through)
	}
	var markers int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM l1_data_migrations WHERE name='log_stream_continuity_seed'`).Scan(&markers); err != nil {
		t.Fatal(err)
	}
	if markers != 1 {
		t.Fatalf("seed markers = %d, want 1", markers)
	}
}

// Review round 2 (#589): once the ceiling evicted a lost service attempt's
// last row, attempt-summary pruning deleted that attempt behind 32 newer
// ones, cascading its continuity record, so its next upload inside the
// late-evidence window was attempt_not_found. Pruning now reads the
// attempt's own state, not whether rows of it are retained.
func TestAttemptPruningKeepsALostAttemptInsideItsLateEvidenceWindow(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LogRetentionTotalBytes: 1}, map[string]NodePolicy{
		"worker": DefaultNodePolicy("worker"),
	})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "worker")
	job := submitRestartService(t, h, client, "lost-attempt-pruning", nil, nil)
	claim := claimRestartService(t, h, agent, node)
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", job.JobID, claim.Lease.AttemptID)
	appendRetentionLogs(t, h, agent, path, claim.Lease.FencingToken, []contract.LogEvent{
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 0, []byte("before-loss")),
	})
	h.clock.Advance(time.Minute)
	if _, err := h.store.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertAttemptState(t, h, claim.Lease.AttemptID, contract.AttemptLost)
	assertRetainedPayloads(t, h, job.JobID)

	// Thirty-three newer summaries, the newest current: the lost attempt is
	// past the 32-summary floor and has no retained rows.
	for index := 0; index <= DefaultServiceAttemptSummaries; index++ {
		stamp := h.clock.Now().Add(time.Duration(index+1) * time.Second).UnixNano()
		if _, err := h.store.db.Exec(`INSERT INTO attempts(
			attempt_id, job_id, node_id, boot_session_id, state, fencing_token,
			lease_expires_ns, authority_generation, created_ns, updated_ns
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, fmt.Sprintf("newer-%02d", index), job.JobID, node.NodeID,
			node.BootSessionID, contract.AttemptFailed, fmt.Sprintf("newer-fence-%02d", index), stamp,
			node.AuthorityGeneration, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.store.db.Exec("UPDATE jobs SET current_attempt_id=? WHERE job_id=?",
		fmt.Sprintf("newer-%02d", DefaultServiceAttemptSummaries), job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	late := logEvent(claim.Lease.AttemptID, contract.LogStdout, 1, []byte("late"))
	late.Timestamp = h.clock.Now()
	response := appendLogsExpectingOK(t, h, agent, path, claim.Lease.FencingToken, []contract.LogEvent{late})
	if response.Acknowledged[contract.LogStdout] != 1 {
		t.Fatalf("late upload acknowledged %#v, want stdout 1", response.Acknowledged)
	}

	// Once the window has closed, the summary is pruned as before.
	// The late row goes to the ceiling first; pruning then sees no rows.
	h.clock.Advance(DefaultLateEvidenceWindow + time.Minute)
	for pass := 0; pass < 2; pass++ {
		if _, err := h.store.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	var remaining int
	if err := h.store.db.QueryRow("SELECT COUNT(*) FROM attempts WHERE attempt_id=?", claim.Lease.AttemptID).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatal("a lost attempt past its late-evidence window was never pruned")
	}
}

// Review round 2 (#589): a replayed range is checked against every retained
// row it intersects, not only the nearest earlier one.
func TestReplayedRangeIsCheckedAgainstEveryRetainedRowItTouches(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{OneshotLogRetentionBytes: 1}, map[string]NodePolicy{
		"worker": DefaultNodePolicy("worker"),
	})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "worker")
	job := h.submit(client, "replay-ranges", nil)
	claim := claimOneshot(t, h, agent, node, job.JobID)
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", job.JobID, claim.Lease.AttemptID)
	gap := func(first, through uint64) contract.LogEvent {
		event := logEvent(claim.Lease.AttemptID, contract.LogStdout, first, nil)
		event.Bytes = nil
		event.Gap = &contract.LogGap{
			ThroughSequence: through, LostEventCount: through - first + 1, LostByteCount: 1, Reason: contract.LogGapSpoolEviction,
		}
		return event
	}
	var accepted []contract.LogEvent
	for sequence := uint64(0); sequence < 10; sequence++ {
		accepted = append(accepted, logEvent(claim.Lease.AttemptID, contract.LogStdout, sequence, []byte("xx")))
	}
	accepted = append(accepted, gap(10, 20))
	appendRetentionLogs(t, h, agent, path, claim.Lease.FencingToken, accepted)
	// The one-byte cap evicted 0-9 (two bytes each) in the append; the
	// zero-byte gap 10-20 stays.
	var retained int
	if err := h.store.db.QueryRow("SELECT COUNT(*) FROM log_events WHERE job_id=?", job.JobID).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if retained != 1 {
		t.Fatalf("retained rows = %d, want only the gap 10-20", retained)
	}

	for _, test := range []struct {
		name   string
		event  contract.LogEvent
		status int
	}{
		{"reviewer case: evicted head overlapping the retained gap", gap(0, 15), http.StatusConflict},
		{"ends on the retained gap's first sequence", gap(5, 10), http.StatusConflict},
		{"inside the retained gap", gap(11, 12), http.StatusConflict},
		{"retained gap's tail", gap(15, 20), http.StatusConflict},
		{"retained gap's start, shorter", gap(10, 15), http.StatusConflict},
		{"exact replay of the retained gap", gap(10, 20), http.StatusOK},
		{"evicted single event", logEvent(claim.Lease.AttemptID, contract.LogStdout, 5, []byte("xx")), http.StatusOK},
		{"exactly the evicted range", gap(0, 9), http.StatusOK},
		{"starts accepted, ends past the record", gap(15, 25), http.StatusConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			status, _, body := h.do(agent, http.MethodPost, path, AppendLogsRequest{
				FencingToken: claim.Lease.FencingToken, Events: []contract.LogEvent{test.event},
			})
			if status != test.status {
				t.Fatalf("status = %d, want %d body=%s", status, test.status, body)
			}
		})
	}
	response := appendLogsExpectingOK(t, h, agent, path, claim.Lease.FencingToken, []contract.LogEvent{
		logEvent(claim.Lease.AttemptID, contract.LogStdout, 21, []byte("next")),
	})
	if response.Acknowledged[contract.LogStdout] != 21 {
		t.Fatalf("continuation acknowledged %#v, want stdout 21", response.Acknowledged)
	}
}

// Review round 2 (#589): a row is deleted whole, so the byte budget stops
// before a row that does not fit rather than overshooting by up to a row.
func TestEvictionByteBudgetNeverOvershoots(t *testing.T) {
	for _, test := range []struct {
		budget int64
		want   int
	}{{11, 1}, {12, 2}, {13, 2}, {17, 2}, {18, 3}} {
		t.Run(fmt.Sprint(test.budget), func(t *testing.T) {
			store, err := OpenStore(filepath.Join(t.TempDir(), "budget.sqlite"), StoreOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			job, err := submitDirect(store, validJobSpec("byte-budget", nil))
			if err != nil {
				t.Fatal(err)
			}
			insertDetachedLogRows(t, store, job.JobID, "budget-attempt", contract.AttemptSucceeded,
				"aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee")
			tx, err := store.db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			budget := &evictionBudget{events: 100, bytes: test.budget}
			perJob := map[string]*logRetentionStats{}
			if err := evictLogEvents(context.Background(), tx, job.JobID, "", nil, budget, -1, perJob); err != nil {
				t.Fatal(err)
			}
			var evicted int64
			var evictedBytes int64
			if stats := perJob[job.JobID]; stats != nil {
				evicted, evictedBytes = stats.events, stats.bytes
			}
			if evicted != int64(test.want) || evictedBytes > test.budget {
				t.Fatalf("budget %d evicted %d rows / %d bytes, want %d rows within budget", test.budget, evicted, evictedBytes, test.want)
			}
		})
	}
}
