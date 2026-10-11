package l3

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// linkKnowledgeFixture is the sole storage-writing seam for new link tests.
// Optional identities also permit explicit legacy/corrupt shapes. Outcome and
// token history are established through the ordinary Run transitions.
type linkKnowledgeFixture struct {
	runJob, outboxJob  *string
	diagnostic         *contract.APIError
	attemptedAt        *time.Time
	settled            bool
	acknowledged       bool
	cancellation       string
	cancellationReason string
}

func linkID(id string) *string { return &id }

func buildLegacyOrCorruptLinkKnowledge(t *testing.T, s *Store, runID string, fixture linkKnowledgeFixture) {
	t.Helper()
	var attempted, dispatched any
	if fixture.attemptedAt != nil {
		attempted = fixture.attemptedAt.UnixNano()
	}
	if fixture.acknowledged {
		dispatched = s.clock.Now().UnixNano()
	}
	var diagnostic any
	if fixture.diagnostic != nil {
		payload, err := json.Marshal(fixture.diagnostic)
		if err != nil {
			t.Fatal(err)
		}
		diagnostic = string(payload)
	}
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE runs SET l1_job_id=?, job_link_settled=?, dispatch_attempt_ns=?, job_link_failures=0, job_link_retry_ns=0 WHERE run_id=?`, fixture.runJob, fixture.settled, attempted, runID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE dispatch_outbox SET job_id=?, dispatched_ns=?, last_error=? WHERE run_id=?`, fixture.outboxJob, dispatched, diagnostic, runID); err != nil {
		t.Fatal(err)
	}
	if fixture.cancellation != "" {
		var completed any
		if fixture.cancellation != "pending" {
			completed = s.clock.Now().UnixNano()
		}
		if _, err := tx.Exec(`INSERT INTO run_cancellations(run_id, requested_ns, completed_ns, refused) VALUES(?,?,?,?) ON CONFLICT(run_id) DO UPDATE SET completed_ns=excluded.completed_ns, refused=excluded.refused, retry_ns=0`, runID, s.clock.Now().UnixNano(), completed, fixture.cancellation == "refused"); err != nil {
			t.Fatal(err)
		}
	}
	if fixture.cancellationReason != "" {
		payload, err := json.Marshal(contract.APIError{Code: contract.ErrorForbidden, Message: fixture.cancellationReason})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(`UPDATE run_cancellations SET last_error=? WHERE run_id=?`, string(payload), runID); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// migrationFixtureSQL is restricted to historical-shape and persistence-failure
// fixtures. Ordinary tests continue to drive production transitions.
func migrationFixtureSQL(t *testing.T, s *Store, statement string, args ...any) {
	t.Helper()
	if _, err := s.db.Exec(statement, args...); err != nil {
		t.Fatal(err)
	}
}

func prepareLegacyLinkUpgrade(t *testing.T, s *Store) {
	t.Helper()
	migrationFixtureSQL(t, s, `DROP TABLE IF EXISTS run_job_link_migration`)
	migrationFixtureSQL(t, s, `DROP TABLE IF EXISTS run_job_link_validations`)
}

func legacyMigrationShape(t *testing.T, s *Store, runID, shape string) {
	t.Helper()
	switch shape {
	case "settled":
		migrationFixtureSQL(t, s, `UPDATE runs SET job_link_settled=1 WHERE run_id=?`, runID)
	case "regressed":
		migrationFixtureSQL(t, s, `UPDATE dispatch_outbox SET last_error='{"details":{"reason":"l1_regressed","l1_job_id":"job-rejected"}}' WHERE run_id=?`, runID)
	case "cancellation-pending", "cancellation-completed", "cancellation-refused":
		migrationFixtureSQL(t, s, `INSERT INTO run_cancellations(run_id,requested_ns,completed_ns,refused) VALUES(?,?,?,?)`, runID, 1, map[string]any{"cancellation-pending": nil, "cancellation-completed": 2, "cancellation-refused": 2}[shape], shape == "cancellation-refused")
	case "mismatched-key":
		migrationFixtureSQL(t, s, `UPDATE dispatch_outbox SET dispatch_key='wrong-key' WHERE run_id=?`, runID)
	case "no-dispatch-time":
		migrationFixtureSQL(t, s, `UPDATE dispatch_outbox SET dispatched_ns=NULL WHERE run_id=?`, runID)
	case "no-job":
		migrationFixtureSQL(t, s, `UPDATE dispatch_outbox SET job_id=NULL WHERE run_id=?`, runID)
	case "empty-job":
		migrationFixtureSQL(t, s, `UPDATE dispatch_outbox SET job_id='' WHERE run_id=?`, runID)
	case "empty-run-job":
		migrationFixtureSQL(t, s, `UPDATE runs SET l1_job_id='' WHERE run_id=?`, runID)
	case "staged-token":
		migrationFixtureSQL(t, s, `UPDATE dispatch_outbox SET token_delivery='legacy-token' WHERE run_id=?`, runID)
	case "malformed-diagnostic":
		migrationFixtureSQL(t, s, `UPDATE dispatch_outbox SET last_error='broken-json' WHERE run_id=?`, runID)
	case "empty-diagnostic":
		migrationFixtureSQL(t, s, `UPDATE dispatch_outbox SET last_error='' WHERE run_id=?`, runID)
	case "diagnostic":
		migrationFixtureSQL(t, s, `UPDATE dispatch_outbox SET last_error='{"code":"internal","retryable":true}' WHERE run_id=?`, runID)
	case "abbreviated-cancel-reason":
		migrationFixtureSQL(t, s, `UPDATE runs SET failure_reason='canceled before dispatch' WHERE run_id=?`, runID)
	case "old-cancel-reason":
		migrationFixtureSQL(t, s, `UPDATE runs SET failure_reason='the run was canceled before dispatch' WHERE run_id=?`, runID)
	case "new-cancel-reason":
		migrationFixtureSQL(t, s, `UPDATE runs SET failure_reason='canceled before dispatch was confirmed' WHERE run_id=?`, runID)
	case "no-attempt":
		migrationFixtureSQL(t, s, `UPDATE runs SET dispatch_attempt_ns=NULL WHERE run_id=?`, runID)
		migrationFixtureSQL(t, s, `UPDATE dispatch_outbox SET attempt_count=0 WHERE run_id=?`, runID)
	case "backfill-attempt":
		migrationFixtureSQL(t, s, `UPDATE runs SET dispatch_attempt_ns=NULL WHERE run_id=?`, runID)
	case "active":
		migrationFixtureSQL(t, s, `UPDATE runs SET status='dispatching' WHERE run_id=?`, runID)
	case "succeeded":
		migrationFixtureSQL(t, s, `UPDATE runs SET status='succeeded' WHERE run_id=?`, runID)
	case "known-node":
		migrationFixtureSQL(t, s, `UPDATE runs SET node_id='known-node' WHERE run_id=?`, runID)
	case "new-identity":
		migrationFixtureSQL(t, s, `UPDATE runs SET l1_job_id='new-job' WHERE run_id=?`, runID)
		migrationFixtureSQL(t, s, `UPDATE dispatch_outbox SET job_id='new-job' WHERE run_id=?`, runID)
	default:
		t.Fatalf("unknown legacy shape %q", shape)
	}
}

// Exact storage comparison is intentional for rejected historical shapes;
// this is the narrow exception for migration/corruption fixtures in #772.
func snapshotLegacyLinkStorage(t *testing.T, s *Store, runID string) []any {
	t.Helper()
	rows, err := s.db.Query(`SELECT r.*,o.* FROM runs r JOIN dispatch_outbox o ON o.run_id=r.run_id WHERE r.run_id=?`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("missing fixture")
	}
	values := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		t.Fatal(err)
	}
	return values
}

func legacyMigrationFault(t *testing.T, s *Store, phase string, enabled bool) {
	t.Helper()
	if !enabled {
		migrationFixtureSQL(t, s, `DROP TRIGGER IF EXISTS legacy_link_fault`)
		return
	}
	switch phase {
	case "link":
		migrationFixtureSQL(t, s, `CREATE TRIGGER legacy_link_fault BEFORE UPDATE OF l1_job_id ON runs BEGIN SELECT RAISE(ABORT,'injected migration interruption'); END`)
	case "queue":
		migrationFixtureSQL(t, s, `CREATE TABLE run_job_link_validations (run_id TEXT PRIMARY KEY REFERENCES runs(run_id),dispatch_key TEXT NOT NULL,job_id TEXT NOT NULL,failures INTEGER NOT NULL DEFAULT 0,retry_ns INTEGER NOT NULL DEFAULT 0);
 CREATE TRIGGER legacy_link_fault BEFORE INSERT ON run_job_link_validations BEGIN SELECT RAISE(ABORT,'injected migration interruption'); END`)
	case "marker":
		migrationFixtureSQL(t, s, `CREATE TABLE run_job_link_migration (id INTEGER PRIMARY KEY CHECK(id=1));
 CREATE TRIGGER legacy_link_fault BEFORE INSERT ON run_job_link_migration BEGIN SELECT RAISE(ABORT,'injected migration interruption'); END`)
	case "completion":
		migrationFixtureSQL(t, s, `CREATE TRIGGER legacy_link_fault BEFORE DELETE ON run_job_link_validations BEGIN SELECT RAISE(ABORT,'injected validation interruption'); END`)
	default:
		t.Fatalf("unknown interruption %q", phase)
	}
}

// recordOutboxOnlyAcknowledgement writes the state an L3 from before
// completeDispatch linked ended runs left behind: L1's acknowledgement in the
// outbox, and a terminal run with no l1_job_id. Recovery must still link or
// settle these historical rows.
func recordOutboxOnlyAcknowledgement(t *testing.T, store *Store, runID, jobID string) {
	t.Helper()
	if _, err := store.db.Exec(`UPDATE dispatch_outbox SET job_id=?, dispatched_ns=?, last_error=NULL, token_delivery=NULL WHERE run_id=? AND dispatched_ns IS NULL`,
		jobID, store.clock.Now().UnixNano(), runID); err != nil {
		t.Fatal(err)
	}
}

// recordOutboxOnlyAcknowledgementKeepingError records only the acknowledgement,
// leaving the stale retryable diagnostic for recovery's link to clear.
func recordOutboxOnlyAcknowledgementKeepingError(ctx context.Context, store *Store, runID, jobID string) error {
	_, err := store.db.ExecContext(ctx, `UPDATE dispatch_outbox SET job_id=?, dispatched_ns=? WHERE run_id=? AND dispatched_ns IS NULL`,
		jobID, store.clock.Now().UnixNano(), runID)
	return err
}

// ageDispatchAttemptPastSettleHorizon moves the run's last submit attempt
// beyond the settle horizon, and clears any backoff, so the next pass may
// settle an unacknowledged dispatch's absence.
func ageDispatchAttemptPastSettleHorizon(t *testing.T, store *Store, runID string) {
	t.Helper()
	// Advance the fixture clock past both the absence horizon and retry cap.
	// The production attempt and backoff remain intact.
	store.clock = &mutableClock{now: store.clock.Now().Add(unrecordedDispatchSettleHorizon + time.Minute)}
}

func jobLinkBackoff(t *testing.T, s *Store, dispatchKey string) (int, time.Time) {
	t.Helper()
	var runID string
	if err := s.db.QueryRow(`SELECT run_id FROM runs WHERE dispatch_key=?`, dispatchKey).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	failures, retryNS, err := readRecoveryBackoff(t.Context(), s.db, runID)
	if err != nil {
		t.Fatal(err)
	}
	return failures, time.Unix(0, retryNS).UTC()
}

func jobLinkSettled(t *testing.T, s *Store, runID string) bool {
	t.Helper()
	observed, err := readDispatchObservation(t.Context(), s.db, runID)
	if err != nil {
		t.Fatal(err)
	}
	return observed.settled
}

// Recovery fixtures preserve the rest of the captured link while modeling
// old or corrupt ledgers and concurrent newer verdicts. Other recovery tests
// drive the production transitions.
func recordRecoveryRegression(ctx context.Context, s *Store, runID, jobID, message string) error {
	payload, err := json.Marshal(contract.APIError{Code: contract.ErrorNotFound, Message: message, Details: map[string]any{"reason": l1RegressedReason, "l1_job_id": jobID}})
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `UPDATE dispatch_outbox SET last_error=? WHERE run_id=?`, string(payload), runID)
	return err
}

func corruptRecoveryOutboxKey(t *testing.T, s *Store, runID, key string) {
	t.Helper()
	migrationFixtureSQL(t, s, `UPDATE dispatch_outbox SET dispatch_key=? WHERE run_id=?`, key, runID)
}

func recoveryAttemptTimes(t *testing.T, s *Store, key string) (sql.NullInt64, int64) {
	t.Helper()
	var runID string
	if err := s.db.QueryRow(`SELECT run_id FROM runs WHERE dispatch_key=?`, key).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	observed, err := readDispatchObservation(t.Context(), s.db, runID)
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.GetRun(t.Context(), runID)
	if err != nil || run.FinishedAt == nil {
		t.Fatalf("terminal run: %+v %v", run, err)
	}
	return observed.attempted, run.FinishedAt.UnixNano()
}

func recoveryRunAttemptTime(t *testing.T, s *Store, runID string) sql.NullInt64 {
	t.Helper()
	observed, err := readDispatchObservation(t.Context(), s.db, runID)
	if err != nil {
		t.Fatal(err)
	}
	return observed.attempted
}

func snapshotDispatchAcknowledgement(t *testing.T, s *Store, runID string) (string, sql.NullInt64) {
	t.Helper()
	observed, err := readDispatchObservation(t.Context(), s.db, runID)
	if err != nil {
		t.Fatal(err)
	}
	return observed.outboxJob.String, observed.dispatched
}

// Query-plan inspection is a storage-level exception: recovery must walk the
// due prefix of its partial index rather than scan and sort the entire backlog.
func recoveryQueryPlan(t *testing.T, s *Store) []string {
	t.Helper()
	rows, err := s.db.Query(`EXPLAIN QUERY PLAN `+unrecordedDispatchesQuery, int64(0), unrecordedDispatchBatch)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return plan
}

func removeRecoveryAttemptColumn(t *testing.T, s *Store) {
	t.Helper()
	migrationFixtureSQL(t, s, `DROP INDEX runs_job_link_recovery`)
	migrationFixtureSQL(t, s, `ALTER TABLE runs DROP COLUMN dispatch_attempt_ns`)
}

func clearRecoveryAttemptTimes(t *testing.T, s *Store) {
	t.Helper()
	migrationFixtureSQL(t, s, `UPDATE runs SET dispatch_attempt_ns=NULL`)
}
func (s *Store) ensureRunToken(ctx context.Context, runID string) (string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", internalError(err, "begin run token mint")
	}
	defer tx.Rollback()
	token, err := ensureRunTokenTx(ctx, tx, runID, canonicalTime(s.clock.Now()))
	if err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", internalError(err, "commit run token mint")
	}
	return token, nil
}

func linkTestOutbox(t *testing.T, s *Store, runID string) dispatchOutboxRecord {
	t.Helper()
	v, err := readDispatchOutbox(t.Context(), s.db, runID)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func linkTestCancellation(t *testing.T, s *Store, runID string) runCancellationView {
	t.Helper()
	v, err := readRunCancellationView(t.Context(), s.db, runID)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// Ordinary fixtures drive the production transition. Other shapes deliberately
// model partial legacy acknowledgements, settled history or corrupt identities.
func buildLinkKnowledge(t *testing.T, s *Store, runID string, f linkKnowledgeFixture) {
	t.Helper()
	if f.runJob == nil && f.outboxJob == nil && !f.settled && f.diagnostic == nil && f.cancellation == "" && f.attemptedAt == nil {
		return
	}
	observed, observeErr := readDispatchObservation(t.Context(), s.db, runID)
	if observeErr != nil {
		t.Fatal(observeErr)
	}
	canAcknowledge := observed.state == contract.RunPending || observed.state == contract.RunDispatching || observed.state == contract.RunSucceeded || observed.state == contract.RunFailed || observed.runJob.Valid
	if canAcknowledge && (!observed.runJob.Valid || f.runJob != nil && observed.runJob.String == *f.runJob) && (!observed.outboxJob.Valid || f.outboxJob != nil && observed.outboxJob.String == *f.outboxJob) && f.runJob != nil && f.outboxJob != nil && *f.runJob != "" && *f.runJob == *f.outboxJob && f.acknowledged && !f.settled && f.diagnostic == nil && f.cancellation == "" {
		if err := s.completeDispatch(t.Context(), runID, *f.runJob); err != nil {
			t.Fatal(err)
		}
		return
	}
	buildLegacyOrCorruptLinkKnowledge(t, s, runID, f)
}

// resetLegacyUnattemptedDispatch models a retry row left by older ledgers
// whose submit-attempt timestamp was absent. Current beginDispatch always stamps it.
func resetLegacyUnattemptedDispatch(t *testing.T, s *Store, runID string) error {
	_, err := s.db.Exec(`UPDATE runs SET dispatch_attempt_ns=NULL WHERE run_id=?`, runID)
	return err
}

func inspectTestOutbox(s *Store, runID string, inspect func(dispatchOutboxRecord)) error {
	v, err := readDispatchOutbox(context.Background(), s.db, runID)
	if err != nil {
		return err
	}
	inspect(v)
	return nil
}
func inspectTestCancellation(s *Store, runID string, inspect func(runCancellationView)) error {
	v, err := readRunCancellationView(context.Background(), s.db, runID)
	if err != nil {
		return err
	}
	inspect(v)
	return nil
}
