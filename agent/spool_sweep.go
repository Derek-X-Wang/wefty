package agent

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

const (
	// DefaultLogSpoolSweepAfter is how long a dead one-shot spool row is kept
	// before it is swept: L1's default late-evidence window plus a day. The
	// margin covers the lease the attempt still held when its process
	// finished, since L1's window runs from when the attempt was lost, and
	// clock skew between the node and L1. An operator who widens L1's window
	// widens this with it (--log-spool-sweep-after).
	DefaultLogSpoolSweepAfter = l1.DefaultLateEvidenceWindow + 24*time.Hour
	// spoolSweepInterval is how often recovery sweeps when there is no
	// backlog. The sweep rides on recovery's own wake-ups, so an idle node
	// sweeps at its next recovery pass after the interval.
	spoolSweepInterval = time.Hour
	// spoolSweepBatch bounds one sweep transaction. A full batch leaves the
	// sweep due again, so a backlog drains over successive recovery passes.
	spoolSweepBatch = 256
)

// spoolSweep reports one sweep of dead one-shot spool rows.
type spoolSweep struct {
	sealed      int
	undelivered int
	full        bool
}

// sweepDeadOneShotAttempts deletes, at most limit at a time, one-shot spool
// rows that can never again change what L1 records, once they are older
// than cutoff:
//
//   - an incomplete-evidence tombstone: recovery never sends a sealed
//     attempt again, so the tombstone is a local diagnostic only;
//   - a completion recovery still has not delivered although its process
//     finished before cutoff. Renewal stops when the process finishes, so L1
//     lost the attempt at most one lease later and its late-evidence window
//     has since closed: were the completion to land now, L1 would keep only a
//     gap saying the window expired, never the result. A row still pending
//     that long is one L1 keeps refusing, which recovery otherwise re-asks
//     hourly forever.
//
// A service row is never swept: services have their own ring. Neither is an
// attempt this process still owns (live reports it), nor a row L1 can still
// accept: a one-shot with neither a result nor a tombstone is left alone.
// The attempt's spool events and acknowledgements cascade with the row.
func (spool *logSpool) sweepDeadOneShotAttempts(ctx context.Context, cutoff time.Time, limit int, live func(string) bool) (spoolSweep, error) {
	tx, err := spool.db.BeginTx(ctx, nil)
	if err != nil {
		return spoolSweep{}, fmt.Errorf("agent: begin one-shot spool sweep: %w", err)
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT attempt_id, incomplete_json IS NOT NULL FROM spool_attempts
WHERE class=? AND (
  (incomplete_json IS NOT NULL AND sealed_ns < ?)
  OR (incomplete_json IS NULL AND result_json IS NOT NULL AND finished_ns < ?))
ORDER BY created_ns, attempt_id LIMIT ?`, contract.JobClassOneShot, cutoff.UnixNano(), cutoff.UnixNano(), limit)
	if err != nil {
		return spoolSweep{}, fmt.Errorf("agent: select dead one-shot spool rows: %w", err)
	}
	type candidate struct {
		attemptID string
		sealed    bool
	}
	var candidates []candidate
	for rows.Next() {
		var row candidate
		if err := rows.Scan(&row.attemptID, &row.sealed); err != nil {
			_ = rows.Close()
			return spoolSweep{}, fmt.Errorf("agent: scan dead one-shot spool row: %w", err)
		}
		candidates = append(candidates, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return spoolSweep{}, fmt.Errorf("agent: iterate dead one-shot spool rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return spoolSweep{}, fmt.Errorf("agent: close dead one-shot spool rows: %w", err)
	}
	var sweep spoolSweep
	for _, row := range candidates {
		if live != nil && live(row.attemptID) {
			continue
		}
		// The predicate is repeated so a row that changed since the select
		// (a completion delivered, say) is not taken.
		result, err := tx.ExecContext(ctx, `DELETE FROM spool_attempts WHERE attempt_id=? AND class=? AND (
  (incomplete_json IS NOT NULL AND sealed_ns < ?)
  OR (incomplete_json IS NULL AND result_json IS NOT NULL AND finished_ns < ?))`,
			row.attemptID, contract.JobClassOneShot, cutoff.UnixNano(), cutoff.UnixNano())
		if err != nil {
			return spoolSweep{}, fmt.Errorf("agent: sweep dead one-shot spool row: %w", err)
		}
		if changed, err := result.RowsAffected(); err != nil {
			return spoolSweep{}, fmt.Errorf("agent: read one-shot spool sweep: %w", err)
		} else if changed == 0 {
			continue
		}
		if row.sealed {
			sweep.sealed++
		} else {
			sweep.undelivered++
		}
	}
	if err := tx.Commit(); err != nil {
		return spoolSweep{}, fmt.Errorf("agent: commit one-shot spool sweep: %w", err)
	}
	// Only a batch that was full and made progress leaves the sweep due at
	// once; one held up by live attempts waits for the interval.
	sweep.full = len(candidates) >= limit && sweep.sealed+sweep.undelivered > 0
	return sweep, nil
}

// backfillTombstoneSealTimes gives a tombstone sealed before sealed_ns
// existed the seal time its own document records, so the sweep ages it from
// when it was sealed. A document without a readable time is aged from the
// attempt's creation, which is never later than its seal.
func backfillTombstoneSealTimes(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("agent: begin tombstone seal time backfill: %w", err)
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT attempt_id, incomplete_json, created_ns FROM spool_attempts
WHERE incomplete_json IS NOT NULL AND sealed_ns IS NULL`)
	if err != nil {
		return fmt.Errorf("agent: select tombstones without a seal time: %w", err)
	}
	type sealTime struct {
		attemptID string
		sealedNS  int64
	}
	var backfill []sealTime
	for rows.Next() {
		var attemptID string
		var document []byte
		var createdNS int64
		if err := rows.Scan(&attemptID, &document, &createdNS); err != nil {
			_ = rows.Close()
			return fmt.Errorf("agent: scan tombstone without a seal time: %w", err)
		}
		sealedNS := createdNS
		var tombstone incompleteEvidenceTombstone
		if json.Unmarshal(document, &tombstone) == nil && !tombstone.SealedAt.IsZero() {
			sealedNS = tombstone.SealedAt.UnixNano()
		}
		backfill = append(backfill, sealTime{attemptID: attemptID, sealedNS: sealedNS})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("agent: iterate tombstones without a seal time: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("agent: close tombstones without a seal time: %w", err)
	}
	for _, row := range backfill {
		if _, err := tx.ExecContext(ctx, `UPDATE spool_attempts SET sealed_ns=? WHERE attempt_id=?`, row.sealedNS, row.attemptID); err != nil {
			return fmt.Errorf("agent: backfill tombstone seal time: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("agent: commit tombstone seal time backfill: %w", err)
	}
	return nil
}

// sweepDeadOneShotSpool runs one bounded sweep when it is due and reports
// what it removed. It runs on the recovery goroutine, which owns sweepDue.
func (outbox *evidenceOutbox) sweepDeadOneShotSpool(ctx context.Context, now time.Time, report func(error)) {
	if !outbox.sweepDue.IsZero() && now.Before(outbox.sweepDue) {
		return
	}
	after := outbox.spoolSweepAfter
	if after <= 0 {
		after = DefaultLogSpoolSweepAfter
	}
	sweep, err := outbox.spool.sweepDeadOneShotAttempts(ctx, now.Add(-after), spoolSweepBatch, outbox.attemptIsLive)
	if err != nil {
		outbox.sweepDue = now.Add(evidenceRecoveryBackoff(outbox.retryInterval, 1))
		if ctx.Err() == nil && report != nil {
			report(err)
		}
		return
	}
	if sweep.full {
		outbox.sweepDue = now
	} else {
		outbox.sweepDue = now.Add(spoolSweepInterval)
	}
	if swept := sweep.sealed + sweep.undelivered; swept > 0 && report != nil {
		report(fmt.Errorf("swept %d one-shot spool rows older than %s (%d incomplete-evidence tombstones, %d undelivered completions L1 can no longer record as results)",
			swept, after, sweep.sealed, sweep.undelivered))
	}
}
