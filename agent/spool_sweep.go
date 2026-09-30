package agent

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const (
	// spoolSweepInterval is how often recovery sweeps when there is no
	// backlog. The sweep rides on recovery's own wake-ups, so an idle node
	// sweeps at its next recovery pass after the interval.
	spoolSweepInterval = time.Hour
	// spoolSweepBatch bounds one sweep transaction. A full batch leaves the
	// sweep due again, so a backlog drains over successive recovery passes.
	spoolSweepBatch = 256
)

// l1ClosedEvidence reports whether an L1 answer to a delivery of an attempt's
// evidence means L1 can never accept that evidence from anyone, so the spool
// row may go. Each is an answer about the attempt itself, and none of them
// changes with time or with this node's registration:
//
//   - attempt_not_found: the attempt row is gone (its job was removed, or a
//     service attempt summary was pruned);
//   - not_found: the job is gone;
//   - stale_fence, attempt_mismatch: the fence or path is not the attempt's,
//     or the attempt is no longer the job's current or completion-replay
//     attempt;
//   - invalid_request, unsupported_*, not_implemented: the body can never be
//     accepted.
//
// Not included, so such a row is kept: conflict and idempotency_conflict,
// which can clear later (a replay gap's conflict once the missing earlier
// sequences arrive; an idempotency conflict once the conflicting retained
// rows are evicted by retention); lease_expired, which on
// /complete means L1 kept the evidence (as a gap past its late-evidence
// window) and the row is retired as delivered; attempt_not_owned, which a
// node reconfigured with the wrong identity also gets; the node-session codes
// recovery parks on (#549); and every transient answer. L1 never refuses
// evidence as too late: past its window it accepts it as a gap.
func l1ClosedEvidence(code contract.ErrorCode) bool {
	switch code {
	case contract.ErrorAttemptNotFound, contract.ErrorNotFound,
		contract.ErrorStaleFence, contract.ErrorAttemptMismatch,
		contract.ErrorInvalidRequest, contract.ErrorUnsupportedClass, contract.ErrorUnsupportedKind,
		contract.ErrorUnsupportedRuntimeHandler, contract.ErrorNotImplemented:
		return true
	default:
		return false
	}
}

// spoolSweep reports one sweep of one-shot spool rows L1 refused for good.
type spoolSweep struct {
	refused int
	full    bool
}

const spoolSweepPredicate = `class=? AND l1_refused_ns IS NOT NULL`

// sweepDeadOneShotAttempts deletes, at most limit at a time, the one-shot
// spool rows L1 has closed the door on: a delivery of the row's evidence was
// answered with an l1ClosedEvidence code, recorded on the row when it was
// sealed (l1_refused_ns). Nothing is swept by age. L1's late-evidence window
// starts when L1 records the loss, and L1 keeps late results for as long as
// it keeps the attempt, so a completion or log L1 has not answered stays on
// disk however long L1 is unreachable; the one-shot spool byte budget, which
// refuses new output rather than evicting old, bounds the log payload. A
// service row, and an attempt this process still owns (live reports it), is
// never swept. The attempt's spool events and acknowledgements cascade with
// the row.
func (spool *logSpool) sweepDeadOneShotAttempts(ctx context.Context, limit int, live func(string) bool) (spoolSweep, error) {
	tx, err := spool.db.BeginTx(ctx, nil)
	if err != nil {
		return spoolSweep{}, fmt.Errorf("agent: begin one-shot spool sweep: %w", err)
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT attempt_id FROM spool_attempts
WHERE `+spoolSweepPredicate+`
ORDER BY created_ns, attempt_id LIMIT ?`, contract.JobClassOneShot, limit)
	if err != nil {
		return spoolSweep{}, fmt.Errorf("agent: select dead one-shot spool rows: %w", err)
	}
	var candidates []string
	for rows.Next() {
		var attemptID string
		if err := rows.Scan(&attemptID); err != nil {
			_ = rows.Close()
			return spoolSweep{}, fmt.Errorf("agent: scan dead one-shot spool row: %w", err)
		}
		candidates = append(candidates, attemptID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return spoolSweep{}, fmt.Errorf("agent: iterate dead one-shot spool rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return spoolSweep{}, fmt.Errorf("agent: close dead one-shot spool rows: %w", err)
	}
	var sweep spoolSweep
	for _, attemptID := range candidates {
		if live != nil && live(attemptID) {
			continue
		}
		result, err := tx.ExecContext(ctx, `DELETE FROM spool_attempts WHERE attempt_id=? AND `+spoolSweepPredicate,
			attemptID, contract.JobClassOneShot)
		if err != nil {
			return spoolSweep{}, fmt.Errorf("agent: sweep dead one-shot spool row: %w", err)
		}
		if changed, err := result.RowsAffected(); err != nil {
			return spoolSweep{}, fmt.Errorf("agent: read one-shot spool sweep: %w", err)
		} else if changed == 0 {
			continue
		}
		sweep.refused++
	}
	if err := tx.Commit(); err != nil {
		return spoolSweep{}, fmt.Errorf("agent: commit one-shot spool sweep: %w", err)
	}
	// Only a batch that was full and made progress leaves the sweep due at
	// once; one held up by live attempts waits for the interval.
	sweep.full = len(candidates) >= limit && sweep.refused > 0
	return sweep, nil
}

// backfillTombstoneRefusals records, for a tombstone sealed before the
// refusal was kept on the row, the L1 answer its own document records, and
// its seal time as the refusal time when that answer closed the door on the
// attempt. A recorded code, closing or not, marks the row as examined.
func backfillTombstoneRefusals(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("agent: begin tombstone refusal backfill: %w", err)
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT attempt_id, incomplete_json FROM spool_attempts
WHERE incomplete_json IS NOT NULL AND l1_refusal_code IS NULL`)
	if err != nil {
		return fmt.Errorf("agent: select tombstones without a recorded refusal: %w", err)
	}
	type refusal struct {
		attemptID string
		code      contract.ErrorCode
		refusedNS any
	}
	var backfill []refusal
	for rows.Next() {
		var attemptID string
		var document []byte
		if err := rows.Scan(&attemptID, &document); err != nil {
			_ = rows.Close()
			return fmt.Errorf("agent: scan tombstone without a recorded refusal: %w", err)
		}
		row := refusal{attemptID: attemptID}
		var tombstone incompleteEvidenceTombstone
		if json.Unmarshal(document, &tombstone) == nil {
			row.code = tombstone.ErrorCode
			if l1ClosedEvidence(tombstone.ErrorCode) && !tombstone.SealedAt.IsZero() {
				row.refusedNS = tombstone.SealedAt.UnixNano()
			}
		}
		backfill = append(backfill, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("agent: iterate tombstones without a recorded refusal: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("agent: close tombstones without a recorded refusal: %w", err)
	}
	for _, row := range backfill {
		if _, err := tx.ExecContext(ctx, `UPDATE spool_attempts SET l1_refused_ns=?, l1_refusal_code=? WHERE attempt_id=?`,
			row.refusedNS, string(row.code), row.attemptID); err != nil {
			return fmt.Errorf("agent: backfill tombstone refusal: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("agent: commit tombstone refusal backfill: %w", err)
	}
	return nil
}

// sweepDeadOneShotSpool runs one bounded sweep when it is due and reports
// what it removed. It runs on the recovery goroutine, which owns sweepDue.
func (outbox *evidenceOutbox) sweepDeadOneShotSpool(ctx context.Context, now time.Time, report func(error)) {
	if !outbox.sweepDue.IsZero() && now.Before(outbox.sweepDue) {
		return
	}
	sweep, err := outbox.spool.sweepDeadOneShotAttempts(ctx, spoolSweepBatch, outbox.attemptIsLive)
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
	if sweep.refused > 0 && report != nil {
		report(fmt.Errorf("swept %d one-shot spool rows whose evidence L1 refused for good", sweep.refused))
	}
}
