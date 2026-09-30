package agent

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

const (
	// DefaultLogSpoolBackstopAge is the disk-safety backstop for one-shot
	// spool rows L1 never refused: a tombstone sealed, or a completion
	// finished, this long ago is swept even so, and loudly. It matches L1's
	// one-shot log retention age: past it L1 would already have trimmed any
	// log it had taken. It is not derived from L1's late-evidence window, which
	// starts when L1 records the loss, not when the process finished, so an L1
	// that was unreachable for days still takes the evidence afterwards.
	DefaultLogSpoolBackstopAge = l1.DefaultOneshotLogRetentionAge
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
//   - conflict: the attempt is terminal, its service removal revoked
//     evidence, or the body contradicts the attempt's phase;
//   - idempotency_conflict: L1 accepted different evidence at that key;
//   - invalid_request, unsupported_*, not_implemented: the body can never be
//     accepted.
//
// Not included, so such a row waits for the backstop: lease_expired, which on
// /complete means L1 kept the evidence (as a gap past its late-evidence
// window) and the row is retired as delivered; attempt_not_owned, which a
// node reconfigured with the wrong identity also gets; the node-session codes
// recovery parks on (#549); and every transient answer. L1 never refuses
// evidence as too late: past its window it accepts it as a gap.
func l1ClosedEvidence(code contract.ErrorCode) bool {
	switch code {
	case contract.ErrorAttemptNotFound, contract.ErrorNotFound,
		contract.ErrorStaleFence, contract.ErrorAttemptMismatch,
		contract.ErrorConflict, contract.ErrorIdempotencyConflict,
		contract.ErrorInvalidRequest, contract.ErrorUnsupportedClass, contract.ErrorUnsupportedKind,
		contract.ErrorUnsupportedRuntimeHandler, contract.ErrorNotImplemented:
		return true
	default:
		return false
	}
}

// spoolSweep reports one sweep of dead one-shot spool rows.
type spoolSweep struct {
	refused  int
	backstop []string
	full     bool
}

const spoolSweepPredicate = `class=? AND (
  l1_refused_ns IS NOT NULL
  OR (incomplete_json IS NOT NULL AND sealed_ns < ?)
  OR (incomplete_json IS NULL AND result_json IS NOT NULL AND finished_ns < ?))`

// sweepDeadOneShotAttempts deletes, at most limit at a time, one-shot spool
// rows that can no longer change what L1 records:
//
//   - a row L1 has closed the door on: a delivery of its evidence was
//     answered with an l1ClosedEvidence code, recorded on the row when it
//     was sealed (l1_refused_ns, l1_refusal_code);
//   - as a disk-safety backstop only, a row L1 never refused whose tombstone
//     was sealed, or whose completion finished, before backstopCutoff.
//
// Nothing else is swept: not a completion or log L1 has simply not answered
// yet, however old, below the backstop; not a service row; and not an
// attempt this process still owns (live reports it). The attempt's spool
// events and acknowledgements cascade with the row.
func (spool *logSpool) sweepDeadOneShotAttempts(ctx context.Context, backstopCutoff time.Time, limit int, live func(string) bool) (spoolSweep, error) {
	tx, err := spool.db.BeginTx(ctx, nil)
	if err != nil {
		return spoolSweep{}, fmt.Errorf("agent: begin one-shot spool sweep: %w", err)
	}
	defer tx.Rollback()
	cutoff := backstopCutoff.UnixNano()
	rows, err := tx.QueryContext(ctx, `SELECT attempt_id, l1_refused_ns IS NOT NULL FROM spool_attempts
WHERE `+spoolSweepPredicate+`
ORDER BY created_ns, attempt_id LIMIT ?`, contract.JobClassOneShot, cutoff, cutoff, limit)
	if err != nil {
		return spoolSweep{}, fmt.Errorf("agent: select dead one-shot spool rows: %w", err)
	}
	type candidate struct {
		attemptID string
		refused   bool
	}
	var candidates []candidate
	for rows.Next() {
		var row candidate
		if err := rows.Scan(&row.attemptID, &row.refused); err != nil {
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
		result, err := tx.ExecContext(ctx, `DELETE FROM spool_attempts WHERE attempt_id=? AND `+spoolSweepPredicate,
			row.attemptID, contract.JobClassOneShot, cutoff, cutoff)
		if err != nil {
			return spoolSweep{}, fmt.Errorf("agent: sweep dead one-shot spool row: %w", err)
		}
		if changed, err := result.RowsAffected(); err != nil {
			return spoolSweep{}, fmt.Errorf("agent: read one-shot spool sweep: %w", err)
		} else if changed == 0 {
			continue
		}
		if row.refused {
			sweep.refused++
		} else {
			sweep.backstop = append(sweep.backstop, row.attemptID)
		}
	}
	if err := tx.Commit(); err != nil {
		return spoolSweep{}, fmt.Errorf("agent: commit one-shot spool sweep: %w", err)
	}
	// Only a batch that was full and made progress leaves the sweep due at
	// once; one held up by live attempts waits for the interval.
	sweep.full = len(candidates) >= limit && sweep.refused+len(sweep.backstop) > 0
	return sweep, nil
}

// backfillTombstoneSealTimes gives a tombstone sealed before sealed_ns and
// l1_refused_ns existed what its own document records: when it was sealed,
// and, if the L1 answer that sealed it closed the door on the attempt, that
// refusal. A document without a readable time is aged from the attempt's
// creation, which is never later than its seal.
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
		refusal   contract.ErrorCode
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
		row := sealTime{attemptID: attemptID, sealedNS: createdNS}
		var tombstone incompleteEvidenceTombstone
		if json.Unmarshal(document, &tombstone) == nil {
			if !tombstone.SealedAt.IsZero() {
				row.sealedNS = tombstone.SealedAt.UnixNano()
			}
			if l1ClosedEvidence(tombstone.ErrorCode) {
				row.refusal = tombstone.ErrorCode
			}
		}
		backfill = append(backfill, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("agent: iterate tombstones without a seal time: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("agent: close tombstones without a seal time: %w", err)
	}
	for _, row := range backfill {
		var refusedNS, refusalCode any
		if row.refusal != "" {
			refusedNS, refusalCode = row.sealedNS, string(row.refusal)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE spool_attempts SET sealed_ns=?, l1_refused_ns=?, l1_refusal_code=? WHERE attempt_id=?`,
			row.sealedNS, refusedNS, refusalCode, row.attemptID); err != nil {
			return fmt.Errorf("agent: backfill tombstone seal time: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("agent: commit tombstone seal time backfill: %w", err)
	}
	return nil
}

// sweepDeadOneShotSpool runs one bounded sweep when it is due and reports
// what it removed. A row taken by the backstop was never refused by L1, so
// its evidence may never have reached L1: that is reported as a warning
// naming every attempt. It runs on the recovery goroutine, which owns
// sweepDue.
func (outbox *evidenceOutbox) sweepDeadOneShotSpool(ctx context.Context, now time.Time, report func(error)) {
	if !outbox.sweepDue.IsZero() && now.Before(outbox.sweepDue) {
		return
	}
	backstop := outbox.spoolBackstopAge
	if backstop <= 0 {
		backstop = DefaultLogSpoolBackstopAge
	}
	sweep, err := outbox.spool.sweepDeadOneShotAttempts(ctx, now.Add(-backstop), spoolSweepBatch, outbox.attemptIsLive)
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
	if report == nil {
		return
	}
	if sweep.refused > 0 {
		report(fmt.Errorf("swept %d one-shot spool rows whose evidence L1 refused for good", sweep.refused))
	}
	if len(sweep.backstop) > 0 {
		report(fmt.Errorf("WARNING: disk-safety backstop swept %d one-shot spool rows L1 never answered or refused, older than %s; their evidence may never have reached L1: attempts %s",
			len(sweep.backstop), backstop, strings.Join(sweep.backstop, ",")))
	}
}
