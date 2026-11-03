package l1

import (
	"context"
	"database/sql"
	"time"
)

type awakeClockSource interface {
	AwakeElapsed() (time.Duration, error)
}

// The true boot-minus-awake offset is inside this interval. Comparing the
// current lower bound with a saved upper bound cannot invent sleep from the
// time spent making the clock calls, even if the process was descheduled.
type sleepClockOffset struct{ lower, upper int64 }

// leaseWallTime adjusts only a lease comparison, never recorded timestamps.
func leaseWallTime(ctx context.Context, now time.Time) time.Time {
	return now.Add(-currentAttemptClock(ctx).sleepGap)
}

// Include the maximum possible credit only for a decision that could expire
// authority. If measurement fails, both credits are zero and Nodes retain
// their existing wall-based liveness behavior.
func leaseExpiryWallTime(ctx context.Context, now time.Time) time.Time {
	clock := currentAttemptClock(ctx)
	return now.Add(-clock.sleepGap - clock.sleepUncertainty)
}

func initializeLeaseSleepClock(ctx context.Context, q seatApprovalQuery, clock Clock) error {
	sample := currentAttemptClock(withAttemptClock(ctx, clock, clock.Now()))
	if sample.sleepOffset == nil {
		return nil
	}
	// Preserve an existing anchor across reopen, including an unapplied gap.
	_, err := q.ExecContext(ctx, `INSERT OR IGNORE INTO lease_sleep_clock(singleton,boot_id,offset_lo_ns,offset_hi_ns) VALUES(1,?,?,?)`, sample.boot, sample.sleepOffset.lower, sample.sleepOffset.upper)
	return err
}

// withLeaseSleepGap runs at every transaction door after SQLite anchors its view.
// The anchor and deadline changes commit together. A rollback cannot consume a
// gap. Read-only snapshots compare with the pending gap without writing it.
func withLeaseSleepGap(ctx context.Context, tx *sql.Tx, write bool) (context.Context, error) {
	sample := currentAttemptClock(ctx)
	if sample.sleepOffset == nil {
		return ctx, nil
	}
	var boot string
	var anchorLo, anchorHi int64
	err := tx.QueryRowContext(ctx, `SELECT boot_id,offset_lo_ns,offset_hi_ns FROM lease_sleep_clock WHERE singleton=1`).Scan(&boot, &anchorLo, &anchorHi)
	if err != nil && err != sql.ErrNoRows {
		return ctx, err
	}
	gap := int64(0)
	possible := int64(0)
	if err == nil && boot == sample.boot {
		gap = max(0, sample.sleepOffset.lower-anchorHi)
		possible = max(0, sample.sleepOffset.upper-anchorLo)
	}
	// Persist only gap. The remainder can delay an expiry decision, but can
	// neither renew authority nor advance the durable anchor.
	sample.sleepUncertainty = time.Duration(possible - gap)
	if !write {
		sample.ns -= gap
		sample.sleepGap = time.Duration(gap)
		return context.WithValue(ctx, attemptClockKey{}, sample), nil
	}
	if gap > 0 {
		if _, err := tx.ExecContext(ctx, `UPDATE attempts SET
   lease_deadline_ns=CASE WHEN lease_boot_id=? AND lease_deadline_ns>0 THEN lease_deadline_ns+? ELSE lease_deadline_ns END,
   lease_expires_ns=lease_expires_ns+?
   WHERE state IN ('claimed','running','awaiting-input')`, sample.boot, gap, gap); err != nil {
			return ctx, err
		}
		// Node liveness uses last_heartbeat_ns plus fixed stale/dead durations.
		// Credit extends both implicit deadlines without inventing a heartbeat.
		if _, err := tx.ExecContext(ctx, `UPDATE nodes SET heartbeat_sleep_ns=heartbeat_sleep_ns+?
   WHERE state IN ('alive','stale','draining')`, gap); err != nil {
			return ctx, err
		}
	}
	// The anchor bounds the offset already accounted for in deadlines. A
	// positive credit translates both bounds; tightening can only lower hi,
	// since uncredited sleep may have happened after the baseline sample.
	tighten := err == nil && boot == sample.boot && gap == 0 && sample.sleepOffset.upper < anchorHi
	if gap > 0 || tighten || err == sql.ErrNoRows || boot != sample.boot {
		nextLo, nextHi := sample.sleepOffset.lower, sample.sleepOffset.upper
		if err == nil && boot == sample.boot {
			nextLo = anchorLo + gap
			nextHi = anchorHi + gap
			if tighten {
				nextHi = min(anchorHi, sample.sleepOffset.upper)
			}
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO lease_sleep_clock(singleton,boot_id,offset_lo_ns,offset_hi_ns) VALUES(1,?,?,?)
   ON CONFLICT(singleton) DO UPDATE SET boot_id=excluded.boot_id,offset_lo_ns=excluded.offset_lo_ns,offset_hi_ns=excluded.offset_hi_ns`, sample.boot, nextLo, nextHi)
	}
	return context.WithValue(ctx, attemptClockKey{}, sample), err
}
