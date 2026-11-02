package l1

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/bootclock"
)

// attemptClock pins the host boot and elapsed time alongside the transaction's
// wall timestamp. Replies expose wall timestamps and derived lease liveness.
type attemptClock struct {
	boot             string
	ns               int64
	err              error
	sleepOffset      *sleepClockOffset
	sleepGap         time.Duration
	sleepUncertainty time.Duration
}
type attemptClockKey struct{}

var errAttemptClockMissing = errors.New("l1: missing transaction Attempt clock")
var errAttemptLeaseUncertain = errors.New("l1: Attempt lease expiry is not proved by the sleep clock sample")

// Expiry must be proved even with all possible credit. Authority uses the
// original context instead, which includes only credit already proved.
func withAttemptExpiryClock(ctx context.Context) context.Context {
	clock := currentAttemptClock(ctx)
	clock.ns -= int64(clock.sleepUncertainty)
	clock.sleepGap += clock.sleepUncertainty
	clock.sleepUncertainty = 0
	return context.WithValue(ctx, attemptClockKey{}, clock)
}

func withAttemptClock(ctx context.Context, clock Clock, now time.Time) context.Context {
	source, measuresSleep := clock.(awakeClockSource)
	var before time.Duration
	var beforeErr error
	if measuresSleep {
		before, beforeErr = source.AwakeElapsed()
	}
	boot, ns, err := seatTime(clock, now)
	sample := attemptClock{boot: boot, ns: ns, err: err}
	if measuresSleep && !errors.Is(beforeErr, bootclock.ErrAwakeUnavailable) {
		after, afterErr := source.AwakeElapsed()
		if beforeErr != nil || afterErr != nil {
			sample.err = errors.Join(err, beforeErr, afterErr)
		} else if after < before {
			sample.err = errors.Join(err, errors.New("pausing clock moved backwards"))
		} else if err == nil {
			sample.sleepOffset = &sleepClockOffset{lower: ns - int64(after), upper: ns - int64(before)}
		}
	}
	if sample.err != nil {
		sample.err = fmt.Errorf("read Attempt lease clock: %w", sample.err)
	}
	// Elapsed sampling failures must not disable unrelated L1 work. Lease
	// operations refuse authority; reconciliation retries lease expiry later.
	return context.WithValue(ctx, attemptClockKey{}, sample)
}

// Every Store read or write door must pin this clock. A missing sample is a
// programming error, never permission to grant authority using wall time.
func currentAttemptClock(ctx context.Context) attemptClock {
	value, ok := ctx.Value(attemptClockKey{}).(attemptClock)
	if !ok || value.boot == "" && value.err == nil {
		return attemptClock{err: errAttemptClockMissing}
	}
	return value
}

func attemptClockError(ctx context.Context) error { return currentAttemptClock(ctx).err }

func (clock attemptClock) sqlBoot() any {
	if clock.err != nil {
		return nil
	}
	return clock.boot
}

func attemptLeaseExpired(ctx context.Context, now time.Time, a attemptAuthority) (bool, error) {
	clock := currentAttemptClock(ctx)
	if clock.err != nil {
		if errors.Is(clock.err, errAttemptClockMissing) {
			return false, internalError(clock.err, "read Attempt lease clock")
		}
		// A terminal Attempt cannot regain execution authority. Preserve historical
		// evidence and accepted-completion replay without needing a live clock.
		switch a.state {
		case contract.AttemptClaimed, contract.AttemptRunning, contract.AttemptAwaitingInput:
			return false, internalError(clock.err, "read Attempt lease clock")
		default:
			return !now.Before(a.leaseExpires), nil
		}
	}
	active := a.state == contract.AttemptClaimed || a.state == contract.AttemptRunning || a.state == contract.AttemptAwaitingInput
	if clock.boot != "" && a.leaseBootID == clock.boot && a.leaseDeadlineNS > 0 {
		expired := a.leaseDeadlineNS <= clock.ns
		if expired && active && a.leaseDeadlineNS > clock.ns-int64(clock.sleepUncertainty) {
			return false, internalError(errAttemptLeaseUncertain, "read Attempt lease clock")
		}
		return expired, nil
	}
	comparison := now
	if active {
		comparison = leaseWallTime(ctx, now)
	}
	expired := !comparison.Before(a.leaseExpires)
	if expired && active && leaseExpiryWallTime(ctx, now).Before(a.leaseExpires) {
		return false, internalError(errAttemptLeaseUncertain, "read Attempt lease clock")
	}
	return expired, nil
}

func attemptLeaseLive(ctx context.Context, now time.Time, a attemptAuthority) (bool, error) {
	switch a.state {
	case contract.AttemptClaimed, contract.AttemptRunning, contract.AttemptAwaitingInput:
		expired, err := attemptLeaseExpired(ctx, now, a)
		return !expired && err == nil, err
	default:
		return false, nil
	}
}

// SQL authority checks use proved credit. Expiry and generation-closure
// callers supply withAttemptExpiryClock so possible credit prevents early loss.
func attemptLeaseLiveSQL(alias, boot, elapsed, wall string) string {
	return `(` + boot + ` IS NOT NULL AND CASE WHEN ` + alias + `.lease_boot_id<>'' AND ` + alias + `.lease_boot_id=` + boot + ` AND ` + alias + `.lease_deadline_ns>0 THEN ` + alias + `.lease_deadline_ns>` + elapsed + ` ELSE ` + alias + `.lease_expires_ns>` + wall + ` END)`
}

func attemptLeaseArguments(ctx context.Context, now time.Time) []any {
	clock := currentAttemptClock(ctx)
	return []any{sql.Named("lease_boot_id", clock.sqlBoot()), sql.Named("lease_boot_ns", clock.ns), sql.Named("now_ns", leaseWallTime(ctx, now).UnixNano())}
}

func attemptLeaseDeadline(ctx context.Context, now, expires time.Time) (string, int64) {
	clock := currentAttemptClock(ctx)
	if clock.err != nil || clock.boot == "" {
		return "", 0
	}
	return clock.boot, clock.ns + int64(expires.Sub(now))
}

func (s *Store) migrateAttemptLeaseClock(ctx context.Context) error {
	if err := s.ensureColumn(ctx, "attempts", "lease_boot_id", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "attempts", "lease_deadline_ns", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS attempts_boot_lease_expiry ON attempts(state, lease_boot_id, lease_deadline_ns)`)
	return err
}
