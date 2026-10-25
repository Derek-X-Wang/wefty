package l1

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// attemptClock pins the host boot and elapsed time alongside the transaction's
// wall timestamp. Only the wall timestamp is exposed in records and replies.
type attemptClock struct {
	boot string
	ns   int64
	err  error
}
type attemptClockKey struct{}

var errAttemptClockMissing = errors.New("l1: missing transaction Attempt clock")

func withAttemptClock(ctx context.Context, clock Clock, now time.Time) context.Context {
	boot, ns, err := seatTime(clock, now)
	if err != nil {
		err = fmt.Errorf("read Attempt lease clock: %w", err)
	}
	// Elapsed sampling failures must not disable unrelated L1 work. Lease
	// operations refuse authority; reconciliation retries lease expiry later.
	return context.WithValue(ctx, attemptClockKey{}, attemptClock{boot, ns, err})
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
	if clock.boot != "" && a.leaseBootID == clock.boot && a.leaseDeadlineNS > 0 {
		return a.leaseDeadlineNS <= clock.ns, nil
	}
	return !now.Before(a.leaseExpires), nil
}

// attemptLeaseLiveSQL is the SQL form of attemptLeaseExpired's single policy.
// Callers supply parameters, including the pinned host boot and elapsed time.
func attemptLeaseLiveSQL(alias, boot, elapsed, wall string) string {
	return `(` + boot + ` IS NOT NULL AND CASE WHEN ` + alias + `.lease_boot_id<>'' AND ` + alias + `.lease_boot_id=` + boot + ` AND ` + alias + `.lease_deadline_ns>0 THEN ` + alias + `.lease_deadline_ns>` + elapsed + ` ELSE ` + alias + `.lease_expires_ns>` + wall + ` END)`
}

func attemptLeaseArguments(ctx context.Context, now time.Time) []any {
	clock := currentAttemptClock(ctx)
	return []any{sql.Named("lease_boot_id", clock.sqlBoot()), sql.Named("lease_boot_ns", clock.ns), sql.Named("now_ns", now.UnixNano())}
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
