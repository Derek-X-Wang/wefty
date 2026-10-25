package l1

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/bootclock"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
)

// Identity cannot change while L1 lives. Keep its fallible read at OpenStore;
// only the independent elapsed source is sampled during transactions. Seats
// and Attempts both receive this same cached Store clock.
type bootClockSource interface {
	BootIdentity() (string, error)
	BootElapsed() (time.Duration, error)
}

func (systemClock) BootIdentity() (string, error)       { return hostboot.Identity() }
func (systemClock) BootElapsed() (time.Duration, error) { return bootclock.Now() }

type cachedBootClock struct {
	Clock
	boot   string
	source bootClockSource
}

func (c cachedBootClock) BootTime() (string, time.Duration, error) {
	elapsed, err := c.source.BootElapsed()
	return c.boot, elapsed, err
}

func (c cachedBootClock) NewTimer(duration time.Duration) clockTimer {
	return newClockTimer(c.Clock, duration)
}

func cacheStoreBootIdentity(clock Clock) (Clock, error) {
	source, ok := clock.(bootClockSource)
	if !ok {
		// Simple injected clocks retain their one controlled timeline.
		return clock, nil
	}
	boot, err := source.BootIdentity()
	if err != nil {
		return nil, fmt.Errorf("l1: read host boot identity at OpenStore: %w", err)
	}
	if boot == "" {
		return nil, fmt.Errorf("l1: empty host boot identity at OpenStore")
	}
	return cachedBootClock{Clock: clock, boot: boot, source: source}, nil
}

func seatTime(clock Clock, snapshot ...time.Time) (string, int64, error) {
	if c, ok := clock.(interface {
		BootTime() (string, time.Duration, error)
	}); ok {
		boot, ns, err := c.BootTime()
		return boot, int64(ns), err
	}
	// Existing injected clocks have one controlled timeline. Tests that model
	// independent calendar changes supply BootTime as well.
	if len(snapshot) > 0 {
		return "injected-clock", snapshot[0].UnixNano(), nil
	}
	return "injected-clock", clock.Now().UnixNano(), nil
}

func (s *Store) migrateSeatLife(ctx context.Context) error {
	for _, table := range []string{"seat_approvals", "seat_approval_windows", "computers"} {
		cols := []struct{ name, definition string }{{"wait_boot_id", "TEXT NOT NULL DEFAULT ''"}, {"wait_started_ns", "INTEGER NOT NULL DEFAULT 0"}}
		if table == "seat_approval_windows" {
			cols = append(cols, struct{ name, definition string }{"wait_duration_ns", "INTEGER NOT NULL DEFAULT 0"})
		}
		if table == "seat_approvals" {
			cols = append(cols, struct{ name, definition string }{"ended_computer_revision", "INTEGER NOT NULL DEFAULT 0"}, struct{ name, definition string }{"activity_boot_id", "TEXT NOT NULL DEFAULT ''"}, struct{ name, definition string }{"activity_ns", "INTEGER NOT NULL DEFAULT 0"}, struct{ name, definition string }{"warning_sent", "INTEGER NOT NULL DEFAULT 0 CHECK(warning_sent IN (0,1))"}, struct{ name, definition string }{"activity_revision", "INTEGER NOT NULL DEFAULT 0"})
		}
		for _, col := range cols {
			if err := s.ensureColumn(ctx, table, col.name, col.definition); err != nil {
				return err
			}
		}
	}
	_, err := s.db.ExecContext(ctx, `DROP TABLE IF EXISTS seat_clock_anchor;
 CREATE TABLE IF NOT EXISTS seat_activity_holds (
 seat_id TEXT NOT NULL REFERENCES seat_approvals(seat_id) ON DELETE CASCADE,
 kind TEXT NOT NULL CHECK(kind IN ('command','person')),
 hold_id TEXT NOT NULL,
 active INTEGER NOT NULL CHECK(active IN (0,1)),
 PRIMARY KEY(seat_id,kind,hold_id)
 );
 CREATE TABLE IF NOT EXISTS seat_copy_holds (
 seat_id TEXT NOT NULL REFERENCES seat_approvals(seat_id) ON DELETE CASCADE,
 copy_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id) ON DELETE CASCADE,
 boot_id TEXT NOT NULL,
 lease_until_ns INTEGER NOT NULL,
 PRIMARY KEY(seat_id,copy_id)
 );`)
	if err != nil {
		return err
	}
	if err := recoverSeatApprovalClocks(ctx, s.db, s.clock, s.logf); err != nil {
		s.logSeatLifeError("approval_clock_recovery", err)
	}
	return nil
}

func touchSeatActivity(ctx context.Context, q seatApprovalQuery, clock Clock, id string) error {
	boot, ns, err := seatTime(clock)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `UPDATE seat_approvals SET activity_boot_id=?,activity_ns=?,warning_sent=0,activity_revision=activity_revision+1 WHERE seat_id=? AND lifecycle_state='live'`, boot, ns, id)
	return err
}

func seatApprovalRemaining(ctx context.Context, q queryer, clock Clock, a SeatApproval) (time.Duration, error) {
	var boot string
	var started int64
	if err := q.QueryRowContext(ctx, `SELECT wait_boot_id,wait_started_ns FROM seat_approvals WHERE seat_id=?`, a.Request.SeatID).Scan(&boot, &started); err != nil {
		return 0, err
	}
	current, ns, err := seatTime(clock)
	if err != nil {
		return 0, err
	}
	if current != boot {
		return a.Request.AnswerWait, nil
	}
	return max(0, a.Request.AnswerWait-time.Duration(max(0, ns-started))), nil
}

// A new L1 boot may restart bounded pending answers. Existing approval
// windows are closed so a reboot cannot extend an owner grant past its cap.
func recoverSeatApprovalClocks(ctx context.Context, q seatApprovalQuery, clock Clock, loggers ...func(string, ...any)) error {
	var any bool
	if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM seat_approvals WHERE state='requested' AND wait_boot_id!='' UNION ALL SELECT 1 FROM seat_approval_windows WHERE wait_boot_id!='')`).Scan(&any); err != nil {
		return err
	}
	if !any {
		return nil
	}
	boot, ns, err := seatTime(clock)
	if err != nil {
		return err
	}
	now := clock.Now().UnixNano()
	if _, err = q.ExecContext(ctx, `UPDATE seat_approvals SET wait_boot_id=?,wait_started_ns=?,deadline_ns=?+json_extract(CAST(request_json AS TEXT),'$.answer_wait') WHERE state='requested' AND wait_boot_id!='' AND wait_boot_id!=?`, boot, ns, now, boot); err != nil {
		return err
	}
	result, err := q.ExecContext(ctx, `DELETE FROM seat_approval_windows WHERE wait_boot_id!='' AND wait_boot_id!=?`, boot)
	if err != nil {
		return err
	}
	closed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	logger := log.Printf
	if len(loggers) > 0 {
		logger = loggers[0]
	}
	if closed > 0 && logger != nil {
		logger("event=seat_approval_windows_closed reason=l1_host_restarted count=%d", closed)
	}
	return nil
}
