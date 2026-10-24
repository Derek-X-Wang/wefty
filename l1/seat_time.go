package l1

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/bootclock"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
)

// BootTime is an optional extension of the existing injected Clock. Durable
// Seat waits store both values; a different boot cannot reuse an old deadline.
func (systemClock) BootTime() (string, time.Duration, error) {
	id, err := hostboot.Identity()
	if err != nil {
		return "", 0, err
	}
	ns, err := bootclock.Now()
	return id, ns, err
}

func seatTime(clock Clock) (string, int64, error) {
	if c, ok := clock.(interface {
		BootTime() (string, time.Duration, error)
	}); ok {
		boot, ns, err := c.BootTime()
		return boot, int64(ns), err
	}
	// Existing injected clocks have one controlled timeline. Tests that model
	// independent calendar changes supply BootTime as well.
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
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS seat_clock_anchor (singleton INTEGER PRIMARY KEY CHECK(singleton=1),boot_id TEXT NOT NULL,wall_offset_ns INTEGER NOT NULL);
 CREATE TABLE IF NOT EXISTS seat_activity_holds (
 seat_id TEXT NOT NULL REFERENCES seat_approvals(seat_id) ON DELETE CASCADE,
 kind TEXT NOT NULL CHECK(kind IN ('command','person')),
 hold_id TEXT NOT NULL,
 active INTEGER NOT NULL CHECK(active IN (0,1)),
 PRIMARY KEY(seat_id,kind,hold_id)
 );`)
	if err != nil {
		return err
	}
	return s.loadSeatClockAnchor(ctx)
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
	if current != boot || ns < started {
		return 0, nil
	}
	return max(0, a.Request.AnswerWait-time.Duration(ns-started)), nil
}

// The calendar-label anchor is durable too: an L1 restart cannot reinterpret
// existing lease timestamps after the owner changes the calendar clock.
func (s *Store) loadSeatClockAnchor(ctx context.Context) error {
	if _, ok := s.clock.(systemClock); !ok {
		return nil
	}
	var boot string
	var offset int64
	err := s.db.QueryRowContext(ctx, `SELECT boot_id,wall_offset_ns FROM seat_clock_anchor WHERE singleton=1`).Scan(&boot, &offset)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	current, _, err := seatTime(s.clock)
	if err != nil {
		return err
	}
	if boot == current {
		s.clock = systemClock{offset: &offset}
	}
	return nil
}
func persistSeatClockAnchor(ctx context.Context, q seatApprovalQuery, clock Clock) error {
	if _, ok := clock.(systemClock); !ok {
		return nil
	}
	boot, ns, err := seatTime(clock)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `INSERT INTO seat_clock_anchor(singleton,boot_id,wall_offset_ns) VALUES(1,?,?) ON CONFLICT(singleton) DO UPDATE SET boot_id=excluded.boot_id,wall_offset_ns=excluded.wall_offset_ns WHERE boot_id!=excluded.boot_id`, boot, clock.Now().UnixNano()-ns)
	return err
}
