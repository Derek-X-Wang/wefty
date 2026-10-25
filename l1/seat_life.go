package l1

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const (
	DefaultSeatIdleWait                   = 30 * time.Minute
	DefaultSeatWarningWait                = 5 * time.Minute
	DefaultComputerIdleWait               = 10 * time.Minute
	SeatEventEndsSoon       SeatEventKind = "ends_soon"
	SeatEventOwnerReimage   SeatEventKind = "owner_reimage"
	SeatEventOwnerReset     SeatEventKind = "owner_reset"
	SeatEventOwnerRemove    SeatEventKind = "owner_remove"
)

// CheckSeatLife uses the same fence as Seat admission and owner mutations.
// Helper cleanup and memory readback remain the only way room can return.
func (s *Store) CheckSeatLife(ctx context.Context) error {
	if err := s.recoverSeatRunEnds(ctx); err != nil {
		s.logSeatLifeError("command_end_recovery", err)
	}
	if err := func() error {
		w, err := s.beginWriteTransaction(ctx, nil)
		if err != nil {
			return err
		}
		defer w.rollback()
		ctx = w.ctx
		if err = recoverSeatApprovalClocks(ctx, w.tx, s.clock, s.logf); err != nil {
			return err
		}
		return w.commit()
	}(); err != nil {
		s.logSeatLifeError("approval_clock_recovery", err)
	}
	var seats, computers []string
	err := s.withReadSnapshot(ctx, nil, func(ctx context.Context, r readModel) error {
		q := r.(*databaseReads).q
		var err error
		seats, err = seatIDs(ctx, q, `SELECT seat_id FROM seat_approvals WHERE lifecycle_state='live'`)
		if err != nil {
			return err
		}
		computers, err = seatIDs(ctx, q, `SELECT computer_id FROM computers WHERE seat_request_made=1 AND desired_state='running' AND idle_action='delete' AND EXISTS(SELECT 1 FROM computer_seat_identities i WHERE i.computer_id=computers.computer_id) ORDER BY computer_id`)
		return err
	})
	if err != nil {
		s.logSeatLifeError("inventory", err)
		return nil
	}
	for _, id := range seats {
		err := func() error {
			w, err := s.beginWriteTransaction(ctx, nil)
			if err != nil {
				return err
			}
			defer w.rollback()
			ctx = w.ctx
			if err = s.checkSeatLifeTxIfLive(ctx, w.tx, id, w.at); err != nil {
				return err
			}
			return w.commit()
		}()
		if err != nil {
			s.logSeatLifeError(id, err)
			continue
		}
	}
	for _, id := range computers {
		err := s.checkComputerIdle(ctx, id)
		if err != nil {
			s.logSeatLifeError(id, err)
			continue
		}
	}
	return nil
}

func (s *Store) logSeatLifeError(target string, err error) {
	if s.logf == nil {
		return
	}
	code := errorCode(err)
	if code == "" {
		code = contract.ErrorInternal
	}
	s.logf("event=seat_life_retry target=%q code=%s action=retry_next_tick error=%q", target, code, err.Error())
}

func (s *Store) checkComputerIdle(ctx context.Context, id string) error {
	var computer Computer
	due, err := func() (bool, error) {
		w, err := s.beginWriteTransaction(ctx, nil)
		if err != nil {
			return false, err
		}
		defer w.rollback()
		ctx = w.ctx
		due, err := s.computerIdleDue(ctx, w.tx, id, true)
		if err != nil {
			return false, err
		}
		if due {
			computer, err = readComputerAuthority(ctx, w.tx, id, w.at)
			if err != nil {
				return false, err
			}
		}
		return due, w.commit()
	}()
	if err != nil || !due {
		return err
	}
	if err = s.reconfigurationCheckpoint("computer_idle_selected"); err != nil {
		return err
	}
	_, err = s.RemoveComputer(ctx, id, ComputerRemoveRequest{ComputerMutationPrecondition: ComputerMutationPrecondition{IntentRevision: computer.IntentRevision, StorageID: computer.StorageID, StorageGeneration: computer.StorageGeneration, Actor: "idle action"}, idleOnly: true})
	// An owner mutation, another Seat, or another removal may win the fence.
	switch errorCode(err) {
	case contract.ErrorConflict, "stale_intent_revision", "storage_reference_conflict", contract.ErrorNotFound:
		s.logSeatLifeError(id, err)
		return nil
	}
	if err != nil {
		return err
	}
	return s.reconfigurationCheckpoint("computer_idle_started")
}

// A read-only decision is shared by follow authority checks and fenced writes.
// Only a real warning, expiry or upgrade initialization needs the write door.
type seatLifeChange struct {
	initialize bool
	warn       bool
	revision   int64
	endReason  string
}

func (s *Store) planSeatLife(ctx context.Context, q queryer, id string) (seatLifeChange, error) {
	var boot string
	var last, revision int64
	var warned, held bool
	if err := q.QueryRowContext(ctx, `SELECT activity_boot_id,activity_ns,warning_sent,activity_revision,EXISTS(SELECT 1 FROM seat_activity_holds WHERE seat_id=a.seat_id AND active=1) FROM seat_approvals a WHERE seat_id=?`, id).Scan(&boot, &last, &warned, &revision, &held); err != nil {
		return seatLifeChange{}, err
	}
	current, ns, err := seatTime(s.clock)
	if err != nil {
		return seatLifeChange{}, err
	}
	if boot == "" || boot != current {
		return seatLifeChange{initialize: true}, nil
	}
	var copyUntil int64
	if err := q.QueryRowContext(ctx, `SELECT COALESCE(MAX(lease_until_ns),0) FROM seat_copy_holds WHERE seat_id=? AND boot_id=?`, id, current).Scan(&copyUntil); err != nil {
		return seatLifeChange{}, err
	}
	if held || copyUntil > ns {
		return seatLifeChange{}, nil
	}
	last = max(last, copyUntil)
	elapsed := time.Duration(max(0, ns-last))
	change := seatLifeChange{revision: revision, warn: !warned && elapsed >= s.seatIdleWait-s.seatWarningWait}
	if elapsed >= s.seatIdleWait {
		change.endReason = "expired"
	}
	return change, nil
}
func (change seatLifeChange) needsWrite() bool {
	return change.initialize || change.warn || change.endReason != ""
}
func (s *Store) checkSeatLifeTx(ctx context.Context, q seatApprovalQuery, id string, at time.Time) error {
	change, err := s.planSeatLife(ctx, q, id)
	if err != nil {
		return err
	}
	if change.initialize {
		return touchSeatActivity(ctx, q, s.clock, id)
	}
	if change.warn {
		if err := appendSeatEvent(ctx, q, at, SeatLogEvent{SeatID: id, EventID: fmt.Sprintf("ends-soon-%d", change.revision), Kind: SeatEventEndsSoon, Reason: "Seat ends soon"}); err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `UPDATE seat_approvals SET warning_sent=1 WHERE seat_id=?`, id); err != nil {
			return err
		}
	}
	if change.endReason == "" {
		return nil
	}
	value, err := readSeatRequest(ctx, q, id)
	if err != nil {
		return err
	}
	if err := setSeatLifecycle(ctx, q, id, SeatEnding, change.endReason, at); err != nil {
		return err
	}
	return removeLifecycleSeat(ctx, q, value)
}

// computerIdleDue rechecks current settings on every pass. Admission clears the
// stamp, and RemoveComputer repeats the check inside its own write fence.
func (s *Store) computerIdleDue(ctx context.Context, q seatApprovalQuery, id string, initialize bool) (bool, error) {
	var boot, desired, action string
	var started int64
	var busy bool
	err := q.QueryRowContext(ctx, `SELECT wait_boot_id,wait_started_ns,desired_state,idle_action,EXISTS(SELECT 1 FROM computer_seat_identities i JOIN seat_approvals a ON a.seat_id=i.seat_id WHERE i.computer_id=c.computer_id AND COALESCE(NULLIF(a.lifecycle_state,''),a.state) IN ('requested','approved','starting','live')) FROM computers c WHERE computer_id=?`, id).Scan(&boot, &started, &desired, &action, &busy)
	if err != nil {
		return false, err
	}
	if busy || desired != "running" || action != "delete" {
		_, err = q.ExecContext(ctx, `UPDATE computers SET wait_boot_id='',wait_started_ns=0 WHERE computer_id=? AND (wait_boot_id!='' OR wait_started_ns!=0)`, id)
		return false, err
	}
	current, ns, err := seatTime(s.clock)
	if err != nil {
		return false, err
	}
	if boot == "" || boot != current {
		if initialize {
			_, err = q.ExecContext(ctx, `UPDATE computers SET wait_boot_id=?,wait_started_ns=? WHERE computer_id=?`, current, ns, id)
		}
		return false, err
	}
	return time.Duration(max(0, ns-started)) >= s.computerIdleWait, nil
}

// SeatCallHappened is the shared one-time credential call seam. A log follow
// invokes it only at start; cursor reads and follow polls do not invoke it.
func (s *Store) SeatCallHappened(ctx context.Context, id, secret string) error {
	w, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return err
	}
	defer w.rollback()
	ctx = w.ctx
	if err := s.authorizeLiveSeat(ctx, w.tx, id, secret, w.at); err != nil {
		if errorCode(err) == "seat_ended" {
			if e := w.commit(); e != nil {
				return e
			}
		}
		return err
	}
	if err := s.seatCallHappenedTx(ctx, w.tx, id); err != nil {
		return err
	}
	return w.commit()
}
func (s *Store) seatCallHappenedTx(ctx context.Context, q seatApprovalQuery, id string) error {
	return touchSeatActivity(ctx, q, s.clock, id)
}
func (s *Store) authorizeLiveSeat(ctx context.Context, q seatApprovalQuery, id, secret string, at time.Time) error {
	var digest string
	if err := q.QueryRowContext(ctx, `SELECT credential_hash FROM seat_logs WHERE seat_id=?`, id).Scan(&digest); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return protocolError(contract.ErrorForbidden, "invalid Seat credential")
		}
		return err
	}
	if secret == "" || digest != seatCredentialHash(secret) {
		return protocolError(contract.ErrorForbidden, "invalid Seat credential")
	}
	if err := s.checkSeatLifeTxIfLive(ctx, q, id, at); err != nil {
		return err
	}
	value, err := readSeatRequest(ctx, q, id)
	if err != nil {
		return err
	}
	if value.State != SeatLive {
		return protocolError(contract.ErrorCode("seat_ended"), "Seat ended: %s", value.Reason)
	}
	return nil
}
func (s *Store) checkSeatLifeTxIfLive(ctx context.Context, q seatApprovalQuery, id string, at time.Time) error {
	value, err := readSeatRequest(ctx, q, id)
	if err != nil {
		return err
	}
	if value.State == SeatLive {
		return s.checkSeatLifeTx(ctx, q, id, at)
	}
	return nil
}

// SeatCommandRunning is the trusted command backend seam (#1150). Its stable
// command ID makes repeated starts/ends harmless. An ended command cannot
// restart, and a late end after Seat release cannot resurrect its authority.
func (s *Store) SeatCommandRunning(ctx context.Context, id, command string, running bool) error {
	if command == "" || len(command) > 255 {
		return protocolError(contract.ErrorInvalidRequest, "command ID is required")
	}
	return s.seatActivityHold(ctx, id, "command", command, running)
}

// SeatPersonDriving is the trusted tenure backend seam (#1147), never a
// credential route. Only releasing a held screen advances the idle origin.
func (s *Store) SeatPersonDriving(ctx context.Context, id string, driving bool) error {
	return s.seatActivityHold(ctx, id, "person", "screen", driving)
}
func (s *Store) seatActivityHold(ctx context.Context, id, kind, key string, active bool) error {
	w, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return err
	}
	defer w.rollback()
	ctx = w.ctx
	if err := s.seatActivityHoldTx(ctx, w.tx, w.at, id, kind, key, active); err != nil {
		if errorCodeForSeatLifecycle(err) == contract.ErrorCode("seat_ended") {
			if commitErr := w.commit(); commitErr != nil {
				return commitErr
			}
		}
		return err
	}
	return w.commit()
}

// All command backends share the same hold transition, also within admission.
func (s *Store) seatActivityHoldTx(ctx context.Context, tx *sql.Tx, at time.Time, id, kind, key string, active bool) error {
	value, err := readSeatRequest(ctx, tx, id)
	if err != nil {
		return err
	}
	if value.State != SeatLive {
		if !active {
			if _, err := tx.ExecContext(ctx, `UPDATE seat_activity_holds SET active=0 WHERE seat_id=? AND kind=? AND hold_id=?`, id, kind, key); err != nil {
				return err
			}
			return nil
		}
		return protocolError(contract.ErrorCode("seat_ended"), "Seat ended: %s", value.Reason)
	}
	// Check expiry before a new hold can acquire authority. Release is always
	// accepted while the old hold still protects the Seat.
	if active {
		if err := s.checkSeatLifeTx(ctx, tx, id, at); err != nil {
			return err
		}
		value, err = readSeatRequest(ctx, tx, id)
		if err != nil {
			return err
		}
		if value.State != SeatLive {
			return protocolError(contract.ErrorCode("seat_ended"), "Seat ended: %s", value.Reason)
		}
	}
	var old bool
	err = tx.QueryRowContext(ctx, `SELECT active FROM seat_activity_holds WHERE seat_id=? AND kind=? AND hold_id=?`, id, kind, key).Scan(&old)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil && kind == "command" && !old && active {
		return protocolError(contract.ErrorConflict, "command already ended")
	}
	if err == nil && old == active {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) && !active && kind == "person" {
		return nil
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO seat_activity_holds(seat_id,kind,hold_id,active) VALUES(?,?,?,?) ON CONFLICT(seat_id,kind,hold_id) DO UPDATE SET active=excluded.active`, id, kind, key, active); err != nil {
		return err
	}
	if !active {
		if err := touchSeatActivity(ctx, tx, s.clock, id); err != nil {
			return err
		}
	}
	return nil
}

// SeatCommandsToCancel is a durable cancellation seam for #1150. Consumers
// cancel every returned command and report its end through SeatCommandRunning.
// Repeated reads retain outstanding cancellations across backend restarts.
func (s *Store) SeatCommandsToCancel(ctx context.Context, id string) ([]string, error) {
	var commands []string
	err := s.withReadSnapshot(ctx, nil, func(ctx context.Context, r readModel) error {
		var err error
		commands, err = seatIDs(ctx, r.(*databaseReads).q, `SELECT h.hold_id FROM seat_activity_holds h JOIN seat_approvals a ON a.seat_id=h.seat_id WHERE h.seat_id=? AND h.kind='command' AND h.active=1 AND a.lifecycle_state IN ('ending','ended','refused') ORDER BY h.hold_id`, id)
		return err
	})
	return commands, err
}
