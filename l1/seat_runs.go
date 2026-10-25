package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const seatRunSchema = `CREATE TABLE IF NOT EXISTS seat_runs (
 run_id TEXT PRIMARY KEY,
 job_id TEXT NOT NULL UNIQUE REFERENCES jobs(job_id) ON DELETE CASCADE,
 seat_id TEXT NOT NULL,
 params_json BLOB NOT NULL,
 computer_attempt_id TEXT REFERENCES attempts(attempt_id) ON DELETE SET NULL,
 command_attempt_id TEXT REFERENCES attempts(attempt_id) ON DELETE SET NULL,
 state TEXT NOT NULL CHECK(state IN ('queued','running','finished')),
 result_json BLOB
);`

// ResolveSeatRun proves the Seat credential. The resulting public binding may
// be kept in immutable Run params; credentials and helper fences may not.
func (s *Store) ResolveSeatRun(ctx context.Context, id, credential string) (binding contract.SeatRunBinding, err error) {
	if err = s.SeatCallHappened(ctx, id, credential); err != nil {
		return binding, err
	}
	err = s.withReadSnapshot(ctx, nil, func(ctx context.Context, r readModel) error {
		rd := r.(*databaseReads)
		value, e := checkSeatCopyCredential(ctx, rd.q, id, credential, true)
		if e != nil {
			return e
		}
		_, attempt, e := seatCopyBinding(ctx, rd.q, rd.now(), id, value)
		if e != nil {
			return e
		}
		a, e := readAttemptAuthority(ctx, rd.q, attempt)
		if e != nil {
			return e
		}
		binding = contract.SeatRunBinding{DefaultMaxRuntimeSeconds: int64(s.seatCommandMaxRuntime / time.Second), SeatID: id, NodeID: a.nodeID, ComputerID: value.ComputerID, ComputerJobID: a.jobID, ComputerAttemptID: attempt}
		return nil
	})
	return
}
func currentSeatRunBinding(ctx context.Context, q queryer, now time.Time, p contract.SeatRunParams) (SeatCopyAuthority, attemptAuthority, error) {
	value, err := readSeatRequest(ctx, q, p.SeatRun.SeatID)
	if err != nil {
		return SeatCopyAuthority{}, attemptAuthority{}, err
	}
	if value.State != SeatLive || value.ComputerID != p.Binding.ComputerID {
		return SeatCopyAuthority{}, attemptAuthority{}, protocolError(contract.ErrorCode("seat_ended"), "Seat ended or changed Computer")
	}
	seat, attempt, err := seatCopyBinding(ctx, q, now, p.SeatRun.SeatID, value)
	if err != nil {
		return seat, attemptAuthority{}, err
	}
	a, err := readAttemptAuthority(ctx, q, attempt)
	if err != nil {
		return seat, a, err
	}
	b := p.Binding
	if attempt != b.ComputerAttemptID || a.nodeID != b.NodeID || a.jobID != b.ComputerJobID {
		return seat, a, protocolError(contract.ErrorForbidden, "Seat Run belongs to another Attempt")
	}
	return seat, a, nil
}
func admitSeatRun(ctx context.Context, tx *sql.Tx, now time.Time, job Job, trusted bool) error {
	p, present, err := contract.ParseSeatRunParams([]byte(job.Spec.Labels[contract.LabelRunParams]))
	if err != nil {
		return protocolError(contract.ErrorInvalidRequest, "invalid Seat Run: %v", err)
	}
	if !present {
		return nil
	}
	if !trusted || job.Spec.Kind != contract.JobKindProcess || job.Spec.Class != contract.JobClassOneShot || job.Spec.Restartable || job.Spec.Labels["run_id"] == "" || job.Spec.Labels[contract.LabelDispatchAuthority] == contract.LabelTrue {
		return protocolError(contract.ErrorForbidden, "Seat Runs require a non-restartable Run from the run ledger without dispatch authority")
	}
	if err = validateSeatRunAdmission(ctx, tx, now, p); err != nil {
		return err
	}
	pinned := false
	for _, tag := range job.Spec.RoutingTags {
		if tag == contract.StableNodeTagPrefix+p.Binding.NodeID {
			pinned = true
		}
	}
	if !pinned {
		return protocolError(contract.ErrorInvalidRequest, "Seat Run must be Pinned to its Seat's Node")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO seat_runs(run_id,job_id,seat_id,params_json,computer_attempt_id,state) VALUES(?,?,?,?,?,'queued')`, job.Spec.Labels["run_id"], job.JobID, p.SeatRun.SeatID, []byte(job.Spec.Labels[contract.LabelRunParams]), p.Binding.ComputerAttemptID)
	return err
}

// Job submission reads retained Seat execution identity, never the node-only
// approval protocol. Start still rechecks lifecycle and expiry atomically.
func validateSeatRunAdmission(ctx context.Context, q queryer, now time.Time, p contract.SeatRunParams) error {
	var place int
	b := p.Binding
	err := q.QueryRowContext(ctx, `SELECT w.place FROM computer_wanted_seats w
 JOIN computer_seat_identities i ON i.seat_id=w.seat_id
 JOIN seat_logs l ON l.seat_id=w.seat_id
 WHERE w.seat_id=? AND w.computer_id=? AND i.first_attempt_id=? AND l.ended_ns IS NULL`, b.SeatID, b.ComputerID, b.ComputerAttemptID).Scan(&place)
	if errors.Is(err, sql.ErrNoRows) {
		return protocolError(contract.ErrorCode("seat_ended"), "Seat is no longer available")
	}
	if err != nil {
		return internalError(err, "read Seat Run admission")
	}
	_, attempt, err := seatCopyBinding(ctx, q, now, b.SeatID, SeatRequest{ComputerID: b.ComputerID, Place: place})
	if err != nil {
		return err
	}
	a, err := readAttemptAuthority(ctx, q, attempt)
	if err != nil {
		return err
	}
	if attempt != b.ComputerAttemptID || a.nodeID != b.NodeID || a.jobID != b.ComputerJobID {
		return protocolError(contract.ErrorForbidden, "Seat Run belongs to another Attempt")
	}
	return nil
}

type SeatRunControlRequest struct {
	NodeID        string         `json:"node_id"`
	BootSessionID string         `json:"boot_session_id"`
	FencingToken  string         `json:"fencing_token"`
	Phase         string         `json:"phase"`
	Result        *ProcessResult `json:"result,omitempty"`
}
type SeatRunCommand struct {
	RunID   string                  `json:"run_id"`
	Binding contract.SeatRunBinding `json:"binding"`
	ComputerSeatAttemptRequest
	Generation uint64 `json:"generation"`
	Place      int    `json:"place"`
	Cancel     bool   `json:"cancel"`
}

func (s *Store) ControlSeatRun(ctx context.Context, identity, jobID, attemptID string, input SeatRunControlRequest) (command SeatRunCommand, err error) {
	command, err = s.controlSeatRun(ctx, identity, jobID, attemptID, input)
	if err != nil || input.Phase != "poll" {
		return
	}
	commands, e := s.SeatCommandsToCancel(ctx, command.Binding.SeatID)
	if e != nil {
		return command, e
	}
	for _, id := range commands {
		if id == command.RunID {
			command.Cancel = true
		}
	}
	return
}
func (s *Store) controlSeatRun(ctx context.Context, identity, jobID, attemptID string, input SeatRunControlRequest) (command SeatRunCommand, err error) {
	if input.Phase != "start" && input.Phase != "poll" && input.Phase != "finish" {
		return command, protocolError(contract.ErrorInvalidRequest, "invalid Seat Run phase")
	}
	if (input.Phase == "finish") != (input.Result != nil) {
		return command, protocolError(contract.ErrorInvalidRequest, "only finish requires a result")
	}
	w, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return command, err
	}
	defer w.rollback()
	ctx = w.ctx
	a, err := readAttemptAuthority(ctx, w.tx, attemptID)
	if err != nil {
		return command, err
	}
	if err = validateAttemptEvidence(identity, jobID, attemptID, input.FencingToken, a); err != nil {
		return command, err
	}
	if a.nodeID != input.NodeID || a.bootSessionID != input.BootSessionID {
		return command, protocolError(contract.ErrorForbidden, "Seat Run node mismatch")
	}
	var runID, state string
	var commandAttempt sql.NullString
	var previous, params []byte
	if err = w.tx.QueryRowContext(ctx, `SELECT run_id,state,command_attempt_id,result_json,params_json FROM seat_runs WHERE job_id=?`, jobID).Scan(&runID, &state, &commandAttempt, &previous, &params); err != nil {
		return command, err
	}
	p, present, err := contract.ParseSeatRunParams(params)
	if err != nil || !present {
		return command, protocolError(contract.ErrorInvalidRequest, "Attempt is not a Seat Run")
	}
	command.Binding = p.Binding
	command.RunID = runID
	if commandAttempt.Valid && commandAttempt.String != attemptID {
		return command, protocolError(contract.ErrorAttemptMismatch, "Seat Run command belongs to another Attempt")
	}
	if input.Phase == "finish" {
		if err = validateProcessResult(*input.Result); err != nil {
			return command, err
		}
		if input.Result.LimitReached != "" && input.Result.ExitCode != nil {
			return command, protocolError(contract.ErrorInvalidRequest, "a stopped Seat Run has no exit code")
		}
		if !commandAttempt.Valid {
			return command, protocolError(contract.ErrorConflict, "Seat Run has not started")
		}
		encoded, e := json.Marshal(input.Result)
		if e != nil {
			return command, e
		}
		if state == "finished" {
			if string(previous) != string(encoded) {
				return command, protocolError(contract.ErrorConflict, "Seat Run already finished with another result")
			}
			return command, w.commit()
		}
		if _, err = w.tx.ExecContext(ctx, `UPDATE seat_runs SET state='finished',result_json=? WHERE run_id=?`, encoded, runID); err != nil {
			return command, err
		}
		if err = s.seatActivityHoldTx(ctx, w.tx, w.at, p.SeatRun.SeatID, "command", runID, false); err != nil {
			return command, err
		}
		reason, note := "exited", ""
		if input.Result.Signal != "" {
			reason = "signal"
			note = "command stopped by signal " + input.Result.Signal
		}
		if input.Result.LimitReached == contract.LimitReachedMaxRuntime {
			reason = "max_runtime"
			note = fmt.Sprintf("stopped: reached its %s limit. Run it again with a longer --max-runtime.", time.Duration(p.SeatRun.MaxRuntimeSeconds)*time.Second)
		} else if input.Result.ExitCode == nil && input.Result.TerminationCause != contract.TerminationCauseSpontaneous {
			reason = "cancelled_or_failed"
			note = "command has no exit code; inspect its Run for cancellation or failure details"
		}
		if err = appendSeatEvent(ctx, w.tx, w.at, SeatLogEvent{EventID: runID + "-finished", SeatID: p.SeatRun.SeatID, Kind: SeatEventCommandFinished, RunID: runID, ExitCode: input.Result.ExitCode, Reason: reason, Note: note}); err != nil {
			return command, err
		}
		return command, w.commit()
	}
	if err = validateAttemptAuthority(identity, jobID, attemptID, input.FencingToken, a); err != nil {
		return command, err
	}
	if _, err = validateComputerSeatAttemptForRun(ctx, w.tx, w.at, a); err != nil {
		return command, err
	}
	if state == "finished" {
		return command, protocolError(contract.ErrorConflict, "Seat Run already ended")
	}
	seat, computer, e := currentSeatRunBinding(ctx, w.tx, w.at, p)
	if e != nil && input.Phase == "poll" {
		var protocol *Error
		if !errors.As(e, &protocol) {
			return command, internalError(e, "verify Seat Run binding")
		}
		if apiErrorFromDecision(e).Retryable {
			return command, e
		}
		command.Cancel = true
		return command, w.commit()
	}
	if e != nil {
		return command, e
	}
	command.ComputerSeatAttemptRequest = ComputerSeatAttemptRequest{NodeID: computer.nodeID, BootSessionID: computer.bootSessionID, FencingToken: computer.fencingToken}
	command.Generation, command.Place = seat.Generation, seat.Place
	if input.Phase == "start" {
		if state == "running" {
			return command, protocolError(contract.ErrorConflict, "Seat Run command already started")
		}
		if err = s.seatActivityHoldTx(ctx, w.tx, w.at, p.SeatRun.SeatID, "command", runID, true); err != nil {
			return command, err
		}
		if _, err = w.tx.ExecContext(ctx, `UPDATE seat_runs SET state='running',command_attempt_id=? WHERE run_id=?`, attemptID, runID); err != nil {
			return command, err
		}
		if err = appendSeatEvent(ctx, w.tx, w.at, SeatLogEvent{EventID: runID + "-started", SeatID: p.SeatRun.SeatID, Kind: SeatEventCommandStarted, RunID: runID}); err != nil {
			return command, err
		}
	} else if state != "running" {
		return command, protocolError(contract.ErrorConflict, "Seat Run is not running")
	}
	return command, w.commit()
}

// This uses the ordinary Attempt lease fence, including boot-relative expiry.
func validateComputerSeatAttemptForRun(ctx context.Context, q queryer, now time.Time, a attemptAuthority) (bool, error) {
	expired, err := attemptLeaseExpired(ctx, now, a)
	if err != nil {
		return false, err
	}
	if expired {
		return false, protocolError(contract.ErrorConflict, "Seat Run Attempt lease expired")
	}
	if a.state != contract.AttemptClaimed && a.state != contract.AttemptRunning {
		return false, protocolError(contract.ErrorConflict, "Seat Run Attempt is not active")
	}
	return true, nil
}

// A command's final receipt can be lost while ordinary Attempt completion is
// durable. Recover that evidence so its old hold cannot retain a Seat forever.
func (s *Store) recoverSeatRunEnds(ctx context.Context) error {
	type ended struct {
		identity, job, attempt string
		input                  SeatRunControlRequest
	}
	var endedRuns []ended
	err := s.withReadSnapshot(ctx, nil, func(ctx context.Context, r readModel) error {
		q := r.(*databaseReads).q
		rows, err := q.QueryContext(ctx, `SELECT sr.command_attempt_id,a.result_json FROM seat_runs sr JOIN attempts a ON a.attempt_id=sr.command_attempt_id WHERE sr.state='running' AND a.state NOT IN ('claimed','running','awaiting-input') ORDER BY sr.run_id LIMIT 100`)
		if err != nil {
			return err
		}
		type evidence struct {
			attempt string
			result  []byte
		}
		var evidenceRows []evidence
		for rows.Next() {
			var value evidence
			if err = rows.Scan(&value.attempt, &value.result); err != nil {
				rows.Close()
				return err
			}
			evidenceRows = append(evidenceRows, value)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, value := range evidenceRows {
			a, e := readAttemptAuthority(ctx, q, value.attempt)
			if e != nil {
				return e
			}
			result := ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}
			if len(value.result) > 0 {
				result = ProcessResult{}
				if e = json.Unmarshal(value.result, &result); e != nil {
					return e
				}
			}
			// A legacy process completion may contain an exit alongside a limit.
			if result.LimitReached != "" {
				result.ExitCode = nil
				result.Signal = "terminated"
				result.TerminationCause = contract.TerminationCauseAgent
			}
			endedRuns = append(endedRuns, ended{identity: a.identityNodeID, job: a.jobID, attempt: a.attemptID, input: SeatRunControlRequest{NodeID: a.nodeID, BootSessionID: a.bootSessionID, FencingToken: a.fencingToken, Phase: "finish", Result: &result}})
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, value := range endedRuns {
		if _, err = s.ControlSeatRun(ctx, value.identity, value.job, value.attempt, value.input); err != nil {
			return err
		}
	}
	return nil
}

// Seat deadlines use the ordinary Run protocol's whole-second runtime limit.
func ValidateSeatCommandMaxRuntime(value time.Duration) error {
	if value < time.Second || value%time.Second != 0 {
		return fmt.Errorf("Seat command max runtime must be positive whole seconds")
	}
	return nil
}
