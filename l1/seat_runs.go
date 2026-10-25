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

const seatRunBackstopGrace = 5 * time.Minute

// Keep additive columns on one DDL path for fresh and upgraded stores so
// rollback fingerprints agree as well as column shapes.
func (s *Store) migrateSeatRuns(ctx context.Context) error {
	for _, col := range []struct{ name, definition string }{
		{"start_boot_id", "TEXT NOT NULL DEFAULT ''"},
		{"start_ns", "INTEGER NOT NULL DEFAULT 0"},
		{"computer_fencing_token", "TEXT NOT NULL DEFAULT ''"},
	} {
		if err := s.ensureColumn(ctx, "seat_runs", col.name, col.definition); err != nil {
			return err
		}
	}
	return nil
}

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
	command, err = s.controlSeatRun(ctx, identity, jobID, attemptID, input, "")
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
func (s *Store) controlSeatRun(ctx context.Context, identity, jobID, attemptID string, input SeatRunControlRequest, recoveryReason string) (command SeatRunCommand, err error) {
	if input.Phase != "start" && input.Phase != "poll" && input.Phase != "finish" {
		return command, protocolError(contract.ErrorInvalidRequest, "invalid Seat Run phase")
	}
	if (input.Phase == "finish") != (input.Result != nil) {
		return command, protocolError(contract.ErrorInvalidRequest, "only finish requires a result")
	}
	if input.Phase == "poll" {
		return s.pollSeatRun(ctx, identity, jobID, attemptID, input)
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
	if a.nodeID != p.Binding.NodeID {
		return command, protocolError(contract.ErrorForbidden, "Seat Run belongs to another node")
	}
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
		if recoveryReason == "max_runtime_backstop" {
			reason = recoveryReason
			note = "L1 finished the command after its deadline and five-minute receipt grace; the node did not confirm its end. Run it again with a longer --max-runtime if needed."
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
		boot, ns, e := seatTime(s.clock, w.at)
		if e != nil {
			return command, e
		}
		if _, err = w.tx.ExecContext(ctx, `UPDATE seat_runs SET state='running',command_attempt_id=?,start_boot_id=?,start_ns=?,computer_fencing_token=? WHERE run_id=?`, attemptID, boot, ns, computer.fencingToken, runID); err != nil {
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

// Polling an admitted command tests retained authority, not display readiness.
// A live Seat may temporarily report starting while its command keeps running.
func (s *Store) pollSeatRun(ctx context.Context, identity, jobID, attemptID string, input SeatRunControlRequest) (command SeatRunCommand, err error) {
	err = s.withReadSnapshot(ctx, nil, func(ctx context.Context, r readModel) error {
		rd := r.(*databaseReads)
		a, e := readAttemptAuthority(ctx, rd.q, attemptID)
		if e != nil {
			return e
		}
		if e = validateAttemptAuthority(identity, jobID, attemptID, input.FencingToken, a); e != nil {
			return e
		}
		if a.nodeID != input.NodeID || a.bootSessionID != input.BootSessionID {
			return protocolError(contract.ErrorForbidden, "Seat Run node mismatch")
		}
		if _, e = validateComputerSeatAttemptForRun(ctx, rd.q, rd.now(), a); e != nil {
			return e
		}
		var state, commandAttempt, computerFence string
		var params []byte
		if e = rd.q.QueryRowContext(ctx, `SELECT run_id,state,COALESCE(command_attempt_id,''),params_json,computer_fencing_token FROM seat_runs WHERE job_id=?`, jobID).Scan(&command.RunID, &state, &commandAttempt, &params, &computerFence); e != nil {
			return e
		}
		p, present, e := contract.ParseSeatRunParams(params)
		if e != nil || !present {
			return protocolError(contract.ErrorInvalidRequest, "Attempt is not a Seat Run")
		}
		command.Binding = p.Binding
		if a.nodeID != p.Binding.NodeID {
			return protocolError(contract.ErrorForbidden, "Seat Run belongs to another node")
		}
		if commandAttempt != attemptID {
			return protocolError(contract.ErrorAttemptMismatch, "Seat Run command belongs to another Attempt")
		}
		if state != "running" {
			return protocolError(contract.ErrorConflict, "Seat Run is not running")
		}
		value, e := readSeatRequest(ctx, rd.q, p.Binding.SeatID)
		if e != nil {
			return e
		}
		if value.State != SeatLive || value.ComputerID != p.Binding.ComputerID {
			command.Cancel = true
			return nil
		}
		computer, e := readAttemptAuthority(ctx, rd.q, p.Binding.ComputerAttemptID)
		if e != nil {
			if errorCode(e) == contract.ErrorAttemptNotFound {
				command.Cancel = true
				return nil
			}
			return e
		}
		if computer.nodeID != p.Binding.NodeID || computer.jobID != p.Binding.ComputerJobID || (computerFence != "" && computerFence != computer.fencingToken) {
			command.Cancel = true
			return nil
		}
		auth := ComputerSeatAttemptRequest{NodeID: computer.nodeID, BootSessionID: computer.bootSessionID, FencingToken: computer.fencingToken}
		e = validateComputerSeatAttempt(ctx, rd.q, rd.now(), computer.identityNodeID, p.Binding.ComputerID, computer.jobID, computer.attemptID, auth)
		if e != nil {
			switch errorCode(e) {
			case contract.ErrorAttemptNotOwned, contract.ErrorAttemptMismatch, contract.ErrorStaleFence, contract.ErrorNodeSessionReplaced, contract.ErrorConflict, contract.ErrorNotFound:
			default:
				return e
			}
			command.Cancel = true
			return nil
		}
		command.ComputerSeatAttemptRequest = auth
		command.Place = value.Place
		e = rd.q.QueryRowContext(ctx, `SELECT helper_generation FROM computer_seat_observations WHERE seat_id=? AND attempt_id=?`, p.Binding.SeatID, computer.attemptID).Scan(&command.Generation)
		if errors.Is(e, sql.ErrNoRows) {
			return nil
		}
		return e
	})
	return
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

// Recover each receipt independently. Keyset pages move past failed rows so
// a broken early Run cannot starve later Runs or keep their Seats alive.
func (s *Store) recoverSeatRunEnds(ctx context.Context) error {
	type ended struct {
		authority attemptAuthority
		result    ProcessResult
		backstop  bool
	}
	type restart struct {
		run, attempt, boot string
		start              int64
	}
	cursor := ""
	for {
		var endedRuns []ended
		var restarts []restart
		count := 0
		err := s.withReadSnapshot(ctx, nil, func(ctx context.Context, r readModel) error {
			rd := r.(*databaseReads)
			boot, ns, err := seatTime(s.clock, rd.now())
			if err != nil {
				return err
			}
			rows, err := rd.q.QueryContext(ctx, `SELECT sr.run_id,sr.command_attempt_id,sr.params_json,sr.start_boot_id,sr.start_ns,a.state,a.result_json FROM seat_runs sr JOIN attempts a ON a.attempt_id=sr.command_attempt_id WHERE sr.state='running' AND sr.run_id>? ORDER BY sr.run_id LIMIT 100`, cursor)
			if err != nil {
				return err
			}
			type evidence struct {
				run, attempt, boot, state string
				start                     int64
				params, result            []byte
			}
			var values []evidence
			for rows.Next() {
				var v evidence
				if err = rows.Scan(&v.run, &v.attempt, &v.params, &v.boot, &v.start, &v.state, &v.result); err != nil {
					rows.Close()
					return err
				}
				values = append(values, v)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			count = len(values)
			for _, v := range values {
				cursor = v.run
				p, present, e := contract.ParseSeatRunParams(v.params)
				if e != nil || !present {
					s.logSeatLifeError(v.run, fmt.Errorf("invalid retained Seat Run params"))
					continue
				}
				active := v.state == string(contract.AttemptClaimed) || v.state == string(contract.AttemptRunning) || v.state == string(contract.AttemptAwaitingInput)
				// A new L1 host boot has no comparable elapsed origin. Preserve
				// remote commands and restart only this receipt backstop window.
				if active && (v.boot == "" || v.boot != boot) {
					restarts = append(restarts, restart{v.run, v.attempt, v.boot, v.start})
					continue
				}
				due := ns >= v.start && (ns-v.start)/int64(time.Second)-int64(seatRunBackstopGrace/time.Second) >= p.SeatRun.MaxRuntimeSeconds
				if active && !due {
					continue
				}
				a, e := readAttemptAuthority(ctx, rd.q, v.attempt)
				if e != nil {
					s.logSeatLifeError(v.run, e)
					continue
				}
				result := ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}
				if len(v.result) > 0 {
					if e = json.Unmarshal(v.result, &result); e != nil {
						result = ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}
					}
				}
				if result.LimitReached != "" {
					result = ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent, LimitReached: result.LimitReached}
				}
				if validateProcessResult(result) != nil {
					result = ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}
				}
				if due && active {
					result = ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent, LimitReached: contract.LimitReachedMaxRuntime}
				}
				endedRuns = append(endedRuns, ended{a, result, due && active})
			}
			return nil
		})
		if err != nil {
			return err
		}
		for _, v := range restarts {
			if e := s.restartSeatRunBackstop(ctx, v.run, v.attempt, v.boot, v.start); e != nil {
				s.logSeatLifeError(v.run, e)
			}
		}
		for _, v := range endedRuns {
			a := v.authority
			reason := ""
			if v.backstop {
				reason = "max_runtime_backstop"
				if a.state == contract.AttemptClaimed || a.state == contract.AttemptRunning || a.state == contract.AttemptAwaitingInput {
					_, e := s.CompleteAttempt(ctx, a.identityNodeID, a.jobID, a.attemptID, CompletionRequest{FencingToken: a.fencingToken, IdempotencyKey: "seat-run-backstop:" + a.attemptID, Result: v.result})
					if e != nil && errorCode(e) != contract.ErrorLeaseExpired {
						s.logSeatLifeError(a.attemptID, e)
						continue
					}
				}
			}
			input := SeatRunControlRequest{NodeID: a.nodeID, BootSessionID: a.bootSessionID, FencingToken: a.fencingToken, Phase: "finish", Result: &v.result}
			if _, e := s.controlSeatRun(ctx, a.identityNodeID, a.jobID, a.attemptID, input, reason); e != nil {
				s.logSeatLifeError(a.attemptID, e)
			}
		}
		if count < 100 {
			return nil
		}
	}
}

// Re-stamp only the active command and origin observed by recovery. A second
// recovery pass with the old snapshot must not reset a window already started.
func (s *Store) restartSeatRunBackstop(ctx context.Context, runID, attemptID, oldBoot string, oldStart int64) error {
	w, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return err
	}
	defer w.rollback()
	boot, ns, err := seatTime(s.clock, w.at)
	if err != nil {
		return err
	}
	_, err = w.tx.ExecContext(w.ctx, `UPDATE seat_runs SET start_boot_id=?,start_ns=?
 WHERE run_id=? AND command_attempt_id=? AND state='running'
 AND start_boot_id=? AND start_ns=? AND (start_boot_id='' OR start_boot_id!=?)
 AND EXISTS(SELECT 1 FROM attempts a WHERE a.attempt_id=seat_runs.command_attempt_id AND a.state IN ('claimed','running','awaiting-input'))`, boot, ns, runID, attemptID, oldBoot, oldStart, boot)
	if err != nil {
		return err
	}
	return w.commit()
}

// Seat deadlines use the ordinary Run protocol's whole-second runtime limit.
func ValidateSeatCommandMaxRuntime(value time.Duration) error {
	if value < time.Second || value%time.Second != 0 {
		return fmt.Errorf("Seat command max runtime must be positive whole seconds")
	}
	return nil
}
