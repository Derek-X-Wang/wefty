package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type seatRunControl interface {
	ControlSeatRun(context.Context, string, string, l1.SeatRunControlRequest) (l1.SeatRunCommand, error)
}
type seatRunHelper interface {
	ExecComputerSeat(context.Context, ocihelper.ExecComputerSeatRequest, func(ocihelper.SeatCommandEvent) error) error
}
type seatRunRuntime struct {
	control seatRunControl
	helper  seatRunHelper
	clock   Clock
	params  contract.SeatRunParams
}

func (c *Client) ControlSeatRun(ctx context.Context, job, attempt string, r l1.SeatRunControlRequest) (command l1.SeatRunCommand, err error) {
	err = c.post(ctx, attemptPath(job, attempt)+"/seat-run", r, &command)
	return
}
func (r *seatRunRuntime) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	if r.helper == nil || r.control == nil {
		err := errors.New("Seat Run helper or L1 control unavailable")
		return workloadrunner.Admission{Request: request}, workloadrunner.Result{Outcome: spawnFailure(contract.SpawnFailureRuntimeUnavailable, err)}, err
	}
	return workloadrunner.Admission{Request: request}, workloadrunner.Result{}, nil
}
func (r *seatRunRuntime) ReapAndVerify(_ context.Context, request workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	// The command belongs to the existing Computer Attempt. This one-shot never
	// creates a host process, container, mount or a Computer runtime to reap.
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceNoRuntime, BootSessionID: request.Authority.BootSessionID}, nil
}

var errSeatRunDeadline = errors.New("Seat Run maximum runtime reached")

func (r *seatRunRuntime) Run(ctx context.Context, request workloadrunner.Request, sink workloadrunner.OutputSink) (result workloadrunner.Result, runErr error) {
	clock := r.clock
	if clock == nil {
		clock = systemClock{}
	}
	a := request.Authority
	control := l1.SeatRunControlRequest{NodeID: a.NodeID, BootSessionID: a.BootSessionID, FencingToken: a.FencingToken, Phase: "start"}
	command, err := r.control.ControlSeatRun(ctx, a.JobID, a.AttemptID, control)
	if err != nil {
		failed := spawnFailure(contract.SpawnFailureProcessRequest, err)
		control.Phase = "finish"
		wireResult := toL1Result(failed)
		control.Result = &wireResult
		finishCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer stop()
		_, _ = r.control.ControlSeatRun(finishCtx, a.JobID, a.AttemptID, control)
		return workloadrunner.Result{Outcome: failed}, err
	}
	defer func() {
		if directory := request.Execution.HandoffDirectory; directory != "" {
			if err := writeSeatRunResult(directory, result.Outcome); err != nil {
				runErr = errors.Join(runErr, err)
				result.Outcome = contract.ProcessResult{OutputError: err.Error(), LimitReached: result.Outcome.LimitReached}
			}
		}
		control.Phase = "finish"
		wireResult := toL1Result(result.Outcome)
		control.Result = &wireResult
		finishCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer stop()
		_, err := r.control.ControlSeatRun(finishCtx, a.JobID, a.AttemptID, control)
		if err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("record Seat Run end: %w", err))
		}
	}()
	duration := time.Duration(r.params.SeatRun.MaxRuntimeSeconds) * time.Second
	if duration <= 0 {
		duration = contract.DefaultSeatCommandRuntime
	}
	commandCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	timer := clock.NewTimer(duration)
	defer stopTimer(timer)
	poll := clock.NewTimer(time.Second)
	defer stopTimer(poll)
	done, joined, deadlineJoined := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(deadlineJoined)
		select {
		case <-done:
		case <-commandCtx.Done():
		case <-timer.C():
			cancel(errSeatRunDeadline)
		}
	}()
	go func() {
		defer close(joined)
		for {
			select {
			case <-done:
				return
			case <-commandCtx.Done():
				return
			case <-poll.C():
				next := control
				next.Phase = "poll"
				checkCtx, stop := context.WithTimeout(commandCtx, 5*time.Second)
				reply, err := r.control.ControlSeatRun(checkCtx, a.JobID, a.AttemptID, next)
				stop()
				if err != nil {
					if commandCtx.Err() != nil {
						return
					}
					var protocol *ProtocolError
					if classifyAgentProtocolError(err).destination == errorDestinationAttemptAuthority || errors.As(err, &protocol) && protocol.StatusCode >= 400 && protocol.StatusCode < 500 && !protocol.APIError.Retryable {
						cancel(fmt.Errorf("Seat Run authority refused: %w", err))
						return
					}
					// The existing Attempt watchdog owns the last confirmed
					// lease. A transient poll failure never renews that lease
					// and never extends the independent command deadline.
					poll.Reset(time.Second)
					continue
				}
				if reply.Cancel {
					cancel(errors.New("Seat ended; command cancelled"))
					return
				}
				poll.Reset(time.Second)
			}
		}
	}()
	var acknowledged sync.Once
	acknowledge := func() {
		acknowledged.Do(func() {
			if request.Started != nil {
				request.Started()
			}
		})
	}
	var exit *ocihelper.SeatCommandExit
	err = r.helper.ExecComputerSeat(commandCtx, ocihelper.ExecComputerSeatRequest{
		Authority: ocihelper.AttemptAuthority{NodeID: command.NodeID, BootSessionID: command.BootSessionID, JobID: command.Binding.ComputerJobID, AttemptID: command.Binding.ComputerAttemptID, FencingToken: command.FencingToken, Class: contract.JobClassService}, Generation: command.Generation, Place: command.Place, Args: append([]string(nil), r.params.SeatRun.Argv...), MaxRuntime: duration,
	}, func(event ocihelper.SeatCommandEvent) error {
		// A helper refusal is not evidence that a payload exists. The helper
		// stream's first output or exit supplies that evidence.
		if event.Exit != nil || len(event.Data) > 0 {
			acknowledge()
		}
		if event.Exit != nil {
			exit = event.Exit
			return nil
		}
		if sink == nil {
			return nil
		}
		return sink.WriteOutput(commandCtx, contract.LogEvent{AttemptID: a.AttemptID, Stream: contract.LogStream(event.Stream), Timestamp: clock.Now(), Bytes: event.Data})
	})
	close(done)
	<-joined
	<-deadlineJoined
	var rpc *ocihelper.RPCError
	limited := errors.Is(err, ocihelper.ErrSeatCommandRuntimeLimit) || errors.Is(context.Cause(commandCtx), errSeatRunDeadline) || errors.As(err, &rpc) && rpc.Detail == ocihelper.DetailSeatCommandRuntimeLimit
	if limited {
		result.Outcome = contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent, LimitReached: contract.LimitReachedMaxRuntime}
		if sink != nil {
			runErr = sink.WriteOutput(context.WithoutCancel(ctx), contract.LogEvent{AttemptID: a.AttemptID, Stream: contract.LogStderr, Timestamp: clock.Now(), Bytes: []byte(fmt.Sprintf("stopped: reached its %s limit. Run it again with a longer --max-runtime.\n", duration))})
		}
		return result, runErr
	}
	if commandCtx.Err() != nil {
		result.Outcome = contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}
		return result, nil
	}
	if err != nil {
		result.Outcome = contract.ProcessResult{OutputError: err.Error()}
		return result, err
	}
	if exit == nil {
		err = errors.New("Seat command ended without exit evidence")
		result.Outcome = contract.ProcessResult{OutputError: err.Error()}
		return result, err
	}
	if exit.Code != nil {
		result.Outcome.ExitCode = exit.Code
	} else {
		result.Outcome.Signal = strconv.Itoa(exit.Signal)
		result.Outcome.TerminationCause = contract.TerminationCauseSpontaneous
	}
	return result, nil
}

// The node agent writes the same command result document that exec's wrapper
// writes. The host mailbox path and its credentials never enter the Seat.
func writeSeatRunResult(directory string, result contract.ProcessResult) error {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return fmt.Errorf("open Seat Run results: %w", err)
	}
	defer root.Close()
	data, err := json.Marshal(toL1Result(result))
	if err != nil {
		return err
	}
	if err = root.WriteFile("result.json", append(data, '\n'), 0600); err != nil {
		return fmt.Errorf("write Seat Run result: %w", err)
	}
	return nil
}
