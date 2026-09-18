package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// `wefty wait` is the command a script writes between submitting work and
// acting on it.
//
// Its whole job is to turn a run's outcome into an exit code, because that is
// what the thing calling it can actually branch on. Three outcomes, three
// codes: the run succeeded, the run failed, or the wait ran out before the run
// did. The third is deliberately not the second -- "I do not know yet" and "it
// failed" lead to different next actions, and a command that conflated them
// would make every timeout look like a broken build.
//
// It polls. There is no server route for this and there should not be: a
// long-poll would hold a connection open across the one boundary in this system
// most likely to drop it, and the ledger already answers the question on
// demand.

const (
	// waitPollInitial is the first gap. Short, because a run that is already
	// finished should return immediately rather than after a fixed tick.
	waitPollInitial = 250 * time.Millisecond
	// waitPollMax bounds the backoff. Past this the command is idle polling of
	// something slow, and a tighter interval buys nothing.
	waitPollMax = 5 * time.Second
)

// runOutcomeError carries a terminal run's own verdict out to the exit code.
// It is not a failure of the command: the command did exactly its job.
type runOutcomeError struct {
	runID  string
	status contract.RunState
}

func (e *runOutcomeError) Error() string {
	return fmt.Sprintf("run %s %s", e.runID, e.status)
}

// waitTimeoutError is the third outcome, kept separate from a failed run for
// the reason above.
type waitTimeoutError struct {
	runID  string
	status contract.RunState
	waited time.Duration
}

func (e *waitTimeoutError) Error() string {
	return fmt.Sprintf("run %s is still %s after %s", e.runID, e.status, e.waited)
}

func executeWait(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("wait", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var timeout time.Duration
	flags.DurationVar(&timeout, "timeout", 0, "give up after this long (default: wait indefinitely)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty wait RUN_ID [--timeout D]")
	}
	if timeout < 0 {
		return usageError("--timeout must not be negative")
	}
	runID := flags.Arg(0)
	cancelRequest := context.CancelFunc(func() {})

	started := time.Now()
	deadline := time.Time{}
	if timeout > 0 {
		deadline = started.Add(timeout)
	}
	delay := waitPollInitial
	var last contract.RunState
	for {
		// The deadline is checked before every request and applied to it. A
		// request with no bound of its own would defeat --timeout entirely:
		// this client has no timeout, so a ledger that accepts a connection
		// and never answers would hold the wait open forever.
		if expired(deadline) {
			return waitExpired(stdout, runID, last, started, jsonOutput)
		}
		record, err := clients.getRun(requestContext(ctx, deadline, &cancelRequest), runID)
		cancelRequest()
		if err != nil {
			if ctx.Err() != nil {
				// The caller stopped waiting, which is not a timeout.
				return ctx.Err()
			}
			if permanentWaitFailure(err) {
				return err
			}
			// Everything else -- a dropped connection, a ledger restarting, a
			// 5xx -- is a reason to keep waiting rather than to give a verdict.
			// The command's job is the run's outcome, and a transient failure
			// to read it is not one.
			if expired(deadline) {
				return waitExpired(stdout, runID, last, started, jsonOutput)
			}
		} else {
			// A response that arrives after the deadline is not an answer this
			// command may report: --timeout said when to stop believing it.
			if expired(deadline) {
				return waitExpired(stdout, runID, record.Status, started, jsonOutput)
			}
			last = record.Status
			if runIsTerminal(record.Status) {
				return reportTerminalRun(stdout, record, jsonOutput)
			}
		}
		sleep := delay
		if !deadline.IsZero() {
			if remaining := time.Until(deadline); remaining < sleep {
				sleep = remaining
			}
		}
		if sleep > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(sleep):
			}
		}
		if delay < waitPollMax {
			delay *= 2
			if delay > waitPollMax {
				delay = waitPollMax
			}
		}
	}
}

// requestContext bounds one poll by whatever is left of the wait. Without a
// --timeout there is nothing to bound it with, and the caller's own context is
// the only limit.
func requestContext(ctx context.Context, deadline time.Time, cancel *context.CancelFunc) context.Context {
	if deadline.IsZero() {
		*cancel = func() {}
		return ctx
	}
	bounded, stop := context.WithDeadline(ctx, deadline)
	*cancel = stop
	return bounded
}

// permanentWaitFailure separates "this will never work" from "not yet". A 4xx
// is the ledger answering: the run does not exist, or this identity may not
// read it, and waiting longer cannot change either. Anything else is the
// connection or the server, and the run may still be fine.
func permanentWaitFailure(err error) bool {
	var responseErr *apiResponseError
	if !errors.As(err, &responseErr) {
		return false
	}
	return responseErr.StatusCode >= 400 && responseErr.StatusCode < 500
}

func expired(deadline time.Time) bool {
	return !deadline.IsZero() && !time.Now().Before(deadline)
}

// waitExpired reports the third outcome. The status named is the last one
// actually read, so the message describes the run as the command last saw it
// rather than as it was when the wait began.
func waitExpired(stdout io.Writer, runID string, status contract.RunState, started time.Time, jsonOutput bool) error {
	if status == "" {
		status = contract.RunState("unread")
	}
	if jsonOutput {
		if err := writeJSON(stdout, map[string]any{
			"run_id": runID, "status": status, "waited_seconds": time.Since(started).Seconds(),
			"timed_out": true,
		}); err != nil {
			return err
		}
	}
	return &waitTimeoutError{runID: runID, status: status, waited: time.Since(started)}
}

// reportTerminalRun prints the outcome and then turns it into an exit code. The
// status is printed either way, because a script that wants the word as well as
// the code should not have to run a second command for it.
func reportTerminalRun(stdout io.Writer, record contract.RunRecord, jsonOutput bool) error {
	if jsonOutput {
		if err := writeJSON(stdout, record); err != nil {
			return err
		}
	} else if _, err := fmt.Fprintln(stdout, record.Status); err != nil {
		return err
	}
	if record.Status == contract.RunSucceeded {
		return nil
	}
	return &runOutcomeError{runID: record.RunID, status: record.Status}
}

// runIsTerminal reads the ledger's own transition table rather than restating
// which states are ends, so a state added there is terminal here too.
func runIsTerminal(status contract.RunState) bool {
	transitions, known := contract.RunTransitions[status]
	return known && len(transitions) == 0
}
