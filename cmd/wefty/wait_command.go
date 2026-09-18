package main

import (
	"context"
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

	started := time.Now()
	deadline := time.Time{}
	if timeout > 0 {
		deadline = started.Add(timeout)
	}
	delay := waitPollInitial
	for {
		record, err := clients.getRun(ctx, runID)
		if err != nil {
			return err
		}
		if runIsTerminal(record.Status) {
			return reportTerminalRun(stdout, record, jsonOutput)
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			// The last read is the one reported, so the message names the state
			// the run was actually in rather than the one it was in when the
			// wait began.
			if jsonOutput {
				if err := writeJSON(stdout, record); err != nil {
					return err
				}
			}
			return &waitTimeoutError{runID: runID, status: record.Status, waited: time.Since(started)}
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
