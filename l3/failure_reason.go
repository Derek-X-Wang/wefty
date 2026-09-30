package l3

import (
	"fmt"
	"strings"

	"github.com/Derek-X-Wang/wefty/l1"
)

// A failed run says why in one line.
//
// "failed" alone sends a reader to `inspect --execution` and a table of L1
// attempts, and for the one failure the ledger itself decides -- a job that
// exited 0 having reported no envelope a --required-envelope run promised --
// there was nothing to find there at all (#604). So the ledger records the
// reason where it decides the failure, once, and the record carries it.

// JobFailureReason reads the one-line reason a failed L1 job gives: the last
// attempt's own result when it has one, else L1's pre-start terminal reason,
// else the attempt's state. It never invents a cause; a job that left no
// evidence says so.
func JobFailureReason(job l1.Job) string {
	if len(job.Attempts) > 0 {
		attempt := job.Attempts[len(job.Attempts)-1]
		switch {
		case attempt.Result != nil:
			return ProcessResultReason(*attempt.Result)
		case attempt.LateResult != nil && attempt.LateResult.Result != nil:
			return ProcessResultReason(*attempt.LateResult.Result)
		}
		if job.FailureReason == "" {
			return fmt.Sprintf("attempt %s %s with no recorded result", attempt.AttemptID, attempt.State)
		}
	}
	if job.FailureReason != "" {
		return "L1: " + job.FailureReason
	}
	return "the L1 job failed with no recorded result"
}

// ProcessResultReason is one attempt result as a reader names it: the exit
// code, the signal and who sent it, or the spawn/runtime failure.
func ProcessResultReason(result l1.ProcessResult) string {
	var reason string
	switch {
	case result.ExitCode != nil:
		reason = fmt.Sprintf("exit %d", *result.ExitCode)
	case result.SpawnError != nil:
		reason = fmt.Sprintf("spawn %s: %s", result.SpawnError.Code, result.SpawnError.Message)
	case result.RuntimeFailure != nil:
		reason = fmt.Sprintf("runtime %s: %s", result.RuntimeFailure.Code, result.RuntimeFailure.Message)
	case result.OutputError != "":
		reason = "output error: " + result.OutputError
	case result.Signal != "":
		reason = "signal " + result.Signal
		if result.TerminationCause != "" {
			reason += " (" + string(result.TerminationCause) + ")"
		}
	default:
		reason = "no exit code, signal or failure was recorded"
	}
	var flags []string
	if result.OOM {
		flags = append(flags, "out of memory")
	}
	if result.DiskExhausted {
		flags = append(flags, "disk exhausted")
	}
	if len(flags) > 0 {
		reason += " [" + strings.Join(flags, ", ") + "]"
	}
	return reason
}

// jobNodeID is the node that ran the job. L1 names the node of the current
// attempt, and a terminal job has none -- its attempt is over -- so a run
// that finished between two reconciler passes was never attributed (#604).
// The last attempt is the one that produced the outcome.
func jobNodeID(job l1.Job) string {
	if job.NodeID != "" {
		return job.NodeID
	}
	if len(job.Attempts) > 0 {
		return job.Attempts[len(job.Attempts)-1].NodeID
	}
	return ""
}
