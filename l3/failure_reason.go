package l3

import (
	"fmt"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
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
//
// Only the attempt's authoritative result is a cause. Late evidence -- what an
// attempt reported after it had already lost its lease -- is not what failed
// the job: L1 failed it for the lease loss, and a late "exit 0" recorded as
// the reason would say the opposite of what happened. It is kept, marked as
// late, after the cause.
func JobFailureReason(job l1.Job) string {
	if len(job.Attempts) == 0 {
		if job.FailureReason != "" {
			return "L1: " + job.FailureReason
		}
		return "the L1 job failed with no recorded result"
	}
	attempt := job.Attempts[len(job.Attempts)-1]
	var reason string
	switch {
	case attempt.Result != nil:
		reason = ProcessResultReason(*attempt.Result)
	case attempt.State == contract.AttemptLost:
		reason = "the attempt lost its lease"
		if attempt.NodeID != "" {
			reason = "the attempt on node " + attempt.NodeID + " lost its lease"
		}
	case job.FailureReason != "":
		reason = "L1: " + job.FailureReason
	default:
		reason = fmt.Sprintf("attempt %s %s with no recorded result", attempt.AttemptID, attempt.State)
	}
	if late := attempt.LateResult; late != nil {
		switch {
		case late.Result != nil:
			reason += " (late result: " + ProcessResultReason(*late.Result) + ")"
		case late.Gap != nil:
			reason += " (late result unavailable: " + string(late.Gap.Reason) + ")"
		}
	}
	return reason
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

// jobNodeID is the node a run is attributed to, and whether that answer is
// settled.
//
// While the job is live, L1 names the node of its current attempt, and that
// is provisional: an attempt can fail before it starts and the job be
// requeued onto another node. Once the job is terminal L1 has cleared its
// current attempt, so a run that finished between two reconciler passes was
// never attributed at all (#604). The terminal answer is the attempt that
// settled the job -- the last one that succeeded for a succeeded job, the
// last one for a failed job -- and it is settled: it replaces whatever
// provisional node an earlier pass recorded.
func jobNodeID(job l1.Job) (nodeID string, settled bool) {
	switch job.State {
	case contract.JobSucceeded, contract.JobFailed:
	default:
		return job.NodeID, false
	}
	for index := len(job.Attempts) - 1; index >= 0; index-- {
		attempt := job.Attempts[index]
		if job.State == contract.JobSucceeded && attempt.State != contract.AttemptSucceeded {
			continue
		}
		return attempt.NodeID, true
	}
	return job.NodeID, true
}
