package l3

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// A failed run says why in one line.
//
// "failed" alone sends a reader to `inspect --execution` and a table of L1
// attempts, and for the one failure the ledger itself decides -- a job that
// exited 0 having reported no envelope a --required-envelope run promised --
// there was nothing to find there at all (#604). So the ledger records the
// reason where it decides the failure, once, and the record carries it.

// JobFailureReason reads the one-line reason a failed L1 job gives: a job-level
// cancellation decision first, then the last attempt's own result when it has
// one, else L1's pre-start terminal reason,
// else the attempt's state. It never invents a cause; a job that left no
// evidence says so.
//
// Only the attempt's authoritative result is a cause. Late evidence -- what an
// attempt reported after it had already lost its lease -- is not what failed
// the job: L1 failed it for the lease loss, and a late "exit 0" recorded as
// the reason would say the opposite of what happened. It is kept, marked as
// late, after the cause.
func JobFailureReason(job l1.Job) string {
	return SanitizeFailureReason(jobFailureReason(job))
}

func jobFailureReason(job l1.Job) string {
	if job.Outcome == contract.JobOutcomeCanceled {
		return "the L1 job was canceled"
	}
	if len(job.Attempts) == 0 {
		if job.FailureReason != "" {
			return "L1: " + job.FailureReason
		}
		return "the L1 job failed with no recorded result"
	}
	last := len(job.Attempts) - 1
	attempt := job.Attempts[last]
	var reason string
	if outcome, ok := attemptOutcome(job, last, false); ok {
		reason = outcome.reason()
	} else {
		switch {
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
	}
	if late, ok := attemptOutcome(job, last, true); ok {
		reason += " (late result: " + late.reason() + ")"
	} else if attempt.LateResult != nil && attempt.LateResult.Gap != nil {
		reason += " (late result unavailable: " + string(attempt.LateResult.Gap.Reason) + ")"
	}
	return reason
}

// resultOutcome is what a reason needs from one attempt result. It is read
// out of the l1.Job wire type field by field rather than by naming L1's
// result type, which ADR-0006 keeps off the L3 side of the boundary: the job
// projection is the allowlisted wire type, and its attempts ride inside it.
type resultOutcome struct {
	exitCode      *int
	spawn         string
	runtime       string
	outputError   string
	signal        string
	cause         string
	outOfMemory   bool
	diskExhausted bool
}

// attemptOutcome reads the attempt's authoritative result, or with late set
// its non-authoritative late result. ok is false when there is none.
func attemptOutcome(job l1.Job, index int, late bool) (resultOutcome, bool) {
	attempt := job.Attempts[index]
	result := attempt.Result
	if late {
		if attempt.LateResult == nil {
			return resultOutcome{}, false
		}
		result = attempt.LateResult.Result
	}
	if result == nil {
		return resultOutcome{}, false
	}
	outcome := resultOutcome{
		exitCode: result.ExitCode, outputError: result.OutputError, signal: result.Signal,
		cause: string(result.TerminationCause), outOfMemory: result.OOM, diskExhausted: result.DiskExhausted,
	}
	if result.SpawnError != nil {
		outcome.spawn = fmt.Sprintf("spawn %s: %s", result.SpawnError.Code, result.SpawnError.Message)
	}
	if result.RuntimeFailure != nil {
		outcome.runtime = fmt.Sprintf("runtime %s: %s", result.RuntimeFailure.Code, result.RuntimeFailure.Message)
	}
	return outcome, true
}

// reason is one attempt result as a reader names it: the exit code, the
// signal and who sent it, or the spawn/runtime failure.
func (outcome resultOutcome) reason() string {
	var reason string
	switch {
	case outcome.exitCode != nil:
		reason = fmt.Sprintf("exit %d", *outcome.exitCode)
	case outcome.spawn != "":
		reason = outcome.spawn
	case outcome.runtime != "":
		reason = outcome.runtime
	case outcome.outputError != "":
		reason = "output error: " + outcome.outputError
	case outcome.signal != "":
		reason = "signal " + outcome.signal
		if outcome.cause != "" {
			reason += " (" + outcome.cause + ")"
		}
	default:
		reason = "no exit code, signal or failure was recorded"
	}
	var flags []string
	if outcome.outOfMemory {
		flags = append(flags, "out of memory")
	}
	if outcome.diskExhausted {
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

// maxFailureReasonRunes bounds a recorded reason. It is one line a person
// reads beside a status, not a place to keep evidence; the evidence has its
// own homes (the attempt, the protocol rejection, the logs).
const maxFailureReasonRunes = 200

// SanitizeFailureReason makes any reason safe to store and print as one line:
// every run of whitespace or control characters becomes one space, and the
// result is capped. Every failure_reason passes through it, whatever wrote
// it, because some sources -- an L1 spawn message, a gate name -- carry text
// the ledger did not write.
func SanitizeFailureReason(reason string) string {
	fields := strings.FieldsFunc(reason, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) })
	line := strings.Join(fields, " ")
	if runes := []rune(line); len(runes) > maxFailureReasonRunes {
		line = string(runes[:maxFailureReasonRunes-1]) + "…"
	}
	return line
}

// errRunIDMismatch and errAttemptIDMismatch are the two binding refusals a
// protocol write can meet. Their text is fixed, so a rejected-write summary
// may repeat it.
var (
	errRunIDMismatch     = errors.New("run_id must match the authenticated run")
	errAttemptIDMismatch = errors.New("attempt_id must match the authenticated run attempt")
)

// rejectedWriteSummary is the failure_reason for a run failed by a write the
// ledger refused. It is fixed text plus, for a schema failure, the JSON
// pointer of the offending field -- never the value. A format or pattern
// error quotes the rejected value, and a workload that put a token in an
// envelope field would otherwise have it copied into run metadata and onto
// every terminal that runs `wefty wait` (#604 review).
func rejectedWriteSummary(kind string, cause error) string {
	var validation *jsonschema.ValidationError
	switch {
	case errors.As(cause, &validation):
		return fmt.Sprintf("rejected %s: schema validation failed at %s", kind, jsonPointer(firstValidationLeaf(validation).InstanceLocation))
	case errors.Is(cause, errRunIDMismatch), errors.Is(cause, errAttemptIDMismatch):
		return "rejected " + kind + ": " + cause.Error()
	default:
		return "rejected " + kind + ": validation failed"
	}
}

// firstValidationLeaf follows the first cause down to the most specific
// failure, which is the one whose location names the field.
func firstValidationLeaf(validation *jsonschema.ValidationError) *jsonschema.ValidationError {
	for len(validation.Causes) > 0 {
		validation = validation.Causes[0]
	}
	return validation
}

// jsonPointer renders an instance location as an RFC 6901 pointer.
func jsonPointer(location []string) string {
	if len(location) == 0 {
		return "/"
	}
	escaped := make([]string, len(location))
	for index, token := range location {
		escaped[index] = strings.ReplaceAll(strings.ReplaceAll(token, "~", "~0"), "/", "~1")
	}
	return "/" + strings.Join(escaped, "/")
}
