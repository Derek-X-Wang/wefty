package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// DefaultTaskReleaseTimeout bounds exited-task deletion before terminal
// publication.
const DefaultTaskReleaseTimeout = 5 * time.Second

// DefaultLogSealTimeout bounds how long a stream waits for its binary-v2
// logger pipe-EOF seal. The wait starts only once the terminal is published,
// which is after the task release above, so the two bounds are serial: a
// caller waiting for terminal log evidence must budget for both.
const DefaultLogSealTimeout = 5 * time.Second

// taskReleaseRetryInterval paces re-deletion of an exited task while the
// runtime still reports it running. It is a poll cadence inside the existing
// release budget, not an additional timeout: every attempt is made with the
// same context whose deadline is the release budget.
const taskReleaseRetryInterval = 10 * time.Millisecond

// TaskNeverStoppedSealReason is the typed log-evidence reason stamped on a
// stream seal that stayed incomplete because the exited task was never
// released. The runtime observes a process exit and the runtime's task-state
// transition separately, and deletion -- which is what closes the shim's
// logger pipes and therefore what produces the pipe-EOF seal -- is refused
// while the task is still reported running. Without this typed reason an agent
// cannot tell "the streams were never sealed because the task never stopped"
// from "the stream carried a real log gap"; both arrive as an incomplete seal.
const TaskNeverStoppedSealReason = "task_release_task_never_stopped"

// TaskNeverStoppedError reports that the release budget expired with the
// runtime still refusing deletion of an exited task.
type TaskNeverStoppedError struct{ err error }

func (failure *TaskNeverStoppedError) Error() string {
	return fmt.Sprintf("exited OCI task was still reported running when its release budget expired: %v", failure.err)
}

func (failure *TaskNeverStoppedError) Unwrap() error { return failure.err }

// SealReason is the typed log-evidence reason this failure contributes.
func (failure *TaskNeverStoppedError) SealReason() string { return TaskNeverStoppedSealReason }

// publishTerminalAfterTaskRelease releases the exited task and only then makes
// the terminal observable, so log sealing is anchored on a durable exit.
//
// A single deletion attempt is not enough. The exit status and the runtime's
// task state are two separate observations: the exit arrives on Wait while the
// task can still be reported running, and deletion is refused with a failed
// precondition for that window. Releasing on that refusal published the
// terminal with the shim's logger pipes still open, so neither stream ever
// reached pipe EOF and an ordinary exit-0 payload produced incomplete log
// evidence. Deletion is therefore retried inside the existing release budget
// while stillRunning reports that refusal; every other error is terminal.
//
// publish is invoked exactly once, after the release settles, with the typed
// seal reason to stamp on incomplete stream seals (empty when the task
// released cleanly). Callers make the terminal observable from publish so no
// stream can begin its seal deadline before the reason is recorded.
func publishTerminalAfterTaskRelease(timeout time.Duration, release func(context.Context) error, stillRunning func(error) bool, publish func(sealReason string)) error {
	if timeout <= 0 {
		timeout = DefaultTaskReleaseTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var err error
	if release != nil {
		err = releaseExitedTask(ctx, release, stillRunning)
	}
	reason := ""
	var neverStopped *TaskNeverStoppedError
	if errors.As(err, &neverStopped) {
		reason = neverStopped.SealReason()
	}
	if publish != nil {
		publish(reason)
	}
	return err
}

// releaseExitedTask retries deletion while the runtime still reports the
// exited task running, bounded by the context's release budget.
func releaseExitedTask(ctx context.Context, release func(context.Context) error, stillRunning func(error) bool) error {
	for {
		err := release(ctx)
		if err == nil || stillRunning == nil || !stillRunning(err) {
			return err
		}
		timer := time.NewTimer(taskReleaseRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return &TaskNeverStoppedError{err: err}
		case <-timer.C:
		}
	}
}

// taskAbsenceObservation is one answer to "does the engine still hold this
// attempt's task?" asked after that task's Wait broke.
type taskAbsenceObservation int

const (
	// taskObservationUnproven means the engine did not answer for the task:
	// the engine connection failed, or the engine is still tearing down the
	// task's lost shim and could not reach it. Asking again may settle it.
	taskObservationUnproven taskAbsenceObservation = iota
	// taskObservationGone means the engine answered that the task no longer
	// exists or has stopped.
	taskObservationGone
	// taskObservationLive means the engine answered that the task still
	// exists in a live state, so the broken Wait was not the task ending.
	taskObservationLive
	// taskObservationEngineChanged means the engine that answered is not
	// provably the engine process the attempt started under: its identity
	// changed, or was never captured. A restarted engine that lost task state
	// answers NotFound for every task, so its answer proves nothing about one
	// shim.
	taskObservationEngineChanged
)

// engineIdentity names one running engine process: its persistent instance
// UUID plus the process ID and PID namespace serving it. The UUID alone
// survives a restart, so continuity rests on the process.
type engineIdentity struct {
	InstanceUUID string
	PID          uint64
	PIDNamespace uint64
}

func (identity engineIdentity) complete() bool {
	return identity.InstanceUUID != "" && identity.PID != 0
}

// observeTaskAbsenceOnSameEngine gates a task-absence answer on engine
// continuity. The task answer counts only if the engine identity read after it
// equals the identity captured when the attempt started: the same process
// answered, so it never restarted in between and its NotFound is its own
// record of reaping this task's lost shim. A missing baseline or a changed
// identity is final; an identity that cannot be read is retried like any other
// unanswered question and fails closed when the bound runs out.
func observeTaskAbsenceOnSameEngine(ctx context.Context, baseline engineIdentity, task func(context.Context) taskAbsenceObservation, identity func(context.Context) (engineIdentity, error)) taskAbsenceObservation {
	if !baseline.complete() || task == nil || identity == nil {
		return taskObservationEngineChanged
	}
	answer := task(ctx)
	if answer != taskObservationGone {
		return answer
	}
	current, err := identity(ctx)
	if err != nil || !current.complete() {
		return taskObservationUnproven
	}
	if current != baseline {
		return taskObservationEngineChanged
	}
	return taskObservationGone
}

// taskLossScopeProbeInterval paces re-asking the engine about one task while
// it cleans up after a lost shim. It is a poll cadence inside the bound the
// caller passes, not an additional timeout.
const taskLossScopeProbeInterval = 25 * time.Millisecond

// proveTaskLossAttemptScoped decides whether a broken task Wait ended only
// that attempt. It is the positive proof behind a Watch result's
// runtime_failure_attempt_scoped claim.
//
// A killed shim breaks its task's Wait with the shim connection error, while
// containerd itself keeps serving; containerd then reaps the dead shim and
// drops the task. A containerd that stopped answering breaks the same Wait for
// every attempt at once, and a restarted containerd either reattaches
// still-running tasks or, having lost their state, reports every one NotFound.
// Only the first is bounded by one attempt. The claim therefore needs the same
// engine process the attempt started under to answer, inside the bound, that
// this task is gone or stopped (observeTaskAbsenceOnSameEngine). An engine
// that never answers, a task it still reports live, an engine whose identity
// changed or cannot be proven, a Wait ended by this helper's own cancellation,
// or a missing probe all leave the failure unscoped, which keeps it
// helper/engine-loss evidence exactly as before.
func proveTaskLossAttemptScoped(timeout time.Duration, waitErr error, observe func(context.Context) taskAbsenceObservation) bool {
	if waitErr == nil || observe == nil || errors.Is(waitErr, context.Canceled) || errors.Is(waitErr, context.DeadlineExceeded) {
		return false
	}
	if timeout <= 0 {
		timeout = DefaultTaskReleaseTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		switch observe(ctx) {
		case taskObservationGone:
			return true
		case taskObservationLive, taskObservationEngineChanged:
			return false
		}
		timer := time.NewTimer(taskLossScopeProbeInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
}
