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
