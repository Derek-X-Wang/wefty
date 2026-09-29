package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestTerminalPublicationWaitsForExitedTaskRelease(t *testing.T) {
	ready := make(chan struct{})
	releaseEntered := make(chan struct{})
	allowRelease := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- publishTerminalAfterTaskRelease(time.Second, func(context.Context) error {
			close(releaseEntered)
			<-allowRelease
			return nil
		}, nil, func(sealReason string) {
			if sealReason != "" {
				t.Errorf("clean release recorded seal reason %q", sealReason)
			}
			close(ready)
		})
	}()
	<-releaseEntered
	select {
	case <-ready:
		t.Fatal("terminal became observable before the exited task released its logger pipes")
	case <-time.After(20 * time.Millisecond):
	}
	close(allowRelease)
	<-ready
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// errTaskStillRunning stands in for containerd's failed-precondition refusal to
// delete a task the shim still reports running.
var errTaskStillRunning = errors.New("task must be stopped before deletion: running: failed precondition")

// TestTerminalPublicationRetriesReleaseWhileTaskIsStillReportedRunning is the
// regression for issue #423. The exit status arrives on Wait before the
// runtime's task state leaves running, so the first deletion is refused. A
// single attempt published the terminal anyway, which started both stream seal
// deadlines while the shim's logger pipes were still open -- no pipe EOF, no
// seal, and an ordinary exit-0 payload carried incomplete log evidence.
func TestTerminalPublicationRetriesReleaseWhileTaskIsStillReportedRunning(t *testing.T) {
	var attempts atomic.Int64
	ready := make(chan struct{})
	var recorded string
	err := publishTerminalAfterTaskRelease(2*time.Second, func(context.Context) error {
		if attempts.Add(1) < 4 {
			return errTaskStillRunning
		}
		return nil
	}, func(err error) bool { return errors.Is(err, errTaskStillRunning) }, func(sealReason string) {
		recorded = sealReason
		if attempts.Load() < 4 {
			t.Errorf("terminal was published after %d release attempts, before the task stopped", attempts.Load())
		}
		close(ready)
	})
	if err != nil {
		t.Fatalf("release that eventually stopped returned %v", err)
	}
	<-ready
	if recorded != "" {
		t.Fatalf("released task recorded seal reason %q, want none", recorded)
	}
	if got := attempts.Load(); got != 4 {
		t.Fatalf("release attempts = %d, want 4", got)
	}
}

// TestTerminalPublicationRecordsTypedReasonWhenTaskNeverStops proves the bound
// still exists and that its expiry is named, not silently folded into a bare
// runtime precondition string.
func TestTerminalPublicationRecordsTypedReasonWhenTaskNeverStops(t *testing.T) {
	ready := make(chan struct{})
	var recorded string
	started := time.Now()
	err := publishTerminalAfterTaskRelease(120*time.Millisecond, func(context.Context) error {
		return errTaskStillRunning
	}, func(err error) bool { return errors.Is(err, errTaskStillRunning) }, func(sealReason string) {
		recorded = sealReason
		close(ready)
	})
	<-ready
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("release exceeded its budget by %v", elapsed)
	}
	var neverStopped *TaskNeverStoppedError
	if !errors.As(err, &neverStopped) {
		t.Fatalf("release error = %v, want a typed never-stopped failure", err)
	}
	if !errors.Is(err, errTaskStillRunning) {
		t.Fatalf("release error %v dropped the runtime refusal", err)
	}
	if recorded != TaskNeverStoppedSealReason {
		t.Fatalf("recorded seal reason = %q, want %q", recorded, TaskNeverStoppedSealReason)
	}
}

// A broken task Wait is attempt-scoped only when the engine answers, for that
// exact task, that it is gone or stopped -- what containerd says after it reaps
// a killed shim. Silence, a task still live, or a Wait this helper cancelled
// leave the failure unscoped, so it stays engine-loss evidence (#560).
func TestProveTaskLossAttemptScopedNeedsTheEngineToAnswerForTheTask(t *testing.T) {
	shimClosed := errors.New("rpc error: code = Unknown desc = ttrpc: closed")
	sequence := func(answers ...taskAbsenceObservation) (func(context.Context) taskAbsenceObservation, *int) {
		calls := 0
		return func(context.Context) taskAbsenceObservation {
			answer := answers[min(calls, len(answers)-1)]
			calls++
			return answer
		}, &calls
	}
	for _, test := range []struct {
		name    string
		waitErr error
		answers []taskAbsenceObservation
		want    bool
	}{
		{name: "shim reaped after its cleanup window", waitErr: shimClosed, answers: []taskAbsenceObservation{taskObservationUnproven, taskObservationUnproven, taskObservationGone}, want: true},
		{name: "task gone at once", waitErr: shimClosed, answers: []taskAbsenceObservation{taskObservationGone}, want: true},
		{name: "engine never answers", waitErr: errors.New("rpc error: code = Unavailable desc = connection refused"), answers: []taskAbsenceObservation{taskObservationUnproven}},
		{name: "task still live", waitErr: shimClosed, answers: []taskAbsenceObservation{taskObservationLive, taskObservationGone}},
		{name: "wait cancelled by this helper", waitErr: fmt.Errorf("wait: %w", context.Canceled), answers: []taskAbsenceObservation{taskObservationGone}},
		{name: "no wait failure", answers: []taskAbsenceObservation{taskObservationGone}},
	} {
		t.Run(test.name, func(t *testing.T) {
			observe, calls := sequence(test.answers...)
			if got := proveTaskLossAttemptScoped(200*time.Millisecond, test.waitErr, observe); got != test.want {
				t.Fatalf("attempt scoped = %t after %d observations, want %t", got, *calls, test.want)
			}
		})
	}
	if proveTaskLossAttemptScoped(time.Second, shimClosed, nil) {
		t.Fatal("a missing engine probe proved a runtime failure attempt-scoped")
	}
}

// Task absence alone does not prove a shim loss: a containerd that restarted
// and lost task state answers NotFound for every task. The answer counts only
// when the engine identity read after it equals the one the attempt started
// under (#560 review).
func TestObserveTaskAbsenceOnSameEngineRequiresEngineContinuity(t *testing.T) {
	started := engineIdentity{InstanceUUID: "containerd-uuid", PID: 100, PIDNamespace: 4026531836, StartTime: 5821}
	gone := func(context.Context) taskAbsenceObservation { return taskObservationGone }
	identityIs := func(identity engineIdentity, err error) func(context.Context) (engineIdentity, error) {
		return func(context.Context) (engineIdentity, error) { return identity, err }
	}
	restarted := started
	restarted.PID, restarted.StartTime = 200, 9120
	reusedPID := started
	reusedPID.StartTime = 9120
	otherNamespace := started
	otherNamespace.PIDNamespace = 4026532000
	for _, test := range []struct {
		name     string
		baseline engineIdentity
		task     func(context.Context) taskAbsenceObservation
		identity func(context.Context) (engineIdentity, error)
		want     taskAbsenceObservation
	}{
		{name: "same engine answers gone", baseline: started, task: gone, identity: identityIs(started, nil), want: taskObservationGone},
		{name: "restarted engine answers gone", baseline: started, task: gone, identity: identityIs(restarted, nil), want: taskObservationEngineChanged},
		{name: "restarted engine reused the PID", baseline: started, task: gone, identity: identityIs(reusedPID, nil), want: taskObservationEngineChanged},
		{name: "engine in another PID namespace answers gone", baseline: started, task: gone, identity: identityIs(otherNamespace, nil), want: taskObservationEngineChanged},
		{name: "identity unreadable", baseline: started, task: gone, identity: identityIs(engineIdentity{}, errors.New("introspection unavailable")), want: taskObservationUnproven},
		{name: "identity incomplete", baseline: started, task: gone, identity: identityIs(engineIdentity{InstanceUUID: "containerd-uuid"}, nil), want: taskObservationUnproven},
		{name: "identity without a start time", baseline: started, task: gone, identity: identityIs(engineIdentity{InstanceUUID: "containerd-uuid", PID: 100, PIDNamespace: 4026531836}, nil), want: taskObservationUnproven},
		{name: "no identity at attempt start", task: gone, identity: identityIs(started, nil), want: taskObservationEngineChanged},
		{name: "task still live", baseline: started, task: func(context.Context) taskAbsenceObservation { return taskObservationLive }, identity: identityIs(started, nil), want: taskObservationLive},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := observeTaskAbsenceOnSameEngine(t.Context(), test.baseline, test.task, test.identity); got != test.want {
				t.Fatalf("observation = %d, want %d", got, test.want)
			}
		})
	}
	calls := 0
	changed := func(context.Context) taskAbsenceObservation { calls++; return taskObservationEngineChanged }
	if proveTaskLossAttemptScoped(time.Second, errors.New("ttrpc: closed"), changed) || calls != 1 {
		t.Fatalf("a changed engine scoped the failure or was re-asked (%d calls)", calls)
	}
}

// The scope probe is carved out of the task-release budget, never added to it,
// so a broken Wait still reaches its sealed terminal inside the agent's fixed
// post-KILL Watch budget (5 s release + 5 s seal + 1 s margin).
func TestScopeProbeBoundIsCarvedOutOfTheReleaseBudget(t *testing.T) {
	if got := scopeProbeBound(DefaultTaskReleaseTimeout); got != 2*time.Second {
		t.Fatalf("default probe bound = %s, want 2s", got)
	}
	for _, release := range []time.Duration{0, time.Millisecond, 400 * time.Millisecond, DefaultTaskReleaseTimeout, time.Minute} {
		effective := release
		if effective <= 0 {
			effective = DefaultTaskReleaseTimeout
		}
		if probe := scopeProbeBound(release); probe <= 0 || probe > effective/2 || probe > taskLossScopeProbeBudget {
			t.Fatalf("probe bound for release %s = %s, want a positive share of at most half the release and %s", release, probe, taskLossScopeProbeBudget)
		}
	}
}

// A deadline already spent by the probe still publishes the terminal at once
// instead of falling back to a fresh release budget.
func TestTerminalPublicationByPassedDeadlinePublishesAtOnce(t *testing.T) {
	published := make(chan struct{})
	begin := time.Now()
	err := publishTerminalAfterTaskReleaseBy(begin.Add(-time.Millisecond), func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}, nil, func(string) { close(published) })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("release error = %v, want the spent deadline", err)
	}
	select {
	case <-published:
	default:
		t.Fatal("terminal was not published")
	}
	if elapsed := time.Since(begin); elapsed > time.Second {
		t.Fatalf("publication after a spent deadline took %s", elapsed)
	}
}
