package oci

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func TestOCIMaxRuntimeStopsExactAttempt(t *testing.T) {
	for _, ignoreTERM := range []bool{false, true} {
		name := "TERM_handled"
		if ignoreTERM {
			name = "TERM_ignored"
		}
		t.Run(name, func(t *testing.T) {
			startedAt := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
			clock := &runtimeTestClock{now: startedAt.Add(3 * time.Second), timers: make(chan *runtimeTestTimer, 4)}
			engine := &adapterTestEngine{startedAt: startedAt, watchSignals: make(chan ocihelper.Signal, 2), ignoreTERM: ignoreTERM, termExitCode: intPointer(0)}
			adapter, closeAdapter := startAdapterTestServer(t, engine)
			defer closeAdapter()
			WithClock(clock)(adapter)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			defer func() {
				select {
				case engine.watchSignals <- ocihelper.SignalKILL:
				default:
				}
			}()
			request := adapterTestRequest()
			request.Limits = &contract.JobLimits{MaxRuntimeSeconds: 10}
			request.TerminationGrace = 2 * time.Second
			ready := make(chan struct{})
			request.Started = func() { close(ready) }
			type outcome struct {
				result workloadrunner.Result
				err    error
			}
			finished := make(chan outcome, 1)
			go func() { result, err := adapter.Run(ctx, request, nil); finished <- outcome{result, err} }()
			select {
			case <-ready:
			case <-time.After(5 * time.Second):
				t.Fatal("attempt never started")
			}
			runtimeTimer := clock.next(t)
			if runtimeTimer.duration != 10*time.Second {
				t.Fatalf("runtime remaining = %s, want the full 10s on the agent clock despite a 3s-old helper StartedAt", runtimeTimer.duration)
			}
			select {
			case result := <-finished:
				t.Fatalf("stopped before runtime deadline: %+v", result)
			default:
			}
			runtimeTimer.fire()
			if ignoreTERM {
				grace := clock.next(t)
				if grace.duration != 2*time.Second {
					t.Fatalf("TERM grace = %s", grace.duration)
				}
				grace.fire()
			}
			var got outcome
			select {
			case got = <-finished:
			case <-time.After(5 * time.Second):
				t.Fatal("runtime expiry did not join termination")
			}
			if got.err == nil || !strings.Contains(got.err.Error(), "maximum runtime exceeded") || got.result.Outcome.RuntimeFailure != nil || got.result.Outcome.SpawnError != nil {
				t.Fatalf("runtime result = %+v error=%v", got.result, got.err)
			}
			if ignoreTERM {
				if got.result.Outcome.Signal != "killed" || got.result.Outcome.TerminationCause != contract.TerminationCauseAgent {
					t.Fatalf("KILL result=%+v", got.result)
				}
			} else if got.result.Outcome.ExitCode == nil || *got.result.Outcome.ExitCode != 0 || got.result.Outcome.TerminationInitiator != contract.TerminationCauseAgent {
				t.Fatalf("TERM result=%+v", got.result)
			}
			engine.mu.Lock()
			signals := append([]ocihelper.Signal(nil), engine.signals...)
			authority, deletes := engine.lastRun.Authority, engine.runtimeDeletes
			engine.mu.Unlock()
			want := []ocihelper.Signal{ocihelper.SignalTERM}
			if ignoreTERM {
				want = append(want, ocihelper.SignalKILL)
			}
			if !reflect.DeepEqual(signals, want) || authority != HelperAuthority(request.Authority) || deletes != 0 {
				t.Fatalf("stop authority=%+v signals=%v deletes=%d", authority, signals, deletes)
			}
		})
	}
}

type runtimeTestClock struct {
	now    time.Time
	timers chan *runtimeTestTimer
}

func (clock *runtimeTestClock) Now() time.Time { return clock.now }
func (clock *runtimeTestClock) NewTimer(duration time.Duration) Timer {
	timer := &runtimeTestTimer{duration: duration, c: make(chan time.Time, 1)}
	clock.timers <- timer
	return timer
}
func (clock *runtimeTestClock) next(t *testing.T) *runtimeTestTimer {
	t.Helper()
	select {
	case timer := <-clock.timers:
		return timer
	case <-time.After(time.Second):
		t.Fatal("OCI did not arm runtime/termination timer")
		return nil
	}
}

type runtimeTestTimer struct {
	duration time.Duration
	c        chan time.Time
}

func (timer *runtimeTestTimer) C() <-chan time.Time { return timer.c }
func (timer *runtimeTestTimer) Stop() bool          { return true }
func (timer *runtimeTestTimer) fire()               { timer.c <- time.Time{} }
