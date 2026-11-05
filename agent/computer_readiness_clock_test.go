package agent

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

// Keep awake time separate from Now so a sleep can advance wall time and fire
// a scheduling timer without consuming any of the readiness budget.
type computerReadinessTestClock struct {
	*manualClock
	awake    time.Duration
	awakeErr error
}

func newComputerReadinessTestClock() *computerReadinessTestClock {
	return &computerReadinessTestClock{manualClock: newManualClock(time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)), awake: time.Hour}
}

func (clock *computerReadinessTestClock) AwakeElapsed() (time.Duration, error) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.awake, clock.awakeErr
}

// Move readings without delivering timers, so tests can choose the deadline
// edge independently of a successful probe or an endpoint connect timeout.
func (clock *computerReadinessTestClock) elapse(awake, sleep time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.awake += awake
	clock.now = clock.now.Add(awake + sleep)
	clock.wall = clock.wall.Add(awake + sleep)
}

func (clock *computerReadinessTestClock) fire(t *testing.T, deadline time.Time) {
	t.Helper()
	clock.mu.Lock()
	for _, timer := range clock.timers {
		if timer.active && timer.deadline.Equal(deadline) {
			timer.active = false
			now := clock.now
			clock.mu.Unlock()
			timer.channel <- now
			return
		}
	}
	clock.mu.Unlock()
	t.Fatalf("no active timer at %s", deadline)
}

func assertComputerStartupTimeout(t *testing.T, err error) {
	t.Helper()
	var readiness *computerReadinessError
	if !errors.As(err, &readiness) || readiness.Code != contract.SpawnFailureStartupReadinessTimeout {
		t.Fatalf("readiness error = %#v, want startup_readiness_timeout", err)
	}
}

func TestComputerReadinessSleepRearmsDeadline(t *testing.T) {
	for _, phase := range []string{"probe", "interval"} {
		t.Run(phase, func(t *testing.T) {
			clock := newComputerReadinessTestClock()
			anchor := captureComputerReadinessClock(clock)
			originalDeadline := clock.Now().Add(DefaultComputerReadinessDeadline)
			clock.elapse(10*time.Second, 0)
			backend := newComputerBackend(t, computerBackendOptions{})
			defer backend.Close()
			releaseProbe := make(chan struct{})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			dial := func(ctx context.Context, _ string) (net.Conn, error) {
				if phase == "interval" {
					select {
					case <-releaseProbe:
					default:
						return nil, errors.New("display starting")
					}
				} else {
					select {
					case <-releaseProbe:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				return backend.dial(ctx)
			}
			started := make(chan computerReadinessClock, 1)
			started <- anchor
			observations := make(chan bool, 2)
			done := make(chan error, 1)
			go func() {
				done <- monitorComputerReadiness(ctx, clock, started, dial, func(ready bool) { observations <- ready })
			}()
			clock.waitForDeadline(t, originalDeadline)
			intervalDeadline := clock.Now().Add(DefaultComputerReadinessProbeInterval)
			if phase == "interval" {
				clock.waitForDeadline(t, intervalDeadline)
			} else {
				clock.waitForDeadline(t, clock.Now().Add(DefaultComputerReadinessConnectTimeout))
			}
			clock.elapse(0, 2*time.Minute)
			clock.fire(t, originalDeadline)
			// A re-armed timer proves the fired deadline was handled before the
			// successful probe. It must preserve the 50 seconds still awake.
			clock.waitForDeadline(t, clock.Now().Add(50*time.Second))
			wantNoComputerReadinessObservation(t, observations)
			close(releaseProbe)
			if phase == "interval" {
				clock.fire(t, intervalDeadline)
			}
			wantComputerReadinessObservation(t, observations, true)
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("sleep consumed readiness budget: %v", err)
			}
		})
	}
}

func TestComputerReadinessAwakeDeadline(t *testing.T) {
	clock := newComputerReadinessTestClock()
	anchor := captureComputerReadinessClock(clock)
	deadline := clock.Now().Add(DefaultComputerReadinessDeadline)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	blocked := func(ctx context.Context, _ string) (net.Conn, error) { <-ctx.Done(); return nil, ctx.Err() }
	done := make(chan error, 1)
	go func() { done <- probeComputerBackends(ctx, clock, anchor, blocked) }()
	clock.waitForDeadline(t, deadline)
	clock.elapse(DefaultComputerReadinessDeadline, 0)
	clock.fire(t, deadline)
	assertComputerStartupTimeout(t, <-done)
}

func TestComputerReadinessSuccessfulProbeRechecksAwakeBudget(t *testing.T) {
	for _, elapsed := range []time.Duration{10 * time.Second, 60 * time.Second, 61 * time.Second} {
		t.Run(elapsed.String(), func(t *testing.T) {
			clock := newComputerReadinessTestClock()
			anchor := captureComputerReadinessClock(clock)
			backend := newComputerBackend(t, computerBackendOptions{})
			defer backend.Close()
			dial := func(ctx context.Context, name string) (net.Conn, error) {
				if name == workloadrunner.AttemptEndpointControl {
					// No timer is delivered: only the successful-probe recheck can
					// refuse a late result. Now also jumps across a host sleep.
					clock.elapse(elapsed, 2*time.Minute)
				}
				return backend.dial(ctx)
			}
			err := probeComputerBackends(t.Context(), clock, anchor, dial)
			if elapsed < DefaultComputerReadinessDeadline {
				if err != nil {
					t.Fatalf("successful probe after sleep: %v", err)
				}
			} else {
				assertComputerStartupTimeout(t, err)
			}
		})
	}
}

func TestComputerReadinessUnavailableAwakeSourceUsesAgentAnchor(t *testing.T) {
	for _, mode := range []string{"absent", "anchor error", "later error"} {
		t.Run(mode, func(t *testing.T) {
			fake := newComputerReadinessTestClock()
			var clock Clock = fake
			if mode == "absent" {
				clock = fake.manualClock
			} else if mode == "anchor error" {
				fake.awakeErr = errors.New("awake clock unavailable")
			}
			anchor := captureComputerReadinessClock(clock)
			if mode == "anchor error" {
				fake.awakeErr = nil // A later recovery cannot replace the anchor.
			} else if mode == "later error" {
				fake.awakeErr = errors.New("awake clock unavailable")
			}
			// The agent anchor has already spent five seconds before polling.
			fake.Advance(5 * time.Second)
			if remaining := anchor.remaining(clock); remaining != 55*time.Second {
				t.Fatalf("fallback remaining = %s, want 55s from the agent anchor", remaining)
			}
			deadline := anchor.now.Add(DefaultComputerReadinessDeadline)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			blocked := func(ctx context.Context, _ string) (net.Conn, error) { <-ctx.Done(); return nil, ctx.Err() }
			done := make(chan error, 1)
			go func() { done <- probeComputerBackends(ctx, clock, anchor, blocked) }()
			fake.waitForDeadline(t, deadline)
			fake.Advance(55 * time.Second)
			assertComputerStartupTimeout(t, <-done)
		})
	}
}

func TestComputerServiceReadinessAnchorsAtStartedReport(t *testing.T) {
	for _, test := range []struct {
		name       string
		helperAge  time.Duration
		roundTrips time.Duration
		ready      bool
	}{
		{name: "stale helper gets full agent window", helperAge: 24 * time.Hour, roundTrips: 59 * time.Second, ready: true},
		{name: "post-start round trips consume budget", roundTrips: 60 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			clock := newComputerReadinessTestClock()
			runtime := &opaqueEndpointRuntime{release: make(chan struct{}), startedAt: clock.Now().Add(-test.helperAge)}
			cache := NewComputerPolicyCache(clock, "node-1", "boot-1")
			defer cache.Close()
			backend := newComputerBackend(t, computerBackendOptions{})
			defer backend.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			publications := make(chan bool, 2)
			done := make(chan error, 1)
			go func() {
				_, err := runComputerService(ctx, runtime, workloadrunner.Request{
					OCIStartedAt: func(time.Time) { clock.elapse(test.roundTrips, 0) },
				}, nil, computerServiceConfig{
					clock: clock, fabric: &recordingComputerServiceFabric{identity: fabric.Identity{}},
					authorizer: cache, auditor: &recordingComputerAuditor{}, publicationOperation: testComputerPublicationOperation,
					computerID: "computer-1", jobID: "job-1", attemptID: "attempt-1", storageID: "storage-1", storageGeneration: 1, fencingToken: "fence-1",
					dial:    func(ctx context.Context, _ string) (net.Conn, error) { return backend.dial(ctx) },
					publish: func(_ context.Context, ready bool, _ string, _ int64) error { publications <- ready; return nil },
				})
				done <- err
			}()
			if test.ready {
				select {
				case ready := <-publications:
					if !ready {
						t.Fatal("stale helper timestamp prevented readiness")
					}
				case err := <-done:
					t.Fatalf("stale helper timestamp consumed readiness window: %v", err)
				case <-time.After(testwait.Bound):
					t.Fatal("Computer did not publish ready")
				}
				cancel()
				<-done
			} else {
				select {
				case err := <-done:
					assertComputerStartupTimeout(t, err)
				case <-time.After(testwait.Bound):
					t.Fatal("post-start round trips did not consume readiness budget")
				}
				wantNoComputerReadinessObservation(t, publications)
			}
		})
	}
}
