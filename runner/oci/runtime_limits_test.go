package oci

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
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
			store, claim := runtimeLimitL1Attempt(t, &request)
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
			case <-time.After(testwait.Bound):
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
			case <-time.After(testwait.Bound):
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
			encoded, err := json.Marshal(got.result.Outcome)
			if err != nil {
				t.Fatal(err)
			}
			// The node carries exit-code provenance beside the result, as the
			// production agent does; the public result has no initiator field.
			var wire map[string]json.RawMessage
			if err := json.Unmarshal(encoded, &wire); err != nil {
				t.Fatal(err)
			}
			delete(wire, "termination_initiator")
			encoded, err = json.Marshal(wire)
			if err != nil {
				t.Fatal(err)
			}
			var result l1.ProcessResult
			if err := json.Unmarshal(encoded, &result); err != nil {
				t.Fatal(err)
			}
			job, err := store.CompleteAttempt(t.Context(), "agent", claim.Job.JobID, claim.Lease.AttemptID, l1.CompletionRequest{
				FencingToken: claim.Lease.FencingToken, IdempotencyKey: "limit", Result: result,
				TerminationInitiator: got.result.Outcome.TerminationInitiator,
			})
			if err != nil {
				t.Fatal(err)
			}
			job, err = store.GetJob(t.Context(), claim.Job.JobID)
			if err != nil {
				t.Fatal(err)
			}
			job.Attempts, err = store.ListJobAttempts(t.Context(), claim.Job.JobID)
			if err != nil {
				t.Fatal(err)
			}
			if len(job.Attempts) != 1 {
				t.Fatalf("attempts=%+v", job.Attempts)
			}
			if job.State != contract.JobFailed || job.Attempts[0].State != contract.AttemptFailed || job.Attempts[0].Result == nil || job.Attempts[0].Result.LimitReached != contract.LimitReachedMaxRuntime {
				t.Fatalf("L1 completion=%+v attempts=%+v", job, job.Attempts)
			}
			if !reflect.DeepEqual(job.Attempts[0].Result.ExitCode, got.result.Outcome.ExitCode) || job.Attempts[0].Result.Signal != got.result.Outcome.Signal || job.Attempts[0].Result.TerminationCause != got.result.Outcome.TerminationCause {
				t.Fatal("L1 changed payload evidence")
			}
		})
	}
}

func runtimeLimitL1Attempt(t *testing.T, request *workloadrunner.Request) (*l1.Store, *l1.Claim) {
	t.Helper()
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	_, err = store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, contract.NodeRegistration{
		NodeID: "node", BootSessionID: "boot", RootInstanceID: "root", OS: "linux", Architecture: "amd64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:oci": true, "runtime_handler:" + request.RuntimeHandler: true}, CapabilityRevision: 1, CapabilityObservedAt: time.Now().UTC(), MissingCapabilities: []string{},
	}, l1.DefaultNodePolicy(), true)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.CreateJob(t.Context(), contract.JobSpec{SchemaVersion: 1, DispatchKey: "oci-limit", Kind: "oci", Class: "one-shot", RuntimeHandler: request.RuntimeHandler, Execution: request.Execution, Limits: request.Limits})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimJob(t.Context(), "agent", "node", "boot", "one-shot")
	if err != nil || claim == nil {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	request.Authority.JobID, request.Authority.AttemptID, request.Authority.FencingToken = claim.Job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken
	request.Authority.RemovalGeneration = claim.Lease.AttemptID
	request.OCIImageResolved = func(ctx context.Context, image workloadrunner.OCIImageObservation) error {
		_, err := store.ObserveAttemptImage(ctx, "agent", claim.Job.JobID, claim.Lease.AttemptID, l1.ImageObservationRequest{
			FencingToken: claim.Lease.FencingToken, SubmittedReference: image.SubmittedReference, TopLevelDigest: image.TopLevelDigest, TopLevelMediaType: image.TopLevelMediaType, IndexDigest: image.IndexDigest, PlatformManifestDigest: image.PlatformManifestDigest,
			Platform: l1.OCIPlatform{OS: image.PlatformOS, Architecture: image.PlatformArchitecture, Variant: image.PlatformVariant}, RuntimeHandler: image.RuntimeHandler, Snapshotter: image.Snapshotter,
		})
		return err
	}
	request.OCIStarted = func(ctx context.Context, _ workloadrunner.OCIImageObservation) error {
		_, err := store.StartAttempt(ctx, "agent", claim.Job.JobID, claim.Lease.AttemptID, l1.StartedRequest{FencingToken: claim.Lease.FencingToken})
		return err
	}
	return store, claim
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
	case <-time.After(testwait.Bound):
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
