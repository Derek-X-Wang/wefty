//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

// A Cancel directive keeps the attempt's authority alive until the payload
// is gone (#1444). L1 accepts renewals until the cancel settlement deadline;
// the loop delivers the directive once and keeps renewing and feeding the
// helper deadman until the attempt completes or L1 refuses a renewal.
func TestCancelDirectiveKeepsRenewingUntilCompletionOrRefusal(t *testing.T) {
	const refuseAt = 4
	for _, exit := range []string{"completion", "refusal"} {
		t.Run(exit, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				clock := systemClock{}
				var renewals atomic.Int32
				client := &Client{baseURL: "http://wefty.invalid", operationTimeout: time.Second, httpClient: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
					if r.URL.Path != "/v1/agent/jobs/job/attempts/attempt/lease" {
						t.Errorf("unexpected request %s", r.URL.Path)
					}
					if renewals.Add(1) >= refuseAt && exit == "refusal" {
						body, _ := json.Marshal(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorLeaseExpired}})
						return &http.Response{StatusCode: http.StatusConflict, Body: io.NopCloser(strings.NewReader(string(body))), Header: make(http.Header)}, nil
					}
					raw, _ := json.Marshal(l1.AttemptLease{AttemptID: "attempt", FencingToken: "fence", LeaseTTL: time.Second, Directive: l1.AttemptDirectiveCancel})
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(raw))), Header: make(http.Header)}, nil
				})}}
				lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{clock: clock, client: client, renewalInterval: 100 * time.Millisecond})
				claim := l1.Claim{Job: l1.Job{JobID: "job", Spec: contract.JobSpec{Kind: contract.JobKindOCI, Class: contract.JobClassOneShot}},
					Lease: l1.AttemptLease{AttemptID: "attempt", FencingToken: "fence", LeaseTTL: time.Second}}
				renewer := newRecordingDeadmanRenewer()
				admission := newAttemptDeadmanAdmission(renewer, claim, clock, t.Logf)
				if err := admission.admit(readyOCIHelperGeneration()); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				failures := make(chan destinationError, 1)
				done := make(chan struct{})
				go func() {
					defer close(done)
					lifecycle.renewalLoop(ctx, claim, localAuthority{deadline: clock.Now().Add(time.Second)}, failures, disabledAttemptWatch{}, admission)
				}()
				if first := <-failures; !errors.Is(first.err, errAttemptDirectiveCancel) {
					t.Fatalf("first delivery=%v, want the Cancel directive", first.err)
				}
				if exit == "completion" {
					time.Sleep(time.Second)
					synctest.Wait()
					got := int(renewals.Load())
					calls, _, _ := renewer.snapshot()
					if got < 5 || calls != got {
						t.Fatalf("renewals=%d deadman grants=%d; want renewing and the deadman fed after Cancel", got, calls)
					}
					select {
					case again := <-failures:
						t.Fatalf("Cancel reached the runner again: %v", again.err)
					default:
					}
					// The lifecycle stops renewal once the attempt has completed.
					cancel()
					<-done
				} else {
					<-done
					if got := renewals.Load(); got != refuseAt {
						t.Fatalf("renewals=%d, want renewing to stop at the refusal (%d)", got, refuseAt)
					}
					if calls, _, _ := renewer.snapshot(); calls != refuseAt-1 {
						t.Fatalf("deadman grants=%d, want one per accepted renewal (%d)", calls, refuseAt-1)
					}
					refusal := <-failures
					if protocolErrorCode(refusal.err) != contract.ErrorLeaseExpired {
						t.Fatalf("refusal=%v, want the lease-expired lost-lease path", refusal.err)
					}
				}
				// After either exit the deadman is no longer fed, so a dead
				// agent's container is still reaped by the helper.
				renewalsAtExit := renewals.Load()
				callsAtExit, _, _ := renewer.snapshot()
				if err := admission.queue(l1.AttemptLease{AttemptID: "attempt", LeaseTTL: time.Second}, clock.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				time.Sleep(time.Second)
				synctest.Wait()
				if calls, _, _ := renewer.snapshot(); calls != callsAtExit || renewals.Load() != renewalsAtExit {
					t.Fatalf("after exit: deadman grants %d -> %d, renewals %d -> %d", callsAtExit, calls, renewalsAtExit, renewals.Load())
				}
			})
		})
	}
}

// Stop and Restart still end renewal at the first directive and never feed
// the helper deadman.
func TestStopAndRestartDirectivesStillEndRenewal(t *testing.T) {
	for _, tt := range []struct {
		directive l1.AttemptDirective
		want      error
	}{
		{l1.AttemptDirectiveStop, errAttemptDirectiveStop},
		{l1.AttemptDirectiveRestart, errAttemptDirectiveRestart},
	} {
		t.Run(string(tt.directive), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				clock := systemClock{}
				var renewals atomic.Int32
				client := &Client{baseURL: "http://wefty.invalid", operationTimeout: time.Second, httpClient: &http.Client{Transport: captureRoundTrip(func(*http.Request) (*http.Response, error) {
					renewals.Add(1)
					raw, _ := json.Marshal(l1.AttemptLease{AttemptID: "attempt", FencingToken: "fence", LeaseTTL: time.Second, Directive: tt.directive})
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(raw))), Header: make(http.Header)}, nil
				})}}
				lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{clock: clock, client: client, renewalInterval: 100 * time.Millisecond})
				claim := l1.Claim{Job: l1.Job{JobID: "job", Spec: contract.JobSpec{Kind: contract.JobKindOCI, Class: contract.JobClassOneShot}},
					Lease: l1.AttemptLease{AttemptID: "attempt", FencingToken: "fence", LeaseTTL: time.Second}}
				renewer := newRecordingDeadmanRenewer()
				admission := newAttemptDeadmanAdmission(renewer, claim, clock, t.Logf)
				if err := admission.admit(readyOCIHelperGeneration()); err != nil {
					t.Fatal(err)
				}
				failures := make(chan destinationError, 1)
				done := make(chan struct{})
				go func() {
					defer close(done)
					lifecycle.renewalLoop(t.Context(), claim, localAuthority{deadline: clock.Now().Add(time.Second)}, failures, disabledAttemptWatch{}, admission)
				}()
				<-done
				if failure := <-failures; !errors.Is(failure.err, tt.want) {
					t.Fatalf("directive=%v, want %v", failure.err, tt.want)
				}
				if calls, _, _ := renewer.snapshot(); renewals.Load() != 1 || calls != 0 {
					t.Fatalf("renewals=%d deadman grants=%d; want one renewal and no grant", renewals.Load(), calls)
				}
			})
		})
	}
}

// slowKillOCIRuntime is a payload that ignores TERM: after the agent cancels
// it, the runtime takes longer than a whole lease to report its exit, as the
// grace period, KILL and helper release do on a real engine.
type slowKillOCIRuntime struct {
	*cancelResultOCIRuntime
	hold     time.Duration
	canceled chan struct{}
}

func (r *slowKillOCIRuntime) Run(ctx context.Context, request workloadrunner.Request, sink workloadrunner.OutputSink) (workloadrunner.Result, error) {
	go func() {
		<-ctx.Done()
		close(r.canceled)
	}()
	result, err := r.cancelResultOCIRuntime.Run(ctx, request, sink)
	time.Sleep(r.hold)
	return result, err
}

// A canceled OCI one-shot whose payload outlives one lease keeps its result
// (#1444): the node renews through the forced stop, so L1 accepts the
// completion inside the cancel settlement window instead of losing the
// attempt at the first lease expiry.
func TestCancelSlowForcedStopKeepsLeaseAndResult(t *testing.T) {
	const lease = 2 * time.Second
	for _, delivery := range []string{"renewal"} {
		t.Run(delivery, func(t *testing.T) {
			network := plain.NewNetwork()
			_, stopServer := startFailureServerWithPoliciesAndLease(t, network, nil,
				map[string]l1.NodePolicy{"node-1": l1.DefaultNodePolicy("oci-slow-kill")}, lease)
			defer stopServer()
			submitter, err := NewClient(network.NewFabric(fabric.Identity{NodeID: "ordinary-app", Tags: []string{l1.DefaultClientPrincipalTag}}), "wefty://control-plane")
			if err != nil {
				t.Fatal(err)
			}
			defer submitter.Close()
			digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			spec := contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: "slow-kill-" + delivery, Kind: contract.JobKindOCI, Class: contract.JobClassOneShot,
				RoutingTags: []string{"oci-slow-kill"}, RuntimeHandler: "io.containerd.runc.v2",
				Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "example.invalid/result:v1", Digest: &digest}, Argv: []string{"/payload"}}},
			}
			var job l1.Job
			if err := submitter.post(t.Context(), "/v1/jobs", spec, &job); err != nil {
				t.Fatal(err)
			}
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			runtime := &slowKillOCIRuntime{
				cancelResultOCIRuntime: &cancelResultOCIRuntime{phase: "started", ready: make(chan struct{}), release: make(chan struct{}), publicationOCIRuntime: publicationOCIRuntime{fakeRunMailboxRuntime: newFakeRunMailboxRuntime()}},
				hold:                   2*lease + lease/2, canceled: make(chan struct{}),
			}
			// Heartbeat-delivered Cancel is pinned deterministically by
			// TestHeartbeatCancelKeepsRenewingThroughSlowStop; here only
			// renewal can deliver it.
			heartbeatInterval := time.Hour
			renewer := newRecordingDeadmanRenewer()
			nodeAgent, err := New(Config{
				Fabric: network.NewFabric(fabric.Identity{NodeID: "agent", Tags: []string{l1.DefaultAgentPrincipalTag}}), ControlPlaneAddress: "wefty://control-plane",
				NodeID: "node-1", BootSessionID: "boot-slow-kill", Version: "test", OS: "linux", Architecture: "amd64",
				Capabilities: map[string]bool{"kind:oci": true, "runtime_handler:io.containerd.runc.v2": true},
				CapabilityProbe: capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
					return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true, "runtime_handler:io.containerd.runc.v2": true}}, nil
				}),
				AttemptDeadman: renewer, OCIBootBarrier: readyOCIBootBarrier{},
				OCIIntent: func(context.Context) (OCIIntentObservation, error) {
					return OCIIntentObservation{Enabled: true, Revision: 1}, nil
				},
				WorkloadRuntimes: map[string]WorkloadRuntime{contract.JobKindOCI: runtime}, ManagedRootDirectory: root, LogSpoolDirectory: t.TempDir(), HandoffRoot: t.TempDir(),
				HeartbeatInterval: heartbeatInterval, ClaimInterval: 5 * time.Millisecond, RenewalInterval: 100 * time.Millisecond, Logf: t.Logf,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer nodeAgent.Close()
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- nodeAgent.Run(ctx) }()
			defer func() {
				cancel()
				if err := <-done; err != nil {
					t.Errorf("agent Run: %v", err)
				}
			}()
			select {
			case <-runtime.ready:
			case <-time.After(testwait.Bound):
				t.Fatal("OCI payload never started")
			}
			var pending l1.Job
			if err := submitter.post(t.Context(), "/v1/jobs/"+job.JobID+"/cancel", nil, &pending); err != nil {
				t.Fatal(err)
			}
			select {
			case <-runtime.canceled:
			case <-time.After(testwait.Bound):
				t.Fatal("the agent never canceled the payload")
			}
			grantsAtCancel, _, _ := renewer.snapshot()
			deadline := time.Now().Add(runtime.hold + 10*time.Second*raceTimeoutScale)
			var finished l1.Job
			for {
				if err := submitter.request(t.Context(), http.MethodGet, "/v1/jobs/"+job.JobID, nil, &finished); err != nil {
					t.Fatal(err)
				}
				if finished.State == contract.JobFailed {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("job did not settle: %+v", finished)
				}
				time.Sleep(10 * time.Millisecond)
			}
			if finished.Outcome != "canceled" || len(finished.Attempts) != 1 || finished.Attempts[0].Result == nil || finished.Attempts[0].State == contract.AttemptLost {
				t.Fatalf("canceled without completion evidence: attempts=%+v", finished.Attempts)
			}
			if grants, _, _ := renewer.snapshot(); grants <= grantsAtCancel {
				t.Fatalf("helper deadman grants %d -> %d during the forced stop; want it fed while the lease is renewed", grantsAtCancel, grants)
			}
			if runs := runtime.payloadRuns.Load(); runs != 1 {
				t.Fatalf("payload runs=%d, want one", runs)
			}
		})
	}
}

// slowStopExecutor ignores TERM: once canceled it takes longer than a whole
// lease to exit, as a forced stop does on a real engine.
type slowStopExecutor struct {
	started chan struct{}
	hold    time.Duration
}

func (e *slowStopExecutor) Run(ctx context.Context, _ processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
	close(e.started)
	<-ctx.Done()
	time.Sleep(e.hold)
	return contract.ProcessResult{Signal: "killed", TerminationCause: contract.TerminationCauseAgent}, nil
}

// A Cancel delivered by heartbeat cancels the attempt with the same cause as a
// renewal directive. L1 has not yet told the renewal loop, so its renewals
// carry no directive; renewal must still continue through the slow stop and
// end once the result is final (#1444).
func TestHeartbeatCancelKeepsRenewingThroughSlowStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := systemClock{}
		lease := l1.AttemptLease{AttemptID: "attempt", FencingToken: "fence", LeaseTTL: time.Second}
		var renewals, completions atomic.Int32
		client := &Client{baseURL: "http://control-plane.invalid", operationTimeout: time.Second, httpClient: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
			switch {
			case strings.HasSuffix(r.URL.Path, "/lease"):
				renewals.Add(1)
				raw, _ := json.Marshal(lease)
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(raw))), Header: make(http.Header)}, nil
			case strings.HasSuffix(r.URL.Path, "/complete"):
				completions.Add(1)
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}, nil
		})}}
		payload := &slowStopExecutor{started: make(chan struct{}), hold: 3 * time.Second}
		lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{clock: clock, client: client, watchdog: newAuthorityWatchdog(clock), runtimes: testRuntimeSet(payload),
			observer: newLifecycleObserver(clock), renewalInterval: 100 * time.Millisecond, completionRetry: 100 * time.Millisecond})
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		done := make(chan error, 1)
		go func() {
			_, err := lifecycle.execute(ctx, l1.Claim{Job: l1.Job{JobID: "job", Spec: contract.JobSpec{Kind: contract.JobKindProcess, Class: contract.JobClassOneShot}}, Lease: lease}, clock.Now())
			done <- err
		}()
		<-payload.started
		cancel(errAttemptDirectiveCancel)
		synctest.Wait()
		atCancel := renewals.Load()
		time.Sleep(payload.hold / 2)
		synctest.Wait()
		if during := renewals.Load(); during-atCancel < 10 {
			t.Fatalf("renewals %d -> %d while the canceled payload was stopping; want the lease kept", atCancel, during)
		}
		if err := <-done; err != nil {
			t.Fatalf("heartbeat cancel lifecycle: %v", err)
		}
		if completions.Load() != 1 {
			t.Fatalf("completions=%d, want the result delivered once", completions.Load())
		}
		atExit := renewals.Load()
		time.Sleep(time.Second)
		synctest.Wait()
		if renewals.Load() != atExit {
			t.Fatalf("renewals continued after the attempt ended: %d -> %d", atExit, renewals.Load())
		}
	})
}
