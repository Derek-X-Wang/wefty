package agent

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestServiceRefusalPreservesRemovalBarrierAndDeadline(t *testing.T) {
	for _, wake := range []bool{true, false} {
		for _, blocked := range []bool{true, false} {
			name := map[bool]string{true: "wake", false: "awake"}[wake] + "/" + map[bool]string{true: "blocked", false: "prepared"}[blocked]
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					clock := newAuthorityTestClock()
					lease := l1.AttemptLease{AttemptID: "service-attempt", FencingToken: "fence", LeaseTTL: 30 * time.Second, SleepCreditBoot: hostboot.DigestIdentity(sleepTestBoot)}
					calls := 0
					client := &Client{baseURL: "http://control-plane.invalid", operationTimeout: time.Minute, httpClient: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
						if strings.HasSuffix(r.URL.Path, "/lease") {
							calls++
							return &http.Response{StatusCode: http.StatusConflict, Body: io.NopCloser(strings.NewReader(`{"error":{"code":"lease_expired"}}`)), Header: make(http.Header)}, nil
						}
						return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}, nil
					})}}
					payload := &wakePayloadExecutor{started: make(chan context.Context, 1), stopped: make(chan error, 1)}
					var payloadCtx context.Context
					preparing, prepared := false, false
					release := make(chan struct{})
					lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
						clock: clock, client: client, managedResource: &preflightManagedResource{}, watchdog: newAuthorityWatchdog(clock), runtimes: testRuntimeSet(payload), observer: newLifecycleObserver(clock), renewalInterval: 14 * time.Second, completionRetry: time.Second,
						prepareAuthorityLoss: func(_ context.Context, jobID string) error {
							if jobID != "service-job" || payloadCtx.Err() != nil {
								t.Errorf("preparation must precede cancellation: job=%s cause=%v", jobID, context.Cause(payloadCtx))
							}
							preparing = true
							if blocked {
								<-release
							}
							prepared = true
							return nil
						},
					})
					ctx, cancel := context.WithCancel(context.Background())
					defer func() { close(release); cancel(); synctest.Wait() }()
					done := make(chan error, 1)
					claimStarted := clock.Now()
					go func() {
						_, err := lifecycle.execute(ctx, l1.Claim{Job: l1.Job{JobID: "service-job", Spec: contract.JobSpec{Kind: contract.JobKindProcess, Class: contract.JobClassService}}, Lease: lease}, claimStarted)
						done <- err
					}()
					synctest.Wait()
					select {
					case payloadCtx = <-payload.started:
					default:
						t.Fatal("service payload did not start")
					}
					if wake {
						clock.advance(time.Second, time.Minute)
					} else {
						clock.advance(14*time.Second, 0)
					}
					if calls != 1 || !preparing {
						t.Fatalf("refusal did not start preparation: renewals=%d preparing=%v", calls, preparing)
					}
					if blocked {
						if payloadCtx.Err() != nil || prepared {
							t.Fatalf("cancel preceded durable preparation/deadline: cause=%v prepared=%v", context.Cause(payloadCtx), prepared)
						}
						// No renewal may follow the refusal, even across another sleep.
						clock.advance(lease.LeaseTTL-clock.Now().Sub(claimStarted)-time.Second, time.Minute)
						if payloadCtx.Err() != nil || calls != 1 {
							t.Fatalf("authority ended early or refusal retried: cause=%v renewals=%d", context.Cause(payloadCtx), calls)
						}
						clock.advance(time.Second, 0)
						if !errors.Is(context.Cause(payloadCtx), errAuthorityDeadlineExceeded) || prepared {
							t.Fatalf("blocked preparation hid deadline fence: cause=%v prepared=%v", context.Cause(payloadCtx), prepared)
						}
					} else {
						if !prepared || payloadCtx.Err() == nil {
							t.Fatalf("prepared refusal did not cancel immediately: prepared=%v cause=%v", prepared, context.Cause(payloadCtx))
						}
						select {
						case err := <-done:
							if err == nil {
								t.Fatal("refused service returned success")
							}
						default:
							t.Fatal("prepared refusal did not join")
						}
					}
					clock.advance(time.Minute, 0)
					if calls != 1 {
						t.Fatalf("renewal continued after refusal: %d", calls)
					}
				})
			})
		}
	}
}

func TestServiceRefusalClosesDeadmanAdmissionBeforePreparation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := newAuthorityTestClock()
		claim := l1.Claim{Job: l1.Job{JobID: "service-job", Spec: contract.JobSpec{Class: contract.JobClassService}}, Lease: l1.AttemptLease{LeaseTTL: 30 * time.Second}}
		renewer := &recordingDeadmanRenewer{}
		admission := newAttemptDeadmanAdmission(renewer, claim, clock, nil)
		authority := localAuthority{deadline: clock.Now().Add(claim.Lease.LeaseTTL)}
		if err := admission.queue(claim.Lease, authority.deadline); err != nil {
			t.Fatal(err)
		}
		client := &Client{baseURL: "http://control-plane.invalid", operationTimeout: time.Minute, httpClient: &http.Client{Transport: captureRoundTrip(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusConflict, Body: io.NopCloser(strings.NewReader(`{"error":{"code":"lease_expired"}}`)), Header: make(http.Header)}, nil
		})}}
		prepared := false
		lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{clock: clock, client: client, renewalInterval: 14 * time.Second, prepareAuthorityLoss: func(context.Context, string) error {
			admission.mu.Lock()
			defer admission.mu.Unlock()
			if !admission.terminal || admission.pending != nil || renewer.calls != 0 {
				t.Errorf("preparation started with open helper grants: terminal=%v pending=%v helper_calls=%d", admission.terminal, admission.pending != nil, renewer.calls)
			}
			prepared = true
			return nil
		}})
		failures, done := make(chan destinationError, 1), make(chan struct{})
		go func() {
			defer close(done)
			lifecycle.renewalLoop(t.Context(), claim, authority, failures, disabledAttemptWatch{}, admission)
		}()
		synctest.Wait()
		clock.advance(14*time.Second, 0)
		select {
		case <-done:
		default:
			t.Fatal("refusal did not end renewal grants")
		}
		if !prepared || len(failures) != 1 {
			t.Fatalf("preparation/failure missing: prepared=%v failures=%d", prepared, len(failures))
		}
	})
}
