package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/bootclock"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

const sleepTestBoot = "darwin:00000000-0000-0000-0000-000000000001"

type authorityTestClock struct {
	*manualClock
	boot, awake                    time.Duration
	bootErr, awakeErr, identityErr error
	// Model descheduling inside a bracket without inventing sleep. The
	// watchdog and the renewal loop sample concurrently, so the bracket
	// toggle is guarded.
	bracketWidth time.Duration
	bracketMu    sync.Mutex
	bracketEnd   bool
}

func newAuthorityTestClock() *authorityTestClock {
	return &authorityTestClock{manualClock: newManualClock(time.Now()), boot: time.Hour, awake: time.Hour}
}
func (c *authorityTestClock) BootIdentity() (string, error)       { return sleepTestBoot, c.identityErr }
func (c *authorityTestClock) BootElapsed() (time.Duration, error) { return c.boot, c.bootErr }
func (c *authorityTestClock) AwakeElapsed() (time.Duration, error) {
	c.bracketMu.Lock()
	c.bracketEnd = !c.bracketEnd
	end := c.bracketEnd
	c.bracketMu.Unlock()
	if !end {
		return c.awake + c.bracketWidth, c.awakeErr
	}
	return c.awake, c.awakeErr
}
func (c *authorityTestClock) advance(awake, sleep time.Duration) {
	c.boot += awake + sleep
	c.awake += awake
	c.AdvanceWall(sleep)
	c.Advance(awake)
	synctest.Wait()
}

type sleepRenewalFixture struct {
	clock    *authorityTestClock
	ctx      context.Context
	cancel   context.CancelCauseFunc
	watch    attemptWatch
	failures chan destinationError
	done     chan struct{}
	calls    int
	lease    l1.AttemptLease
}

func newSleepRenewalFixture(t *testing.T, reply func(*sleepRenewalFixture, *http.Request) (*http.Response, error), configure ...func(*sleepRenewalFixture)) *sleepRenewalFixture {
	t.Helper()
	f := &sleepRenewalFixture{clock: newAuthorityTestClock(), failures: make(chan destinationError, 1), done: make(chan struct{}), lease: l1.AttemptLease{AttemptID: "attempt", FencingToken: "fence", LeaseTTL: 30 * time.Second, SleepCreditBoot: hostboot.DigestIdentity(sleepTestBoot)}}
	for _, option := range configure {
		option(f)
	}
	f.ctx, f.cancel = context.WithCancelCause(context.Background())
	f.watch = newAuthorityWatchdog(f.clock).Start(f.ctx, localAuthority{deadline: f.clock.Now().Add(f.lease.LeaseTTL), sleepCreditBoot: f.lease.SleepCreditBoot}, f.cancel)
	client := &Client{baseURL: "http://control-plane.invalid", operationTimeout: time.Minute, httpClient: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) { f.calls++; return reply(f, r) })}}
	lifecycle := &attemptLifecycle{dependencies: attemptLifecycleDependencies{clock: f.clock, client: client, renewalInterval: 14 * time.Second, completionRetry: time.Second}}
	go func() {
		defer close(f.done)
		lifecycle.renewalLoop(f.ctx, l1.Claim{Job: l1.Job{JobID: "job"}, Lease: f.lease}, localAuthority{deadline: f.clock.Now().Add(f.lease.LeaseTTL), sleepCreditBoot: f.lease.SleepCreditBoot}, f.failures, f.watch, nil)
	}()
	synctest.Wait()
	t.Cleanup(func() { f.cancel(context.Canceled); f.watch.Stop(); synctest.Wait() })
	return f
}
func sleepLeaseReply(lease l1.AttemptLease) *http.Response {
	body, _ := json.Marshal(lease)
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(body))), Header: make(http.Header)}
}
func (f *sleepRenewalFixture) live(t *testing.T) {
	t.Helper()
	if f.ctx.Err() != nil {
		t.Fatalf("payload canceled: %v", context.Cause(f.ctx))
	}
	if err := f.watch.Check(); err != nil {
		t.Fatalf("authority fenced: %v", err)
	}
}

func TestSameHostWakeRenewsBeforeFencingPayload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newSleepRenewalFixture(t, func(f *sleepRenewalFixture, _ *http.Request) (*http.Response, error) {
			return sleepLeaseReply(f.lease), nil
		})
		f.clock.advance(time.Second, time.Minute)
		if f.calls != 1 {
			t.Fatalf("wake renewals=%d, want immediate renewal before 14s timer", f.calls)
		}
		f.live(t)
		// Accepted renewal starts a fresh lease. The original deadline is passed.
		f.clock.advance(29*time.Second, 0)
		f.live(t)
	})
}
func TestSameHostWakeRefusalIsImmediate(t *testing.T) {
	for _, code := range []contract.ErrorCode{contract.ErrorLeaseExpired, contract.ErrorConflict, contract.ErrorStaleFence} {
		t.Run(string(code), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newSleepRenewalFixture(t, func(_ *sleepRenewalFixture, _ *http.Request) (*http.Response, error) {
					body, _ := json.Marshal(contract.ErrorResponse{Error: contract.APIError{Code: code}})
					return &http.Response{StatusCode: http.StatusConflict, Body: io.NopCloser(strings.NewReader(string(body))), Header: make(http.Header)}, nil
				})
				f.clock.advance(time.Second, time.Minute)
				select {
				case refusal := <-f.failures:
					if refusal.destination != errorDestinationAttemptAuthority {
						t.Fatalf("refusal destination=%v", refusal.destination)
					}
					// The execute arbiter consumes this destination and cancels the payload.
				default:
					t.Fatalf("%s did not refuse immediately: calls=%d cause=%v", code, f.calls, context.Cause(f.ctx))
				}
				if f.calls != 1 {
					t.Fatalf("refusal retried: %d", f.calls)
				}
			})
		})
	}
}

func TestSameHostWakeTransientExpiresAtCreditedDeadline(t *testing.T) {
	for _, mode := range []string{"transport", "uncertain", "silent"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newSleepRenewalFixture(t, func(_ *sleepRenewalFixture, r *http.Request) (*http.Response, error) {
					if mode == "silent" {
						<-r.Context().Done()
						return nil, r.Context().Err()
					}
					if mode == "uncertain" {
						return &http.Response{StatusCode: 500, Body: io.NopCloser(strings.NewReader(`{"error":{"code":"internal","message":"Attempt lease expiry is not proved by the sleep clock sample"}}`)), Header: make(http.Header)}, nil
					}
					return nil, errors.New("network down")
				})
				f.clock.advance(time.Second, time.Minute)
				if f.calls != 1 {
					t.Fatalf("no immediate renewal: %d", f.calls)
				}
				f.live(t)
				f.clock.advance(28*time.Second, 0)
				f.live(t)
				f.clock.advance(time.Second, 0)
				if !errors.Is(context.Cause(f.ctx), errAuthorityDeadlineExceeded) {
					t.Fatalf("payload did not fence at credited deadline: %v", context.Cause(f.ctx))
				}
			})
		})
	}
}

func TestSameHostDarkWakeInFlightThenSecondSleep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reply := make(chan struct{})
		f := newSleepRenewalFixture(t, func(f *sleepRenewalFixture, r *http.Request) (*http.Response, error) {
			select {
			case <-reply:
				return sleepLeaseReply(f.lease), nil
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		})
		f.clock.advance(time.Second, time.Minute)
		f.live(t)
		f.clock.advance(9*time.Second, 0)
		f.live(t)
		f.clock.advance(time.Second, 2*time.Minute)
		f.live(t)
		close(reply)
		synctest.Wait()
		f.live(t)
		if f.calls < 1 {
			t.Fatal("no renewal")
		}
		// The reply granted a fresh lease after the second wake.
		f.clock.advance(20*time.Second, 0)
		f.live(t)
	})
}

func TestAuthoritySleepProofRequired(t *testing.T) {
	for _, mode := range []string{"missing", "remote", "identity_error", "wall_jump", "process_stop", "boot_error", "awake_unavailable", "wide_bracket", "backward_wall_process_stop"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := newAuthorityTestClock()
				digest := hostboot.DigestIdentity(sleepTestBoot)
				switch mode {
				case "missing":
					digest = ""
				case "remote":
					digest = hostboot.DigestIdentity("remote")
				case "identity_error":
					c.identityErr = errors.New("identity unavailable")
				}
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(context.Canceled)
				watch := newAuthorityWatchdog(c).Start(ctx, localAuthority{deadline: c.Now().Add(30 * time.Second), sleepCreditBoot: digest}, cancel)
				defer watch.Stop()
				synctest.Wait()
				switch mode {
				case "wall_jump":
					c.AdvanceWall(time.Minute)
					c.advance(time.Second, 0)
				case "process_stop":
					c.advance(31*time.Second, 0)
				case "backward_wall_process_stop":
					c.AdvanceWall(-time.Hour)
					c.boot += 31 * time.Second
					c.awake += 31 * time.Second
					c.Advance(time.Second)
					synctest.Wait()
				default:
					if mode == "boot_error" {
						c.bootErr = errors.New("boot unavailable")
					}
					if mode == "awake_unavailable" {
						c.awakeErr = bootclock.ErrAwakeUnavailable
					}
					if mode == "wide_bracket" {
						c.bracketWidth = time.Minute
					}
					c.advance(time.Second, time.Minute)
				}
				if !errors.Is(context.Cause(ctx), errAuthorityDeadlineExceeded) {
					t.Fatalf("%s did not fence: %v", mode, context.Cause(ctx))
				}
			})
		})
	}
}

func TestSameHostLatestReplyControlsSleepCredit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newSleepRenewalFixture(t, func(f *sleepRenewalFixture, _ *http.Request) (*http.Response, error) {
			lease := f.lease
			lease.SleepCreditBoot = ""
			return sleepLeaseReply(lease), nil
		})
		f.clock.advance(time.Second, time.Minute)
		f.live(t)
		f.clock.advance(time.Second, time.Minute)
		if !errors.Is(context.Cause(f.ctx), errAuthorityDeadlineExceeded) {
			t.Fatalf("missing latest proof retained sleep authority: %v", context.Cause(f.ctx))
		}
	})
}

// L1 and agent see the same fake host clocks, including reconciliation before
// renewal at each wake. The transport is in-process, but serializes actual
// AttemptLease JSON through Client.Renew.
type authorityL1Clock struct{ *authorityTestClock }

func (c authorityL1Clock) Now() time.Time { return c.WallNow() }

func TestSameHostWakeL1AndAgentCreditTogether(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newAuthorityTestClock()
		store, err := l1.OpenStore(filepath.Join(t.TempDir(), "wake.sqlite"), l1.StoreOptions{Clock: authorityL1Clock{c}, LeaseDuration: 30 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		node, err := store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent-node"}, contract.NodeRegistration{NodeID: "node", BootSessionID: "session", OS: "linux", Architecture: "arm64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1, CapabilityObservedAt: c.WallNow(), MissingCapabilities: []string{}}, l1.DefaultNodePolicy("linux"), true)
		if err != nil {
			t.Fatal(err)
		}
		job := createAgentTestJob(t, store, "same-host-wake")
		claim, err := store.ClaimJob(t.Context(), "agent-node", node.NodeID, node.BootSessionID, job.Spec.Class)
		if err != nil || claim == nil {
			t.Fatalf("claim=%+v err=%v", claim, err)
		}
		if claim.Lease.SleepCreditBoot != hostboot.DigestIdentity(sleepTestBoot) {
			t.Fatalf("L1 claim has no local proof: %+v", claim.Lease)
		}
		f := newSleepRenewalFixture(t, func(_ *sleepRenewalFixture, _ *http.Request) (*http.Response, error) {
			result, err := store.Reconcile(t.Context())
			if err != nil || result.ExpiredAttempts != 0 || result.DeadNodes != 0 {
				t.Errorf("wake reconcile=%+v err=%v", result, err)
				return nil, errors.New("reconcile failed")
			}
			lease, err := store.RenewLease(t.Context(), "agent-node", job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken)
			if err != nil {
				t.Errorf("wake L1 renewal: %v", err)
				return nil, err
			}
			return sleepLeaseReply(lease), nil
		}, func(f *sleepRenewalFixture) { f.clock = c; f.lease = claim.Lease })
		c.advance(time.Second, time.Minute)
		f.live(t)
		c.advance(time.Second, 2*time.Minute)
		f.live(t)
		if f.calls != 2 {
			t.Fatalf("renewals=%d", f.calls)
		}
		attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
		if err != nil || len(attempts) != 1 || attempts[0].State == contract.AttemptLost {
			t.Fatalf("same Attempt was lost: %+v err=%v", attempts, err)
		}
	})
}

type wakePayloadExecutor struct {
	started chan context.Context
	stopped chan error
}

func (r *wakePayloadExecutor) Run(ctx context.Context, _ processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
	r.started <- ctx
	<-ctx.Done()
	r.stopped <- context.Cause(ctx)
	return contract.ProcessResult{Signal: "canceled", TerminationCause: contract.TerminationCauseAgent}, ctx.Err()
}

func TestSameHostWakePayloadLifecycle(t *testing.T) {
	for _, refuse := range []bool{false, true} {
		t.Run(map[bool]string{false: "grant", true: "refuse"}[refuse], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := newAuthorityTestClock()
				lease := l1.AttemptLease{AttemptID: "payload-attempt", FencingToken: "fence", LeaseTTL: 30 * time.Second, SleepCreditBoot: hostboot.DigestIdentity(sleepTestBoot)}
				calls := 0
				client := &Client{baseURL: "http://control-plane.invalid", operationTimeout: time.Minute, httpClient: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
					if strings.HasSuffix(r.URL.Path, "/lease") {
						calls++
						if refuse {
							body, _ := json.Marshal(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorLeaseExpired}})
							return &http.Response{StatusCode: 409, Body: io.NopCloser(strings.NewReader(string(body))), Header: make(http.Header)}, nil
						}
						return sleepLeaseReply(lease), nil
					}
					return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}, nil
				})}}
				payload := &wakePayloadExecutor{started: make(chan context.Context, 1), stopped: make(chan error, 1)}
				lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{clock: c, client: client, watchdog: newAuthorityWatchdog(c), runtimes: testRuntimeSet(payload), observer: newLifecycleObserver(c), renewalInterval: 14 * time.Second, completionRetry: time.Second})
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() {
					_, err := lifecycle.execute(ctx, l1.Claim{Job: l1.Job{JobID: "payload-job", Spec: contract.JobSpec{Kind: contract.JobKindProcess, Class: contract.JobClassOneShot}}, Lease: lease}, c.Now())
					done <- err
				}()
				synctest.Wait()
				var payloadCtx context.Context
				select {
				case payloadCtx = <-payload.started:
				default:
					t.Fatal("payload did not start")
				}
				c.advance(time.Second, time.Minute)
				if calls != 1 {
					t.Fatalf("wake renewals=%d", calls)
				}
				if refuse {
					if payloadCtx.Err() == nil {
						t.Fatal("refusal did not cancel real payload")
					}
					select {
					case err := <-done:
						if err == nil {
							t.Fatal("refusal returned success")
						}
					default:
						t.Fatal("refused lifecycle did not finish")
					}
				} else {
					if payloadCtx.Err() != nil {
						t.Fatalf("granted wake canceled payload: %v", context.Cause(payloadCtx))
					}
					select {
					case err := <-done:
						t.Fatalf("payload ended on credited wake: %v", err)
					default:
					}
					cancel()
					synctest.Wait()
					select {
					case <-done:
					default:
						t.Fatal("payload shutdown did not join")
					}
				}
			})
		})
	}
}

func TestSameHostDarkWakeLostReplyRetriesAfterSecondSleep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lost := make(chan struct{})
		f := newSleepRenewalFixture(t, func(f *sleepRenewalFixture, r *http.Request) (*http.Response, error) {
			deadline, ok := r.Context().Deadline()
			if !ok || deadline.Sub(time.Now()) > renewalRequestTimeout(29*time.Second, time.Minute) {
				t.Errorf("request exceeded credited remaining time: deadline=%s", deadline)
			}
			if f.calls == 1 {
				<-lost
				return nil, errors.New("reply lost after L1 committed")
			}
			return sleepLeaseReply(f.lease), nil
		})
		f.clock.advance(time.Second, time.Minute)
		f.live(t)
		f.clock.advance(9*time.Second, 0)
		f.live(t)
		f.clock.advance(time.Second, 2*time.Minute)
		f.live(t)
		close(lost)
		synctest.Wait()
		if f.calls != 2 {
			t.Fatalf("lost reply not retried immediately after second wake: calls=%d", f.calls)
		}
		f.live(t)
	})
}

func TestSameHostLateReplyCannotReviveAuthority(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newAuthorityTestClock()
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		authority := localAuthority{deadline: c.Now().Add(30 * time.Second), sleepCreditBoot: hostboot.DigestIdentity(sleepTestBoot)}
		watch := newAuthorityWatchdog(c).Start(ctx, authority, cancel)
		defer watch.Stop()
		synctest.Wait()
		c.advance(time.Second, time.Minute)
		if err := watch.Check(); err != nil {
			t.Fatalf("wake fenced: %v", err)
		}
		// Check a late successful reply itself, before a timer could observe expiry.
		c.boot += 30 * time.Second
		c.awake += 30 * time.Second
		c.manualClock.mu.Lock()
		c.manualClock.now = c.manualClock.now.Add(30 * time.Second)
		c.manualClock.wall = c.manualClock.wall.Add(30 * time.Second)
		c.manualClock.mu.Unlock()
		watch.Renewed(localAuthority{deadline: c.Now().Add(time.Minute), sleepCreditBoot: authority.sleepCreditBoot})
		if !errors.Is(context.Cause(ctx), errAuthorityDeadlineExceeded) {
			t.Fatalf("late reply revived payload: %v", context.Cause(ctx))
		}
		if !errors.Is(watch.Check(), errAuthorityDeadlineExceeded) {
			t.Fatal("expired authority was not final")
		}
	})
}
