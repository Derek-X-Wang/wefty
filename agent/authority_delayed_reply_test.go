package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestSameHostDelayedDarkWakeReplyMustNotOutliveL1(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newAuthorityTestClock()
		store, err := l1.OpenStore(filepath.Join(t.TempDir(), "delayed.sqlite"), l1.StoreOptions{Clock: authorityL1Clock{c}, LeaseDuration: 30 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		node, err := store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent-node"}, contract.NodeRegistration{NodeID: "node", BootSessionID: "session", OS: "linux", Architecture: "arm64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1, CapabilityObservedAt: c.WallNow(), MissingCapabilities: []string{}}, l1.DefaultNodePolicy("linux"), true)
		if err != nil {
			t.Fatal(err)
		}
		job := createAgentTestJob(t, store, "delayed-wake")
		claim, err := store.ClaimJob(t.Context(), "agent-node", node.NodeID, node.BootSessionID, job.Spec.Class)
		if err != nil || claim == nil {
			t.Fatalf("claim=%+v err=%v", claim, err)
		}
		deliver := make(chan struct{})
		f := newSleepRenewalFixture(t, func(f *sleepRenewalFixture, _ *http.Request) (*http.Response, error) {
			if f.calls > 1 {
				return nil, errors.New("network unavailable after delayed reply")
			}
			lease, err := store.RenewLease(t.Context(), "agent-node", job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken)
			if err != nil {
				t.Errorf("first renewal: %v", err)
				return nil, err
			}
			<-deliver
			return sleepLeaseReply(lease), nil
		}, func(f *sleepRenewalFixture) { f.clock = c; f.lease = claim.Lease })
		c.advance(time.Second, time.Minute)   // L1 grants at awake t=1; deadline t=31.
		c.advance(9*time.Second, 0)           // Short DarkWake; reply is still in flight.
		c.advance(time.Second, 2*time.Minute) // Second wake at cumulative awake t=11.
		close(deliver)
		synctest.Wait()              // Accept only the remainder of the t=1 grant.
		c.advance(19*time.Second, 0) // Awake t=30: still inside the real grant.
		f.live(t)
		c.advance(time.Second, 0) // Awake t=31: exact L1 grant expiry.
		result, err := store.Reconcile(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
		if err != nil || len(attempts) != 1 {
			t.Fatalf("attempts=%+v err=%v", attempts, err)
		}
		if result.ExpiredAttempts != 1 || attempts[0].State != contract.AttemptLost {
			t.Fatalf("L1 did not expire the real grant: result=%+v attempts=%+v", result, attempts)
		}
		if !errors.Is(context.Cause(f.ctx), errAuthorityDeadlineExceeded) {
			t.Fatalf("delayed grant outlived L1: cause=%v", context.Cause(f.ctx))
		}
	})
}

func TestRenewalGrantRequestCheckpoint(t *testing.T) {
	for _, mode := range []string{"same_host", "older_L1_missing", "remote_mismatch", "proof_dropped_in_reply", "same_host_expired"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				f := newSleepRenewalFixture(t, func(f *sleepRenewalFixture, _ *http.Request) (*http.Response, error) {
					if f.calls > 1 {
						return nil, errors.New("network unavailable after first grant")
					}
					<-release
					lease := f.lease
					lease.LeaseTTL = 30 * time.Second
					if mode == "proof_dropped_in_reply" {
						lease.SleepCreditBoot = ""
					}
					return sleepLeaseReply(lease), nil
				}, func(f *sleepRenewalFixture) {
					f.lease.LeaseTTL = 2 * time.Minute
					if mode == "older_L1_missing" {
						f.lease.SleepCreditBoot = ""
					}
					if mode == "remote_mismatch" {
						f.lease.SleepCreditBoot = hostboot.DigestIdentity("remote")
					}
				})
				f.clock.advance(14*time.Second, 0)
				if f.calls != 1 {
					t.Fatalf("request not sent: %d", f.calls)
				}
				awake := time.Second
				if mode == "same_host_expired" {
					awake = 31 * time.Second
				}
				f.clock.advance(awake, time.Minute)
				// The old two-minute authority is still live. Only validating the new
				// thirty-second grant against its request checkpoint can reject this reply.
				f.live(t)
				close(release)
				synctest.Wait()
				if mode == "same_host" {
					f.live(t)
					f.clock.advance(29*time.Second, 0)
				}
				if !errors.Is(context.Cause(f.ctx), errAuthorityDeadlineExceeded) {
					t.Fatalf("%s accepted stale grant: cause=%v", mode, context.Cause(f.ctx))
				}
			})
		})
	}
}

func TestClaimGrantRequestCheckpoint(t *testing.T) {
	for _, mode := range []string{"same_host", "older_L1_missing", "remote_mismatch", "same_host_expired", "same_host_RTT", "wall_jump"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := newAuthorityTestClock()
				lease := l1.AttemptLease{AttemptID: "claim-attempt", FencingToken: "fence", LeaseTTL: 30 * time.Second, SleepCreditBoot: hostboot.DigestIdentity(sleepTestBoot)}
				if mode == "older_L1_missing" {
					lease.SleepCreditBoot = ""
				}
				if mode == "remote_mismatch" {
					lease.SleepCreditBoot = hostboot.DigestIdentity("remote")
				}
				claim := l1.Claim{Job: l1.Job{JobID: "claim-job", Spec: contract.JobSpec{Kind: contract.JobKindProcess, Class: contract.JobClassOneShot}}, Lease: lease}
				release := make(chan struct{})
				claimCalls := 0
				client := &Client{baseURL: "http://control-plane.invalid", operationTimeout: time.Minute, transport: &http.Transport{}, httpClient: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
					if strings.HasSuffix(r.URL.Path, "/claim") {
						claimCalls++
						<-release
						raw, _ := json.Marshal(claim)
						// SleepCreditBoot is actually absent in this old-L1 wire response.
						if mode == "older_L1_missing" && strings.Contains(string(raw), "sleep_credit_boot") {
							t.Error("old-L1 response unexpectedly carries proof")
						}
						return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(raw))), Header: make(http.Header)}, nil
					}
					// Allow shutdown completion to drain even when a deliberately
					// broken admission rule made the test fail with a live payload.
					if strings.HasSuffix(r.URL.Path, "/complete") {
						return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}, nil
					}
					return nil, errors.New("network unavailable after claim")
				})}}
				registration := contract.NodeRegistration{NodeID: "node", BootSessionID: "boot", OS: "linux", Capabilities: map[string]bool{"kind:process": true}}
				session := newAgentSession(client, registration, newCapabilityState(registration.Capabilities, nil, c, 0, nil), time.Minute, time.Minute, c, newLifecycleObserver(c), nil, 1, 1)
				defer session.close()
				payload := &wakePayloadExecutor{started: make(chan context.Context, 1), stopped: make(chan error, 1)}
				lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{clock: c, client: client, watchdog: newAuthorityWatchdog(c), runtimes: testRuntimeSet(payload), observer: newLifecycleObserver(c), renewalInterval: 14 * time.Second, completionRetry: time.Second})
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() {
					failure := session.claimClassLoop(ctx, make(chan struct{}), workloadClassOneShot, contract.JobClassOneShot, func(ctx context.Context, claim l1.Claim, started time.Time) (errorDestination, error) {
						defer cancel()
						return lifecycle.execute(ctx, claim, started)
					})
					done <- failure.err
				}()
				synctest.Wait()
				if claimCalls != 1 {
					t.Fatalf("claim requests=%d", claimCalls)
				}
				awake, sleep := time.Second, time.Minute
				if mode == "same_host_RTT" {
					awake = 10 * time.Second
				}
				if mode == "same_host_expired" {
					awake = 31 * time.Second
				}
				if mode == "wall_jump" {
					c.AdvanceWall(time.Minute)
					sleep = 0
				}
				c.advance(awake, sleep)
				close(release)
				synctest.Wait()
				survives := mode == "same_host" || mode == "same_host_RTT"
				var payloadCtx context.Context
				select {
				case payloadCtx = <-payload.started:
				default:
				}
				if survives {
					if payloadCtx == nil || payloadCtx.Err() != nil {
						t.Fatalf("same-host claim did not admit live payload: ctx=%v", payloadCtx)
					}
					// No round trip may refresh the claim deadline from t=0 to receipt.
					c.advance(30*time.Second-awake, 0)
					if !errors.Is(context.Cause(payloadCtx), errAuthorityDeadlineExceeded) {
						t.Fatalf("claim RTT extended initial authority: %v", context.Cause(payloadCtx))
					}
				} else if payloadCtx != nil {
					t.Fatalf("%s spawned payload from a fenced claim", mode)
				}
				select {
				case <-done:
				default:
					t.Fatal("claim loop did not join")
				}
			})
		})
	}
}
