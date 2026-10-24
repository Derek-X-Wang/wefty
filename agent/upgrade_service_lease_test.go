//go:build darwin || linux

package agent

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestUpgradeServicesLeaseStopsOnOperatorStop(t *testing.T) {
	f := newUpgradeFailureFixtureWithLease(t, func(a *Agent) {
		base := a.session.client.httpClient.Transport
		a.session.client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
			if strings.HasSuffix(r.URL.Path, "/complete") {
				<-r.Context().Done()
				return nil, r.Context().Err()
			}
			return base.RoundTrip(r)
		})
	}, 2*time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	_, err := f.a.QuiesceUpgradeServices(ctx, f.request)
	cancel()
	var failure *UpgradeServiceQuiescenceError
	if !errors.As(err, &failure) || failure.Phase != "completion" {
		t.Fatalf("expected blocked completion: %v", err)
	}
	lease := upgradeServiceTestLease(t, f, f.request.Services[0].AttemptID)
	job, err := f.store.SetServiceDesiredState(t.Context(), f.service.JobID, contract.ServiceDesiredStopped)
	if err != nil || job.State != contract.JobStopping {
		t.Fatalf("operator stop: %+v %v", job, err)
	}
	select {
	case <-lease.done:
	case <-time.After(time.Second):
		t.Fatal("operator stop left the completion Lease keeper running")
	}
	ctx, cancel = context.WithTimeout(t.Context(), 3*time.Second)
	_, err = f.a.QuiesceUpgradeServices(ctx, f.request)
	cancel()
	if !errors.As(err, &failure) || failure.Phase != "lease" || failure.JobID != f.service.JobID || failure.AttemptID != f.request.Services[0].AttemptID || !errors.Is(err, errAttemptDirectiveStop) {
		t.Fatalf("operator stop lost its explicit recovery state: %v", err)
	}
	deadline := time.Now().Add(4 * time.Second)
	for {
		if _, err := f.store.Reconcile(t.Context()); err != nil {
			t.Fatal(err)
		}
		job, err = f.store.GetJob(t.Context(), f.service.JobID)
		if err != nil {
			t.Fatal(err)
		}
		if job.State != contract.JobStopping {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("operator-stopped service stayed stopping after its Lease expired")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if job.DesiredState != contract.ServiceDesiredStopped {
		t.Fatalf("operator intent changed: %+v", job)
	}
	if receipt := f.a.logSpool.inspectCompletion(t.Context(), f.request.Services[0].AttemptID); receipt.State != "durable_completion" {
		t.Fatalf("stop discarded pending evidence: %+v", receipt)
	}
	f.assertKept(t)
}

func upgradeServiceTestLease(t *testing.T, f *upgradeFailureFixture, attempt string) *upgradeServiceLease {
	t.Helper()
	f.a.session.claimMu.Lock()
	defer f.a.session.claimMu.Unlock()
	lease := f.a.session.retirementAttempts[attempt].completionLease
	if lease == nil {
		t.Fatal("missing completion Lease keeper")
	}
	return lease
}

func TestUpgradeServicesLeaseStopsForEachSettledAttempt(t *testing.T) {
	for _, lostReceipt := range []bool{false, true} {
		name := "accepted_receipt"
		if lostReceipt {
			name = "lost_receipt"
		}
		t.Run(name, func(t *testing.T) {
			var firstJob, blockedJob atomic.Value
			var renewals, terminalRejections atomic.Int32
			firstCompletion := make(chan struct{})
			releaseCompletion := make(chan struct{})
			var completionOnce sync.Once
			var blocked atomic.Bool
			blocked.Store(true)
			f := newUpgradeFailureFixtureWithServices(t, func(a *Agent) {
				base := a.session.client.httpClient.Transport
				a.session.client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
					first, _ := firstJob.Load().(string)
					other, _ := blockedJob.Load().(string)
					isFirst := first != "" && strings.Contains(r.URL.Path, "/jobs/"+first+"/")
					if isFirst && strings.HasSuffix(r.URL.Path, "/lease") {
						renewals.Add(1)
						if !lostReceipt && blocked.Load() {
							// Settlement itself must join this in-flight request. A terminal
							// rejection on a later renewal cannot be its only exit.
							<-r.Context().Done()
							return nil, r.Context().Err()
						}
					}
					if strings.HasSuffix(r.URL.Path, "/complete") && blocked.Load() {
						if other != "" && strings.Contains(r.URL.Path, "/jobs/"+other+"/") {
							<-r.Context().Done()
							return nil, r.Context().Err()
						}
						if isFirst && !lostReceipt {
							completionOnce.Do(func() { close(firstCompletion) })
							select {
							case <-releaseCompletion:
							case <-r.Context().Done():
								return nil, r.Context().Err()
							}
						}
						if isFirst && lostReceipt {
							response, err := base.RoundTrip(r)
							if err != nil {
								return response, err
							}
							response.Body.Close()
							return nil, errors.New("injected loss of accepted completion response")
						}
					}
					response, err := base.RoundTrip(r)
					if isFirst && strings.HasSuffix(r.URL.Path, "/lease") && response != nil && response.StatusCode == http.StatusConflict {
						terminalRejections.Add(1)
					}
					return response, err
				})
			}, 2*time.Second, 2)
			firstJob.Store(f.request.Services[0].JobID)
			blockedJob.Store(f.request.Services[1].JobID)
			ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
			defer cancel()
			barrier := make(chan error, 1)
			go func() { _, err := f.a.QuiesceUpgradeServices(ctx, f.request); barrier <- err }()
			if !lostReceipt {
				select {
				case <-firstCompletion:
				case <-ctx.Done():
					t.Fatal("service did not reach completion")
				}
				// Return the barrier before A settles. No later barrier call can be
				// responsible for stopping A's keeper while B remains blocked.
				cancel()
			}
			if err := <-barrier; err == nil {
				t.Fatal("barrier ignored blocked service")
			}
			if !lostReceipt {
				close(releaseCompletion)
			}
			retirementWait(t, "first service completion accepted by L1", func() bool {
				attempts, err := f.store.ListJobAttempts(t.Context(), f.request.Services[0].JobID)
				return err == nil && len(attempts) == 1 && attempts[0].Result != nil
			})
			lease := upgradeServiceTestLease(t, f, f.request.Services[0].AttemptID)
			select {
			case <-lease.done:
			case <-time.After(time.Second):
				t.Fatal("settled Attempt kept renewing while its sibling was blocked")
			}
			expectedReceipt := "delivered"
			if lostReceipt {
				expectedReceipt = "durable_completion"
				if terminalRejections.Load() == 0 {
					t.Fatal("keeper exit did not follow a real L1 terminal renewal rejection")
				}
			}
			if receipt := f.a.logSpool.inspectCompletion(t.Context(), f.request.Services[0].AttemptID); receipt.State != expectedReceipt {
				t.Fatalf("keeper exit changed completion evidence: %+v; expected %s", receipt, expectedReceipt)
			}
			count := renewals.Load()
			time.Sleep(600 * time.Millisecond)
			if got := renewals.Load(); got != count {
				t.Fatalf("settled Attempt sent more renewals: before=%d after=%d", count, got)
			}
			other := upgradeServiceTestLease(t, f, f.request.Services[1].AttemptID)
			select {
			case <-other.done:
				t.Fatal("settling one service stopped its blocked sibling's keeper")
			default:
			}
			f.assertKept(t)
			// A terminal /lease conflict ends renewal, not completion replay. L1 must
			// still acknowledge the immutable completion whose first receipt was lost.
			blocked.Store(false)
			ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if _, err := f.a.QuiesceUpgradeServices(ctx, f.request); err != nil {
				t.Fatal("completion replay after keeper exit:", err)
			}
			if receipt := f.a.logSpool.inspectCompletion(t.Context(), f.request.Services[0].AttemptID); receipt.State != "delivered" {
				t.Fatalf("accepted completion was not replayed: %+v", receipt)
			}
			f.assertKept(t)
		})
	}
}
