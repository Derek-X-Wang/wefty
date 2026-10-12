//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestOCIIntentFixturesSurviveRenewalDuringSetup(t *testing.T) {
	t.Run("intent_stop", func(t *testing.T) { runOCIIntentStopFixture(t, true) })
	t.Run("prestarted_runtime_loss", func(t *testing.T) { runPreStartedOCIRuntimeLossFixture(t, true) })
}

// Keep the original fixtures and assertions shared. Only the controlled case
// decorates the normal executor; real claiming, renewal, watchdog and outbox
// remain active. Cleanup joins resident work before the caller closes SQLite.
func startOCIIntentFixtureAgent(t *testing.T, a *Agent, store *l1.Store, jobID string, releaseRuntime func(), controlled bool) (context.CancelFunc, <-chan error, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	joined := make(chan struct{})
	var first atomic.Bool
	go func() {
		defer close(joined)
		if !controlled {
			result <- a.Run(ctx)
			return
		}
		a.outbox.startRecovery(ctx, a.session.client, func(err error) { t.Logf("fixture recovery: %v", err) })
		result <- a.session.run(ctx, func(attemptContext context.Context, claim l1.Claim, started time.Time) (errorDestination, error) {
			lifecycle := a.newAttemptLifecycle()
			if !first.CompareAndSwap(false, true) {
				return lifecycle.execute(attemptContext, claim, started)
			}
			gate := &fixtureRenewalWatchdog{attemptWatchdog: lifecycle.dependencies.watchdog, second: make(chan struct{}), receipts: make(chan struct{}, 2)}
			lifecycle.dependencies.watchdog = gate
			var arrived atomic.Bool
			var setupCause error
			lifecycle.dependencies.logSinkFactory = func(setupContext context.Context, claim l1.Claim) (attemptLogSink, error) {
				arrived.Store(true)
				// Drive the two real L1 renewals only after their timer is
				// armed, and wait for each receipt before moving time again.
				// Loaded runners cannot spend this fixture's authority TTL.
				clock := a.session.clock.(*manualClock)
				for range 2 {
					deadline := clock.Now().Add(a.renewalInterval)
					if err := awaitFixtureCondition(setupContext, "setup renewal timer", func() bool {
						return clock.hasDeadline(deadline)
					}); err != nil {
						return nil, err
					}
					clock.Advance(a.renewalInterval)
					receiptContext, cancelReceipt := context.WithTimeout(setupContext, hostedFixtureTimeout)
					select {
					case <-gate.receipts:
					case <-receiptContext.Done():
						cancelReceipt()
						return nil, fmt.Errorf("phase=setup renewal receipt: %w", receiptContext.Err())
					}
					cancelReceipt()
				}
				select {
				case <-gate.second:
				case <-setupContext.Done():
				case <-ctx.Done():
					// Teardown must release setup even though finalization is
					// deliberately detached from ordinary execution cancellation.
				}
				setupCause = context.Cause(setupContext)
				return a.outbox.newLogSink(setupContext, a.session.client, claim)
			}
			destination, err := lifecycle.execute(attemptContext, claim, started)
			if gate.renewals.Load() < 2 {
				receipt := a.outbox.spool.inspectCompletion(context.WithoutCancel(ctx), claim.Lease.AttemptID)
				attempts, listErr := store.ListJobAttempts(context.WithoutCancel(ctx), jobID)
				current, jobErr := store.GetJob(context.WithoutCancel(ctx), jobID)
				payload, _ := json.Marshal(struct {
					Attempts []l1.Attempt
					Job      l1.Job
					Receipt  any
				}{attempts, current, receipt})
				t.Logf("setup renewal evidence: attempt=%s ttl=%s sink_arrived=%t renewals=%d setup_cause=%v destination=%v error=%v list_error=%v job_error=%v durable=%s", claim.Lease.AttemptID, claim.Lease.LeaseTTL, arrived.Load(), gate.renewals.Load(), setupCause, destination, err, listErr, jobErr, payload)
				if arrived.Load() && gate.renewals.Load() == 1 && err != nil && err.Error() == "agent: renew lease: OCI attempt deadman renewer is not wired" && setupCause == context.Canceled && receipt.Result.SpawnError != nil && receipt.Result.SpawnError.Code == contract.SpawnFailureLogSinkSetup {
					t.Errorf("CAUSAL RED: successful first L1 renewal canceled unwired OCI fixture before durable sink construction")
				} else {
					t.Errorf("INVALID PROBE: expected two successful renewals before setup; first failure was at another stage")
				}
			} else {
				t.Logf("controlled setup released after %d real renewals; lifecycle destination=%v error=%v", gate.renewals.Load(), destination, err)
			}
			return destination, err
		})
	}()
	cleanup := func() { cancel(); releaseRuntime(); <-joined }
	return cancel, result, cleanup
}

type fixtureRenewalWatchdog struct {
	attemptWatchdog
	renewals atomic.Int32
	second   chan struct{}
	receipts chan struct{}
}

func (watchdog *fixtureRenewalWatchdog) Start(ctx context.Context, authority localAuthority, cancel context.CancelCauseFunc) attemptWatch {
	return &fixtureRenewalWatch{attemptWatch: watchdog.attemptWatchdog.Start(ctx, authority, cancel), owner: watchdog}
}

type fixtureRenewalWatch struct {
	attemptWatch
	owner *fixtureRenewalWatchdog
}

func (watch *fixtureRenewalWatch) Renewed(authority localAuthority) {
	watch.attemptWatch.Renewed(authority)
	count := watch.owner.renewals.Add(1)
	if count <= 2 {
		watch.owner.receipts <- struct{}{}
	}
	if count == 2 {
		close(watch.owner.second)
	}
}
