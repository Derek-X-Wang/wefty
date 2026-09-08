//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestOCIOutcomeStopSurvivesRenewalDuringSetup(t *testing.T) {
	runOCIOutcomeStopFixture(t, true)
}

// Keep the original fixtures and assertions shared. Only the controlled case
// decorates the normal executor; real claiming, renewal, watchdog and outbox
// remain active. Cleanup joins resident work before the caller closes SQLite.
func startOCIOutcomeFixtureAgent(t *testing.T, a *Agent, store *l1.Store, jobID string, releaseRuntime func(), controlled bool) (context.CancelFunc, <-chan error, func()) {
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
			gate := &outcomeFixtureRenewalWatchdog{attemptWatchdog: lifecycle.dependencies.watchdog, second: make(chan struct{})}
			lifecycle.dependencies.watchdog = gate
			var arrived atomic.Bool
			var setupCause error
			lifecycle.dependencies.logSinkFactory = func(setupContext context.Context, claim l1.Claim) (attemptLogSink, error) {
				arrived.Store(true)
				select {
				case <-gate.second:
					t.Logf("pre-Run sink gate released: attempt=%s accepted_renewals=%d", claim.Lease.AttemptID, gate.renewals.Load())
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

type outcomeFixtureRenewalWatchdog struct {
	attemptWatchdog
	renewals atomic.Int32
	second   chan struct{}
}

func (watchdog *outcomeFixtureRenewalWatchdog) Start(ctx context.Context, authority localAuthority, cancel context.CancelCauseFunc) attemptWatch {
	return &outcomeFixtureRenewalWatch{attemptWatch: watchdog.attemptWatchdog.Start(ctx, authority, cancel), owner: watchdog}
}

type outcomeFixtureRenewalWatch struct {
	attemptWatch
	owner *outcomeFixtureRenewalWatchdog
}

func (watch *outcomeFixtureRenewalWatch) Renewed(authority localAuthority) {
	watch.attemptWatch.Renewed(authority)
	if watch.owner.renewals.Add(1) == 2 {
		close(watch.owner.second)
	}
}
