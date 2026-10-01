package l3

import (
	"context"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// appearingLookupClient answers not_found until the test makes the job
// appear, standing in for an L1 that commits a submit after recovery's first
// lookup and whose response L3 lost.
type appearingLookupClient struct {
	job     *l1.Job
	lookups int
}

func (c *appearingLookupClient) LookupJobByDispatchKey(_ context.Context, key string) (l1.Job, error) {
	c.lookups++
	if c.job == nil {
		return l1.Job{}, &DispatchNotFoundError{DispatchKey: key}
	}
	return *c.job, nil
}

func runIDForDispatchKey(t *testing.T, s *Store, dispatchKey string) string {
	t.Helper()
	var runID string
	if err := s.db.QueryRow(`SELECT run_id FROM runs WHERE dispatch_key=?`, dispatchKey).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	return runID
}

func jobLinkSettled(t *testing.T, s *Store, runID string) bool {
	t.Helper()
	var settled int
	if err := s.db.QueryRow(`SELECT job_link_settled FROM runs WHERE run_id=?`, runID).Scan(&settled); err != nil {
		t.Fatal(err)
	}
	return settled == 1
}

// A not_found inside the settle horizon is not final: L1 may commit the
// submit after the lookup, so the run is asked again after the backoff and is
// linked once the job is visible.
func TestDispatchNotFoundBeforeTheSettleHorizonRetriesAndLinksWhenTheJobAppears(t *testing.T) {
	s, _, clock := recoveryStore(t)
	key := endedUnrecordedRuns(t, s, clock, 1)[0]
	runID := runIDForDispatchKey(t, s, key)
	lookup := &appearingLookupClient{}
	reconciler, err := NewReconciler(s, &recordingJobClient{}, ReconcilerConfig{DispatchLookup: lookup})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("an absence inside the horizon is not an error yet: %v", err)
	}
	execution, err := s.GetRunExecution(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if lookup.lookups != 1 || jobLinkSettled(t, s, runID) || execution.DispatchError != nil {
		t.Fatalf("absence inside the horizon: lookups=%d settled=%v diagnostic=%+v", lookup.lookups, jobLinkSettled(t, s, runID), execution.DispatchError)
	}
	if failures, retryAt := jobLinkBackoff(t, s, key); failures != 1 || !retryAt.Equal(clock.now.Add(unrecordedDispatchRetryBase)) {
		t.Fatalf("absence backoff = %d failures, retry at %v", failures, retryAt)
	}

	lookup.job = &l1.Job{JobID: "job-committed-late", State: contract.JobQueued}
	clock.now = clock.now.Add(unrecordedDispatchRetryBase)
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	execution, err = s.GetRunExecution(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if lookup.lookups != 2 || execution.L1JobID != "job-committed-late" || execution.DispatchError != nil {
		t.Fatalf("late-committed job: lookups=%d execution=%+v", lookup.lookups, execution)
	}
}

// Only an absence at or after the horizon, one hour after the run's last
// submit attempt, settles the run as dispatch_not_found.
func TestDispatchNotFoundSettlesOnlyAtTheSettleHorizon(t *testing.T) {
	s, _, clock := recoveryStore(t)
	key := endedUnrecordedRuns(t, s, clock, 1)[0]
	runID := runIDForDispatchKey(t, s, key)
	attempted := clock.now
	lookup := &appearingLookupClient{}
	reconciler, err := NewReconciler(s, &recordingJobClient{}, ReconcilerConfig{DispatchLookup: lookup})
	if err != nil {
		t.Fatal(err)
	}
	for _, at := range []time.Duration{0, unrecordedDispatchSettleHorizon - time.Minute} {
		clock.now = attempted.Add(at)
		if err := reconciler.ReconcileOnce(context.Background()); err != nil {
			t.Fatalf("absence at +%v was reported: %v", at, err)
		}
		if jobLinkSettled(t, s, runID) {
			t.Fatalf("absence at +%v settled before the horizon", at)
		}
	}
	clock.now = attempted.Add(unrecordedDispatchSettleHorizon)
	if err := reconciler.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("settled absence was not reported")
	}
	execution, err := s.GetRunExecution(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if lookup.lookups != 3 || !jobLinkSettled(t, s, runID) || execution.DispatchError == nil || execution.DispatchError.Details["reason"] != dispatchNotFoundReason {
		t.Fatalf("at the horizon: lookups=%d settled=%v diagnostic=%+v", lookup.lookups, jobLinkSettled(t, s, runID), execution.DispatchError)
	}
}

// A submit error that L3 records after the run settled must not replace the
// settled dispatch_not_found diagnostic.
func TestLateSubmitErrorDoesNotClobberASettledDiagnostic(t *testing.T) {
	s, _, clock := recoveryStore(t)
	key := endedUnrecordedRuns(t, s, clock, 1)[0]
	runID := runIDForDispatchKey(t, s, key)
	clock.now = clock.now.Add(unrecordedDispatchSettleHorizon)
	reconciler, err := NewReconciler(s, &recordingJobClient{}, ReconcilerConfig{DispatchLookup: &appearingLookupClient{}})
	if err != nil {
		t.Fatal(err)
	}
	_ = reconciler.ReconcileOnce(context.Background())
	if !jobLinkSettled(t, s, runID) {
		t.Fatal("fixture did not settle")
	}
	ctx := context.Background()
	if err := s.recordDispatchError(ctx, runID, &Error{Code: contract.ErrorConflict, Message: "late transport error", Retryable: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.failDispatch(ctx, runID, &Error{Code: contract.ErrorInvalidRequest, Message: "late refusal"}); err != nil {
		t.Fatal(err)
	}
	execution, err := s.GetRunExecution(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if execution.DispatchError == nil || execution.DispatchError.Details["reason"] != dispatchNotFoundReason {
		t.Fatalf("late submit error replaced the settled diagnostic: %+v", execution.DispatchError)
	}
}
