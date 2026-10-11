package l3

import (
	"context"
	"errors"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// These are preservation pins for #781, not new behavior. The parent already
// implements these rules. Both found answers and typed absences are bound to
// the requested key or identity, as the real L1 client requires.
type recoveryIdentityClient struct {
	JobClient
	lookupJob        l1.Job
	acknowledgedJob  *l1.Job
	lookups, submits int
	gets             []string
}

func (c *recoveryIdentityClient) LookupJobByDispatchKey(_ context.Context, key string) (l1.Job, error) {
	c.lookups++
	if c.lookupJob.Spec.DispatchKey != key {
		return l1.Job{}, &DispatchNotFoundError{DispatchKey: key}
	}
	return c.lookupJob, nil
}

func (c *recoveryIdentityClient) GetJob(_ context.Context, id string) (l1.Job, error) {
	c.gets = append(c.gets, id)
	if c.acknowledgedJob == nil || c.acknowledgedJob.JobID != id {
		return l1.Job{}, &JobNotFoundError{JobID: id}
	}
	return *c.acknowledgedJob, nil
}

func (c *recoveryIdentityClient) SubmitJob(context.Context, contract.JobSpec) (l1.Job, error) {
	c.submits++
	return l1.Job{}, errors.New("recovery must not replay submission")
}

func TestRecoveryDifferentJobLookupUsesAcknowledgedIdentity(t *testing.T) {
	for _, found := range []bool{true, false} {
		name := "acknowledged-absent"
		if found {
			name = "acknowledged-found"
		}
		t.Run(name, func(t *testing.T) {
			s, _, clock := recoveryStore(t)
			ctx := context.Background()
			key := endedUnrecordedRuns(t, s, clock, 1)[0]
			id := runIDForDispatchKey(t, s, key)
			recordOutboxOnlyAcknowledgement(t, s, id, "job-acknowledged")
			before := snapshotTerminalRun(t, s, id)
			client := &recoveryIdentityClient{lookupJob: l1.Job{JobID: "job-different", Spec: contract.JobSpec{DispatchKey: key}}}
			if found {
				// Legacy acknowledged Jobs need not carry dispatch-key provenance.
				client.acknowledgedJob = &l1.Job{JobID: "job-acknowledged", State: contract.JobQueued}
			}
			r, err := NewReconciler(s, client, ReconcilerConfig{DispatchLookup: client})
			if err != nil {
				t.Fatal(err)
			}
			errs := r.recoverUnrecordedDispatches(ctx)
			if found && len(errs) != 0 || !found && len(errs) == 0 {
				t.Fatalf("recovery errors = %v, acknowledged found = %v", errs, found)
			}
			execution, err := s.GetRunExecution(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if client.lookups != 1 || len(client.gets) != 1 || client.gets[0] != "job-acknowledged" || client.submits != 0 {
				t.Fatalf("recovery reads/submits = lookups %d, gets %v, submits %d", client.lookups, client.gets, client.submits)
			}
			if found {
				if execution.L1JobID != "job-acknowledged" || execution.DispatchError != nil {
					t.Fatalf("different Job replaced the acknowledgement: %+v", execution)
				}
			} else {
				if execution.L1JobID != "" || !isL1Regression(execution.DispatchError, "job-acknowledged") {
					t.Fatalf("acknowledged absence lost the original identity: %+v", execution)
				}
				if errs := r.recoverUnrecordedDispatches(ctx); len(errs) != 0 || client.lookups != 1 || len(client.gets) != 1 || client.submits != 0 {
					t.Fatalf("regression replayed: errors %v, lookups %d, gets %v, submits %d", errs, client.lookups, client.gets, client.submits)
				}
			}
			if after := snapshotTerminalRun(t, s, id); after != before {
				t.Fatalf("recovery changed terminal history: %+v -> %+v", before, after)
			}
		})
	}
}

func TestRecoveryLegacyDiagnosticIdentity(t *testing.T) {
	for _, shape := range []string{"diagnostic-only", "conflicting-stored-identities"} {
		t.Run(shape, func(t *testing.T) {
			s, _, clock := recoveryStore(t)
			ctx := context.Background()
			key := endedUnrecordedRuns(t, s, clock, 1)[0]
			id := runIDForDispatchKey(t, s, key)
			attempt := clock.now
			fixture := linkKnowledgeFixture{attemptedAt: &attempt, diagnostic: &contract.APIError{Code: contract.ErrorNotFound, Message: "prior regression", Details: map[string]any{"reason": l1RegressedReason, "l1_job_id": "job-original"}}}
			if shape == "conflicting-stored-identities" {
				fixture.outboxJob = linkID("job-conflicting")
				fixture.acknowledged = true
			}
			buildLinkKnowledge(t, s, id, fixture)
			before := snapshotTerminalRun(t, s, id)
			client := &recoveryIdentityClient{lookupJob: l1.Job{JobID: "job-original", Spec: contract.JobSpec{DispatchKey: key}}, acknowledgedJob: &l1.Job{JobID: "job-conflicting"}}
			r, err := NewReconciler(s, client, ReconcilerConfig{DispatchLookup: client})
			if err != nil {
				t.Fatal(err)
			}
			errs := r.recoverUnrecordedDispatches(ctx)
			execution, err := s.GetRunExecution(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if shape == "diagnostic-only" {
				if len(errs) != 0 || execution.L1JobID != "job-original" || execution.DispatchError != nil || len(client.gets) != 0 {
					t.Fatalf("fresh lookup failed to resolve diagnostic-only identity: %+v, errors %v, gets %v", execution, errs, client.gets)
				}
			} else {
				if len(errs) == 0 || execution.L1JobID != "" || !isL1Regression(execution.DispatchError, "job-original") {
					t.Fatalf("conflict changed stored ownership: %+v, errors %v", execution, errs)
				}
				if failures, retry := jobLinkBackoff(t, s, key); failures != 1 || !retry.Equal(clock.now.Add(unrecordedDispatchRetryBase)) {
					t.Fatalf("identity conflict did not back off: %d, %v", failures, retry)
				}
			}
			if client.submits != 0 || client.lookups != 1 {
				t.Fatalf("recovery calls = lookups %d, submits %d", client.lookups, client.submits)
			}
			if after := snapshotTerminalRun(t, s, id); after != before {
				t.Fatalf("recovery changed terminal history: %+v -> %+v", before, after)
			}
		})
	}
}

func TestRecoveryDelayedAcknowledgementPreservesRegression(t *testing.T) {
	for _, shape := range []string{"outbox-identity", "diagnostic-only"} {
		t.Run(shape, func(t *testing.T) {
			s, _, clock := recoveryStore(t)
			ctx := context.Background()
			key := endedUnrecordedRuns(t, s, clock, 1)[0]
			id := runIDForDispatchKey(t, s, key)
			attempt := clock.now
			fixture := linkKnowledgeFixture{attemptedAt: &attempt, settled: true, diagnostic: &contract.APIError{Code: contract.ErrorNotFound, Message: "recorded regression", Details: map[string]any{"reason": l1RegressedReason, "l1_job_id": "job-original"}}}
			if shape == "outbox-identity" {
				fixture.outboxJob = linkID("job-original")
				fixture.acknowledged = true
			}
			buildLinkKnowledge(t, s, id, fixture)
			before := snapshotTerminalRun(t, s, id)
			clock.now = clock.now.Add(unrecordedDispatchRetryBase)
			if err := s.completeDispatch(ctx, id, "job-original"); err != nil {
				t.Fatal(err)
			}
			execution, err := s.GetRunExecution(ctx, id)
			if err != nil || execution.L1JobID != "" || !isL1Regression(execution.DispatchError, "job-original") || execution.DispatchError.Message != "recorded regression" {
				t.Fatalf("delayed acknowledgement overturned regression: %+v, %v", execution, err)
			}
			knowledge, err := s.runJobLink(ctx, id)
			if err != nil || knowledge.state != linkRegressed || knowledge.jobID != "job-original" {
				t.Fatalf("delayed acknowledgement lost regression identity: %+v, %v", knowledge, err)
			}
			if after := snapshotTerminalRun(t, s, id); after != before {
				t.Fatalf("delayed acknowledgement changed terminal history: %+v -> %+v", before, after)
			}
		})
	}
}
