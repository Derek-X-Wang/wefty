package l3

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

type countingDispatchRecoveryClient struct {
	JobClient
	JobDispatchLookupClient
	submits int
	lookups int
}

func (c *countingDispatchRecoveryClient) SubmitJob(ctx context.Context, spec contract.JobSpec) (l1.Job, error) {
	c.submits++
	return c.JobClient.SubmitJob(ctx, spec)
}

func (c *countingDispatchRecoveryClient) LookupJobByDispatchKey(ctx context.Context, key string) (l1.Job, error) {
	c.lookups++
	return c.JobDispatchLookupClient.LookupJobByDispatchKey(ctx, key)
}

type terminalRunSnapshot struct {
	Status                        contract.RunState
	FailureReason                 sql.NullString
	UpdatedNS, StartedNS, EndedNS sql.NullInt64
	TokenExpiry                   sql.NullInt64
}

func snapshotTerminalRun(t *testing.T, store *Store, runID string) terminalRunSnapshot {
	t.Helper()
	var snapshot terminalRunSnapshot
	if err := store.db.QueryRow(`SELECT r.status,r.failure_reason,r.updated_ns,r.started_ns,r.finished_ns,t.expires_ns
		FROM runs r JOIN run_tokens t ON t.run_id=r.run_id WHERE r.run_id=?`, runID).Scan(
		&snapshot.Status, &snapshot.FailureReason, &snapshot.UpdatedNS, &snapshot.StartedNS, &snapshot.EndedNS, &snapshot.TokenExpiry); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func failRunAfterDispatchAttempt(t *testing.T, store *Store, runID string) {
	t.Helper()
	if err := store.rejectProtocolWrite(context.Background(), runID, "envelope", "unrecorded-dispatch",
		[]byte(`{}`), "unrecorded-dispatch-hash", "invalid envelope", errors.New("invalid envelope")); err != nil {
		t.Fatal(err)
	}
}

func TestRunEndedInTheCrashWindowIsLinkedByLookup(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	run := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "unrecorded-crash")

	crashing := &loseSubmitResponseClient{JobClient: h.l1Client}
	reconciler, err := NewReconciler(h.l3Store, crashing, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(ctx); err == nil {
		t.Fatal("lost submit response was not reported")
	}
	if crashing.submitted.JobID == "" {
		t.Fatal("L1 did not commit the job before the response was lost")
	}
	failRunAfterDispatchAttempt(t, h.l3Store, run.RunID)
	before := snapshotTerminalRun(t, h.l3Store, run.RunID)
	h.restartLedger()

	client := &countingDispatchRecoveryClient{JobClient: h.l1Client, JobDispatchLookupClient: h.l1Client}
	recovered, err := NewReconciler(h.l3Store, client, ReconcilerConfig{DispatchLookup: client})
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if client.submits != 0 || client.lookups != 1 {
		t.Fatalf("recovery submitted/looked up = %d/%d, want 0/1", client.submits, client.lookups)
	}
	after := snapshotTerminalRun(t, h.l3Store, run.RunID)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("lookup changed terminal run: before=%+v after=%+v", before, after)
	}
	execution, err := h.l3Store.GetRunExecution(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if execution.L1JobID != crashing.submitted.JobID || execution.DispatchError != nil {
		t.Fatalf("recovered execution = %+v, want job %q", execution, crashing.submitted.JobID)
	}
	found, err := h.l1Client.GetJob(ctx, execution.L1JobID)
	if err != nil || found.JobID != crashing.submitted.JobID {
		t.Fatalf("linked L1 job = %#v, %v", found, err)
	}
	var attributionPending int
	if err := h.l3Store.db.QueryRow(`SELECT node_attribution_pending FROM runs WHERE run_id=?`, run.RunID).Scan(&attributionPending); err != nil {
		t.Fatal(err)
	}
	if attributionPending != 1 {
		t.Fatalf("node_attribution_pending = %d, want 1 while the job is queued", attributionPending)
	}
}

func TestRegressedOrEmptyL1CreatesNoJobAndLeavesTheRunUnchanged(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		acknowledged string
		wantReason   string
		prepare      func(*testing.T, *integrationHarness) RunAccepted
	}{
		{
			name: "unacknowledged crash window", wantReason: dispatchNotFoundReason,
			prepare: func(t *testing.T, h *integrationHarness) RunAccepted {
				run := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "empty-crash")
				crashing := &loseSubmitResponseClient{JobClient: h.l1Client}
				r, _ := NewReconciler(h.l3Store, crashing, ReconcilerConfig{})
				if err := r.ReconcileOnce(context.Background()); err == nil || crashing.submitted.JobID == "" {
					t.Fatalf("crash fixture = job %q, err %v", crashing.submitted.JobID, err)
				}
				failRunAfterDispatchAttempt(t, h.l3Store, run.RunID)
				return run
			},
		},
		{
			name: "acknowledged in-flight response", acknowledged: "job-acknowledged", wantReason: l1RegressedReason,
			prepare: func(t *testing.T, h *integrationHarness) RunAccepted {
				run := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "empty-acknowledged")
				if _, err := h.l3Store.ensureRunToken(context.Background(), run.RunID); err != nil {
					t.Fatal(err)
				}
				if err := h.l3Store.beginDispatch(context.Background(), run.RunID); err != nil {
					t.Fatal(err)
				}
				failRunAfterDispatchAttempt(t, h.l3Store, run.RunID)
				if err := h.l3Store.completeDispatch(context.Background(), run.RunID, "job-acknowledged"); err != nil {
					t.Fatal(err)
				}
				return run
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			h := newIntegrationHarness(t)
			run := testCase.prepare(t, h)
			record, err := h.l3Store.GetRun(context.Background(), run.RunID)
			if err != nil {
				t.Fatal(err)
			}
			before := snapshotTerminalRun(t, h.l3Store, run.RunID)
			empty, probe := newEmptyL1LookupClient(t)
			client := &countingDispatchRecoveryClient{JobClient: empty, JobDispatchLookupClient: empty}
			reconciler, err := NewReconciler(h.l3Store, client, ReconcilerConfig{DispatchLookup: client})
			if err != nil {
				t.Fatal(err)
			}
			if err := reconciler.ReconcileOnce(context.Background()); err == nil {
				t.Fatal("authoritative absence was not reported")
			}
			if client.submits != 0 || client.lookups != 1 {
				t.Fatalf("empty-L1 submitted/looked up = %d/%d, want 0/1", client.submits, client.lookups)
			}
			var jobs int
			if err := probe.QueryRow(`SELECT COUNT(*) FROM jobs`).Scan(&jobs); err != nil {
				t.Fatal(err)
			}
			if jobs != 0 {
				t.Fatalf("recovery created %d jobs on empty L1", jobs)
			}
			if after := snapshotTerminalRun(t, h.l3Store, run.RunID); !reflect.DeepEqual(after, before) {
				t.Fatalf("absence changed terminal run: before=%+v after=%+v", before, after)
			}
			execution, err := h.l3Store.GetRunExecution(context.Background(), run.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if execution.L1JobID != "" || execution.DispatchError == nil || execution.DispatchError.Retryable || execution.DispatchError.Details["reason"] != testCase.wantReason {
				t.Fatalf("absence diagnostic = %+v", execution)
			}
			if testCase.acknowledged == "" {
				if execution.DispatchError.Details["dispatch_key"] != record.DispatchKey {
					t.Fatalf("dispatch_not_found details = %#v", execution.DispatchError.Details)
				}
			} else if !isL1Regression(execution.DispatchError, testCase.acknowledged) {
				t.Fatalf("regression diagnostic = %#v", execution.DispatchError)
			}
			if err := reconciler.ReconcileOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			if client.lookups != 1 || client.submits != 0 {
				t.Fatalf("settled recovery repeated: submits/lookups = %d/%d", client.submits, client.lookups)
			}
		})
	}
}

func TestRunEndedDuringItsInFlightSubmitIsLinked(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	run := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "unrecorded-in-flight")
	client := &failRunAfterSubmitClient{JobClient: h.l1Client, store: h.l3Store, runID: run.RunID}
	reconciler, err := NewReconciler(h.l3Store, client, ReconcilerConfig{DispatchLookup: h.l1Client})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if client.submits != 1 || client.job.JobID == "" {
		t.Fatalf("submit calls/job = %d/%q", client.submits, client.job.JobID)
	}
	execution, err := h.l3Store.GetRunExecution(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	var outboxJob string
	var dispatched sql.NullInt64
	if err := h.l3Store.db.QueryRow(`SELECT job_id,dispatched_ns FROM dispatch_outbox WHERE run_id=?`, run.RunID).Scan(&outboxJob, &dispatched); err != nil {
		t.Fatal(err)
	}
	record, err := h.l3Store.GetRun(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if !dispatched.Valid || execution.L1JobID != client.job.JobID || outboxJob != client.job.JobID {
		t.Fatalf("linked IDs = execution %q outbox %q job %q dispatched=%v", execution.L1JobID, outboxJob, client.job.JobID, dispatched)
	}
	if record.Status != contract.RunFailed || !strings.Contains(record.FailureReason, "rejected envelope") {
		t.Fatalf("terminal run changed = %+v", record)
	}
}

// An L1 job stored before L1 recorded run-ledger provenance is outside the
// scoped lookup, but L1 still holds the job it acknowledged. Absence from the
// lookup alone must not be recorded as a regression.
func TestAcknowledgedJobOutsideTheLookupScopeIsLinkedNotRegressed(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	run := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "unrecorded-legacy")
	token, err := h.l3Store.ensureRunToken(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.l3Store.beginDispatch(ctx, run.RunID); err != nil {
		t.Fatal(err)
	}
	intents, err := h.l3Store.pendingDispatches(ctx)
	if err != nil || len(intents) != 1 || intents[0].RunID != run.RunID {
		t.Fatalf("pending dispatches = %+v, %v", intents, err)
	}
	// A job no configured run ledger submitted has the provenance of one
	// stored before submitted_by_run_ledger existed. Current L1 lets only the
	// ledger name a run, so the fixture drops the run identity labels.
	legacy, err := NewL1Client(h.network.NewFabric(fabric.Identity{NodeID: "legacy-submitter", Tags: []string{l1.DefaultClientPrincipalTag}}), DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(legacy.CloseIdleConnections)
	spec := intents[0].jobSpec(token)
	delete(spec.Labels, contract.LabelRunID)
	delete(spec.Labels, contract.LabelHandoffOwnerRunID)
	acknowledged, err := legacy.SubmitJob(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	failRunAfterDispatchAttempt(t, h.l3Store, run.RunID)
	if err := h.l3Store.completeDispatch(ctx, run.RunID, acknowledged.JobID); err != nil {
		t.Fatal(err)
	}
	before := snapshotTerminalRun(t, h.l3Store, run.RunID)

	client := &countingDispatchRecoveryClient{JobClient: h.l1Client, JobDispatchLookupClient: h.l1Client}
	reconciler, err := NewReconciler(h.l3Store, client, ReconcilerConfig{DispatchLookup: client})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if client.submits != 0 || client.lookups != 1 {
		t.Fatalf("recovery submitted/looked up = %d/%d, want 0/1", client.submits, client.lookups)
	}
	execution, err := h.l3Store.GetRunExecution(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if execution.L1JobID != acknowledged.JobID || execution.DispatchError != nil {
		t.Fatalf("recovered execution = %+v, want job %q with no diagnostic", execution, acknowledged.JobID)
	}
	if after := snapshotTerminalRun(t, h.l3Store, run.RunID); !reflect.DeepEqual(after, before) {
		t.Fatalf("link changed terminal run: before=%+v after=%+v", before, after)
	}
}

type failRunAfterSubmitClient struct {
	JobClient
	store   *Store
	runID   string
	submits int
	job     l1.Job
}

func (c *failRunAfterSubmitClient) SubmitJob(ctx context.Context, spec contract.JobSpec) (l1.Job, error) {
	c.submits++
	job, err := c.JobClient.SubmitJob(ctx, spec)
	if err != nil {
		return l1.Job{}, err
	}
	c.job = job
	if err := c.store.rejectProtocolWrite(ctx, c.runID, "envelope", "in-flight", []byte(`{}`), "in-flight-hash", "invalid envelope", errors.New("invalid envelope")); err != nil {
		return l1.Job{}, err
	}
	return job, nil
}

func TestUnrecordedDispatchesUseThePartialIndex(t *testing.T) {
	store, _, _ := recoveryStore(t)
	rows, err := store.db.Query(`EXPLAIN QUERY PLAN ` + unrecordedDispatchesQuery)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if joined := strings.Join(plan, "; "); !strings.Contains(joined, "runs_unrecorded_dispatch") {
		t.Fatalf("unrecorded dispatch query does not use the partial index: %s", joined)
	}
}

func TestPermanentRefusalKeepsItsDiagnosticWhenNoJobIsFound(t *testing.T) {
	h := newIntegrationHarness(t)
	run := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "permanent-refusal")
	refusal := &permanentSubmitErrorClient{JobClient: h.l1Client, runID: run.RunID}
	lookup := &countingDispatchRecoveryClient{JobClient: refusal, JobDispatchLookupClient: h.l1Client}
	reconciler, err := NewReconciler(h.l3Store, lookup, ReconcilerConfig{DispatchLookup: lookup})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("permanent dispatch refusal was not reported")
	}
	execution, err := h.l3Store.GetRunExecution(context.Background(), run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if execution.DispatchError == nil || execution.DispatchError.Code != contract.ErrorDispatchKeyConflict || execution.DispatchError.Message != "dispatch key conflict" {
		t.Fatalf("permanent refusal was replaced: %+v", execution.DispatchError)
	}
	if lookup.lookups != 1 {
		t.Fatalf("lookup calls = %d, want 1", lookup.lookups)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if lookup.lookups != 1 {
		t.Fatalf("settled refusal looked up again: %d", lookup.lookups)
	}
}

func newEmptyL1LookupClient(t *testing.T) (*L1Client, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "empty-l1.sqlite")
	store, err := l1.OpenStore(path, l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	probe, err := sql.Open("sqlite", path)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "empty-authority"})
	ledgerFabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger", Tags: []string{l1.DefaultClientPrincipalTag}})
	server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{})
	if err != nil {
		probe.Close()
		store.Close()
		t.Fatal(err)
	}
	listener, err := serverFabric.Listen("tcp", DefaultL1Address)
	if err != nil {
		probe.Close()
		store.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	client, err := NewL1Client(ledgerFabric, DefaultL1Address)
	if err != nil {
		cancel()
		<-done
		probe.Close()
		store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		client.CloseIdleConnections()
		cancel()
		if err := <-done; err != nil {
			t.Errorf("serve empty L1: %v", err)
		}
		if err := probe.Close(); err != nil {
			t.Errorf("close empty L1 probe: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("close empty L1: %v", err)
		}
	})
	return client, probe
}
