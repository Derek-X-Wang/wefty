package l3

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
)

// submittedJobClient reports each dispatch key L1 is asked to create.
type submittedJobClient struct{ submitted chan string }

func (c *submittedJobClient) SubmitJob(_ context.Context, spec contract.JobSpec) (l1.Job, error) {
	c.submitted <- spec.DispatchKey
	return l1.Job{JobID: "job-" + spec.DispatchKey, State: contract.JobQueued, Spec: spec}, nil
}

func (c *submittedJobClient) GetJob(_ context.Context, jobID string) (l1.Job, error) {
	return l1.Job{}, errors.New("not used")
}

// An accepted run reaches L1 without waiting for the reconcile cadence (#945).
// The cadence here is an hour and the ledger clock never advances, so only
// the acceptance itself can start the dispatch.
func TestAcceptedRunWakesDispatchBeforeTheNextPass(t *testing.T) {
	store, _, _ := recoveryStore(t)
	first, _, err := store.CreateRun(context.Background(), CreateRunInput{IdempotencyKey: "startup", Actor: "test", Request: inlineRunRequest("#!/bin/sh\nexit 0\n")})
	if err != nil {
		t.Fatal(err)
	}
	dispatchKey := func(runID string) string {
		t.Helper()
		var key string
		if err := store.db.QueryRow(`SELECT dispatch_key FROM runs WHERE run_id=?`, runID).Scan(&key); err != nil {
			t.Fatal(err)
		}
		return key
	}
	jobs := &submittedJobClient{submitted: make(chan string, 4)}
	reconciler, err := NewReconciler(store, jobs, ReconcilerConfig{Interval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	network := plain.NewNetwork()
	ledger := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
	server, err := NewServer(ledger, store, ServerConfig{Reconciler: reconciler, Logs: emptyComputerJobLogs{}})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := ledger.Listen("tcp", DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve: %v", err)
		}
	}()
	submitted := func(what string) string {
		t.Helper()
		select {
		case key := <-jobs.submitted:
			return key
		case <-time.After(testwait.Bound):
			t.Fatalf("%s never reached L1", what)
			return ""
		}
	}
	// The startup pass dispatches the run that was already waiting; after it
	// the next pass is an hour away.
	if key, want := submitted("the run pending at startup"), dispatchKey(first.RunID); key != want {
		t.Fatalf("startup dispatch = %q, want %q", key, want)
	}

	caller := network.NewFabric(fabric.Identity{NodeID: "operator", Tags: []string{DefaultCallerPrincipalTag}})
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return caller.Dial(ctx, network, DefaultL3Address)
	}}}
	defer client.CloseIdleConnections()
	h := &integrationHarness{t: t}
	status, _, body := h.do(client, http.MethodPost, "/v1/runs", inlineRunRequest("#!/bin/sh\nexit 0\n"), http.Header{"Idempotency-Key": []string{"woken"}})
	if status != http.StatusCreated {
		t.Fatalf("submit status = %d body=%s", status, body)
	}
	var accepted RunAccepted
	if err := json.Unmarshal(body, &accepted); err != nil {
		t.Fatal(err)
	}
	if key, want := submitted("a run accepted between passes"), dispatchKey(accepted.RunID); key != want {
		t.Fatalf("woken dispatch = %q, want %q", key, want)
	}
}
