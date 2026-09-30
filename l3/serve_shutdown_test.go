package l3

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

// stallingJobClient holds a dispatch in flight past cancellation, the way an
// L1 call that has already been answered is still being recorded.
type stallingJobClient struct {
	entered chan struct{}
	release chan struct{}
}

func (c *stallingJobClient) SubmitJob(context.Context, contract.JobSpec) (l1.Job, error) {
	close(c.entered)
	<-c.release
	return l1.Job{}, errors.New("released after shutdown")
}

func (c *stallingJobClient) GetJob(context.Context, string) (l1.Job, error) {
	return l1.Job{}, errors.New("not used")
}

func TestServeJoinsItsReconcilerBeforeReturning(t *testing.T) {
	store, _, _ := recoveryStore(t)
	if _, _, err := store.CreateRun(context.Background(), CreateRunInput{IdempotencyKey: "join", Actor: "test", Request: inlineRunRequest("#!/bin/sh\nexit 0\n")}); err != nil {
		t.Fatal(err)
	}
	jobs := &stallingJobClient{entered: make(chan struct{}), release: make(chan struct{})}
	reconciler, err := NewReconciler(store, jobs, ReconcilerConfig{Interval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	ledger := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "run-ledger"})
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

	select {
	case <-jobs.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("reconciler never dispatched the pending run")
	}
	cancel()
	select {
	case err := <-done:
		close(jobs.release)
		t.Fatalf("Serve returned (%v) while its reconciler was still dispatching", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(jobs.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after its reconciler stopped")
	}
}
