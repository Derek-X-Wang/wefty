package l3

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

type recoveryJobClient struct {
	JobClient
	get     func(context.Context, string) (l1.Job, error)
	submits int
}

func (c *recoveryJobClient) GetJob(ctx context.Context, id string) (l1.Job, error) {
	return c.get(ctx, id)
}
func (c *recoveryJobClient) SubmitJob(ctx context.Context, spec contract.JobSpec) (l1.Job, error) {
	c.submits++
	return c.JobClient.SubmitJob(ctx, spec)
}

func TestAuthoritativeMissingJobFailsClosed(t *testing.T) {
	h := newIntegrationHarness(t)
	accepted := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "regression")
	c := &recoveryJobClient{JobClient: h.l1Client}
	c.get = h.l1Client.GetJob
	r, _ := NewReconciler(h.l3Store, c, ReconcilerConfig{})
	if err := r.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	before, err := h.l3Store.GetRunExecution(context.Background(), accepted.RunID)
	if err != nil {
		t.Fatal(err)
	}
	// The same requested identity is absent in a fresh authority.
	cold := newIntegrationHarness(t)
	c.get = cold.l1Client.GetJob
	if err := r.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("missing job must be reported")
	}
	run, err := h.l3Store.GetRun(context.Background(), accepted.RunID)
	if err != nil {
		t.Fatal(err)
	}
	after, err := h.l3Store.GetRunExecution(context.Background(), accepted.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != contract.RunFailed || after.DispatchError == nil {
		t.Fatalf("missing job wedged: status=%s execution=%+v", run.Status, after)
	}
	if after.L1JobID != before.L1JobID || after.DispatchAttempts != before.DispatchAttempts || after.DispatchError.Details["reason"] != "l1_regressed" {
		t.Fatalf("lost identity/reason: %+v", after)
	}
	if err := r.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.submits != 1 {
		t.Fatalf("replayed %d submits", c.submits)
	}
}

type recoveryRoundTripper func(*http.Request) (*http.Response, error)

func (f recoveryRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestL1RequestHasOperationDeadline(t *testing.T) {
	h := newIntegrationHarness(t)
	if h.l1Client.operationTimeout != 10*time.Second {
		t.Fatalf("operation default=%s", h.l1Client.operationTimeout)
	}
	if transport, ok := h.l1Client.client.Transport.(*http.Transport); !ok || transport.ResponseHeaderTimeout != 10*time.Second {
		t.Fatalf("header default=%+v", transport)
	}
	original := h.l1Client.client.Transport
	defer func() { h.l1Client.client.Transport = original }()
	h.l1Client.client.Transport = recoveryRoundTripper(func(r *http.Request) (*http.Response, error) {
		if _, ok := r.Context().Deadline(); !ok {
			t.Error("L1 request has no operation deadline")
		}
		return nil, errors.New("stop before network")
	})
	_, _ = h.l1Client.GetJob(context.Background(), "job")
}
