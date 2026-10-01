package l3

import (
	"context"
	"errors"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// endRunDuringSubmitClient submits normally, except that the first submit for
// trigger ends victim before it returns. A pass lists both runs before either
// is dispatched, so the victim reaches its dispatch step already terminal.
type endRunDuringSubmitClient struct {
	JobClient
	store           *Store
	trigger, victim string
	failAll         bool
	submitted       map[string]int
}

func (c *endRunDuringSubmitClient) SubmitJob(ctx context.Context, spec contract.JobSpec) (l1.Job, error) {
	runID := spec.Labels[contract.LabelRunID]
	if c.submitted == nil {
		c.submitted = make(map[string]int)
	}
	c.submitted[runID]++
	if c.failAll {
		return l1.Job{}, errors.New("injected transient submit failure")
	}
	job, err := c.JobClient.SubmitJob(ctx, spec)
	if err != nil || runID != c.trigger {
		return job, err
	}
	if err := c.store.rejectProtocolWrite(ctx, c.victim, "envelope", "terminal-race", []byte(`{}`), "terminal-race-hash", "invalid envelope", errors.New("invalid envelope")); err != nil {
		return l1.Job{}, err
	}
	return job, nil
}

// A retry for a run that turned terminal after the pass listed it must not
// reach L1. The terminal transition cleared the staged bearer; a dispatch that
// re-minted one and submitted anyway would create new work for an ended run if
// L1 had lost the original job (#619 part 9 review).
func TestRetryForARunThatEndedBeforeItsAttemptDoesNotSubmit(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	trigger := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "terminal-race-trigger")
	victim := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "terminal-race-victim")
	client := &endRunDuringSubmitClient{JobClient: h.l1Client, store: h.l3Store, trigger: trigger.RunID, victim: victim.RunID, failAll: true}
	reconciler, err := NewReconciler(h.l3Store, client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	// The first pass stages the victim's bearer and leaves it for a retry.
	if err := reconciler.ReconcileOnce(ctx); err == nil {
		t.Fatal("transient submit failures were not reported")
	}
	if client.submitted[victim.RunID] != 1 {
		t.Fatalf("first-pass victim submits = %d, want 1", client.submitted[victim.RunID])
	}
	var tokensBefore int
	if err := h.l3Store.db.QueryRow(`SELECT COUNT(*) FROM run_tokens WHERE run_id=?`, victim.RunID).Scan(&tokensBefore); err != nil {
		t.Fatal(err)
	}

	client.failAll = false
	if err := reconciler.ReconcileOnce(ctx); err != nil {
		t.Fatalf("an abandoned attempt is not a pass error: %v", err)
	}
	if client.submitted[trigger.RunID] != 2 {
		t.Fatalf("trigger submits = %d, want 2", client.submitted[trigger.RunID])
	}
	if got := client.submitted[victim.RunID]; got != 1 {
		t.Fatalf("victim submits = %d after it turned terminal, want 1", got)
	}
	record, err := h.l3Store.GetRun(ctx, victim.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Status != contract.RunFailed {
		t.Fatalf("victim status = %q, want failed", record.Status)
	}
	var attempts, tokensAfter int
	var delivery *string
	if err := h.l3Store.db.QueryRow(`SELECT attempt_count, token_delivery FROM dispatch_outbox WHERE run_id=?`, victim.RunID).Scan(&attempts, &delivery); err != nil {
		t.Fatal(err)
	}
	if err := h.l3Store.db.QueryRow(`SELECT COUNT(*) FROM run_tokens WHERE run_id=?`, victim.RunID).Scan(&tokensAfter); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || delivery != nil || tokensAfter != tokensBefore {
		t.Fatalf("abandoned attempt changed the outbox: attempts=%d delivery=%v tokens %d -> %d", attempts, delivery, tokensBefore, tokensAfter)
	}
	if _, err := h.l1Client.LookupJobByDispatchKey(ctx, record.DispatchKey); !isMissingDispatch(err, record.DispatchKey) {
		t.Fatalf("L1 holds a job for the ended run: %v", err)
	}
}

// The retry's bearer hand-out and its run-status check are one step: a run
// whose bearer a retry already staged, and which then ended, is refused the
// attempt rather than handed the bearer it would submit with.
func TestBeginDispatchRefusesARunThatIsTerminal(t *testing.T) {
	s, _, _ := recoveryStore(t)
	ctx := context.Background()
	record, _, err := s.CreateRun(ctx, CreateRunInput{IdempotencyKey: "terminal-begin", Actor: "test", Request: inlineRunRequest("#!/bin/sh\nexit 0\n")})
	if err != nil {
		t.Fatal(err)
	}
	staged, err := s.beginDispatch(ctx, record.RunID)
	if err != nil || staged == "" {
		t.Fatalf("first attempt = %q, %v", staged, err)
	}
	if err := s.recordDispatchError(ctx, record.RunID, errors.New("transient")); err != nil {
		t.Fatal(err)
	}
	if err := s.rejectProtocolWrite(ctx, record.RunID, "envelope", "terminal-begin", []byte(`{}`), "terminal-begin-hash", "invalid envelope", errors.New("invalid envelope")); err != nil {
		t.Fatal(err)
	}
	token, err := s.beginDispatch(ctx, record.RunID)
	if !errors.Is(err, errDispatchAbandoned) || token != "" {
		t.Fatalf("attempt for a terminal run = %q, %v; want errDispatchAbandoned and no bearer", token, err)
	}
	var attempts int
	var delivery *string
	if err := s.db.QueryRow(`SELECT attempt_count, token_delivery FROM dispatch_outbox WHERE run_id=?`, record.RunID).Scan(&attempts, &delivery); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || delivery != nil {
		t.Fatalf("abandoned attempt recorded attempts=%d delivery=%v", attempts, delivery)
	}
}
