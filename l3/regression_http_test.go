package l3

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestL1SerializedTransientDispatchRetries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "authority.sqlite")
	authority, err := l1.OpenStore(path, l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer authority.Close()
	probe, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	if _, err = probe.Exec(`CREATE TRIGGER unavailable BEFORE INSERT ON jobs BEGIN SELECT RAISE(ABORT,'private backend failure'); END`); err != nil {
		t.Fatal(err)
	}
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "authority"})
	callerFabric := network.NewFabric(fabric.Identity{NodeID: "ledger", Tags: []string{l1.DefaultClientPrincipalTag}})
	server, err := l1.NewServer(serverFabric, authority, l1.ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := serverFabric.Listen("tcp", DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := serveL1(ctx, server, listener)
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	client, err := NewL1Client(callerFabric, DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	s, _, _ := recoveryStore(t)
	record, _, err := s.CreateRun(ctx, CreateRunInput{IdempotencyKey: "transient", Actor: "test", Request: inlineRunRequest("#!/bin/sh\nexit 0\n")})
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewReconciler(s, client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.ReconcileOnce(ctx); err == nil {
		t.Fatal("database failure not reported")
	}
	projection, err := s.GetRunExecution(ctx, record.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if projection.DispatchError == nil || projection.DispatchError.Code != contract.ErrorInternal || !projection.DispatchError.Retryable || projection.DispatchError.Message != "internal server error" || projection.L1JobID != "" || projection.DispatchAttempts != 1 {
		t.Fatalf("transient serialized incorrectly: %+v", projection)
	}
	run, err := s.GetRun(ctx, record.RunID)
	if err != nil || run.Status != contract.RunDispatching {
		t.Fatalf("transient failed run: %+v %v", run, err)
	}
	if _, err = probe.Exec(`DROP TRIGGER unavailable`); err != nil {
		t.Fatal(err)
	}
	if err = r.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err = r.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	projection, err = s.GetRunExecution(ctx, record.RunID)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	var key string
	if err = probe.QueryRow(`SELECT COUNT(*),dispatch_key FROM jobs`).Scan(&count, &key); err != nil {
		t.Fatal(err)
	}
	if count != 1 || key != record.DispatchKey || projection.L1JobID == "" || projection.DispatchAttempts != 2 || projection.DispatchError != nil {
		t.Fatalf("retry lost identity/duplicated: jobs=%d key=%s projection=%+v", count, key, projection)
	}
}

func TestL1RegressionExecutionReadBoundary(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	run := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "read-boundary")
	r, _ := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err := r.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	projection, err := h.l3Store.GetRunExecution(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	original := h.l1Client.client.Transport
	defer func() { h.l1Client.client.Transport = original }()
	status, body := 404, `{"error":{"code":"not_found","message":"absent","retryable":false}}`
	h.l1Client.client.Transport = recoveryRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})
	read := func() (int, RunExecution) {
		t.Helper()
		code, _, raw := h.do(h.caller, http.MethodGet, "/v1/runs/"+run.RunID+"/execution", nil, nil)
		var value RunExecution
		if code == 200 {
			if err := json.Unmarshal(raw, &value); err != nil {
				t.Fatal(err)
			}
		}
		return code, value
	}
	if code, _ := read(); code != 404 {
		t.Fatalf("unrecorded missing read=%d", code)
	}
	if got := snapshotRecovery(t, h.l3Store, run.RunID); got.state != contract.RunQueued || got.reason.Valid {
		t.Fatalf("read mutated state: %+v", got)
	}
	if err = r.ReconcileOnce(ctx); err == nil {
		t.Fatal("missing not reported")
	}
	durable := snapshotRecovery(t, h.l3Store, run.RunID)
	code, value := read()
	if code != 200 || value.Job != nil || value.L1JobID != projection.L1JobID || !isL1Regression(value.DispatchError, projection.L1JobID) {
		t.Fatalf("regression hidden: status=%d value=%+v", code, value)
	}
	for _, tt := range []struct {
		name   string
		status int
		body   string
		want   int
	}{
		{"malformed", 404, `<html>proxy</html>`, 500},
		{"auth", 401, `{"error":{"code":"unauthorized","message":"denied","retryable":false}}`, 401},
		{"transient", 503, `{"error":{"code":"internal","message":"unavailable","retryable":true}}`, 503},
		{"wrong code", 404, `{"error":{"code":"attempt_not_found","message":"absent","retryable":false}}`, 409},
	} {
		t.Run(tt.name, func(t *testing.T) {
			status, body = tt.status, tt.body
			if code, _ := read(); code != tt.want {
				t.Fatalf("nonmatching read=%d want=%d", code, tt.want)
			}
		})
	}

	// A remote message or a forged/mismatched durable marker cannot hide errors.
	status, body = 404, `{"error":{"code":"not_found","message":"absent","retryable":false}}`
	for _, details := range []map[string]any{
		{"reason": "different", "l1_job_id": projection.L1JobID},
		{"reason": l1RegressedReason, "l1_job_id": "other-job"},
	} {
		payload, err := json.Marshal(contract.APIError{Code: contract.ErrorNotFound, Message: "marker", Details: details})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = h.l3Store.db.Exec(`UPDATE dispatch_outbox SET last_error=? WHERE run_id=?`, string(payload), run.RunID); err != nil {
			t.Fatal(err)
		}
		if code, _ := read(); code != 404 {
			t.Fatalf("mismatched marker read=%d", code)
		}
	}
	if _, err = h.l3Store.db.Exec(`UPDATE dispatch_outbox SET last_error=? WHERE run_id=?`, durable.reason.String, run.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err = h.l3Store.db.Exec(`UPDATE runs SET status=? WHERE run_id=?`, contract.RunQueued, run.RunID); err != nil {
		t.Fatal(err)
	}
	if code, _ := read(); code != 404 {
		t.Fatalf("nonterminal marker read=%d", code)
	}
	if _, err = h.l3Store.db.Exec(`UPDATE runs SET status=? WHERE run_id=?`, contract.RunFailed, run.RunID); err != nil {
		t.Fatal(err)
	}
	sibling := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "read-sibling")
	token, err := h.l3Store.ensureRunToken(ctx, sibling.RunID)
	if err != nil {
		t.Fatal(err)
	}
	workflow := h.client(fabric.Identity{NodeID: "workflow"}, DefaultL3Address)
	code, _, raw := h.do(workflow, http.MethodGet, "/v1/runs/"+run.RunID+"/execution", nil, http.Header{"Authorization": []string{"Bearer " + token}})
	assertAPIError(t, code, raw, http.StatusForbidden, contract.ErrorForbidden)
	if got := snapshotRecovery(t, h.l3Store, run.RunID); got != durable {
		t.Fatalf("read changed durable record: %+v", got)
	}
}

func TestL1RegressionFailedChildReleasesParentAndHealthyPeer(t *testing.T) {
	s, _, _ := recoveryStore(t)
	ctx := context.Background()
	parent, _ := dispatchedRecoveryRun(t, s, "parent")
	if err := s.projectJobState(ctx, parent, contract.JobRunning); err != nil {
		t.Fatal(err)
	}
	request := inlineRunRequest("#!/bin/sh\nexit 0\n")
	request.ParentRunID = parent.RunID
	child, _, err := s.CreateRun(ctx, CreateRunInput{IdempotencyKey: "child", Actor: "test", Request: request})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ensureRunToken(ctx, child.RunID); err != nil {
		t.Fatal(err)
	}
	if err = s.beginDispatch(ctx, child.RunID); err != nil {
		t.Fatal(err)
	}
	if err = s.completeDispatch(ctx, child.RunID, "missing-child"); err != nil {
		t.Fatal(err)
	}
	peer, _ := dispatchedRecoveryRun(t, s, "peer")
	if err = s.projectJobState(ctx, peer, contract.JobRunning); err != nil {
		t.Fatal(err)
	}
	c := &recoveryJobClient{get: func(_ context.Context, id string) (l1.Job, error) {
		if id == "missing-child" {
			return l1.Job{}, &JobNotFoundError{JobID: id, Cause: &Error{Code: contract.ErrorNotFound}}
		}
		return l1.Job{JobID: id, State: contract.JobSucceeded}, nil
	}}
	r, _ := NewReconciler(s, c, ReconcilerConfig{})
	if err = r.ReconcileOnce(ctx); err == nil {
		t.Fatal("missing child not reported")
	}
	for id, want := range map[string]contract.RunState{child.RunID: contract.RunFailed, parent.RunID: contract.RunFailed, peer.RunID: contract.RunSucceeded} {
		if got := snapshotRecovery(t, s, id); got.state != want {
			t.Fatalf("run %s state=%s want=%s", id, got.state, want)
		}
	}
	if err = r.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
}
