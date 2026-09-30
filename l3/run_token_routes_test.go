package l3

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// TestRunTokenRoutesAreExactlyWhereARunTokenIsServed checks the declared
// registry against the live handler: every declared route serves a run token
// for its own run, and every other route L3 exposes either refuses a run token
// or ignores it in favour of node identity, which is why it must stay off the
// agent's run bridge (#595).
func TestRunTokenRoutesAreExactlyWhereARunTokenIsServed(t *testing.T) {
	h := newIntegrationHarness(t)
	accepted := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "run-token-routes")
	reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	token := claimRunTokens(t, h, 1)[accepted.RunID]
	if token == "" {
		t.Fatal("claim carried no run token")
	}
	scope, err := h.l3Store.AuthenticateRunToken(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	workflow := h.client(fabric.Identity{NodeID: "workflow-node"}, DefaultL3Address)
	auth := func(extra ...string) http.Header {
		header := http.Header{"Authorization": []string{"Bearer " + token}}
		for index := 0; index+1 < len(extra); index += 2 {
			header.Set(extra[index], extra[index+1])
		}
		return header
	}
	child := inlineRunRequest("#!/bin/sh\necho child\n")
	child.ParentRunID = accepted.RunID
	bodies := map[string]any{
		"POST /v1/runs":                    child,
		"POST /v1/runs/{run_id}/envelopes": validEnvelope(accepted.RunID, scope.AttemptID, "route-envelope"),
		"POST /v1/runs/{run_id}/gates":     validGate(accepted.RunID, scope.AttemptID, "route-gate"),
	}
	for _, route := range RunTokenRoutes() {
		key := route.Method + " " + route.Path
		path := strings.ReplaceAll(route.Path, "{run_id}", accepted.RunID)
		status, header, body := h.do(workflow, route.Method, path, bodies[key], auth("Idempotency-Key", "route-child"))
		t.Logf("%s -> %d", key, status)
		if status == http.StatusUnauthorized || status == http.StatusForbidden || status >= 500 ||
			!strings.HasPrefix(header.Get("Content-Type"), "application/json") {
			t.Errorf("%s with the run's own token: status=%d type=%q body=%s; want it served", key, status, header.Get("Content-Type"), body)
		}
	}

	type refusal struct {
		method, path string
		status       int
		code         contract.ErrorCode
	}
	refusedForRunTokens := []refusal{
		{http.MethodGet, "/v1/runs", http.StatusForbidden, contract.ErrorForbidden},
		{http.MethodGet, "/v1/runs?origin=computer:computer-1", http.StatusForbidden, contract.ErrorForbidden},
		{http.MethodPost, "/v1/runs/{run_id}/rerun", http.StatusForbidden, contract.ErrorForbidden},
		{http.MethodPost, "/v1/runs/{run_id}/cancel", http.StatusNotImplemented, contract.ErrorNotImplemented},
		{http.MethodGet, "/v1/computer/self", http.StatusForbidden, contract.ErrorForbidden},
		{http.MethodPost, "/v1/workflows/workflow-1/versions", http.StatusForbidden, contract.ErrorForbidden},
		{http.MethodGet, "/v1/workflows/workflow-1/versions/1", http.StatusForbidden, contract.ErrorForbidden},
	}
	for _, test := range refusedForRunTokens {
		path := strings.ReplaceAll(test.path, "{run_id}", accepted.RunID)
		status, _, body := h.do(workflow, test.method, path, json.RawMessage(`{}`), auth())
		assertAPIError(t, status, body, test.status, test.code)
		if declaresRunTokenRoute(test.method, strings.SplitN(test.path, "?", 2)[0]) {
			t.Errorf("%s %s refuses run tokens but is declared a run-token route", test.method, test.path)
		}
	}
	// Node-authenticated administration never consults the bearer at all, so
	// a run token neither grants nor limits it: it must never be declared.
	for _, path := range []string{"/v1/computer-token/mint", "/v1/computer-token/revoke",
		"/v1/computer-token/revoke-attempt", "/v1/computer-token/revoke-host", "/v1/computers/{computer_id}/inflight"} {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			if declaresRunTokenRoute(method, path) {
				t.Errorf("node administration route %s %s is declared a run-token route", method, path)
			}
		}
	}
}

func declaresRunTokenRoute(method, path string) bool {
	return slices.Contains(RunTokenRoutes(), RunTokenRoute{Method: method, Path: path})
}
