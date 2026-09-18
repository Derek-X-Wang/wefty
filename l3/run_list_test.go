package l3

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// TestGeneralRunListingIsNewestFirstAndFiltersByStatus is the operator's view:
// what is happening here, most recent first.
func TestGeneralRunListingIsNewestFirstAndFiltersByStatus(t *testing.T) {
	h := newIntegrationHarness(t)
	first := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "list-first")
	second := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "list-second")
	third := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "list-third")

	status, _, body := h.do(h.caller, http.MethodGet, "/v1/runs", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("list status = %d body=%s", status, body)
	}
	var page RunListPage
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Runs) != 3 {
		t.Fatalf("listed %d runs, want 3: %+v", len(page.Runs), page.Runs)
	}
	if page.Runs[0].RunID != third.RunID || page.Runs[2].RunID != first.RunID {
		t.Fatalf("listing is not newest first: %s, %s, %s",
			page.Runs[0].RunID, page.Runs[1].RunID, page.Runs[2].RunID)
	}
	if page.Runs[0].Trigger.Type == "" || page.Runs[0].CreatedAt.IsZero() {
		t.Fatalf("summary lost its provenance: %#v", page.Runs[0])
	}

	// A limit is the whole of the request: the head of the list.
	status, _, body = h.do(h.caller, http.MethodGet, "/v1/runs?limit=2", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("limited list status = %d body=%s", status, body)
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Runs) != 2 || page.Runs[0].RunID != third.RunID {
		t.Fatalf("limited listing = %+v", page.Runs)
	}

	// A status nobody is in is an empty listing, not an error.
	status, _, body = h.do(h.caller, http.MethodGet, "/v1/runs?status=succeeded", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("status-filtered list status = %d body=%s", status, body)
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Runs) != 0 {
		t.Fatalf("succeeded listing = %+v", page.Runs)
	}
	_ = second

	// Every filtered run is in the state asked for.
	status, _, body = h.do(h.caller, http.MethodGet, "/v1/runs?status=pending", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("pending list status = %d body=%s", status, body)
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Runs) != 3 {
		t.Fatalf("pending listing = %+v", page.Runs)
	}
	for _, run := range page.Runs {
		if run.Status != contract.RunPending {
			t.Fatalf("a %s run answered a pending filter", run.Status)
		}
	}
}

// TestGeneralRunListingRefusesWhatItCannotAnswer keeps the two listings apart
// and says so rather than quietly ignoring a parameter.
func TestGeneralRunListingRefusesWhatItCannotAnswer(t *testing.T) {
	h := newIntegrationHarness(t)
	h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "list-refusals")

	for _, query := range []string{
		"/v1/runs?status=not-a-state",
		// status belongs to the general listing. The origin arm refuses it
		// rather than returning an unfiltered page that looks filtered.
		"/v1/runs?origin=computer:one&status=failed",
		"/v1/runs?origin=computer:one&status=",
		"/v1/runs?limit=0",
		"/v1/runs?limit=huge",
		// A caller that sent a cursor believes this listing pages; it does not.
		"/v1/runs?cursor=abc",
		"/v1/runs?include_descendants=true",
	} {
		status, _, body := h.do(h.caller, http.MethodGet, query, nil, nil)
		if status != http.StatusBadRequest {
			t.Fatalf("%s status = %d body=%s", query, status, body)
		}
	}
}

// TestTheCurrentStepTravelsWithTheListingAndTheLineage is the observation the
// ticket is for: a running run says where it is, in both places a reader looks.
func TestTheCurrentStepTravelsWithTheListingAndTheLineage(t *testing.T) {
	h := newIntegrationHarness(t)
	accepted := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "list-steps")
	// The envelopes are inserted directly: this test is about what the ledger
	// derives from a run's step brackets, not about the credential path that
	// puts them there, which protocol_integration_test.go already covers.
	insertStepEnvelope(t, h, accepted.RunID, "checkout", "started", 0)
	insertStepEnvelope(t, h, accepted.RunID, "checkout", "ended", 1)
	insertStepEnvelope(t, h, accepted.RunID, "gates", "started", 2)

	status, _, body := h.do(h.caller, http.MethodGet, "/v1/runs", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("list status = %d body=%s", status, body)
	}
	var page RunListPage
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Runs) != 1 || page.Runs[0].CurrentStep != "gates" {
		t.Fatalf("listing does not carry the current step: %+v", page.Runs)
	}

	// A lineage entry carries the same answer, so a tree of runs reads as
	// progress rather than as a list of identifiers. A root run's own lineage
	// holds no entry for itself, so the annotation is exercised against an
	// entry naming this run.
	lineage := RunLineage{RunID: accepted.RunID, Descendants: []LineageEntry{
		{RunID: accepted.RunID, Status: contract.RunRunning, Depth: 1},
		{RunID: accepted.RunID, Status: contract.RunSucceeded, Depth: 1},
	}}
	if err := h.l3Server.annotateLineageSteps(context.Background(), &lineage); err != nil {
		t.Fatal(err)
	}
	if lineage.Descendants[0].CurrentStep != "gates" {
		t.Fatalf("a running lineage entry lost its step: %+v", lineage.Descendants[0])
	}
	// A terminal run is in no step, and is not asked.
	if lineage.Descendants[1].CurrentStep != "" {
		t.Fatalf("a terminal lineage entry reported a step: %+v", lineage.Descendants[1])
	}

	// The same derivation the ledger serves is the one the tests pin.
	envelopes, err := h.l3Store.ListEnvelopes(context.Background(), accepted.RunID)
	if err != nil {
		t.Fatal(err)
	}
	derived := DeriveRunSteps(envelopes)
	if derived.Current != "gates" || len(derived.Steps) != 2 {
		t.Fatalf("derived steps = %#v", derived)
	}
	if derived.Steps[0].Seconds == nil || *derived.Steps[0].Seconds != 1 {
		t.Fatalf("the closed step has no duration: %#v", derived.Steps[0])
	}
}

// insertStepEnvelope writes one step bracket exactly as the agent publishes it.
func insertStepEnvelope(t *testing.T, h *integrationHarness, runID, name, status string, offsetSeconds int) {
	t.Helper()
	created := time.Date(2026, 9, 17, 12, 0, offsetSeconds, 0, time.UTC)
	extensions, err := json.Marshal(map[string]any{
		mailboxExtensionNamespace: map[string]any{"kind": "step", "step_status": status, "name": name},
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope := contract.Envelope{
		SchemaVersion: contract.SchemaVersionV1,
		EnvelopeID:    runID + "-" + name + "-" + status,
		RunID:         runID, StepID: name, Status: contract.EnvelopePartial,
		Summary: "step " + name + " " + status, Extensions: extensions, CreatedAt: created,
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.l3Store.db.Exec(
		`INSERT INTO envelopes(envelope_id, run_id, idempotency_key, body_hash, body_json, accepted_ns)
		 VALUES(?, ?, ?, ?, ?, ?)`,
		envelope.EnvelopeID, runID, envelope.EnvelopeID, envelope.EnvelopeID, body, created.UnixNano()); err != nil {
		t.Fatal(err)
	}
}
