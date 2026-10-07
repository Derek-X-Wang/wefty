package l3

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

type pagingTestPage struct {
	Runs       []RunSummary `json:"runs"`
	NextCursor string       `json:"next_cursor"`
}

func readPagingTestPage(t *testing.T, h *integrationHarness, client *http.Client, query string) pagingTestPage {
	t.Helper()
	status, _, body := h.do(client, http.MethodGet, "/v1/runs?"+query, nil, nil)
	if status != http.StatusOK {
		t.Fatalf("list %s: status=%d body=%s", query, status, body)
	}
	var page pagingTestPage
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	return page
}

func TestGeneralRunPagingWalkBeyond500WithInserts(t *testing.T) {
	h := newIntegrationHarness(t)
	expected := map[string]bool{}
	// Groups share creation timestamps, including across page boundaries.
	for i := 0; i < 617; i++ {
		h.l3Store.clock = ClockFunc(func() time.Time { return time.Date(2026, 1, 1, 0, 0, i/100, 0, time.UTC) })
		record, _, err := h.l3Store.CreateRun(t.Context(), CreateRunInput{IdempotencyKey: fmt.Sprintf("paging-%d", i), Actor: h.callerUser, Request: inlineRunRequest("#!/bin/sh\nexit 0\n")})
		if err != nil {
			t.Fatal(err)
		}
		expected[record.RunID] = true
	}
	h.l3Store.clock = systemClock{}
	seen := map[string]bool{}
	cursor := ""
	var previous RunSummary
	for pageNumber := 0; ; pageNumber++ {
		page := readPagingTestPage(t, h, h.caller, "limit=73&cursor="+url.QueryEscape(cursor))
		if len(page.Runs) == 0 {
			t.Fatal("unexpected empty page")
		}
		for _, run := range page.Runs {
			if !expected[run.RunID] || seen[run.RunID] {
				t.Fatalf("unexpected or repeated run %s", run.RunID)
			}
			if previous.RunID != "" && (run.CreatedAt.After(previous.CreatedAt) || (run.CreatedAt.Equal(previous.CreatedAt) && run.RunID >= previous.RunID)) {
				t.Fatal("listing order changed")
			}
			previous = run
			seen[run.RunID] = true
		}
		if page.NextCursor == "" {
			break
		}
		if pageNumber > 20 {
			t.Fatal("cursor walk did not terminate")
		}
		// Inserts committed between requests must not shift the walk into duplicates
		// or hide old rows. Insert at the cursor timestamp on both sides of
		// its Run ID, as well as at a newer timestamp. Only the smaller ID
		// belongs to the rest of this walk.
		olderID := previous.RunID[:len(previous.RunID)-1]
		newerID := previous.RunID + "0"
		done := make(chan error, 1)
		go func() {
			_, _, err := h.l3Store.CreateRun(t.Context(), CreateRunInput{IdempotencyKey: fmt.Sprintf("insert-%d", pageNumber), Actor: h.callerUser, Request: inlineRunRequest("#!/bin/sh\nexit 0\n")})
			if err == nil {
				err = insertPagingTestRun(h.l3Store, olderID, h.callerUser, contract.RunPending, previous.CreatedAt.UnixNano())
			}
			if err == nil {
				err = insertPagingTestRun(h.l3Store, newerID, h.callerUser, contract.RunPending, previous.CreatedAt.UnixNano())
			}
			done <- err
		}()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		expected[olderID] = true
		cursor = page.NextCursor
	}
	if len(seen) != len(expected) {
		t.Fatalf("walk returned %d of %d runs", len(seen), len(expected))
	}
}

func TestGeneralRunPagingExactFiltersAndScope(t *testing.T) {
	h := newIntegrationHarness(t)
	expected := map[string]bool{}
	for i := 0; i < 12; i++ {
		actor := h.callerUser
		if i%2 != 0 {
			actor = h.callerUser + "-other"
		}
		record, _, err := h.l3Store.CreateRun(t.Context(), CreateRunInput{IdempotencyKey: fmt.Sprintf("filter-%d", i), Actor: actor, Request: inlineRunRequest("#!/bin/sh\nexit 0\n")})
		if err != nil {
			t.Fatal(err)
		}
		state := contract.RunPending
		if i%3 == 0 {
			state = contract.RunFailed
		}
		if _, err := h.l3Store.db.Exec(`UPDATE runs SET status=? WHERE run_id=?`, state, record.RunID); err != nil {
			t.Fatal(err)
		}
		if actor == h.callerUser && state == contract.RunPending {
			expected[record.RunID] = true
		}
	}
	// Independently applying each filter must not accidentally require the other.
	if page := readPagingTestPage(t, h, h.caller, "submitter=me&limit=500"); len(page.Runs) != 6 {
		t.Fatalf("mine returned %d rows", len(page.Runs))
	}
	if page := readPagingTestPage(t, h, h.caller, "status=pending&limit=500"); len(page.Runs) != 8 {
		t.Fatalf("status returned %d rows", len(page.Runs))
	}
	query := url.Values{"status": {"pending"}, "submitter": {"me"}, "limit": {"1"}}
	first := readPagingTestPage(t, h, h.caller, query.Encode())
	if first.NextCursor == "" {
		t.Fatal("filtered page lost its cursor")
	}
	seen := map[string]bool{}
	cursor := ""
	for {
		query.Set("cursor", cursor)
		page := readPagingTestPage(t, h, h.caller, query.Encode())
		for _, run := range page.Runs {
			if !expected[run.RunID] || seen[run.RunID] || run.Trigger.Principal != h.callerUser || run.Status != contract.RunPending {
				t.Fatalf("wrong filtered row: %+v", run)
			}
			seen[run.RunID] = true
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if len(seen) != len(expected) {
		t.Fatalf("filter returned %d, want %d", len(seen), len(expected))
	}
	// A child is submitted by its parent Run actor, not that parent's human actor.
	childRequest := inlineRunRequest("#!/bin/sh\nexit 0\n")
	childRequest.ParentRunID = first.Runs[0].RunID
	if _, _, err := h.l3Store.CreateRun(t.Context(), CreateRunInput{IdempotencyKey: "filter-child", Actor: "run:" + first.Runs[0].RunID, Request: childRequest}); err != nil {
		t.Fatal(err)
	}
	if page := readPagingTestPage(t, h, h.caller, "submitter=me&limit=500"); len(page.Runs) != 6 {
		t.Fatalf("mine included child: %+v", page)
	}
	originCursor := encodeComputerRunCursor(computerRunCursor{ComputerID: "any", CreatedNS: 1, RunID: "run"})
	status, _, body := h.do(h.caller, http.MethodGet, "/v1/runs?cursor="+url.QueryEscape(originCursor), nil, nil)
	if status != http.StatusBadRequest {
		t.Fatalf("origin cursor entered general listing: status=%d body=%s", status, body)
	}
	other := h.client(fabric.Identity{NodeID: "other-device", UserID: h.callerUser + "-other", Tags: []string{DefaultCallerPrincipalTag}}, DefaultL3Address)
	changed := []struct {
		client *http.Client
		query  string
	}{
		{h.caller, "limit=1&submitter=me&status=failed"},
		{h.caller, "limit=1&status=pending"},
		{other, "limit=1&submitter=me&status=pending"},
		{h.caller, "origin=computer:any&limit=1"},
	}
	for _, test := range changed {
		status, _, body := h.do(test.client, http.MethodGet, "/v1/runs?"+test.query+"&cursor="+url.QueryEscape(first.NextCursor), nil, nil)
		if status != http.StatusBadRequest {
			t.Fatalf("changed scope: status=%d body=%s", status, body)
		}
	}
	for _, query := range []string{"submitter=other", "submitter=", "submitter=me&submitter=me", "origin=computer:any&submitter=me", "cursor=" + base64.RawURLEncoding.EncodeToString([]byte(`{"created_ns":1,"run_id":"run"} {}`))} {
		status, _, body := h.do(h.caller, http.MethodGet, "/v1/runs?"+query, nil, nil)
		if status != http.StatusBadRequest {
			t.Fatalf("invalid query %s: status=%d body=%s", query, status, body)
		}
	}
	// A second device of the same submitting actor still sees exactly the same rows.
	same := h.client(fabric.Identity{NodeID: "second-device", UserID: h.callerUser, Tags: []string{DefaultCallerPrincipalTag}}, DefaultL3Address)
	if page := readPagingTestPage(t, h, same, "submitter=me&status=pending&limit=500"); len(page.Runs) != len(expected) {
		t.Fatalf("same actor got %d rows", len(page.Runs))
	}
	// With no user identity, actor derivation falls back to the node identity.
	node := h.client(fabric.Identity{NodeID: "node-actor", Tags: []string{DefaultCallerPrincipalTag}}, DefaultL3Address)
	record, _, err := h.l3Store.CreateRun(t.Context(), CreateRunInput{IdempotencyKey: "node-actor", Actor: "node-actor", Request: inlineRunRequest("#!/bin/sh\nexit 0\n")})
	if err != nil {
		t.Fatal(err)
	}
	if page := readPagingTestPage(t, h, node, "submitter=me"); len(page.Runs) != 1 || page.Runs[0].RunID != record.RunID {
		t.Fatalf("node actor page = %+v", page)
	}
	token, err := h.l3Store.ensureRunToken(t.Context(), record.RunID)
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"submitter=me", "cursor=" + url.QueryEscape(first.NextCursor)} {
		status, _, body := h.do(h.caller, http.MethodGet, "/v1/runs?"+query, nil, http.Header{"Authorization": {"Bearer " + token}})
		if status != http.StatusForbidden {
			t.Fatalf("run token enumerated: status=%d body=%s", status, body)
		}
	}
}
