package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Like #767's service-log tests, serve cutoff-shaped pages: the L1-only
// WEFTY_TEST_READ_PAGE_CUTOFF hook does not affect the cmd/wefty test binary.
// Keep the real CLI parser, L3 client, cursor query and output path in play.
func runLogPagingClients(t *testing.T, stub *cutoffStubLogServer) *apiClients {
	t.Helper()
	clients, _ := readRetryStub(t, func(_ int32, r *http.Request, w http.ResponseWriter) {
		if r.URL.Path == "/v1/runs/paging-run" {
			_ = json.NewEncoder(w).Encode(contract.RunRecord{Status: contract.RunSucceeded})
			return
		}
		limit := r.URL.Query().Get("limit")
		if r.URL.Path != "/v1/runs/paging-run/logs" || (limit != "200" && limit != "1") {
			t.Errorf("unexpected log request: %s", r.URL)
			http.NotFound(w, r)
			return
		}
		// Reuse the service fixture's one-row page and failure injection.
		query := r.URL.Query()
		if index, ok := stub.eventsFromCursor(query.Get("cursor")); ok && index == len(stub.events) {
			// Real L1 keeps next_cursor even on the empty terminal page.
			_ = json.NewEncoder(w).Encode(l1.LogPage{Events: []contract.LogEvent{}, NextCursor: query.Get("cursor")})
			return
		}
		query.Set("class", contract.JobClassService)
		r.URL.RawQuery = query.Encode()
		stub.ServeHTTP(w, r)
	})
	return clients
}

func runLogPagingEvents() []contract.LogEvent {
	return stubLogEvents([]struct{ attempt, contents string }{
		{"paging-attempt", "first event\n"},
		{"paging-attempt", "second event\n"},
		{"paging-attempt", "last event"},
	})
}

func TestRunLogsPaging(t *testing.T) {
	clients := runLogPagingClients(t, &cutoffStubLogServer{events: runLogPagingEvents()})
	read := func(jsonOutput bool, args ...string) (string, string) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		err := execute(t.Context(), clients, jsonOutput, append([]string{"logs", "paging-run"}, args...), &stdout, &stderr)
		if err != nil {
			t.Fatalf("logs %v: %v; stderr=%s", args, err, &stderr)
		}
		return stdout.String(), stderr.String()
	}
	firstJSON, jsonErr := read(true)
	var first l1.LogPage
	if err := json.Unmarshal([]byte(firstJSON), &first); err != nil || len(first.Events) != 1 || first.NextCursor != "1" || jsonErr != "" {
		t.Fatalf("default JSON page changed: %s stderr=%q error=%v", firstJSON, jsonErr, err)
	}
	secondJSON, _ := read(true, "--cursor", first.NextCursor)
	var second l1.LogPage
	if err := json.Unmarshal([]byte(secondJSON), &second); err != nil || len(second.Events) != 1 || string(second.Events[0].Bytes) != "second event\n" || second.NextCursor != "2" {
		t.Fatalf("cursor did not continue: %s error=%v", secondJSON, err)
	}
	out, notice := read(false)
	if out != "first event\n" || notice != "wefty: more log events exist; resume with --cursor 1 or use --all\n" {
		t.Fatalf("plain page/notice: stdout=%q stderr=%q", out, notice)
	}
	out, notice = read(false, "--cursor", "2")
	if out != "last event" || notice != "" {
		t.Fatalf("hint changed unterminated stdout: stdout=%q stderr=%q", out, notice)
	}
	out, notice = read(false, "--cursor", "3")
	if out != "" || notice != "" {
		t.Fatalf("empty page must be quiet: stdout=%q stderr=%q", out, notice)
	}
	out, notice = read(false, "--all")
	if out != "first event\nsecond event\nlast event" || notice != "" {
		t.Fatalf("--all must drain each event once: stdout=%q stderr=%q", out, notice)
	}
	out, notice = read(false, "--all", "--cursor", first.NextCursor)
	if out != "second event\nlast event" || notice != "" {
		t.Fatalf("--all must resume: stdout=%q stderr=%q", out, notice)
	}
	out, notice = read(true, "--all")
	var seen []string
	scanJSONEvents(t, []byte(out), func(event contract.LogEvent) { seen = append(seen, string(event.Bytes)) })
	if !equalStrings(seen, []string{"first event\n", "second event\n", "last event"}) || notice != "" {
		t.Fatalf("--all JSON events=%q stderr=%q", seen, notice)
	}
	out, notice = read(false, "--follow", "--cursor", first.NextCursor, "--poll-interval", "1ms")
	if out != "second event\nlast event" || notice != "" {
		t.Fatalf("--follow must start at cursor and drain: stdout=%q stderr=%q", out, notice)
	}
}

func TestRunLogsPagingStderrNotice(t *testing.T) {
	events := runLogPagingEvents()[:2]
	events[0].Stream = contract.LogStderr
	events[0].Bytes = []byte("stderr without newline")
	clients := runLogPagingClients(t, &cutoffStubLogServer{events: events})
	var stdout, stderr bytes.Buffer
	if err := execute(t.Context(), clients, false, []string{"logs", "paging-run"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "stderr without newline\nwefty:") {
		t.Fatalf("notice must have its own stderr line: stdout=%q stderr=%q", &stdout, &stderr)
	}
}

func TestRunLogsPlainPageNotice(t *testing.T) {
	fallback := "wefty: log read may be partial; resume with --cursor next-page or use --all for every retained event\n"
	for _, test := range []struct {
		name        string
		status      contract.RunState
		more        bool
		probeError  bool
		statusError bool
		wantNotice  string
	}{
		{name: "whole log terminal succeeded", status: contract.RunSucceeded},
		{name: "whole log terminal failed", status: contract.RunFailed},
		{name: "more events", more: true, wantNotice: "wefty: more log events exist; resume with --cursor next-page or use --all\n"},
		{name: "still running", status: contract.RunRunning, wantNotice: "wefty: run is still running; more log events may follow; use --follow --cursor next-page\n"},
		{name: "still queued", status: contract.RunQueued, wantNotice: "wefty: run is still queued; more log events may follow; use --follow --cursor next-page\n"},
		{name: "probe error", probeError: true, wantNotice: fallback},
		{name: "status error", statusError: true, wantNotice: fallback},
	} {
		t.Run(test.name, func(t *testing.T) {
			clients, requests := readRetryStub(t, func(seen int32, r *http.Request, w http.ResponseWriter) {
				switch seen {
				case 1:
					if r.URL.Path != "/v1/runs/paging-run/logs" || r.URL.Query().Get("cursor") != "start-page" || r.URL.Query().Get("limit") != "200" {
						t.Errorf("unexpected page request: %s", r.URL)
					}
					_ = json.NewEncoder(w).Encode(l1.LogPage{Events: runLogPagingEvents()[:1], NextCursor: "next-page"})
				case 2:
					if r.URL.Path != "/v1/runs/paging-run/logs" || r.URL.Query().Get("cursor") != "next-page" || r.URL.Query().Get("limit") != "1" {
						t.Errorf("unexpected probe request: %s", r.URL)
					}
					if test.probeError {
						writeStubAPIError(w, http.StatusServiceUnavailable, contract.ErrorUnavailable, "probe failed", true)
						return
					}
					page := l1.LogPage{NextCursor: "after-probe"}
					if test.more {
						page.Events = runLogPagingEvents()[1:2]
					}
					_ = json.NewEncoder(w).Encode(page)
				case 3:
					if test.more || test.probeError || r.URL.Path != "/v1/runs/paging-run" {
						t.Errorf("unexpected status request: %s", r.URL)
					}
					if test.statusError {
						writeStubAPIError(w, http.StatusServiceUnavailable, contract.ErrorUnavailable, "status failed", true)
						return
					}
					_ = json.NewEncoder(w).Encode(contract.RunRecord{Status: test.status})
				default:
					t.Errorf("unexpected extra request %d: %s", seen, r.URL)
					http.NotFound(w, r)
				}
			})
			var stdout, stderr bytes.Buffer
			err := execute(t.Context(), clients, false, []string{"logs", "paging-run", "--cursor", "start-page"}, &stdout, &stderr)
			// main exits 0 when execute returns nil, including probe failures.
			if err != nil || stdout.String() != "first event\n" || stderr.String() != test.wantNotice {
				t.Fatalf("plain read: error=%v stdout=%q stderr=%q; want stderr=%q", err, &stdout, &stderr, test.wantNotice)
			}
			wantRequests := int32(3)
			if test.more || test.probeError {
				wantRequests = 2
			}
			if requests.Load() != wantRequests {
				t.Fatalf("requests=%d, want %d", requests.Load(), wantRequests)
			}
		})
	}
}

func TestRunLogsAllStopsAtWalkPageCap(t *testing.T) {
	clients := runLogPagingClients(t, &cutoffStubLogServer{events: runLogPagingEvents()})
	savedCap := logWalkPageCap
	logWalkPageCap = 2
	defer func() { logWalkPageCap = savedCap }()
	for _, jsonOutput := range []bool{false, true} {
		var stdout, stderr bytes.Buffer
		err := execute(t.Context(), clients, jsonOutput, []string{"logs", "paging-run", "--all"}, &stdout, &stderr)
		if err == nil || !strings.Contains(err.Error(), "stopped after 2 pages") || !strings.Contains(err.Error(), "--all --cursor 2") {
			t.Fatalf("cap must fail with resume cursor: %v", err)
		}
		if strings.Contains(stderr.String(), "use --all") || strings.Contains(err.Error(), "use --all") || strings.Contains(stdout.String(), "CURSOR") {
			t.Fatalf("cap must not suggest use --all: stdout=%q stderr=%q error=%v", &stdout, &stderr, err)
		}
		if !jsonOutput && stdout.String() != "first event\nsecond event\n" {
			t.Fatalf("walk ran past cap: %q", &stdout)
		}
	}
}

func TestRunLogsAllRetryBound(t *testing.T) {
	for _, failures := range []int{2, logWalkRetryLimit + 1} {
		stub := &cutoffStubLogServer{events: runLogPagingEvents(), retryableFailures: map[int]int{1: failures}}
		clients := runLogPagingClients(t, stub)
		var stdout, stderr bytes.Buffer
		err := execute(t.Context(), clients, false, []string{"logs", "paging-run", "--all", "--poll-interval", "1ms"}, &stdout, &stderr)
		if failures == 2 {
			if err != nil || stdout.String() != "first event\nsecond event\nlast event" {
				t.Fatalf("retry must resume without duplication: %v stdout=%q", err, &stdout)
			}
		} else if !isRetryableL1Answer(err) || stdout.String() != "first event\n" {
			t.Fatalf("retry bound must return last error: %v stdout=%q", err, &stdout)
		}
		if stub.servedRetryableFailures() != failures {
			t.Fatalf("served %d failures, want %d", stub.servedRetryableFailures(), failures)
		}
	}
}

func TestRunLogsAllFollowUsageError(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := execute(t.Context(), nil, false, []string{"logs", "paging-run", "--all", "--follow"}, &stdout, &stderr)
	var usage usageError
	if !errors.As(err, &usage) || !strings.Contains(err.Error(), "cannot follow") {
		t.Fatalf("--all --follow must fail before network access: %v", err)
	}
}
