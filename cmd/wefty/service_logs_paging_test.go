package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

// #767: a service log read is one page of an adaptive page sequence. Without
// --follow the CLI can continue from a cursor (--cursor, NEXT CURSOR out) or
// drain the retained log (--all). L1 answers every log page with next_cursor,
// so for logs an empty page, not a missing cursor, is the end.

func appendServiceLogEvent(
	t *testing.T, store *l1.Store, identityNodeID, jobID string,
	lease l1.AttemptLease, sequence uint64, contents string,
) {
	t.Helper()
	_, err := store.AppendLogs(context.Background(), identityNodeID, jobID, lease.AttemptID, l1.AppendLogsRequest{
		FencingToken: lease.FencingToken,
		Events: []contract.LogEvent{{
			AttemptID: lease.AttemptID, Stream: contract.LogStdout, Sequence: sequence,
			Timestamp: time.Now().UTC(), Bytes: []byte(contents),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestServiceLogsPagedReadsFromRealL1(t *testing.T) {
	harness := newServiceCLIHarness(t)
	ctx := context.Background()
	scriptPath := filepath.Join(t.TempDir(), "paging.sh")
	if err := os.WriteFile(scriptPath, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	createdOutput := runServiceCLI(t, ctx, harness.clients, true,
		"services", "create", "--script", scriptPath, "--tag", "log-paging")
	var created struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(createdOutput, &created); err != nil {
		t.Fatal(err)
	}
	nodeIdentity := fabric.Identity{NodeID: "log-paging-node"}
	node, err := harness.store.RegisterNode(ctx, nodeIdentity, contract.NodeRegistration{
		NodeID: "log-paging-service", BootSessionID: "boot-log-paging", RootInstanceID: "root-log-paging",
		OS: "linux", Architecture: "amd64", AgentVersion: "log-paging-test",
		Capabilities: map[string]bool{"kind:process": true},
	}, l1.NodePolicy{Tags: []string{"log-paging"}, MaxOneshotSlots: 4, MaxServiceSlots: 2}, true)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := harness.store.ClaimJob(ctx, nodeIdentity.NodeID, node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil || claim.Job.JobID != created.JobID {
		t.Fatalf("claim service for log paging = %#v, %v", claim, err)
	}
	appendServiceLogEvent(t, harness.store, nodeIdentity.NodeID, created.JobID, claim.Lease, 0, "first event\n")
	appendServiceLogEvent(t, harness.store, nodeIdentity.NodeID, created.JobID, claim.Lease, 1, "second event\n")
	appendServiceLogEvent(t, harness.store, nodeIdentity.NodeID, created.JobID, claim.Lease, 2, "third event\n")

	type page struct {
		Events     []contract.LogEvent `json:"events"`
		NextCursor string              `json:"next_cursor"`
	}
	readPage := func(args ...string) page {
		t.Helper()
		output := runServiceCLI(t, ctx, harness.clients, true, append([]string{"services", "logs", created.JobID}, args...)...)
		var decoded page
		if err := json.Unmarshal(output, &decoded); err != nil {
			t.Fatalf("log page %v is not the page object: %v\n%s", args, err, output)
		}
		return decoded
	}

	// The full one-page read is unchanged: every event fits, and the page
	// object still carries next_cursor.
	full := readPage()
	if len(full.Events) != 3 || full.NextCursor == "" {
		t.Fatalf("default page = %+v", full)
	}

	// --limit=1 makes the real store return one event per page with a
	// continuation cursor, the shape the adaptive read cutoff produces.
	first := readPage("--limit=1")
	if len(first.Events) != 1 || string(first.Events[0].Bytes) != "first event\n" || first.NextCursor == "" {
		t.Fatalf("first one-event page = %+v", first)
	}
	humanFirst := runServiceCLI(t, ctx, harness.clients, false, "services", "logs", created.JobID, "--limit=1")
	for _, want := range []string{"--- attempt " + claim.Lease.AttemptID + " ---", "first event", "NEXT CURSOR"} {
		if !bytes.Contains(humanFirst, []byte(want)) {
			t.Fatalf("human first page missing %q:\n%s", want, humanFirst)
		}
	}
	if bytes.Contains(humanFirst, []byte("second event")) || bytes.Contains(humanFirst, []byte("third event")) {
		t.Fatalf("human first page showed events past the page:\n%s", humanFirst)
	}

	second := readPage("--limit=1", "--cursor", first.NextCursor)
	if len(second.Events) != 1 || string(second.Events[0].Bytes) != "second event\n" || second.NextCursor == "" {
		t.Fatalf("cursor continuation page = %+v", second)
	}
	humanSecond := runServiceCLI(t, ctx, harness.clients, false, "services", "logs", created.JobID, "--limit=1", "--cursor", first.NextCursor)
	for _, want := range []string{"--- attempt " + claim.Lease.AttemptID + " ---", "second event", "NEXT CURSOR"} {
		if !bytes.Contains(humanSecond, []byte(want)) {
			t.Fatalf("human continuation page missing %q:\n%s", want, humanSecond)
		}
	}

	// An empty page ends the log: L1 still answers it with next_cursor,
	// but there is nothing to show, so no NEXT CURSOR hint may print.
	third := readPage("--limit=1", "--cursor", second.NextCursor)
	if len(third.Events) != 1 || string(third.Events[0].Bytes) != "third event\n" {
		t.Fatalf("final event page = %+v", third)
	}
	emptyPage := readPage("--limit=1", "--cursor", third.NextCursor)
	if len(emptyPage.Events) != 0 {
		t.Fatalf("past-the-end page = %+v", emptyPage)
	}
	var emptyOut, emptyErr bytes.Buffer
	if err := execute(ctx, harness.clients, false,
		[]string{"services", "logs", created.JobID, "--limit=1", "--cursor", third.NextCursor},
		&emptyOut, &emptyErr); err != nil {
		t.Fatalf("empty-page human read = %v, stderr=%s", err, emptyErr.String())
	}
	if bytes.Contains(emptyOut.Bytes(), []byte("NEXT CURSOR")) || emptyOut.Len() != 0 {
		t.Fatalf("empty page printed a hint or noise:\nstdout=%q", emptyOut.String())
	}

	// --all drains the retained log one page at a time and ends at the
	// empty poll.
	allHuman := runServiceCLI(t, ctx, harness.clients, false, "services", "logs", created.JobID, "--all", "--limit=1")
	for _, want := range []string{"first event", "second event", "third event"} {
		if !bytes.Contains(allHuman, []byte(want)) {
			t.Fatalf("--all human read missing %q:\n%s", want, allHuman)
		}
	}
	if bytes.Contains(allHuman, []byte("NEXT CURSOR")) {
		t.Fatalf("--all drained the log yet advertised a cursor:\n%s", allHuman)
	}
	allJSONOutput := runServiceCLI(t, ctx, harness.clients, true, "services", "logs", created.JobID, "--all", "--limit=1")
	var seen []string
	scanJSONEvents(t, allJSONOutput, func(event contract.LogEvent) {
		seen = append(seen, string(event.Bytes))
	})
	if !equalStrings(seen, []string{"first event\n", "second event\n", "third event\n"}) {
		t.Fatalf("--all --json events = %q", seen)
	}

	// --all may start at a cursor: it walks the remaining pages to the
	// end (the `--all` convention of `nodes`, `computers` and `runs list`).
	tailHuman := runServiceCLI(t, ctx, harness.clients, false,
		"services", "logs", created.JobID, "--all", "--limit=1", "--cursor", first.NextCursor)
	for _, want := range []string{"second event", "third event"} {
		if !bytes.Contains(tailHuman, []byte(want)) {
			t.Fatalf("--all from cursor missing %q:\n%s", want, tailHuman)
		}
	}
	if bytes.Contains(tailHuman, []byte("first event")) {
		t.Fatalf("--all from cursor replayed pages before the cursor:\n%s", tailHuman)
	}
	if bytes.Contains(tailHuman, []byte("NEXT CURSOR")) {
		t.Fatalf("--all from cursor drained the log yet advertised a cursor:\n%s", tailHuman)
	}

	// A follow may start where a previous page ended instead of repeating
	// history: the follow loop already advances cursors, so it can
	// sensibly start from one. There is no --follow-for window; the test
	// cancels once the expected events are seen, under a watchdog.
	followCtx, cancelFollow := context.WithTimeout(ctx, 10*time.Second)
	defer cancelFollow()
	stdout := &markerOutput{marker: "third event", done: make(chan struct{})}
	var followErrOutput bytes.Buffer
	res := make(chan error, 1)
	go func() {
		res <- execute(followCtx, harness.clients, false, []string{
			"services", "logs", created.JobID, "--cursor", first.NextCursor,
			"--follow", "--poll-interval", "2ms",
		}, stdout, &followErrOutput)
	}()
	select {
	case <-stdout.done:
	case <-followCtx.Done():
		t.Fatalf("follow from cursor never saw the expected events within 10s:\nstdout=%s\nstderr=%s",
			stdout.String(), followErrOutput.String())
	}
	cancelFollow()
	if err := <-res; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("follow from cursor = %v, stderr=%s", err, followErrOutput.String())
	}
	followed := stdout.String()
	for _, want := range []string{"second event", "third event"} {
		if !bytes.Contains([]byte(followed), []byte(want)) {
			t.Fatalf("follow from cursor missing %q:\n%s", want, followed)
		}
	}
	if bytes.Contains([]byte(followed), []byte("first event")) {
		t.Fatalf("follow from cursor replayed the page it was told to resume past:\n%s", followed)
	}

	// Invalid combinations stay usage errors.
	for _, args := range [][]string{
		{"--all", "--follow"},
		{"--all", "--follow", "--follow-for", "5ms"},
	} {
		var stdout, stderr bytes.Buffer
		err := execute(ctx, harness.clients, true, append([]string{"services", "logs", created.JobID}, args...), &stdout, &stderr)
		var usage usageError
		if !errors.As(err, &usage) {
			t.Fatalf("services logs %v = %v (%T), want a usage error", args, err, err)
		}
	}
}

// markerOutput is a test writer that closes done the first time the expected
// marker is seen, so a follow can be stopped by its own evidence instead of a
// wall-clock window.
type markerOutput struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	marker  string
	done    chan struct{}
	marking bool
}

func (w *markerOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	n, err := w.buf.Write(p)
	hit := w.marker != "" && !w.marking && bytes.Contains(w.buf.Bytes(), []byte(w.marker))
	if hit {
		w.marking = true
	}
	w.mu.Unlock()
	if hit {
		close(w.done)
	}
	return n, err
}

func (w *markerOutput) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// cutoffStubLogServer serves the adaptive log-page shape the read cutoff
// produces: one event per page even when the caller asks for more, a next
// cursor on every page, and an empty page once the walk reaches the end.
// WEFTY_TEST_READ_PAGE_CUTOFF is read by l1's TestMain only, so it does not
// apply here; this stub reproduces that page shape without it. It can also
// fail chosen polls the way a retryable L1 answer fails, to test what a
// --all walk does mid-walk.
type cutoffStubLogServer struct {
	mu                 sync.Mutex
	events             []contract.LogEvent
	retryableFailures  map[int]int
	retryableServed    int
	nonRetryableCursor int
	nonRetryableOn     bool
	nonRetryableServed int
}

func (stub *cutoffStubLogServer) eventsFromCursor(cursor string) (index int, ok bool) {
	if cursor == "" {
		return 0, true
	}
	parsed, err := strconv.Atoi(cursor)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

func (stub *cutoffStubLogServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if !strings.HasSuffix(request.URL.Path, "/logs") {
		http.NotFound(writer, request)
		return
	}
	if request.URL.Query().Get("class") != contract.JobClassService {
		http.Error(writer, `{"error":{"code":"invalid_request","message":"class is required"}}`, http.StatusBadRequest)
		return
	}
	index, ok := stub.eventsFromCursor(request.URL.Query().Get("cursor"))
	if !ok {
		http.Error(writer, `{"error":{"code":"invalid_request","message":"cursor is invalid"}}`, http.StatusBadRequest)
		return
	}
	stub.mu.Lock()
	failRetryable := stub.retryableFailures[index] > 0
	if failRetryable {
		stub.retryableFailures[index]--
		stub.retryableServed++
	}
	failFinal := stub.nonRetryableOn && stub.nonRetryableCursor == index
	if failFinal {
		stub.nonRetryableServed++
	}
	stub.mu.Unlock()
	if failFinal {
		writeStubAPIError(writer, http.StatusInternalServerError,
			contract.ErrorInternal, "stub internal failure", false)
		return
	}
	if failRetryable {
		writeStubAPIError(writer, http.StatusServiceUnavailable,
			contract.ErrorUnavailable, "stub unavailable", true)
		return
	}
	page := l1.LogPage{Events: []contract.LogEvent{}}
	if index >= 0 && index < len(stub.events) {
		page.Events = append(page.Events, stub.events[index])
		page.NextCursor = strconv.Itoa(index + 1)
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(page)
}

func writeStubAPIError(writer http.ResponseWriter, status int, code contract.ErrorCode, message string, retryable bool) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(contract.ErrorResponse{Error: contract.APIError{
		Code: code, Message: message, Retryable: retryable,
	}})
}

func (stub *cutoffStubLogServer) servedRetryableFailures() int {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	return stub.retryableServed
}

func scanJSONEvents(t *testing.T, output []byte, visit func(contract.LogEvent)) {
	t.Helper()
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		var event contract.LogEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("expecting one log event per line: %v (%s)", err, line)
		}
		visit(event)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
}

// newStubServiceLogsClients serves a stub L1 log API in-process and returns
// the clients a `services logs` invocation talks to.
func newStubServiceLogsClients(t *testing.T, stub *cutoffStubLogServer) *apiClients {
	t.Helper()
	network := plain.NewNetwork()
	serving := network.NewFabric(fabric.Identity{NodeID: "log-stub-serving"})
	listener, err := serving.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: stub}
	go func() { _ = httpServer.Serve(listener) }()
	t.Cleanup(func() { _ = httpServer.Close() })
	calling := network.NewFabric(fabric.Identity{NodeID: "log-stub-calling"})
	return &apiClients{
		l1:     newAPIClient("L1", "l1", calling, listener.Addr().String()),
		wait:   waitForContext,
		images: newRegistryResolver(nil),
	}
}

func stubLogEvents(specs []struct {
	attempt  string
	contents string
}) []contract.LogEvent {
	events := make([]contract.LogEvent, 0, len(specs))
	for index, spec := range specs {
		events = append(events, contract.LogEvent{
			AttemptID: spec.attempt, Stream: contract.LogStdout, Sequence: uint64(index),
			Timestamp: time.Now().UTC(), Bytes: []byte(spec.contents),
		})
	}
	return events
}

func TestServiceLogsAllWalksCutoffShapedPages(t *testing.T) {
	stub := &cutoffStubLogServer{}
	attemptA := "stub-attempt-a"
	attemptB := "stub-attempt-b"
	// The last event has no trailing newline: the NEXT CURSOR hint that
	// follows it must not glue onto the log line.
	stub.events = stubLogEvents([]struct {
		attempt  string
		contents string
	}{
		{attemptA, "stub event 0\n"},
		{attemptA, "stub event 1\n"},
		{attemptB, "stub event 2\n"},
		{attemptB, "stub event 3\n"},
		{attemptB, "stub event 4"},
	})
	clients := newStubServiceLogsClients(t, stub)
	ctx := context.Background()

	// The default read is still one page: a stub page stops early despite
	// the default --limit, exactly as an adaptive page at the cutoff does.
	onePageOutput := runServiceCLI(t, ctx, clients, true, "services", "logs", "stub-job")
	var onePage l1.LogPage
	if err := json.Unmarshal(onePageOutput, &onePage); err != nil {
		t.Fatalf("stub one-page read is not a page object: %v\n%s", err, onePageOutput)
	}
	if len(onePage.Events) != 1 || onePage.NextCursor == "" {
		t.Fatalf("stub one-page read = %+v", onePage)
	}

	// --all walks the stub's one-event pages to the empty one, and the
	// attempt markers stay continuous across page boundaries.
	allHuman := runServiceCLI(t, ctx, clients, false, "services", "logs", "stub-job", "--all")
	for _, want := range []string{
		"--- attempt " + attemptA + " ---",
		"stub event 0", "stub event 1",
		"--- attempt " + attemptB + " ---",
		"stub event 2", "stub event 3", "stub event 4",
	} {
		if !bytes.Contains(allHuman, []byte(want)) {
			t.Fatalf("--all human walk missing %q:\n%s", want, allHuman)
		}
	}
	if bytes.Contains(allHuman, []byte("NEXT CURSOR")) {
		t.Fatalf("--all human walk advertised a cursor:\n%s", allHuman)
	}

	var stubSeen []string
	allJSONOutput := runServiceCLI(t, ctx, clients, true, "services", "logs", "stub-job", "--all")
	scanJSONEvents(t, allJSONOutput, func(event contract.LogEvent) {
		stubSeen = append(stubSeen, string(event.Bytes))
	})
	if !equalStrings(stubSeen, []string{
		"stub event 0\n", "stub event 1\n", "stub event 2\n", "stub event 3\n", "stub event 4",
	}) {
		t.Fatalf("--all --json events = %q", stubSeen)
	}

	// A continuation read resumes exactly after the page that handed over.
	continuation := runServiceCLI(t, ctx, clients, true,
		"services", "logs", "stub-job", "--cursor", "3")
	var continuationPage l1.LogPage
	if err := json.Unmarshal(continuation, &continuationPage); err != nil {
		t.Fatalf("stub continuation is not a page object: %v\n%s", err, continuation)
	}
	if len(continuationPage.Events) != 1 || string(continuationPage.Events[0].Bytes) != "stub event 3\n" {
		t.Fatalf("stub continuation = %+v", continuationPage.Events)
	}
	humanContinuation := runServiceCLI(t, ctx, clients, false, "services", "logs", "stub-job", "--cursor", "3")
	for _, want := range []string{"stub event 3", "NEXT CURSOR"} {
		if !bytes.Contains(humanContinuation, []byte(want)) {
			t.Fatalf("human continuation missing %q:\n%s", want, humanContinuation)
		}
	}

	// The last page has an event whose line does not end in a newline, so
	// the hint gets its own line in human output too.
	lastPageHuman := runServiceCLI(t, ctx, clients, false, "services", "logs", "stub-job", "--cursor", "4")
	if !bytes.Contains(lastPageHuman, []byte("\nNEXT CURSOR\t5")) {
		t.Fatalf("hint glued onto an unterminated log line or lost:\n%s", lastPageHuman)
	}
	if bytes.Contains(lastPageHuman, []byte("stub event 4NEXT CURSOR")) {
		t.Fatalf("hint glued onto the log line:\n%s", lastPageHuman)
	}

	// An empty page is the end: no events, no hint.
	var emptyOut, emptyErr bytes.Buffer
	if err := execute(ctx, clients, false,
		[]string{"services", "logs", "stub-job", "--cursor", "5"}, &emptyOut, &emptyErr); err != nil {
		t.Fatalf("empty-page human read = %v, stderr=%s", err, emptyErr.String())
	}
	if bytes.Contains(emptyOut.Bytes(), []byte("NEXT CURSOR")) || emptyOut.Len() != 0 {
		t.Fatalf("empty page printed a hint or noise:\nstdout=%q", emptyOut.String())
	}

	// --all accepts --cursor and walks the remaining pages to the end.
	tailHuman := runServiceCLI(t, ctx, clients, false,
		"services", "logs", "stub-job", "--all", "--cursor", "2")
	for _, want := range []string{"stub event 2", "stub event 3", "stub event 4"} {
		if !bytes.Contains(tailHuman, []byte(want)) {
			t.Fatalf("--all from cursor missing %q:\n%s", want, tailHuman)
		}
	}
	if bytes.Contains(tailHuman, []byte("stub event 0")) || bytes.Contains(tailHuman, []byte("stub event 1")) {
		t.Fatalf("--all from cursor replayed pages before the cursor:\n%s", tailHuman)
	}
	if bytes.Contains(tailHuman, []byte("NEXT CURSOR")) {
		t.Fatalf("--all from cursor drained the log yet advertised a cursor:\n%s", tailHuman)
	}
}

// TestServiceLogsAllStopsAtWalkPageCap bounds the walk: a log still growing
// after the cap's worth of pages stops the walk with the resume point, exit 0,
// hint on stdout in human mode and stderr in --json mode.
func TestServiceLogsAllStopsAtWalkPageCap(t *testing.T) {
	stub := &cutoffStubLogServer{}
	stub.events = stubLogEvents([]struct {
		attempt  string
		contents string
	}{
		{"stub-cap-a", "cap event 0\n"},
		{"stub-cap-a", "cap event 1\n"},
		{"stub-cap-a", "cap event 2\n"},
		{"stub-cap-b", "cap event 3\n"},
		{"stub-cap-b", "cap event 4\n"},
	})
	clients := newStubServiceLogsClients(t, stub)
	ctx := context.Background()
	savedCap := logWalkPageCap
	logWalkPageCap = 2
	defer func() { logWalkPageCap = savedCap }()

	var stdout, stderr bytes.Buffer
	// Reaching the cap is an incomplete read: it fails, naming the resume
	// cursor, rather than exiting as if the walk had reached the end.
	if err := execute(ctx, clients, false, []string{
		"services", "logs", "stub-job", "--all",
	}, &stdout, &stderr); err == nil || !strings.Contains(err.Error(), "--all --cursor 2") {
		t.Fatalf("capped --all = %v, want the incomplete-read error naming the cursor; stderr=%s", err, stderr.String())
	}
	for _, want := range []string{"cap event 0", "cap event 1", "NEXT CURSOR\t2"} {
		if !bytes.Contains(stdout.Bytes(), []byte(want)) {
			t.Fatalf("capped --all human stdout missing %q:\nstdout=%s", want, stdout.String())
		}
	}
	if bytes.Contains(stdout.Bytes(), []byte("cap event 2")) {
		t.Fatalf("capped --all human walked past the cap:\nstdout=%s", stdout.String())
	}

	var jsonStdout, jsonStderr bytes.Buffer
	if err := execute(ctx, clients, true, []string{
		"services", "logs", "stub-job", "--all",
	}, &jsonStdout, &jsonStderr); err == nil || !strings.Contains(err.Error(), "--all --cursor 2") {
		t.Fatalf("capped --all --json = %v, want the incomplete-read error; stderr=%s", err, jsonStderr.String())
	}
	var jsonSeen []string
	scanJSONEvents(t, jsonStdout.Bytes(), func(event contract.LogEvent) {
		jsonSeen = append(jsonSeen, string(event.Bytes))
	})
	if !equalStrings(jsonSeen, []string{"cap event 0\n", "cap event 1\n"}) {
		t.Fatalf("capped --all --json events = %q", jsonSeen)
	}
	if !bytes.Contains(jsonStderr.Bytes(), []byte("NEXT CURSOR\t2")) {
		t.Fatalf("capped --all --json stderr missing the resume hint:\nstderr=%s", jsonStderr.String())
	}
	if bytes.Contains(jsonStdout.Bytes(), []byte("NEXT CURSOR")) {
		t.Fatalf("capped --all --json put the hint on stdout:\nstdout=%s", jsonStdout.String())
	}

	// The printed hint resumes the walk to its end; restore the default
	// cap first so the remainder fits inside this one walk.
	logWalkPageCap = savedCap
	resume := runServiceCLI(t, ctx, clients, false,
		"services", "logs", "stub-job", "--all", "--cursor", "2")
	for _, want := range []string{"cap event 2", "cap event 3", "cap event 4"} {
		if !bytes.Contains(resume, []byte(want)) {
			t.Fatalf("resumed --all missing %q:\n%s", want, resume)
		}
	}
	if bytes.Contains(resume, []byte("NEXT CURSOR")) {
		t.Fatalf("resumed --all drained the log yet advertised a cursor:\n%s", resume)
	}
}

// TestServiceLogsAllRetriesRetryablePoll carries the #763 rule over to --all:
// a retryable 503 mid-walk is one bad poll, not a verdict. The walk retries at
// the poll interval and completes; a non-retryable answer ends it.
func TestServiceLogsAllRetriesRetryablePoll(t *testing.T) {
	stub := &cutoffStubLogServer{}
	stub.events = stubLogEvents([]struct {
		attempt  string
		contents string
	}{
		{"stub-retry-a", "retry event 0\n"},
		{"stub-retry-a", "retry event 1\n"},
		{"stub-retry-a", "retry event 2\n"},
		{"stub-retry-b", "retry event 3\n"},
		{"stub-retry-b", "retry event 4\n"},
	})
	// One bad poll before the second event's page and one before the
	// fourth's, so the walk hit retries in the middle, not only at the top.
	stub.retryableFailures = map[int]int{1: 2, 3: 1}
	clients := newStubServiceLogsClients(t, stub)
	ctx := context.Background()

	retryOutput := runServiceCLI(t, ctx, clients, false,
		"services", "logs", "stub-job", "--all", "--poll-interval", "2ms")
	for _, want := range []string{"retry event 0", "retry event 1", "retry event 2", "retry event 3", "retry event 4"} {
		if !bytes.Contains(retryOutput, []byte(want)) {
			t.Fatalf("--all after retryable polls missing %q:\n%s", want, retryOutput)
		}
	}
	if stub.servedRetryableFailures() != 3 {
		t.Fatalf("stub served %d retryable failures, want 3 (retries at cursors 1 and 3)", stub.servedRetryableFailures())
	}
	if bytes.Contains(retryOutput, []byte("NEXT CURSOR")) {
		t.Fatalf("--all after retries drained the log yet advertised a cursor:\n%s", retryOutput)
	}
}

func TestServiceLogsAllStopsOnNonRetryablePoll(t *testing.T) {
	stub := &cutoffStubLogServer{}
	stub.events = stubLogEvents([]struct {
		attempt  string
		contents string
	}{
		{"stub-fatal-a", "fatal event 0\n"},
		{"stub-fatal-a", "fatal event 1\n"},
		{"stub-fatal-a", "fatal event 2\n"},
	})
	stub.nonRetryableCursor = 2
	stub.nonRetryableOn = true
	clients := newStubServiceLogsClients(t, stub)
	ctx := context.Background()

	var stdout, stderr bytes.Buffer
	err := execute(ctx, clients, false, []string{
		"services", "logs", "stub-job", "--all", "--poll-interval", "2ms",
	}, &stdout, &stderr)
	var responseErr *apiResponseError
	if !errors.As(err, &responseErr) || responseErr.APIError.Code != contract.ErrorInternal || responseErr.APIError.Retryable {
		t.Fatalf("--all with a non-retryable mid-walk answer = %v (%T), want the L1 internal error", err, err)
	}
	for _, want := range []string{"fatal event 0", "fatal event 1"} {
		if !bytes.Contains(stdout.Bytes(), []byte(want)) {
			t.Fatalf("walk stopped before its earlier events:\nstdout=%s", stdout.String())
		}
	}
	if bytes.Contains(stdout.Bytes(), []byte("fatal event 2")) {
		t.Fatalf("walk printed events from the page that failed:\nstdout=%s", stdout.String())
	}
}

// TestServiceLogsPagingUsageErrors keeps the invalid combinations out of the
// network path entirely. Usage exits already have real-binary typed-exit
// coverage; no new exit code appears in #767.
func TestServiceLogsPagingUsageErrors(t *testing.T) {
	for _, args := range [][]string{
		{"job", "--all", "--follow"},
		{"job", "--all", "--follow", "--follow-for", "5ms"},
	} {
		var stdout, stderr bytes.Buffer
		err := execute(context.Background(), nil, false, append([]string{"services", "logs"}, args...), &stdout, &stderr)
		var usage usageError
		if !errors.As(err, &usage) {
			t.Fatalf("services logs %v = %v (%T), want a usage error", args, err, err)
		}
	}
}

// A one-shot --all walk does not retry forever: after logWalkRetryLimit
// consecutive retryable answers it reports the last one instead.
func TestServiceLogsAllGivesUpAfterRetryLimit(t *testing.T) {
	stub := &cutoffStubLogServer{}
	stub.events = stubLogEvents([]struct {
		attempt  string
		contents string
	}{
		{"stub-limit-a", "limit event 0\n"},
		{"stub-limit-a", "limit event 1\n"},
	})
	stub.retryableFailures = map[int]int{1: logWalkRetryLimit + 10}
	clients := newStubServiceLogsClients(t, stub)
	var stdout, stderr bytes.Buffer
	err := execute(context.Background(), clients, false, []string{
		"services", "logs", "stub-job", "--all", "--poll-interval", "1ms",
	}, &stdout, &stderr)
	if !isRetryableL1Answer(err) {
		t.Fatalf("--all under endless retryable answers = %v, want the last retryable answer", err)
	}
	if got := stub.servedRetryableFailures(); got != logWalkRetryLimit+1 {
		t.Fatalf("stub served %d retryable answers, want %d", got, logWalkRetryLimit+1)
	}
	if !bytes.Contains(stdout.Bytes(), []byte("limit event 0")) {
		t.Fatalf("--all lost the events read before the retries:\n%s", stdout.String())
	}
}
