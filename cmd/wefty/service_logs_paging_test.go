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
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

// #767: a service log read is one page of an adaptive page sequence. Without
// --follow the CLI can now name the next cursor (--cursor in, NEXT CURSOR out)
// or drain the whole retained log (--all), the way `services list` and
// `runs list` page. An empty poll is the end of the log; L1 answers pages with
// next_cursor.

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

	// A follow may start where a previous page ended instead of repeating
	// history. This is the chosen --cursor + --follow rule: the follow
	// loop already advances cursors, so it can sensibly start from one.
	followOutput := runServiceCLI(t, ctx, harness.clients, true,
		"services", "logs", created.JobID, "--cursor", first.NextCursor,
		"--follow", "--follow-for", "30ms", "--poll-interval", "2ms")
	var followed []string
	scanJSONEvents(t, followOutput, func(event contract.LogEvent) {
		followed = append(followed, string(event.Bytes))
	})
	if !equalStrings(followed, []string{"second event\n", "third event\n"}) {
		t.Fatalf("follow from cursor events = %q, want exactly the events past the cursor", followed)
	}

	// Invalid combinations stay usage errors.
	for _, args := range [][]string{
		{"--all", "--cursor", first.NextCursor},
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

// cutoffStubLogServer serves the adaptive log-page shape the read cutoff
// produces: one event per page even when the caller asks for more, a next
// cursor on every page, and an empty page once the walk reaches the end.
// WEFTY_TEST_READ_PAGE_CUTOFF is read by l1's TestMain only, so it does not
// apply here; this stub reproduces that page shape without it.
type cutoffStubLogServer struct {
	events []contract.LogEvent
}

func (stub *cutoffStubLogServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if !strings.HasSuffix(request.URL.Path, "/logs") {
		http.NotFound(writer, request)
		return
	}
	cursor := request.URL.Query().Get("cursor")
	index := 0
	if cursor != "" {
		parsed, err := strconv.Atoi(cursor)
		if err != nil {
			http.Error(writer, `{"error":{"code":"invalid_request","message":"cursor is invalid"}}`, http.StatusBadRequest)
			return
		}
		index = parsed
	}
	if request.URL.Query().Get("class") != contract.JobClassService {
		http.Error(writer, `{"error":{"code":"invalid_request","message":"class is required"}}`, http.StatusBadRequest)
		return
	}
	page := l1.LogPage{Events: []contract.LogEvent{}}
	if index < len(stub.events) {
		page.Events = append(page.Events, stub.events[index])
		page.NextCursor = strconv.Itoa(index + 1)
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(page)
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

func TestServiceLogsAllWalksCutoffShapedPages(t *testing.T) {
	stub := &cutoffStubLogServer{}
	attemptA := "stub-attempt-a"
	attemptB := "stub-attempt-b"
	specs := []struct {
		attempt  string
		contents string
	}{
		{attemptA, "stub event 0\n"},
		{attemptA, "stub event 1\n"},
		{attemptB, "stub event 2\n"},
		{attemptB, "stub event 3\n"},
		{attemptB, "stub event 4\n"},
	}
	for index, spec := range specs {
		stub.events = append(stub.events, contract.LogEvent{
			AttemptID: spec.attempt, Stream: contract.LogStdout, Sequence: uint64(index),
			Timestamp: time.Now().UTC(), Bytes: []byte(spec.contents),
		})
	}
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
	clients := &apiClients{
		l1:     newAPIClient("L1", "l1", calling, listener.Addr().String()),
		wait:   waitForContext,
		images: newRegistryResolver(nil),
	}
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
		"stub event 0\n", "stub event 1\n", "stub event 2\n", "stub event 3\n", "stub event 4\n",
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
}

// TestServiceLogsPagingUsageErrors keeps the invalid combinations out of the
// network path entirely. Usage exits already have real-binary typed-exit
// coverage; no new exit code appears in #767.
func TestServiceLogsPagingUsageErrors(t *testing.T) {
	for _, args := range [][]string{
		{"job", "--all", "--cursor", "opaque"},
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
