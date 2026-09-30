package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

func taggedNode(nodeID string, state contract.NodeState, tags ...string) l1.Node {
	node := readyNode(nodeID)
	node.State = state
	node.AuthoritativeTags = tags
	return node
}

func TestRoutingWarningNamesARunNothingCanTake(t *testing.T) {
	t.Parallel()

	nodes := []l1.Node{
		taggedNode("node-a", contract.NodeAlive, "mac", "wefty:node:node-a"),
		taggedNode("node-dead", contract.NodeDead, "linux"),
	}
	tests := map[string]struct {
		kind string
		tags []string
		warn bool
	}{
		"matching tags":            {kind: contract.JobKindProcess, tags: []string{"mac"}},
		"no tags":                  {kind: contract.JobKindProcess},
		"tags compare normalized":  {kind: contract.JobKindProcess, tags: []string{" MAC "}},
		"a tag no node carries":    {kind: contract.JobKindProcess, tags: []string{"no-such-tag"}, warn: true},
		"only a dead node matches": {kind: contract.JobKindProcess, tags: []string{"linux"}, warn: true},
		"half the tags":            {kind: contract.JobKindProcess, tags: []string{"mac", "linux"}, warn: true},
	}
	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			warning := routingWarningFor(nodes, test.kind, test.tags)
			if (warning != "") != test.warn {
				t.Fatalf("warning = %q, want warn=%t", warning, test.warn)
			}
		})
	}
	processOnly := readyNode("node-p")
	processOnly.Capabilities = map[string]bool{"kind:process": true}
	if routingWarningFor([]l1.Node{processOnly}, contract.JobKindOCI, nil) == "" {
		t.Fatal("an OCI run on a process-only fleet was not warned about")
	}
}

// TestSubmitWarnsButAcceptsAnUnroutableRun is #604 item 2: the run is still
// accepted, and the warning reaches stderr and the JSON document.
func TestSubmitWarnsButAcceptsAnUnroutableRun(t *testing.T) {
	t.Parallel()

	clients := ledgerStub(t, map[string]any{
		"/v1/runs":  l3.RunAccepted{RunID: "run-new", StatusURL: "/v1/runs/run-new", LogsURL: "/v1/runs/run-new/logs"},
		"/v1/nodes": l1.NodeList{Nodes: []l1.Node{taggedNode("node-a", contract.NodeAlive, "mac")}},
	})
	script := t.TempDir() + "/job.sh"
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if err := executeSubmit(t.Context(), clients, true,
		[]string{"--script=" + script, "--tag=no-such-tag"}, &out, &errOut); err != nil {
		t.Fatalf("an unroutable run was refused: %v", err)
	}
	if !strings.Contains(errOut.String(), "wefty: warning: no alive node can run kind=process with tags no-such-tag") {
		t.Fatalf("stderr = %q", errOut.String())
	}
	var accepted acceptedOutput
	if err := json.Unmarshal(out.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}
	if accepted.RunID != "run-new" || len(accepted.Warnings) != 1 {
		t.Fatalf("submit --json = %s", out.String())
	}

	out.Reset()
	errOut.Reset()
	if err := executeSubmit(t.Context(), clients, true,
		[]string{"--script=" + script, "--tag=mac"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if errOut.Len() != 0 || strings.Contains(out.String(), "warnings") {
		t.Fatalf("a routable run was warned about: %q %s", errOut.String(), out.String())
	}
}

const noEligibleNode = "no tag-eligible node advertises required capabilities: kind:process"

func queuedStub(t *testing.T) *apiClients {
	t.Helper()
	created := time.Now().UTC().Add(-2 * time.Minute)
	return ledgerStub(t, map[string]any{
		"/v1/runs": l3.RunListPage{Runs: []l3.RunSummary{
			{RunID: "run-q", Status: contract.RunQueued, Trigger: contract.Trigger{Type: "manual"}, CreatedAt: created},
		}},
		"/v1/runs/run-q":      contract.RunRecord{RunID: "run-q", Status: contract.RunQueued},
		"/v1/runs/run-q/logs": l1.LogPage{},
		"/v1/runs/run-q/execution": l3.RunExecution{RunID: "run-q", L1JobID: "job-q",
			Job: &l1.Job{JobID: "job-q", State: contract.JobQueued, Status: "unschedulable", UnschedulableReason: noEligibleNode}},
		"/v1/nodes":  l1.NodeList{Nodes: []l1.Node{readyNode("node-a")}},
		"/v1/whoami": l1.AuthenticatedPerson{UserID: "alice"},
	})
}

func TestRunsListShowsAQueuedRunNoNodeCanTake(t *testing.T) {
	t.Parallel()

	clients := queuedStub(t)
	var out bytes.Buffer
	if err := executeRuns(t.Context(), clients, false, []string{"list"}, &out, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"queued (no eligible node)", "run-q: " + noEligibleNode, "2m0s", "DURATION"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("runs list is missing %q:\n%s", want, out.String())
		}
	}
	out.Reset()
	if err := executeRuns(t.Context(), clients, true, []string{"list"}, &out, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var page runListingPage
	if err := json.Unmarshal(out.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Runs) != 1 || page.Runs[0].UnschedulableReason != noEligibleNode || page.Runs[0].RunID != "run-q" {
		t.Fatalf("runs list --json = %s", out.String())
	}
}

func TestStatusNotesAQueuedRunNoNodeCanTake(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	if err := executeStatus(t.Context(), queuedStub(t), true, nil, &out, &bytes.Buffer{}); err != nil {
		t.Fatalf("a stuck run made the cluster not ready: %v", err)
	}
	var status clusterStatus
	if err := json.Unmarshal(out.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, limitation := range status.Limitations {
		if limitation.Code == "queued_run_unschedulable" && strings.Contains(limitation.Detail, "run-q") {
			found = true
		}
	}
	if !status.Ready || !found {
		t.Fatalf("status = %s", out.String())
	}
}

// TestStatusSaysDrainingNotDead is #604 item 6: a stopping agent is draining,
// and "no node is alive" sent people to start a replacement that fails.
func TestStatusSaysDrainingNotDead(t *testing.T) {
	t.Parallel()

	h := newStatusHarness(t)
	h.nodes = l1.NodeList{Nodes: []l1.Node{taggedNode("node-a", contract.NodeDraining)}}
	var out bytes.Buffer
	err := executeStatus(t.Context(), h.clients(), true, nil, &out, &bytes.Buffer{})
	if code := commandExitCode(err); code != exitNotReady {
		t.Fatalf("exit %d, want %d", code, exitNotReady)
	}
	var status clusterStatus
	if err := json.Unmarshal(out.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if len(status.Reasons) != 1 || status.Reasons[0].Code != "nodes_draining" ||
		!strings.Contains(status.Verdict, "draining") || !strings.Contains(status.Verdict, "node-a") {
		t.Fatalf("status = %s", out.String())
	}
}

// TestFollowingAQueuedRunSaysItIsWaiting: silence and then "context
// canceled" was all a follower of a never-started run saw (#604).
//
// Not parallel: it shortens the notice grace, a package variable, and Go runs
// every parallel test only after the serial ones have returned.
func TestFollowingAQueuedRunSaysItIsWaiting(t *testing.T) {
	previous := followWaitingNoticeAfter
	followWaitingNoticeAfter = 100 * time.Millisecond
	t.Cleanup(func() { followWaitingNoticeAfter = previous })

	ctx, cancel := context.WithTimeout(t.Context(), 400*time.Millisecond)
	defer cancel()
	var out, errOut bytes.Buffer
	err := executeLogs(ctx, queuedStub(t), false, []string{"run-q", "--follow", "--poll-interval=50ms"}, &out, &errOut)
	if err == nil {
		t.Fatal("an interrupted follow reported success")
	}
	if strings.Contains(err.Error(), "context") || !strings.Contains(err.Error(), "stopped following run run-q while it was queued") {
		t.Fatalf("error = %q", err.Error())
	}
	if strings.Count(errOut.String(), "waiting for a node") != 1 ||
		!strings.Contains(errOut.String(), "no eligible node: "+noEligibleNode) {
		t.Fatalf("stderr = %q", errOut.String())
	}
}
