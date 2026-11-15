package scripts

import (
	"testing"
)

// #1692: computer-seats runs on pull requests only from the lead's batch/*
// heads. Member PRs skip it; push to main and manual dispatch still run it.
// The realtiming half of #1692 is pinned in
// realtiming_superseded_trigger_contract_test.go.
func TestComputerSeatsRunsOnPullRequestsOnlyFromBatchHeads(t *testing.T) {
	workflow, _ := readWorkflow(t, "../.github/workflows/computer-seats.yml")
	if len(workflow.Jobs) == 0 {
		t.Fatal("computer-seats workflow has no jobs")
	}
	runs := func(name, event string, batchHead bool) bool {
		t.Helper()
		node, err := parseWorkflowGuard(workflow.Jobs[name].If)
		if err != nil {
			t.Fatalf("job %s: %v", name, err)
		}
		if node == nil {
			return true
		}
		got, ok := evaluateGuardFixture(t, node, map[string]any{
			"github.event_name":                     event,
			"github.ref":                            "refs/heads/main",
			"startsWith(github.head_ref, 'batch/')": batchHead,
		}).(bool)
		if !ok {
			t.Fatalf("job %s guard is not boolean", name)
		}
		return got
	}
	for name := range workflow.Jobs {
		if runs(name, "pull_request", false) {
			t.Errorf("job %s runs for a member pull request (head not batch/*); only batch heads and main carry this lane", name)
		}
	}
	for _, tc := range []struct {
		event     string
		batchHead bool
	}{
		{"pull_request", true},
		{"push", false},
		{"workflow_dispatch", false},
	} {
		if !runs("default-computer-seats", tc.event, tc.batchHead) {
			t.Errorf("default-computer-seats does not run for %s (batch head %v)", tc.event, tc.batchHead)
		}
	}
}
