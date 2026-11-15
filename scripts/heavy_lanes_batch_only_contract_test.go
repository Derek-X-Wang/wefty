package scripts

import (
	"os"
	"slices"
	"testing"

	"gopkg.in/yaml.v3"
)

// #1692: computer-seats runs on pull requests only from the lead's batch/*
// heads or with the full-ci label. Member PRs, and runs started by adding any
// other label, skip it; push to main and manual dispatch still run it.
// The realtiming half of #1692 is pinned in
// realtiming_superseded_trigger_contract_test.go.
func TestComputerSeatsRunsOnPullRequestsOnlyFromBatchHeads(t *testing.T) {
	workflow, _ := readWorkflow(t, "../.github/workflows/computer-seats.yml")
	if len(workflow.Jobs) == 0 {
		t.Fatal("computer-seats workflow has no jobs")
	}
	runs := func(name, event string, pr prTrigger) bool {
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
			"startsWith(github.head_ref, 'batch/')": pr.batchHead,
			"contains(github.event.pull_request.labels.*.name, 'full-ci')": pr.fullCI,
			"github.event.action":     pr.action,
			"github.event.label.name": pr.label,
		}).(bool)
		if !ok {
			t.Fatalf("job %s guard is not boolean", name)
		}
		return got
	}
	for _, pr := range memberPullRequestTriggers() {
		for name := range workflow.Jobs {
			if runs(name, "pull_request", pr) {
				t.Errorf("job %s runs for member pull request trigger %+v; only batch heads, full-ci PRs and main carry this lane", name, pr)
			}
		}
	}
	for _, tc := range []struct {
		event string
		pr    prTrigger
	}{
		{"pull_request", prTrigger{batchHead: true, action: "synchronize"}},
		{"pull_request", prTrigger{fullCI: true, action: "synchronize"}},
		{"pull_request", prTrigger{fullCI: true, action: "labeled", label: "full-ci"}},
		{"push", prTrigger{}},
		{"workflow_dispatch", prTrigger{}},
	} {
		if !runs("default-computer-seats", tc.event, tc.pr) {
			t.Errorf("default-computer-seats does not run for %s %+v", tc.event, tc.pr)
		}
	}
}

// #1692: adding the full-ci label must start a run, so both heavy lanes listen
// for labeled. In computer-seats, a full-ci labeled run joins the PR's group
// and replaces its skipped run. Any other label gets its own group, so it
// cannot cancel a real run.
func TestHeavyLanesStartOnFullCILabelWithoutCancellingRealRuns(t *testing.T) {
	for _, path := range []string{"../.github/workflows/service-acceptance-realtiming.yml", "../.github/workflows/computer-seats.yml"} {
		payload, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var workflow struct {
			On struct {
				PullRequest struct {
					Types []string `yaml:"types"`
				} `yaml:"pull_request"`
			} `yaml:"on"`
			Concurrency struct {
				Group string `yaml:"group"`
			} `yaml:"concurrency"`
		}
		if err := yaml.Unmarshal(payload, &workflow); err != nil {
			t.Fatal(err)
		}
		for _, action := range []string{"opened", "synchronize", "reopened", "labeled"} {
			if !slices.Contains(workflow.On.PullRequest.Types, action) {
				t.Errorf("%s pull_request types %v lack %s", path, workflow.On.PullRequest.Types, action)
			}
		}
		if path != "../.github/workflows/computer-seats.yml" {
			continue // realtiming's group is pinned in realtiming_superseded_concurrency_contract_test.go
		}
		group := func(action, label, runID string) string {
			return renderConcurrencyGroup(t, workflow.Concurrency.Group, map[string]any{
				"github.event.action":              action,
				"github.event.label.name":          label,
				"github.event.pull_request.number": 1692,
				"github.ref":                       "refs/pull/1692/merge",
				"github.run_id":                    runID,
			})
		}
		real := group("synchronize", "", "1")
		if real != "computer-seats-1692" || group("labeled", "full-ci", "2") != real {
			t.Errorf("real or full-ci runs left the per-PR group: %q, %q", real, group("labeled", "full-ci", "2"))
		}
		if a, b := group("labeled", "needs-triage", "3"), group("labeled", "needs-triage", "4"); a == real || a == b {
			t.Errorf("other-label runs got groups %q and %q; they would cancel the real run %q", a, b, real)
		}
	}
}
