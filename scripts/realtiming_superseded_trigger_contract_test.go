package scripts

import (
	"slices"
	"testing"
)

// #1603: a newer main push cancels the older acceptance-image run
// (cancel-in-progress). The workflow_run it still fires must skip every
// realtiming job, so the run concludes skipped: not success (#241), and not a
// red failure. Every other trigger conclusion keeps the fail-closed result gate.
// #1692: a member pull request (head not batch/*, no full-ci label) skips every
// job too, and so does a run started by adding any label other than full-ci. A
// batch/* or full-ci pull request keeps the fail-closed gate.
func TestRealtimingSupersededTriggerSkipsEveryJob(t *testing.T) {
	workflow, _ := readWorkflow(t, "../.github/workflows/service-acceptance-realtiming.yml")
	names := make([]string, 0, len(workflow.Jobs))
	for name := range workflow.Jobs {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, required := range []string{"resolve-published-artifact", "realtiming-result"} {
		if !slices.Contains(names, required) {
			t.Fatalf("realtiming workflow has no %s job", required)
		}
	}

	// values describes the trigger; a pull_request event has no
	// github.event.workflow_run, so its conclusion is '' or nil.
	values := func(event string, conclusion any, resolveResult string, pr prTrigger) map[string]any {
		return map[string]any{
			"startsWith(github.head_ref, 'batch/')":                        pr.batchHead,
			"contains(github.event.pull_request.labels.*.name, 'full-ci')": pr.fullCI,
			"github.event.action":                                          pr.action,
			"github.event.label.name":                                      pr.label,
			"always()":                                                     true,
			"cancelled()":                                                  false,
			"github.event_name":                                            event,
			"github.event.workflow_run.conclusion":                         conclusion,
			"github.event.workflow_run.event":                              "push",
			"github.event.workflow_run.head_branch":                        "main",
			"needs.resolve-published-artifact.result":                      resolveResult,
			"needs.resolve-published-artifact.outputs.available":           "",
			"vars.TSNET_SMOKE_REQUIRED":                                    "true",
			"vars.TSNET_CI_TESTER_REQUIRED":                                "true",
		}
	}
	runs := func(name string, fixture map[string]any) bool {
		t.Helper()
		node, err := parseWorkflowGuard(workflow.Jobs[name].If)
		if err != nil {
			t.Fatalf("job %s: %v", name, err)
		}
		if node == nil {
			return true
		}
		got, ok := evaluateGuardFixture(t, node, fixture).(bool)
		if !ok {
			t.Fatalf("job %s guard is not boolean", name)
		}
		return got
	}

	t.Run("cancelled trigger skips every job", func(t *testing.T) {
		// resolve-published-artifact is skipped first; its dependents then see
		// result skipped. Checking every job catches a future unguarded job too.
		fixture := values("workflow_run", "cancelled", "skipped", prTrigger{action: "completed"})
		for _, name := range names {
			if runs(name, fixture) {
				t.Errorf("job %s runs for a cancelled acceptance-image trigger; the run would not conclude skipped", name)
			}
		}
	})

	t.Run("member pull request skips every job", func(t *testing.T) {
		for _, pr := range memberPullRequestTriggers() {
			for _, conclusion := range []any{"", nil} {
				fixture := values("pull_request", conclusion, "skipped", pr)
				for _, name := range names {
					if runs(name, fixture) {
						t.Errorf("job %s runs for member pull request trigger %+v; only batch heads, full-ci PRs and main carry this lane", name, pr)
					}
				}
			}
		}
	})

	t.Run("other triggers keep the fail-closed gate", func(t *testing.T) {
		for _, tc := range []struct {
			event      string
			conclusion any
			pr         prTrigger
		}{
			{"workflow_run", "success", prTrigger{}},
			{"workflow_run", "failure", prTrigger{}},
			{"workflow_run", "timed_out", prTrigger{}},
			{"workflow_run", "startup_failure", prTrigger{}},
			{"workflow_run", "action_required", prTrigger{}},
			{"workflow_run", "neutral", prTrigger{}},
			{"workflow_run", "skipped", prTrigger{}},
			{"workflow_run", "stale", prTrigger{}},
			// GitHub evaluates a missing property as ''; nil covers null too.
			{"pull_request", "", prTrigger{batchHead: true, action: "synchronize"}},
			{"pull_request", nil, prTrigger{batchHead: true, action: "opened"}},
			{"pull_request", "", prTrigger{fullCI: true, action: "synchronize"}},
			{"pull_request", "", prTrigger{fullCI: true, action: "labeled", label: "full-ci"}},
			{"pull_request", nil, prTrigger{batchHead: true, fullCI: true, action: "reopened"}},
		} {
			for _, name := range []string{"resolve-published-artifact", "realtiming-result"} {
				pr := tc.pr
				if tc.event == "workflow_run" {
					pr = prTrigger{action: "completed"}
				}
				if !runs(name, values(tc.event, tc.conclusion, "success", pr)) {
					t.Errorf("%s does not run for %s trigger conclusion %v; a failed or missing artifact must still fail closed", name, tc.event, tc.conclusion)
				}
			}
		}
	})
}

// prTrigger is the pull_request payload the #1692 gate reads.
type prTrigger struct {
	batchHead bool   // head branch starts with batch/
	fullCI    bool   // the PR carries the full-ci label
	action    string // github.event.action
	label     string // github.event.label.name on a labeled event
}

// memberPullRequestTriggers are pull_request events that must skip the heavy
// lanes: a plain member PR, and any label other than full-ci being added, even
// to a batch or full-ci PR (that run must not replace a real one).
func memberPullRequestTriggers() []prTrigger {
	return []prTrigger{
		{action: "opened"},
		{action: "synchronize"},
		{action: "reopened"},
		{action: "labeled", label: "needs-triage"},
		{batchHead: true, action: "labeled", label: "needs-triage"},
		{fullCI: true, action: "labeled", label: "enhancement"},
	}
}
