package scripts

import (
	"slices"
	"testing"
)

// #1603: a newer main push cancels the older acceptance-image run
// (cancel-in-progress). The workflow_run it still fires must skip every
// realtiming job, so the run concludes skipped: not success (#241), and not a
// red failure. Every other trigger conclusion keeps the fail-closed result gate.
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
	values := func(event string, conclusion any, resolveResult string) map[string]any {
		return map[string]any{
			"always()":                                           true,
			"cancelled()":                                        false,
			"github.event_name":                                  event,
			"github.event.workflow_run.conclusion":               conclusion,
			"github.event.workflow_run.event":                    "push",
			"github.event.workflow_run.head_branch":              "main",
			"needs.resolve-published-artifact.result":            resolveResult,
			"needs.resolve-published-artifact.outputs.available": "",
			"vars.TSNET_SMOKE_REQUIRED":                          "true",
			"vars.TSNET_CI_TESTER_REQUIRED":                      "true",
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
		fixture := values("workflow_run", "cancelled", "skipped")
		for _, name := range names {
			if runs(name, fixture) {
				t.Errorf("job %s runs for a cancelled acceptance-image trigger; the run would not conclude skipped", name)
			}
		}
	})

	t.Run("other triggers keep the fail-closed gate", func(t *testing.T) {
		for _, tc := range []struct {
			event      string
			conclusion any
		}{
			{"workflow_run", "success"},
			{"workflow_run", "failure"},
			{"workflow_run", "timed_out"},
			{"workflow_run", "startup_failure"},
			{"workflow_run", "action_required"},
			{"workflow_run", "neutral"},
			{"workflow_run", "skipped"},
			{"workflow_run", "stale"},
			// GitHub evaluates a missing property as ''; nil covers null too.
			{"pull_request", ""},
			{"pull_request", nil},
		} {
			for _, name := range []string{"resolve-published-artifact", "realtiming-result"} {
				if !runs(name, values(tc.event, tc.conclusion, "success")) {
					t.Errorf("%s does not run for %s trigger conclusion %v; a failed or missing artifact must still fail closed", name, tc.event, tc.conclusion)
				}
			}
		}
	})
}
