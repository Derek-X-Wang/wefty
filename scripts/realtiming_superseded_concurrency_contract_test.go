package scripts

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/rhysd/actionlint"
	"gopkg.in/yaml.v3"
)

// #1605: GitHub does not order workflow_run dispatch. A realtiming run whose
// acceptance-image trigger was cancelled (#1603) must not share the workflow
// concurrency group with a real run, or with cancel-in-progress it can cancel
// a newer real run. Real main and pull request runs keep their shared groups.
func TestRealtimingSupersededTriggerHasItsOwnConcurrencyGroup(t *testing.T) {
	payload, err := os.ReadFile("../.github/workflows/service-acceptance-realtiming.yml")
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Concurrency struct {
			Group  string `yaml:"group"`
			Cancel any    `yaml:"cancel-in-progress"`
		} `yaml:"concurrency"`
	}
	if err := yaml.Unmarshal(payload, &workflow); err != nil {
		t.Fatal(err)
	}
	if workflow.Concurrency.Cancel != true {
		t.Fatalf("cancel-in-progress = %v; the lane keeps cancelling superseded real runs", workflow.Concurrency.Cancel)
	}
	group := func(event string, conclusion any, prNumber any, runID string) string {
		ref := "refs/heads/main"
		if event == "pull_request" {
			ref = fmt.Sprintf("refs/pull/%v/merge", prNumber)
		}
		return renderConcurrencyGroup(t, workflow.Concurrency.Group, map[string]any{
			"github.event.workflow_run.conclusion": conclusion,
			"github.event.pull_request.number":     prNumber,
			"github.ref":                           ref,
			"github.run_id":                        runID,
		})
	}

	real := map[string]string{}
	for _, conclusion := range []string{"success", "failure", "timed_out", "startup_failure", "action_required", "neutral", "skipped", "stale"} {
		real[conclusion] = group("workflow_run", conclusion, nil, "200"+conclusion)
		if real[conclusion] != "service-acceptance-realtiming-refs/heads/main" {
			t.Errorf("main run for trigger conclusion %s left the shared group: %q", conclusion, real[conclusion])
		}
	}
	pr := group("pull_request", nil, 1605, "300")
	if pr != "service-acceptance-realtiming-1605" || group("pull_request", "", 1605, "301") != pr {
		t.Errorf("pull request run left its per-PR group: %q", pr)
	}

	first := group("workflow_run", "cancelled", nil, "100")
	second := group("workflow_run", "cancelled", nil, "101")
	if first == second {
		t.Errorf("two cancelled-trigger runs share group %q; one could cancel the other", first)
	}
	for _, cancelled := range []string{first, second} {
		if !strings.HasPrefix(cancelled, "service-acceptance-realtiming-") || cancelled == pr {
			t.Errorf("cancelled-trigger group %q is not a distinct realtiming group", cancelled)
		}
		for conclusion, shared := range real {
			if cancelled == shared {
				t.Errorf("cancelled-trigger run shares group %q with a real run (trigger %s)", cancelled, conclusion)
			}
		}
	}
}

// renderConcurrencyGroup expands each ${{ }} segment with GitHub's value
// semantics for && and ||, for the expression forms the group uses.
func renderConcurrencyGroup(t *testing.T, template string, values map[string]any) string {
	t.Helper()
	var out strings.Builder
	for {
		start := strings.Index(template, "${{")
		if start < 0 {
			out.WriteString(template)
			return out.String()
		}
		end := strings.Index(template[start:], "}}")
		if end < 0 {
			t.Fatalf("unterminated expression in %q", template)
		}
		out.WriteString(template[:start])
		node, err := parseWorkflowGuard(template[start : start+end+2])
		if err != nil {
			t.Fatal(err)
		}
		out.WriteString(fmt.Sprint(evaluateGroupValue(t, node, values)))
		template = template[start+end+2:]
	}
}

func evaluateGroupValue(t *testing.T, node actionlint.ExprNode, values map[string]any) any {
	t.Helper()
	truthy := func(value any) bool {
		switch v := value.(type) {
		case nil:
			return false
		case bool:
			return v
		case string:
			return v != ""
		case int:
			return v != 0
		}
		return true
	}
	switch n := node.(type) {
	case *actionlint.StringNode:
		return n.Value
	case *actionlint.CompareOpNode:
		left, right := evaluateGroupValue(t, n.Left, values), evaluateGroupValue(t, n.Right, values)
		switch n.Kind {
		case actionlint.CompareOpNodeKindEq:
			return left == right
		case actionlint.CompareOpNodeKindNotEq:
			return left != right
		}
	case *actionlint.LogicalOpNode:
		left := evaluateGroupValue(t, n.Left, values)
		switch n.Kind {
		case actionlint.LogicalOpNodeKindAnd:
			if !truthy(left) {
				return left
			}
			return evaluateGroupValue(t, n.Right, values)
		case actionlint.LogicalOpNodeKindOr:
			if truthy(left) {
				return left
			}
			return evaluateGroupValue(t, n.Right, values)
		}
	case *actionlint.FuncCallNode:
		if strings.EqualFold(n.Callee, "format") && len(n.Args) > 0 {
			format, ok := evaluateGroupValue(t, n.Args[0], values).(string)
			if !ok {
				t.Fatal("format string is not a literal")
			}
			for i, arg := range n.Args[1:] {
				format = strings.ReplaceAll(format, fmt.Sprintf("{%d}", i), fmt.Sprint(evaluateGroupValue(t, arg, values)))
			}
			return format
		}
	case *actionlint.ObjectDerefNode, *actionlint.VariableNode:
		for expression, value := range values {
			candidate, err := parseWorkflowGuard(expression)
			if err != nil {
				t.Fatal(err)
			}
			if guardKey(candidate) == guardKey(node) {
				return value
			}
		}
	}
	t.Fatalf("unsupported or unbound group expression: %s", guardKey(node))
	return nil
}
