package scripts

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/rhysd/actionlint"
)

// Use the already pinned actionlint parser so quotes, wrappers and parentheses
// cannot turn string contents into status calls or mandatory conditions. This is
// deliberately a conjunction policy, not a general Boolean implication solver.
func parseWorkflowGuard(guard string) (actionlint.ExprNode, error) {
	guard = strings.TrimSpace(guard)
	if guard == "" {
		return nil, nil
	}
	if strings.HasPrefix(guard, "${{") {
		if !strings.HasSuffix(guard, "}}") {
			return nil, fmt.Errorf("unterminated expression wrapper: %q", guard)
		}
		guard = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(guard, "${{"), "}}"))
	}
	lexer := actionlint.NewExprLexer(guard + "}}")
	node, err := actionlint.NewExprParser().Parse(lexer)
	if err != nil {
		return nil, fmt.Errorf("invalid guard %q: %v", guard, err)
	}
	if lexer.Offset() != len(guard)+2 {
		return nil, fmt.Errorf("unsupported trailing expression text in %q", guard)
	}
	return node, nil
}

func guardTerms(node actionlint.ExprNode) []actionlint.ExprNode {
	if node == nil {
		return nil
	}
	if binary, ok := node.(*actionlint.LogicalOpNode); ok && binary.Kind == actionlint.LogicalOpNodeKindAnd {
		return append(guardTerms(binary.Left), guardTerms(binary.Right)...)
	}
	return []actionlint.ExprNode{node}
}

func guardKey(node actionlint.ExprNode) string {
	// Include every node's type: logical and comparison operator enums overlap.
	var value any = node
	switch n := node.(type) {
	case *actionlint.LogicalOpNode:
		value = []any{n.Kind, guardKey(n.Left), guardKey(n.Right)}
	case *actionlint.CompareOpNode:
		value = []any{n.Kind, guardKey(n.Left), guardKey(n.Right)}
	case *actionlint.NotOpNode:
		value = guardKey(n.Operand)
	case *actionlint.ObjectDerefNode:
		value = []any{guardKey(n.Receiver), n.Property}
	case *actionlint.ArrayDerefNode:
		value = guardKey(n.Receiver)
	case *actionlint.IndexAccessNode:
		value = []any{guardKey(n.Operand), guardKey(n.Index)}
	case *actionlint.FuncCallNode:
		args := []string{strings.ToLower(n.Callee)}
		for _, arg := range n.Args {
			args = append(args, guardKey(arg))
		}
		value = args
	}
	payload, _ := json.Marshal(value)
	return fmt.Sprintf("%T:%s", node, payload)
}

func guardStatusCall(node actionlint.ExprNode) bool {
	call, ok := node.(*actionlint.FuncCallNode)
	return ok && slices.Contains([]string{"always", "cancelled", "success", "failure"}, strings.ToLower(call.Callee))
}

func guardHasStatus(node actionlint.ExprNode) bool {
	found := false
	actionlint.VisitExprNode(node, func(n, _ actionlint.ExprNode, entering bool) {
		if entering && guardStatusCall(n) {
			found = true
		}
	})
	return found
}

func skipTolerantGuard(node actionlint.ExprNode) bool {
	tolerant := false
	for _, term := range guardTerms(node) {
		call, positive := term.(*actionlint.FuncCallNode)
		if positive && strings.EqualFold(call.Callee, "always") && len(call.Args) == 0 {
			tolerant = true
			continue
		}
		if not, ok := term.(*actionlint.NotOpNode); ok {
			call, ok := not.Operand.(*actionlint.FuncCallNode)
			if ok && strings.EqualFold(call.Callee, "cancelled") && len(call.Args) == 0 {
				tolerant = true
				continue
			}
		}
		// success(), failure(), cancellation handlers, negations and OR branches
		// need a separate policy; merely containing a status call is insufficient.
		if guardHasStatus(term) {
			return false
		}
	}
	return tolerant
}

func guardEligibility(node actionlint.ExprNode) []actionlint.ExprNode {
	var terms []actionlint.ExprNode
	for _, term := range guardTerms(node) {
		status := term
		if not, ok := status.(*actionlint.NotOpNode); ok {
			status = not.Operand
		}
		if guardStatusCall(status) {
			continue
		}
		terms = append(terms, term)
	}
	return terms
}

func pinsGuardCondition(dependent actionlint.ExprNode, conditions []actionlint.ExprNode) bool {
	if len(conditions) == 0 {
		return false
	}
	mandatory := map[string]bool{}
	for _, term := range guardEligibility(dependent) {
		if !guardHasStatus(term) {
			mandatory[guardKey(term)] = true
		}
	}
	for _, condition := range conditions {
		if guardHasStatus(condition) || !mandatory[guardKey(condition)] {
			return false
		}
	}
	return true
}

func validateSkipGuards(t *testing.T, jobs map[string]workflowJob) error {
	t.Helper()
	guards := map[string]actionlint.ExprNode{}
	names := make([]string, 0, len(jobs))
	for name, job := range jobs {
		node, err := parseWorkflowGuard(job.If)
		if err != nil {
			return fmt.Errorf("job %s: %w", name, err)
		}
		guards[name] = node
		names = append(names, name)
	}
	slices.Sort(names)
	for _, source := range names {
		conditions := guardEligibility(guards[source])
		if len(conditions) == 0 {
			continue
		}
		// Traverse separately for every source, through even exempt/tolerant jobs.
		affected := map[string]bool{source: true}
		for changed := true; changed; {
			changed = false
			for _, name := range names {
				if affected[name] {
					continue
				}
				for _, need := range workflowNeeds(t, jobs[name].Needs) {
					if affected[need] {
						affected[name] = true
						changed = true
						break
					}
				}
			}
		}
		for _, name := range names {
			if name == source || !affected[name] {
				continue
			}
			if !skipTolerantGuard(guards[name]) && !pinsGuardCondition(guards[name], conditions) {
				return fmt.Errorf("job %s transitively needs conditional job %s (%q); guard %q must use mandatory always()/!cancelled() without conflicting status calls, or pin every source condition as a conjunction (other implication forms unsupported)", name, source, jobs[source].If, jobs[name].If)
			}
		}
	}
	return nil
}

func validateResolverConsumer(guard string) error {
	node, err := parseWorkflowGuard(guard)
	if err != nil {
		return err
	}
	if !skipTolerantGuard(node) {
		return fmt.Errorf("resolver consumer lacks a supported skip-tolerant guard: %q", guard)
	}
	for _, required := range []string{"!cancelled()", "needs.resolve-published-artifact.result == 'success'", "needs.resolve-published-artifact.outputs.available == 'true'"} {
		want, err := parseWorkflowGuard(required)
		if err != nil {
			return err
		}
		found := false
		for _, term := range guardTerms(node) {
			if guardKey(term) == guardKey(want) {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("resolver consumer guard %q lacks mandatory conjunct %s", guard, required)
		}
	}
	return nil
}

const realtimingDiagnosticPreamble = `set -eu
printf '%s\n' \
  "resolve-published-artifact=$RESOLVE_RESULT" \
  "artifact-available=$ARTIFACT_AVAILABLE" \
  "service-acceptance-realtiming=$REALTIMING_RESULT"
test "$RESOLVE_RESULT" = success
test "$ARTIFACT_AVAILABLE" = true
test "$REALTIMING_RESULT" = success
`

func realtimingDiagnosticStep(job workflowJob) (workflowStep, error) {
	var matches []workflowStep
	for _, step := range job.Steps {
		if step.Name == "Fail closed unless the artifact lane executed and passed" || step.Name == "Fail closed unless the published lane executed and passed" {
			matches = append(matches, step)
		}
	}
	if len(matches) != 1 {
		return workflowStep{}, fmt.Errorf("expected exactly one fail-closed assertion step; got %d", len(matches))
	}
	step := matches[0]
	node, err := parseWorkflowGuard(step.If)
	if err != nil {
		return step, err
	}
	always, _ := parseWorkflowGuard("always()")
	if guardKey(node) != guardKey(always) {
		return step, fmt.Errorf("actual fail-closed assertion step must have unconditional always() guard")
	}
	for key, value := range map[string]string{
		"RESOLVE_RESULT":     "${{ needs.resolve-published-artifact.result }}",
		"ARTIFACT_AVAILABLE": "${{ needs.resolve-published-artifact.outputs.available }}",
		"REALTIMING_RESULT":  "${{ needs.service-acceptance-realtiming.result }}",
	} {
		if step.Env[key] != value {
			return step, fmt.Errorf("diagnostic %s binding = %v, want %s", key, step.Env[key], value)
		}
	}
	// An exact executable prefix rules out comments/string decoys, early assertions
	// or failures before all statuses print. The remaining receipt checks stay intact.
	if !strings.HasPrefix(step.Run, realtimingDiagnosticPreamble) {
		return step, fmt.Errorf("diagnostic assertion must print all three statuses before its fail-closed tests")
	}
	return step, nil
}

// Bounded evaluator for the real consumer fixtures, not a production expression
// runtime. Unsupported syntax fails the fixture instead of assuming eligibility.
func evaluateGuardFixture(t *testing.T, node actionlint.ExprNode, values map[string]any) any {
	t.Helper()
	switch n := node.(type) {
	case *actionlint.StringNode:
		return n.Value
	case *actionlint.BoolNode:
		return n.Value
	case *actionlint.NotOpNode:
		value, ok := evaluateGuardFixture(t, n.Operand, values).(bool)
		if !ok {
			t.Fatal("fixture ! operand is not boolean")
		}
		return !value
	case *actionlint.CompareOpNode:
		left, right := evaluateGuardFixture(t, n.Left, values), evaluateGuardFixture(t, n.Right, values)
		if n.Kind == actionlint.CompareOpNodeKindEq {
			return left == right
		}
		if n.Kind == actionlint.CompareOpNodeKindNotEq {
			return left != right
		}
	case *actionlint.LogicalOpNode:
		left, ok := evaluateGuardFixture(t, n.Left, values).(bool)
		if !ok {
			t.Fatal("fixture logical operand is not boolean")
		}
		if n.Kind == actionlint.LogicalOpNodeKindAnd {
			return left && evaluateGuardFixture(t, n.Right, values) == true
		}
		if n.Kind == actionlint.LogicalOpNodeKindOr {
			return left || evaluateGuardFixture(t, n.Right, values) == true
		}
	case *actionlint.ObjectDerefNode, *actionlint.VariableNode, *actionlint.FuncCallNode:
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
	t.Fatalf("unsupported or unbound fixture expression: %s", guardKey(node))
	return nil
}
