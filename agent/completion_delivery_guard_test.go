package agent

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// A small source-pattern ratchet, not a SQL or data-flow parser. Pin each
// protected selector by named function and count, including method values.
// Exact exceptions also fail when their calls are removed or changed.
func TestCompletionDeliveryProtectedMutations(t *testing.T) {
	exceptions := []struct {
		site, reason string
		count        int
	}{
		{"completion_delivery.go:evidenceOutbox.deliverCompletion:completionDelivered", "Shared completion delivery receipt.", 1},
		{"completion_delivery.go:evidenceOutbox.deliverCompletion:suppressCompletion", "Shared bounded intent suppression.", 1},
		{"completion_delivery.go:evidenceOutbox.deliverCompletion:withholdCompletion", "Shared unavailable-intent disposition.", 1},
		{"completion_delivery.go:evidenceOutbox.deliverCompletion:sealAttemptEvidence", "Shared completion refusal seal.", 1},
		{"completion_delivery.go:evidenceOutbox.deliverCompletion:beginCompletion", "Shared intent observation and request fence.", 1},
		{"evidence_outbox.go:evidenceOutbox.completionDelivered:completionDelivered", "Existing persistence forwarding method; no delivery policy.", 1},
		{"evidence_outbox.go:evidenceOutbox.suppressCompletion:recordCompletionDisposition", "Existing bounded suppression persistence retry.", 1},
		{"evidence_outbox.go:evidenceOutbox.withholdCompletion:recordCompletionDisposition", "Existing withheld persistence mutation.", 1},
		{"evidence_outbox.go:evidenceOutbox.recoverLogs:sealIncomplete", "Replay log rejection seals evidence after gap replacement fails.", 3},
		{"evidence_outbox.go:evidenceOutbox.sealIncomplete:sealAttemptEvidence", "Existing replay log seal and diagnostic; completion uses the mutation directly.", 1},
		{"evidence_outbox.go:evidenceOutbox.sealAttemptEvidence:sealIncomplete", "Existing persistence forwarding method records the seal time.", 1},
	}
	allowed := map[string]int{}
	for _, exception := range exceptions {
		if exception.reason == "" || exception.count <= 0 || allowed[exception.site] != 0 {
			t.Fatalf("invalid exception: %+v", exception)
		}
		allowed[exception.site] = exception.count
	}
	protected := map[string]bool{
		"completionDelivered": true, "suppressCompletion": true,
		"withholdCompletion": true, "sealAttemptEvidence": true,
		"beginCompletion": true, "recordCompletionDisposition": true, "sealIncomplete": true,
	}
	actual := map[string]int{}
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, data, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			name := "package"
			if fn, ok := decl.(*ast.FuncDecl); ok {
				name = fn.Name.Name
				if fn.Recv != nil {
					receiver := fn.Recv.List[0].Type
					if pointer, ok := receiver.(*ast.StarExpr); ok {
						receiver = pointer.X
					}
					if ident, ok := receiver.(*ast.Ident); ok {
						name = ident.Name + "." + name
					}
				}
			}
			ast.Inspect(decl, func(node ast.Node) bool {
				if selector, ok := node.(*ast.SelectorExpr); ok && protected[selector.Sel.Name] {
					actual[path+":"+name+":"+selector.Sel.Name]++
				}
				return true
			})
		}
	}
	var violations []string
	for site, count := range actual {
		if count != allowed[site] {
			violations = append(violations, fmt.Sprintf("%s: got %d protected calls, allowed %d", site, count, allowed[site]))
		}
	}
	for site := range allowed {
		if actual[site] == 0 {
			violations = append(violations, "remove obsolete exception "+site)
		}
	}
	sort.Strings(violations)
	if len(violations) > 0 {
		t.Fatalf("completion delivery guard:\n%s", strings.Join(violations, "\n"))
	}
}
