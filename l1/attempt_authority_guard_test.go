package l1

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// This is a pattern ratchet, not a SQL parser or data-flow checker (#785).
// Count by named function and effect so unrelated writes in an exception's
// function still fail, and removed or changed exception entries fail too.
func TestAttemptAuthorityProtectedWritesRatchet(t *testing.T) {
	exceptions := []struct {
		site, reason string
		count        int
	}{
		{"store.go:claimJob:detach", "Exhausted queued one-shot pre-start budget clears current binding and leaves replay untouched.", 1},
		{"store.go:claimJob:replay | NULL", "Atomic claim installs the successor and clears predecessor replay; claim stays at its endpoint.", 1},
		{"cancel.go:CancelJob:detach", "Queued one-shot cancellation clears current binding and leaves replay untouched.", 1},
		{"store.go:SetAttemptPublication:health | CASE WHEN ? THEN healthy_since_ns ELSE NULL END", "CASE only retains existing health or clears it; it cannot establish health.", 1},
		{"service_jobs.go:transitionServiceJob:health | CASE WHEN ? THEN NULL ELSE healthy_since_ns END", "CASE only clears or retains existing health; it cannot establish health.", 1},
	}
	allowed := map[string]int{}
	for _, exception := range exceptions {
		if exception.reason == "" || exception.count <= 0 || allowed[exception.site] != 0 {
			t.Fatalf("invalid exception: %+v", exception)
		}
		allowed[exception.site] = exception.count
	}
	actual := map[string]int{}
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") || path == "attempt_authority.go" {
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
			}
			for effect, count := range protectedAttemptWritePatterns(decl) {
				actual[path+":"+name+":"+effect] += count
			}
		}
	}
	var violations []string
	for site, count := range actual {
		if count != allowed[site] {
			violations = append(violations, fmt.Sprintf("%s: got %d protected writes, allowed %d", site, count, allowed[site]))
		}
	}
	for site, count := range allowed {
		if actual[site] == 0 {
			violations = append(violations, fmt.Sprintf("remove obsolete exception %s (%d)", site, count))
		}
	}
	sort.Strings(violations)
	if len(violations) > 0 {
		t.Fatalf("Attempt authority guard:\n%s", strings.Join(violations, "\n"))
	}
}

var attemptWriteAssignments = map[string]*regexp.Regexp{
	"health": regexp.MustCompile(`(?is)\bhealthy_since_ns\s*=\s*(CASE\b[^;]*?\bEND|[^\s,;]+)`),
	"detach": regexp.MustCompile(`(?i)\bcurrent_attempt_id\s*=\s*NULL\b`),
	"replay": regexp.MustCompile(`(?i)\bcompletion_replay_attempt_id\s*=\s*([^\s,;]+)`),
}

var jobStateWrite = regexp.MustCompile(`(?is)\bUPDATE\s+jobs\s+SET\b[^;]*?\bstate\s*=`)
var attemptStateWrite = regexp.MustCompile(`(?is)\bUPDATE\s+attempts\s+SET\b[^;]*?\bstate\s*=`)
var literalRunningWrite = regexp.MustCompile(`(?i)\bstate\s*=\s*'running'`)
var literalLostWrite = regexp.MustCompile(`(?i)\bstate\s*=\s*'lost'`)

func protectedAttemptWritePatterns(node ast.Node) map[string]int {
	writes := map[string]int{}
	ast.Inspect(node, func(n ast.Node) bool {
		if literal, ok := n.(*ast.BasicLit); ok && literal.Kind == token.STRING {
			value, _ := strconv.Unquote(literal.Value)
			for effect, pattern := range attemptWriteAssignments {
				for _, match := range pattern.FindAllStringSubmatch(value, -1) {
					if effect == "health" && strings.EqualFold(match[1], "NULL") {
						continue
					}
					key := effect
					if effect == "health" || effect == "replay" {
						// Pin exempt preservation expressions and claim's NULL,
						// so replacing them with a new grant is not exempt.
						key += " | " + strings.Join(strings.Fields(match[1]), " ")
					}
					writes[key]++
				}
			}
			if jobStateWrite.MatchString(value) && literalRunningWrite.MatchString(value) {
				writes["promotion"]++
			}
			if attemptStateWrite.MatchString(value) && literalLostWrite.MatchString(value) {
				writes["invalidation"]++
			}
		}
		// Existing parameterized writes bind state with contract constants.
		// Inspect only the same call, so predicates and reads do not count.
		if call, ok := n.(*ast.CallExpr); ok {
			var jobWrite, attemptWrite, running, lost bool
			for _, arg := range call.Args {
				if literal, ok := arg.(*ast.BasicLit); ok && literal.Kind == token.STRING {
					value, _ := strconv.Unquote(literal.Value)
					jobWrite = jobWrite || jobStateWrite.MatchString(value)
					attemptWrite = attemptWrite || attemptStateWrite.MatchString(value)
				}
				if selector, ok := arg.(*ast.SelectorExpr); ok {
					running = running || selector.Sel.Name == "JobRunning"
					lost = lost || selector.Sel.Name == "AttemptLost"
				}
			}
			if jobWrite && running {
				writes["promotion"]++
			}
			if attemptWrite && lost {
				writes["invalidation"]++
			}
		}
		return true
	})
	return writes
}
