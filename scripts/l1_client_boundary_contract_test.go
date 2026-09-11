package scripts

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// l1ImportPath is the l1 package itself. ADR-0006 lets non-test l3 code
// import it, but only to reach the narrow allowlist of wire types below.
const l1ImportPath = "github.com/Derek-X-Wang/wefty/l1"

// l1SubpackagePrefix marks every package nested under l1 as off limits to
// l3 entirely, regardless of what it is used for. There are none today
// (l1 is a flat package); the rule stays so one cannot appear as an
// unreviewed shortcut around the allowlist below.
const l1SubpackagePrefix = l1ImportPath + "/"

// allowedL1Identifiers is the ADR-0006 contract surface: the only l1.X
// selectors non-test l3 code may reference. It was seeded from what
// non-test l3 code actually uses today (l3/client.go, l3/server.go,
// l3/types.go).
//
// Extending this set is a deliberate, reviewed decision against the
// ADR-0006 litmus test, not a side effect of an implementation: could a
// third party build their own L3 using only api/openapi/l1-client.v1.json?
// If a new l1 identifier is needed to answer "yes", it belongs in the
// OpenAPI contract before it belongs in this allowlist.
var allowedL1Identifiers = map[string]bool{
	"Job":                     true,
	"LogPage":                 true,
	"DefaultLogPageLimit":     true,
	"MaxLogPageLimit":         true,
	"ComputerTokenScopeProof": true,
}

// l1BoundaryViolation is one place a non-test l3 file crosses the ADR-0006
// boundary: either a forbidden import of an l1 subpackage, or a reference
// to an l1 selector that is not in allowedL1Identifiers.
type l1BoundaryViolation struct {
	Position   token.Position
	Identifier string
	Reason     string
}

func TestL1ClientBoundaryChecksSyntheticSources(t *testing.T) {
	tests := []struct {
		name       string
		source     string
		wantCount  int
		wantSubstr string
	}{
		{
			name: "disallowed l1 selector",
			source: `package l3

import "github.com/Derek-X-Wang/wefty/l1"

func fixture() {
	var s l1.Store
	_ = s
}
`,
			wantCount:  1,
			wantSubstr: "l1.Store",
		},
		{
			name: "disallowed l1 function call",
			source: `package l3

import "github.com/Derek-X-Wang/wefty/l1"

func fixture() {
	l1.OpenStore()
}
`,
			wantCount:  1,
			wantSubstr: "l1.OpenStore",
		},
		{
			name: "disallowed selector through an aliased import",
			source: `package l3

import l1client "github.com/Derek-X-Wang/wefty/l1"

func fixture() {
	l1client.OpenStore()
}
`,
			wantCount:  1,
			wantSubstr: "l1client.OpenStore",
		},
		{
			name: "forbidden l1 subpackage import",
			source: `package l3

import "github.com/Derek-X-Wang/wefty/l1/internal"

func fixture() {
	_ = internal.Thing{}
}
`,
			wantCount:  1,
			wantSubstr: "github.com/Derek-X-Wang/wefty/l1/internal",
		},
		{
			name: "allowlisted selectors only",
			source: `package l3

import "github.com/Derek-X-Wang/wefty/l1"

func fixture() {
	var job l1.Job
	var page l1.LogPage
	limit := l1.DefaultLogPageLimit
	max := l1.MaxLogPageLimit
	var proof l1.ComputerTokenScopeProof
	_, _, _, _, _ = job, page, limit, max, proof
}
`,
			wantCount: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "fixture.go")
			if err := os.WriteFile(path, []byte(test.source), 0o600); err != nil {
				t.Fatal(err)
			}
			violations, err := l1ClientBoundaryViolations(path)
			if err != nil {
				t.Fatal(err)
			}
			if len(violations) != test.wantCount {
				t.Fatalf("violations=%v, want %d for %s", violations, test.wantCount, test.source)
			}
			if test.wantCount == 0 {
				return
			}
			violation := violations[0]
			if violation.Position.Filename != path {
				t.Fatalf("violation filename = %q, want %q", violation.Position.Filename, path)
			}
			if violation.Position.Line == 0 {
				t.Fatalf("violation line not populated: %+v", violation)
			}
			if !strings.Contains(violation.Identifier, test.wantSubstr) {
				t.Fatalf("violation identifier = %q, want substring %q", violation.Identifier, test.wantSubstr)
			}
		})
	}
}

func TestL1ClientBoundaryRestrictsL3ToAllowlistedWireTypes(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate l1 client boundary contract test")
	}
	repositoryRoot := filepath.Dir(filepath.Dir(source))
	l3Root := filepath.Join(repositoryRoot, "l3")

	var violations []string
	err := filepath.WalkDir(l3Root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		// _test.go files legitimately run an in-process L1 store or server
		// and are exempt from the ADR-0006 client boundary.
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		found, err := l1ClientBoundaryViolations(path)
		if err != nil {
			return err
		}
		for _, violation := range found {
			relative, err := filepath.Rel(repositoryRoot, path)
			if err != nil {
				return err
			}
			violations = append(violations, fmt.Sprintf("%s:%d: %s (%s)", relative, violation.Position.Line, violation.Reason, violation.Identifier))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("ADR-0006: non-test l3 code may reference only the allowlisted l1 wire types:\n%s", strings.Join(violations, "\n"))
	}
}

// l1ClientBoundaryViolations parses a single Go source file and reports
// every ADR-0006 boundary crossing in it: an import of an l1 subpackage,
// or an l1.X selector (through any import name, including an alias) that
// is not in allowedL1Identifiers.
func l1ClientBoundaryViolations(path string) ([]l1BoundaryViolation, error) {
	files := token.NewFileSet()
	parsed, err := parser.ParseFile(files, path, nil, 0)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	var violations []l1BoundaryViolation
	l1Aliases := make(map[string]bool)

	for _, imported := range parsed.Imports {
		importPath, err := strconv.Unquote(imported.Path.Value)
		if err != nil {
			continue
		}
		switch {
		case strings.HasPrefix(importPath, l1SubpackagePrefix):
			violations = append(violations, l1BoundaryViolation{
				Position:   files.Position(imported.Pos()),
				Identifier: importPath,
				Reason:     "l3 may not import an l1 subpackage",
			})
		case importPath == l1ImportPath:
			l1Aliases[importedPackageName(importPath, imported)] = true
		}
	}

	if len(l1Aliases) == 0 {
		return violations, nil
	}

	ast.Inspect(parsed, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		identifier, ok := selector.X.(*ast.Ident)
		if !ok || !l1Aliases[identifier.Name] {
			return true
		}
		if !allowedL1Identifiers[selector.Sel.Name] {
			violations = append(violations, l1BoundaryViolation{
				Position:   files.Position(selector.Pos()),
				Identifier: identifier.Name + "." + selector.Sel.Name,
				Reason:     "l1 selector is not in the ADR-0006 allowlist",
			})
		}
		return true
	})

	return violations, nil
}

func importedPackageName(importPath string, imported *ast.ImportSpec) string {
	if imported.Name != nil {
		return imported.Name.Name
	}
	segments := strings.Split(importPath, "/")
	return segments[len(segments)-1]
}
