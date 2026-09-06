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

const agentImportPath = "github.com/Derek-X-Wang/wefty/agent"

func TestOCICapableAgentConfigFixturesRequireIntentAuthority(t *testing.T) {
	tests := []struct {
		name       string
		configBody string
		want       int
	}{
		{
			name:       "bare capability key",
			configBody: `Capabilities: map[string]bool{"oci": true, "process": true}`,
			want:       1,
		},
		{
			name:       "normalized capability key",
			configBody: `Capabilities: map[string]bool{"kind:oci": true, "kind:process": true}`,
			want:       1,
		},
		{
			name:       "OCI workload runtime",
			configBody: `WorkloadRuntimes: map[string]WorkloadRuntime{JobKindOCI: adapter, JobKindProcess: adapter}`,
			want:       1,
		},
		{
			name:       "legitimate process-only configuration",
			configBody: `Capabilities: map[string]bool{"process": true}, WorkloadRuntimes: map[string]WorkloadRuntime{JobKindProcess: adapter}`,
			want:       0,
		},
		{
			name:       "explicit nil authority remains compile-visible",
			configBody: `Capabilities: map[string]bool{"kind:oci": true}, OCIIntent: nil`,
			want:       0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "fixture.go")
			source := "package agent\nfunc fixture() { _ = Config{" + test.configBody + "} }\n"
			if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
				t.Fatal(err)
			}
			violations, err := missingOCIIntentAuthorities(path)
			if err != nil {
				t.Fatal(err)
			}
			if len(violations) != test.want {
				t.Fatalf("violations=%v, want %d for %s", violations, test.want, source)
			}
		})
	}
}

func TestOCICapableAgentConfigLiteralsDeclareIntentAuthority(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate agent Config contract test")
	}
	repositoryRoot := filepath.Dir(filepath.Dir(source))
	var violations []string
	err := filepath.WalkDir(repositoryRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != repositoryRoot && (entry.Name() == ".git" || entry.Name() == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		found, err := missingOCIIntentAuthorities(path)
		if err != nil {
			return err
		}
		for _, position := range found {
			relative, err := filepath.Rel(repositoryRoot, path)
			if err != nil {
				return err
			}
			violations = append(violations, fmt.Sprintf("%s:%d", relative, position.Line))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("OCI-capable agent.Config literals must declare OCIIntent (all build tags are inspected):\n%s", strings.Join(violations, "\n"))
	}
}

func missingOCIIntentAuthorities(path string) ([]token.Position, error) {
	files := token.NewFileSet()
	parsed, err := parser.ParseFile(files, path, nil, 0)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	agentAliases := make(map[string]bool)
	for _, imported := range parsed.Imports {
		importPath, err := strconv.Unquote(imported.Path.Value)
		if err != nil || importPath != agentImportPath {
			continue
		}
		name := "agent"
		if imported.Name != nil {
			name = imported.Name.Name
		}
		agentAliases[name] = true
	}
	var violations []token.Position
	inspect := func(node ast.Node) {
		ast.Inspect(node, func(candidate ast.Node) bool {
			literal, ok := candidate.(*ast.CompositeLit)
			if !ok || !isAgentConfigLiteral(parsed.Name.Name, agentAliases, literal.Type) {
				return true
			}
			if configLiteralOffersOCI(literal) && !configLiteralHasField(literal, "OCIIntent") {
				violations = append(violations, files.Position(literal.Pos()))
			}
			return true
		})
	}
	for _, declaration := range parsed.Decls {
		inspect(declaration)
	}
	// Dynamic or non-literal Config construction is intentionally outside this
	// source contract; Agent.New remains the runtime fail-closed backstop.
	return violations, nil
}

func isAgentConfigLiteral(packageName string, aliases map[string]bool, expression ast.Expr) bool {
	switch typed := expression.(type) {
	case *ast.Ident:
		return packageName == "agent" && typed.Name == "Config"
	case *ast.SelectorExpr:
		identifier, ok := typed.X.(*ast.Ident)
		return ok && aliases[identifier.Name] && typed.Sel.Name == "Config"
	default:
		return false
	}
}

func configLiteralOffersOCI(literal *ast.CompositeLit) bool {
	for _, element := range literal.Elts {
		field, ok := element.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		name, ok := field.Key.(*ast.Ident)
		if !ok {
			continue
		}
		switch name.Name {
		case "CapabilityProbe":
			if !isNilIdentifier(field.Value) {
				return true
			}
		case "Capabilities":
			if expressionNamesOCI(field.Value) {
				return true
			}
		case "WorkloadRuntimes":
			if expressionNamesOCI(field.Value) {
				return true
			}
		}
	}
	return false
}

func configLiteralHasField(literal *ast.CompositeLit, wanted string) bool {
	for _, element := range literal.Elts {
		field, ok := element.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		if name, ok := field.Key.(*ast.Ident); ok && name.Name == wanted {
			return true
		}
	}
	return false
}

func expressionNamesOCI(expression ast.Expr) bool {
	found := false
	ast.Inspect(expression, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.BasicLit:
			if value.Kind == token.STRING {
				decoded, err := strconv.Unquote(value.Value)
				found = found || err == nil && (strings.EqualFold(strings.TrimSpace(decoded), "oci") || strings.EqualFold(strings.TrimSpace(decoded), "kind:oci"))
			}
		case *ast.Ident:
			found = found || value.Name == "JobKindOCI"
		}
		return !found
	})
	return found
}

func isNilIdentifier(expression ast.Expr) bool {
	identifier, ok := expression.(*ast.Ident)
	return ok && identifier.Name == "nil"
}
