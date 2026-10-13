package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Audit the production dispatch, including OS-specific sources. Adding a
// command must add a real-binary help row before its nil-client route is safe.
func TestCommandHelpMatrixCoversDispatch(t *testing.T) {
	prefixes := map[string]string{
		"execute":                   "",
		"executeAuthoringCommand":   "",
		"executeServices":           "services",
		"executeNodes":              "nodes",
		"executeAdmin":              "admin",
		"executeAdminPolicy":        "admin policy",
		"executeAdmins":             "admins",
		"executeJobs":               "jobs",
		"executeComputers":          "computers",
		"executeRuns":               "runs",
		"executeComputerStorage":    "services",
		"executeComputerBackups":    "services backup",
		"executeComputerCustody":    "services custody",
		"executeComputerTakeover":   "services takeover",
		"executeComputerSubmission": "services",
		"executeLocalNode":          "node",
		"executeLocalOCIIntent":     "node oci",
	}
	covered := make(map[string]bool)
	for _, command := range helpCommands {
		covered[command] = true
	}
	found := make(map[string]bool)
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		source, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range source.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			prefix, ok := prefixes[function.Name.Name]
			if !ok {
				continue
			}
			found[function.Name.Name] = true
			visit := func(words ...string) {
				path := strings.TrimSpace(strings.Join(append([]string{prefix}, words...), " "))
				// Root flag aliases are exercised by every spelling in the binary matrix.
				if prefix == "" && len(words) == 1 && isHelpFlag(words[0]) {
					return
				}
				if !covered[path] {
					t.Errorf("%s: %s dispatches %q without a help matrix row", file, function.Name.Name, path)
				}
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				switch node := node.(type) {
				case *ast.SwitchStmt:
					if commandArgumentIndex(node.Tag) != 0 {
						break
					}
					for _, statement := range node.Body.List {
						clause := statement.(*ast.CaseClause)
						for _, expression := range clause.List {
							if name, ok := commandLiteral(expression); ok {
								visit(name)
							}
						}
					}
				case *ast.BinaryExpr:
					// Some dispatchers use if chains, not switches. Capture their command
					// comparisons too, including sessions list and audit tail at depth two.
					names := [2]map[string]bool{make(map[string]bool), make(map[string]bool)}
					ast.Inspect(node, func(child ast.Node) bool {
						comparison, ok := child.(*ast.BinaryExpr)
						if !ok || (comparison.Op != token.EQL && comparison.Op != token.NEQ) {
							return true
						}
						index := commandArgumentIndex(comparison.X)
						name, ok := commandLiteral(comparison.Y)
						if index < 0 || !ok {
							index = commandArgumentIndex(comparison.Y)
							name, ok = commandLiteral(comparison.X)
						}
						if index >= 0 && index < len(names) && ok {
							names[index][name] = true
						}
						return true
					})
					if function.Name.Name == "executeComputerSubmission" && len(names[1]) > 0 {
						names[0]["submission"] = true
					}
					for first := range names[0] {
						visit(first)
						for second := range names[1] {
							visit(first, second)
						}
					}
				}
				return true
			})
		}
	}
	for function := range prefixes {
		if !found[function] {
			t.Errorf("help dispatch audit did not find %s", function)
		}
	}
}

func commandArgumentIndex(expression ast.Expr) int {
	if name, ok := expression.(*ast.Ident); ok && name.Name == "verb" {
		return 1
	}
	indexed, ok := expression.(*ast.IndexExpr)
	if !ok {
		return -1
	}
	name, ok := indexed.X.(*ast.Ident)
	if !ok || name.Name != "args" {
		return -1
	}
	literal, ok := indexed.Index.(*ast.BasicLit)
	if !ok || literal.Kind != token.INT {
		return -1
	}
	index, err := strconv.Atoi(literal.Value)
	if err != nil {
		return -1
	}
	return index
}

func commandLiteral(expression ast.Expr) (string, bool) {
	literal, ok := expression.(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return "", false
	}
	value, err := strconv.Unquote(literal.Value)
	return value, err == nil
}

func TestHelpSpellings(t *testing.T) {
	for _, help := range []string{"-h", "--help", "-help", "--h", "--help=true", "--help=false", "-h=false", "--h=true"} {
		for _, command := range [][]string{
			{"services", "grant", "computer", "person"},
			{"services", "grants", "computer"},
			{"services"}, {"run", "envelope"}, {"workflow", "init"},
		} {
			args := append([]string{"--fabric=invalid", "--json"}, command...)
			args = append(args, help)
			t.Run(strings.Join(args, " "), func(t *testing.T) {
				var stdout, stderr bytes.Buffer
				err := run(context.Background(), args, &stdout, &stderr)
				path := helpCommandPath(command)
				if err != nil || stderr.Len() != 0 || !helpOutputMatchesCommand(stdout.Bytes(), path) {
					t.Fatalf("err=%v stdout=%q stderr=%q; want help for %s", err, &stdout, &stderr, path)
				}
			})
		}
	}
	for _, arg := range []string{"help", "---help", "--helper", "help=true"} {
		if isHelpFlag(arg) {
			t.Errorf("%q is not an implicit flag help spelling", arg)
		}
	}
}

func TestParseCommandFlagsExplicitRoot(t *testing.T) {
	for _, root := range []bool{false, true} {
		// Name must not select routing: a subcommand may even be named wefty.
		flags := flag.NewFlagSet("wefty", flag.ContinueOnError)
		var output bytes.Buffer
		flags.SetOutput(&output)
		err := parseCommandFlags(flags, []string{"resource", "--help"}, root)
		if root {
			if err != nil || flags.NArg() != 2 {
				t.Fatalf("root reordered command args: %v %v", flags.Args(), err)
			}
		} else if !errors.Is(err, flag.ErrHelp) {
			t.Fatalf("subcommand help ignored because of its name: %v", err)
		}
	}
}
