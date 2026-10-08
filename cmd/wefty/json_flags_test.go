package main

import (
	"flag"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/runner/lima"
)

// Protect new command flags, including flags compiled only on another OS, from
// having their values stolen by the global JSON parser.
func TestJSONValueFlagVocabulary(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	helperFiles, err := filepath.Glob("../../internal/workflowhelper/*.go")
	if err != nil {
		t.Fatal(err)
	}
	valueFlags := make(map[string]bool)
	for _, name := range strings.Fields(jsonValueFlags) {
		valueFlags[name] = true
	}
	registeredValues := make(map[string]bool)
	for _, file := range append(files, helperFiles...) {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		source, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		auditJSONFlagSource(file, source, valueFlags, func(name string) {
			registeredValues[name] = true
			for _, prefix := range []string{"--", "-"} {
				args := []string{"submit", prefix + name, "--json=nope"}
				got, enabled, err := removeBoolFlag(args, "--json")
				if err != nil || enabled || !reflect.DeepEqual(got, args) {
					t.Errorf("%s flag %s lost its value: %v json=%t err=%v", file, name, got, enabled, err)
				}
			}
		}, t.Errorf)
	}
	// Imported registrations do not appear in the CLI AST. Check the real
	// Lima FlagSet, including its constant names and value/boolean semantics.
	flags := flag.NewFlagSet("lima sizing", flag.ContinueOnError)
	lima.BindSizingFlags(flags, lima.Sizing{})
	flags.VisitAll(func(registered *flag.Flag) {
		if boolean, ok := registered.Value.(interface{ IsBoolFlag() bool }); ok && boolean.IsBoolFlag() {
			if valueFlags[registered.Name] {
				t.Errorf("boolean Lima flag %s appears in jsonValueFlags", registered.Name)
			}
			return
		}
		registeredValues[registered.Name] = true
		args := []string{"node", "setup-oci", "--" + registered.Name, "--json=nope"}
		got, enabled, err := removeBoolFlag(args, "--json")
		if err != nil || enabled || !reflect.DeepEqual(got, args) {
			t.Errorf("Lima flag %s lost its value: %v json=%t err=%v", registered.Name, got, enabled, err)
		}
	})
	for name := range valueFlags {
		if !registeredValues[name] {
			t.Errorf("jsonValueFlags contains unregistered value flag %s", name)
		}
	}
}

// Keep imported binders explicit: each entry must have its registrations audited
// against the real FlagSet in TestJSONValueFlagVocabulary. Import paths
// ensure package aliases cannot bypass the allowlist.
var auditedJSONFlagBinders = map[string]bool{
	"github.com/Derek-X-Wang/wefty/runner/lima.BindSizingFlags": true,
}

func auditJSONFlagSource(file string, source *ast.File, valueFlags map[string]bool, visit func(string), report func(string, ...any)) {
	imports := make(map[string]string)
	for _, imported := range source.Imports {
		path, err := strconv.Unquote(imported.Path.Value)
		if err != nil {
			report("%v", err)
			continue
		}
		name := filepath.Base(path)
		if imported.Name != nil {
			name = imported.Name.Name
		}
		if name == "." {
			report("dot import in %s prevents imported flag binder audit", file)
		}
		imports[name] = path
	}
	importedFunctions := make(map[*ast.Object]map[string]bool)
	flagSets := map[string]bool{"flags": true}
	factories := map[string]bool{"newFlagSet": true}
	isFlagSetType := func(expr ast.Expr) bool {
		pointer, ok := expr.(*ast.StarExpr)
		if !ok {
			return false
		}
		selector, ok := pointer.X.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		pkg, ok := selector.X.(*ast.Ident)
		return ok && imports[pkg.Name] == "flag" && selector.Sel.Name == "FlagSet"
	}
	ast.Inspect(source, func(node ast.Node) bool {
		if field, ok := node.(*ast.Field); ok && isFlagSetType(field.Type) {
			for _, name := range field.Names {
				flagSets[name.Name] = true
			}
		}
		if declaration, ok := node.(*ast.FuncDecl); ok && declaration.Type.Results != nil {
			for _, result := range declaration.Type.Results.List {
				if isFlagSetType(result.Type) {
					factories[declaration.Name.Name] = true
				}
			}
		}
		return true
	})
	isFlagSet := func(expr ast.Expr) bool {
		if name, ok := expr.(*ast.Ident); ok {
			return flagSets[name.Name]
		}
		if selector, ok := expr.(*ast.SelectorExpr); ok {
			return flagSets[selector.Sel.Name]
		}
		call, ok := expr.(*ast.CallExpr)
		if !ok {
			return false
		}
		if name, ok := call.Fun.(*ast.Ident); ok {
			return factories[name.Name]
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		pkg, ok := selector.X.(*ast.Ident)
		return ok && imports[pkg.Name] == "flag" && selector.Sel.Name == "NewFlagSet"
	}
	// Discover aliases as well as constructors, including OS-specific files.
	for changed := true; changed; {
		changed = false
		ast.Inspect(source, func(node ast.Node) bool {
			var names []ast.Expr
			var values []ast.Expr
			switch declaration := node.(type) {
			case *ast.AssignStmt:
				names, values = declaration.Lhs, declaration.Rhs
			case *ast.ValueSpec:
				for _, name := range declaration.Names {
					names = append(names, name)
				}
				values = declaration.Values
				if isFlagSetType(declaration.Type) {
					for _, name := range declaration.Names {
						flagSets[name.Name] = true
					}
				}
			}
			for i, value := range values {
				if i >= len(names) {
					continue
				}
				if name, ok := names[i].(*ast.Ident); ok && name.Obj != nil {
					references := make(map[string]bool)
					if selector, ok := value.(*ast.SelectorExpr); ok {
						if pkg, ok := selector.X.(*ast.Ident); ok && imports[pkg.Name] != "" {
							references[imports[pkg.Name]+"."+selector.Sel.Name] = true
						}
					}
					if alias, ok := value.(*ast.Ident); ok {
						for reference := range importedFunctions[alias.Obj] {
							references[reference] = true
						}
					}
					for reference := range references {
						if importedFunctions[name.Obj] == nil {
							importedFunctions[name.Obj] = make(map[string]bool)
						}
						if !importedFunctions[name.Obj][reference] {
							importedFunctions[name.Obj][reference] = true
							changed = true
						}
					}
				}
				if !isFlagSet(value) {
					continue
				}
				if name, ok := names[i].(*ast.Ident); ok && !flagSets[name.Name] {
					flagSets[name.Name] = true
					changed = true
				}
			}
			return true
		})
	}
	ast.Inspect(source, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if function, ok := call.Fun.(*ast.Ident); ok && len(importedFunctions[function.Obj]) > 0 {
			for _, arg := range call.Args {
				if !isFlagSet(arg) {
					continue
				}
				for reference := range importedFunctions[function.Obj] {
					if !auditedJSONFlagBinders[reference] {
						report("unaudited imported flag binder %s in %s", reference, file)
					}
				}
			}
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		receiver, named := selector.X.(*ast.Ident)
		if named && imports[receiver.Name] != "" {
			for _, arg := range call.Args {
				if isFlagSet(arg) && !auditedJSONFlagBinders[imports[receiver.Name]+"."+selector.Sel.Name] {
					report("unaudited imported flag binder %s.%s in %s", imports[receiver.Name], selector.Sel.Name, file)
				}
			}
			return true
		}
		method, index, arity := selector.Sel.Name, 0, 3
		registration := true
		switch method {
		case "Bool", "String", "Int", "Int64", "Uint", "Uint64", "Float64", "Duration", "Func", "BoolFunc":
		case "BoolVar", "StringVar", "IntVar", "Int64Var", "UintVar", "Uint64Var", "Float64Var", "DurationVar", "TextVar":
			index, arity = 1, 4
		case "Var":
			index = 1
		default:
			registration = false
		}
		// Registration arity also catches receivers the syntax-only type
		// discovery cannot resolve (for example an inferred struct field).
		if isFlagSet(selector.X) || (registration && len(call.Args) >= arity) {
			if !named || receiver.Name != "flags" {
				report("FlagSet method %s in %s must use receiver flags", method, file)
				return true
			}
		} else {
			return true
		}
		if !registration {
			return true
		}
		if len(call.Args) <= index {
			report("missing flag name in %s", file)
			return true
		}
		literal, ok := call.Args[index].(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			report("nonliteral flag name in %s", file)
			return true
		}
		name, err := strconv.Unquote(literal.Value)
		if err != nil {
			report("%v", err)
			return true
		}
		if method == "Bool" || method == "BoolVar" || method == "BoolFunc" {
			if name == "wait" && file == "access_command.go" {
				for _, verb := range []string{"grant", "revoke"} {
					if !hasJSONFlag([]string{"services", verb, "computer", "person", "--wait", "--json"}) {
						report("boolean --wait swallowed --json for %s", verb)
					}
				}
			} else if valueFlags[name] {
				report("boolean flag %s in %s appears in jsonValueFlags", name, file)
			}
			return true
		}
		visit(name)
		return true
	})
}

func TestJSONValueFlagGuardRejectsHoles(t *testing.T) {
	for _, fixture := range []struct{ name, file, source string }{
		{"other receiver", "command.go", `package main; import "flag"; func command() { other := flag.NewFlagSet("x", 0); other.String("summary", "", "") }`},
		{"other receiver method", "command.go", `package main; import "flag"; func command(other *flag.FlagSet) { other.Parse(nil) }`},
		{"wait outside access", "command.go", `package main; import "flag"; func command(flags *flag.FlagSet) { flags.Bool("wait", false, "") }`},
		{"imported function alias", "command.go", `package main; import "example.com/binder"; import "flag"; func command(flags *flag.FlagSet) { bind := binder.Bind; bind(flags) }`},
		{"new imported binder", "command.go", `package main; import "example.com/binder"; import "flag"; func command(flags *flag.FlagSet) { binder.Bind(flags) }`},
		{"aliased imported binder", "command.go", `package main; import alias "example.com/binder"; import "flag"; func command(flags *flag.FlagSet) { alias.Bind(flags) }`},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			source, err := parser.ParseFile(token.NewFileSet(), fixture.file, fixture.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			rejected := false
			auditJSONFlagSource(fixture.file, source, map[string]bool{"wait": true}, func(string) {}, func(string, ...any) { rejected = true })
			if !rejected {
				t.Fatal("guard accepted the hole")
			}
		})
	}
}

func TestJSONValueFlagGuardTextVar(t *testing.T) {
	for _, test := range []struct {
		name, source string
		wantRejected bool
	}{
		{"flags receiver", `package main; import "flag"; func command(flags *flag.FlagSet) { flags.TextVar(&value, "text", value, "") }`, false},
		{"inferred field receiver", `package main; func command() { holder.other.TextVar(&value, "text", value, "") }`, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, err := parser.ParseFile(token.NewFileSet(), "command.go", test.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			var visited []string
			rejected := false
			auditJSONFlagSource("command.go", source, map[string]bool{"text": true}, func(name string) {
				visited = append(visited, name)
			}, func(string, ...any) { rejected = true })
			if rejected != test.wantRejected || (!rejected && !reflect.DeepEqual(visited, []string{"text"})) {
				t.Fatalf("TextVar audit: visited=%v rejected=%t, want rejected=%t", visited, rejected, test.wantRejected)
			}
		})
	}
}
