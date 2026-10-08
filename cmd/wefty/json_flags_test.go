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
		ast.Inspect(source, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			receiver, ok := selector.X.(*ast.Ident)
			if !ok || receiver.Name != "flags" {
				return true
			}
			method := selector.Sel.Name
			index := 0
			switch method {
			case "Bool", "String", "Int", "Int64", "Uint64", "Float64", "Duration":
			case "BoolVar", "StringVar", "IntVar", "Int64Var", "Uint64Var", "Float64Var", "DurationVar", "Var":
				index = 1
			default:
				return true
			}
			literal, ok := call.Args[index].(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				t.Errorf("nonliteral flag name in %s", file)
				return true
			}
			name, err := strconv.Unquote(literal.Value)
			if err != nil {
				t.Fatal(err)
			}
			if method == "Bool" || method == "BoolVar" {
				if name == "wait" {
					// This one name is overloaded: duration on mutations,
					// boolean on grant/revoke. Exercise both boolean commands.
					for _, verb := range []string{"grant", "revoke"} {
						args := []string{"services", verb, "computer", "person", "--wait", "--json"}
						if !hasJSONFlag(args) {
							t.Errorf("boolean --wait swallowed --json: %v", args)
						}
					}
				} else if valueFlags[name] {
					t.Errorf("boolean flag %s in %s appears in jsonValueFlags", name, file)
				}
				return true
			}
			registeredValues[name] = true
			for _, prefix := range []string{"--", "-"} {
				args := []string{"submit", prefix + name, "--json=nope"}
				got, enabled, err := removeBoolFlag(args, "--json")
				if err != nil || enabled || !reflect.DeepEqual(got, args) {
					t.Errorf("%s flag %s lost its value: %v json=%t err=%v", file, name, got, enabled, err)
				}
			}
			return true
		})
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
