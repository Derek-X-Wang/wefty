package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
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
			case "String", "Int", "Int64", "Uint64", "Float64", "Duration":
			case "StringVar", "IntVar", "Int64Var", "Uint64Var", "Float64Var", "DurationVar", "Var":
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
}
