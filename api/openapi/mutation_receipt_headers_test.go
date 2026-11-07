package openapi_test

import (
	"go/ast"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// #1554: the CLI's receipt plan reads Mutation-Applied, so every route whose
// handler writes it must publish it on each success response.
func TestComputerMutationReceiptHeadersArePublished(t *testing.T) {
	t.Parallel()

	writers := map[string]bool{}
	for _, file := range parseSources(t, "l1") {
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if call, ok := node.(*ast.CallExpr); ok {
					if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "writeComputerMutationHeaders" {
						writers[fn.Name.Name] = true
					}
				}
				return true
			})
		}
	}
	if len(writers) == 0 {
		t.Fatal("found no handler that calls writeComputerMutationHeaders")
	}

	raw, err := os.ReadFile(filepath.Join("..", "..", "l1", "server.go"))
	if err != nil {
		t.Fatal(err)
	}
	registration := regexp.MustCompile(`\.HandleFunc\("([A-Z]+) (/[^"]*)", s\.(\w+)\)`)
	routed := map[string]bool{}
	paths := object(t, readObject(t, clientDoc)["paths"], clientDoc+" paths")
	for _, match := range registration.FindAllStringSubmatch(string(raw), -1) {
		method, route, handler := strings.ToLower(match[1]), match[2], match[3]
		if !writers[handler] {
			continue
		}
		routed[handler] = true
		operation, ok := object(t, paths[route], route)[method]
		if !ok {
			t.Errorf("%s %s (%s) is not published in %s", match[1], route, handler, clientDoc)
			continue
		}
		successes := 0
		for status, response := range object(t, object(t, operation, route)["responses"], route+" responses") {
			if !strings.HasPrefix(status, "2") {
				continue
			}
			successes++
			headers, _ := object(t, response, route+" "+status)["headers"].(map[string]any)
			if _, ok := headers["Mutation-Applied"]; !ok {
				t.Errorf("%s %s %s: handler %s writes Mutation-Applied, but the response does not declare it", match[1], route, status, handler)
			}
		}
		if successes == 0 {
			t.Errorf("%s %s publishes no success response", match[1], route)
		}
	}
	for handler := range writers {
		if !routed[handler] {
			t.Errorf("handler %s writes Mutation-Applied but has no plain HandleFunc registration this check can read", handler)
		}
	}
}
