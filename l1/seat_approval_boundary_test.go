package l1

import (
	"go/ast"
	"go/constant"
	"go/types"
	"strings"
	"testing"
)

// AP-4 follows the production call graph from every registered HTTP handler
// to SQL that touches approval tables, including helpers and read callbacks.
// Only the read and close routes may reach those tables without the node mux.
func TestSeatApprovalRoutesRemainNodeOnly(t *testing.T) {
	_, files, info := readBoundaryTypes(t)
	calls := newBoundaryCalls(files, info)
	tainted := map[*types.Func]bool{}
	byName := map[string][]*types.Func{}
	for fn := range calls.bodies {
		byName[fn.Name()] = append(byName[fn.Name()], fn)
	}
	referencesTainted := func(expr ast.Expr) bool {
		fn := calls.function(expr)
		if fn == nil {
			return false
		}
		if tainted[fn] {
			return true
		}
		// Interface calls resolve to the concrete package implementations.
		if calls.bodies[fn] == nil {
			for _, candidate := range byName[fn.Name()] {
				if tainted[candidate] {
					return true
				}
			}
		}
		return false
	}
	touches := func(node ast.Node) bool {
		found := false
		ast.Inspect(node, func(n ast.Node) bool {
			if found {
				return false
			}
			if expr, ok := n.(ast.Expr); ok {
				value := info.Types[expr].Value
				if value != nil && value.Kind() == constant.String {
					s := constant.StringVal(value)
					if strings.Contains(s, "seat_approvals") || strings.Contains(s, "seat_approval_windows") {
						found = true
						return false
					}
				}
				if referencesTainted(expr) {
					found = true
					return false
				}
			}
			return true
		})
		return found
	}
	for changed := true; changed; {
		changed = false
		for fn, body := range calls.bodies {
			if !tainted[fn] && touches(body) {
				tainted[fn] = true
				changed = true
			}
		}
	}
	if len(tainted) < 6 {
		t.Fatalf("incomplete approval SQL call graph: %d functions", len(tainted))
	}
	agentMux := ""
	agentMounts := 0
	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Handle" || len(call.Args) != 2 {
				return true
			}
			auth, ok := call.Args[1].(*ast.CallExpr)
			if !ok || len(auth.Args) != 2 {
				return true
			}
			method, ok := auth.Fun.(*ast.SelectorExpr)
			if !ok || method.Sel.Name != "authorize" {
				return true
			}
			principal, ok := auth.Args[0].(*ast.Ident)
			if !ok || principal.Name != "agentPrincipal" {
				return true
			}
			mux, ok := auth.Args[1].(*ast.Ident)
			if !ok {
				t.Fatal("node principal must wrap a named mux")
			}
			pattern := info.Types[call.Args[0]].Value
			if pattern == nil || constant.StringVal(pattern) != "/v1/agent/" {
				t.Fatal("node mux must remain under /v1/agent/")
			}
			agentMux = mux.Name
			agentMounts++
			return true
		})
	}
	if agentMounts != 1 {
		t.Fatalf("node-principal mux mounts=%d", agentMounts)
	}
	allowed := map[string]string{
		"GET /v1/seat-approval-windows":        "listSeatApprovalWindows",
		"POST /v1/seat-approval-windows/close": "closeSeatApprovalWindows",
	}
	seen := map[string]bool{}
	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || len(call.Args) != 2 {
				return true
			}
			if sel.Sel.Name == "Handle" {
				// A second mount of the node mux must not expose it through a client
				// principal or a handler without authorization.
				ast.Inspect(call.Args[1], func(n ast.Node) bool {
					if id, ok := n.(*ast.Ident); ok && id.Name == agentMux {
						auth, ok := call.Args[1].(*ast.CallExpr)
						if !ok || len(auth.Args) != 2 {
							t.Error("node mux mounted without node authorization")
							return false
						}
						principal, ok := auth.Args[0].(*ast.Ident)
						if !ok || principal.Name != "agentPrincipal" {
							t.Error("node mux mounted under another principal")
						}
					}
					return true
				})
			}
			if sel.Sel.Name != "HandleFunc" && sel.Sel.Name != "Handle" {
				return true
			}
			if !touches(calls.resolve(call.Args[1])) {
				return true
			}
			mux, ok := sel.X.(*ast.Ident)
			if ok && mux.Name == agentMux {
				return true
			}
			pattern := info.Types[call.Args[0]].Value
			if pattern == nil || pattern.Kind() != constant.String {
				t.Error("approval route must have a constant, reviewed pattern")
				return true
			}
			route := constant.StringVal(pattern)
			expected, allowedRoute := allowed[route]
			if !allowedRoute {
				t.Errorf("non-node route reaches approval SQL: %s", route)
			} else if handlers := byName[expected]; len(handlers) != 1 || calls.function(call.Args[1]) != handlers[0] {
				t.Errorf("approval route %s must use its exact reviewed handler %s", route, expected)
			} else {
				seen[route] = true
			}
			return true
		})
	}
	for route := range allowed {
		if !seen[route] {
			t.Errorf("approval route was not traced to SQL: %s", route)
		}
	}
}
