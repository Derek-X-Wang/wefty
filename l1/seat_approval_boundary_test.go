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
// Request receipts, read/close, log retention reads and exact owner revocation
// routes may reach these tables. Log routes only inspect authority references.
// Approval grants, room reservation and starts remain node-only.
func TestSeatApprovalRoutesRemainNodeOnly(t *testing.T) {
	_, files, info := readBoundaryTypes(t)
	calls := newBoundaryCalls(files, info)
	tainted := map[*types.Func]bool{}
	byName := map[string][]*types.Func{}
	for fn := range calls.bodies {
		byName[fn.Name()] = append(byName[fn.Name()], fn)
	}
	// Computer projections may expose the ended-Seat summary. Review that
	// exact leaf as SELECT-only rather than allowing every Computer route to
	// write approval state. Any new local call or SQL write fails this audit.
	readOnlyLeaf := map[*types.Func]bool{}
	for fn, body := range calls.bodies {
		if fn.Name() != "readComputerEndedSeats" {
			continue
		}
		queries := 0
		ast.Inspect(body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			target := calls.function(call.Fun)
			if target != nil && target.Pkg() == fn.Pkg() && calls.bodies[target] != nil {
				t.Errorf("reviewed approval reader calls local function %s", target.Name())
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch selector.Sel.Name {
			case "ExecContext", "Exec", "BeginTx":
				t.Error("reviewed approval reader gained a write door")
			case "QueryContext", "QueryRowContext":
				queries++
				if len(call.Args) < 2 {
					t.Error("approval reader query lacks constant SQL")
					return true
				}
				sql := info.Types[call.Args[1]].Value
				if sql == nil || sql.Kind() != constant.String {
					t.Error("approval reader query must be constant")
					return true
				}
				statement := strings.ToUpper(strings.TrimSpace(constant.StringVal(sql)))
				if !strings.HasPrefix(statement, "SELECT ") || strings.Contains(statement, ";") {
					t.Error("approval reader must contain only one SELECT")
				}
			}
			return true
		})
		if queries != 1 {
			t.Errorf("reviewed approval reader queries=%d, want one", queries)
		}
		readOnlyLeaf[fn] = true
	}
	if len(readOnlyLeaf) != 1 {
		t.Fatal("ended-Seat summary reader was not audited")
	}
	referencesTainted := func(expr ast.Expr) bool {
		fn := calls.function(expr)
		if fn == nil {
			return false
		}
		if readOnlyLeaf[fn] {
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
	reviewedHandler := func(expr ast.Expr, name string) bool {
		fn := calls.function(expr)
		if fn == nil || fn.Name() != name {
			return false
		}
		sig, ok := fn.Type().(*types.Signature)
		if !ok || sig.Recv() == nil {
			return false
		}
		ptr, ok := sig.Recv().Type().(*types.Pointer)
		if !ok {
			return false
		}
		named, ok := ptr.Elem().(*types.Named)
		return ok && named.Obj().Name() == "Server" && named.Obj().Pkg() == fn.Pkg()
	}
	granting := map[*types.Func]bool{}
	for fn := range calls.bodies {
		switch fn.Name() {
		case "BeginSeatApproval", "AnswerSeatApproval", "saveSeatApproval", "ReserveSeatRoom", "reserveSeatRoomTx", "StartSeatRequest":
			granting[fn] = true
		}
	}
	reachesGrant := func(node ast.Node) bool {
		found := false
		ast.Inspect(node, func(n ast.Node) bool {
			if found {
				return false
			}
			expr, ok := n.(ast.Expr)
			if !ok {
				return true
			}
			fn := calls.function(expr)
			if fn == nil {
				return true
			}
			if granting[fn] {
				found = true
				return false
			}
			if calls.bodies[fn] == nil {
				for _, candidate := range byName[fn.Name()] {
					if granting[candidate] {
						found = true
						return false
					}
				}
			}
			return true
		})
		return found
	}
	for changed := true; changed; {
		changed = false
		for fn, body := range calls.bodies {
			if !granting[fn] && reachesGrant(body) {
				granting[fn] = true
				changed = true
			}
		}
	}
	for fn := range readOnlyLeaf {
		if granting[fn] {
			t.Error("approval reader can grant Seat authority")
		}
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
		"GET /v1/seats/{seat_id}/copy-target":   "getSeatCopyTarget",
		"POST /v1/seats/{seat_id}/copy-outcome": "reportSeatCopyOutcome",
		"POST /v1/seats/{seat_id}/release":      "releaseSeat",
		"GET /v1/seats/{seat_id}/logs":          "getSeatLogs",
		"POST /v1/seats/{seat_id}/logs/read":    "markSeatLogRead",
		"POST /v1/seat-requests":                "createSeatCall",
		"GET /v1/seat-requests/{seat_id}":       "getSeatCall",
		"GET /v1/seat-approval-windows":         "listSeatApprovalWindows",
		"POST /v1/seat-approval-windows/close":  "closeSeatApprovalWindows",
	}
	revocations := map[string]string{
		"PUT /v1/computers/{computer_id}/desired-state":  "setComputerDesiredState",
		"POST /v1/computers/{computer_id}/restart":       "restartComputer",
		"POST /v1/computers/{computer_id}/reimage":       "reimageComputer",
		"POST /v1/computers/{computer_id}/storage-reset": "resetComputerStorage",
		"POST /v1/computers/{computer_id}/projections":   "installComputerProjection",
		"POST /v1/computers/{computer_id}/remove":        "removeComputer",
	}
	for route, handler := range revocations {
		allowed[route] = handler
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
			} else if !reviewedHandler(call.Args[1], expected) {
				t.Errorf("approval route %s must use its exact reviewed handler %s", route, expected)
			} else if reachesGrant(calls.resolve(call.Args[1])) {
				t.Errorf("non-node route can grant Seat authority: %s", route)
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
