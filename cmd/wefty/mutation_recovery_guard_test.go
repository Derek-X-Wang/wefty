package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// unrecoveredMutationAllowlist names each production call that sends an L1
// or L3 mutation without mutate (#1563), keyed "file:function sender method
// path" as written in the source. Every other mutation goes through mutate,
// so a committed change whose post-change read expired (#1511) takes its one
// bounded recovery step. L1 writes that answer only from writeChangedJob and
// appliedResourceReadError, and none of the handlers below reaches either;
// L3 passes it on only for a run cancel. Each key must match exactly one call,
// so a new or changed call in an allowlisted function is reviewed again. The
// guard covers the three apiClient senders that skip mutate; raw streaming
// requests (Seat copy, exec capture upload, Seat sync) are outside it.
var unrecoveredMutationAllowlist = map[string]struct{ route, reason string }{
	`client.go:bootstrapAdmin do http.MethodPost "/v1/admin-bootstrap"`: {"POST /v1/admin-bootstrap",
		"L1 bootstrapAdmin never sends mutation_applied"},
	"client.go:mutateAdmin do method path": {"PUT|DELETE /v1/admin-policy/admins/{user_id}",
		"L1 addAdmin and removeAdmin never send mutation_applied"},
	"client.go:mutateComputerGrant resendOnBareSnapshotExpiry http.MethodPut path": {"PUT /v1/computers/{computer_id}/grants/{user_id}",
		"L1 mutateComputerGrant never sends mutation_applied; a bare snapshot expiry resends the same key"},
	"client.go:setNodeClaims do http.MethodPost path": {"POST /v1/nodes/{node_id}/claims",
		"L1 setNodeClaims never sends mutation_applied"},
	"client.go:interruptRestartableAttempt do http.MethodPost path": {"POST /v1/nodes/{node_id}/restartable-attempts/{attempt_id}/interrupt",
		"L1 interruptRestartableAttempt never sends mutation_applied"},
	`client.go:drainNode do http.MethodPost "/v1/nodes/" + url.PathEscape(nodeID) + "/drain"`: {"POST /v1/nodes/{node_id}/drain",
		"L1 operatorDrainNode never sends mutation_applied"},
	`client.go:submitRun doWithResponse http.MethodPost "/v1/runs"`: {"POST /v1/runs (L3)",
		"L3 stores the keyed Run and dispatches it to L1 later, so no L1 post-change answer reaches this call"},
	"client.go:rerun doWithResponse http.MethodPost path": {"POST /v1/runs/{run_id}/rerun (L3)",
		"L3 stores the keyed Run and dispatches it to L1 later, so no L1 post-change answer reaches this call"},
	"client.go:pruneComputerBackup doWithResponse http.MethodPost path": {"POST /v1/computers/{computer_id}/backups/{backup_id}/prune",
		"L1 pruneComputerBackup never sends mutation_applied"},
	"client.go:exportComputerBackup doWithResponse http.MethodPost path": {"POST /v1/computers/{computer_id}/backups/{backup_id}/export",
		"L1 exportComputerBackup never sends mutation_applied"},
	"client.go:importComputerCustody doWithResponse http.MethodPost path": {"POST /v1/custody-exports/{export_id}/import",
		"L1 importComputerCustody never sends mutation_applied"},
	"client.go:mutateComputerSubmission doWithResponse http.MethodPut path": {"PUT /v1/computers/{computer_id}/submission",
		"L1 mutateComputerSubmission never sends mutation_applied"},
	`cp.go:reportCopyOutcome do http.MethodPost "/v1/seats/" + id + "/copy-outcome"`: {"POST /v1/seats/{seat_id}/copy-outcome",
		"L1 reportSeatCopyOutcome never sends mutation_applied"},
	`seat_logs_command.go:executeSeatLogs do http.MethodPost path + "/read"`: {"POST /v1/seats/{seat_id}/logs/read",
		"L1 markSeatLogRead never sends mutation_applied"},
	`seat_release_command.go:executeSeatRelease resendOnBareSnapshotExpiry http.MethodPost "/v1/seats/" + id + "/release"`: {
		"POST /v1/seats/{seat_id}/release", "L1 releaseSeat never sends mutation_applied; a bare snapshot expiry resends the release"},
	`seat_request_command.go:executeSeatRequest do http.MethodPost "/v1/seat-requests"`: {"POST /v1/seat-requests",
		"L1 createSeatCall never sends mutation_applied"},
	`seat_request_command.go:waitSeatCall do http.MethodPost "/v1/seat-requests"`: {"POST /v1/seat-requests",
		"L1 createSeatCall never sends mutation_applied"},
	`seat_window_command.go:executeSeat do http.MethodPost "/v1/seat-approval-windows/close"`: {"POST /v1/seat-approval-windows/close",
		"L1 closeSeatApprovalWindows never sends mutation_applied"},
}

// mutationSenders are the apiClient methods that send a request without
// mutate's recovery step.
var mutationSenders = map[string]bool{"do": true, "doWithResponse": true, "resendOnBareSnapshotExpiry": true}

// mutationTransport is the implementation itself: these apiClient methods in
// client.go call the senders with the caller's method by design.
var mutationTransport = map[string]bool{"do": true, "doWithResponse": true, "resendOnBareSnapshotExpiry": true, "mutate": true}

// unrecoveredMutations lists every non-GET call of a sender, keyed as the
// allowlist is, and every sender taken as a method value, which the guard
// cannot follow. Package-level function literals count under their variable.
func unrecoveredMutations(fset *token.FileSet, files map[string]*ast.File) map[string][]token.Position {
	found := map[string][]token.Position{}
	for name, file := range files {
		for _, declaration := range file.Decls {
			switch declaration := declaration.(type) {
			case *ast.FuncDecl:
				if declaration.Body != nil && !(name == "client.go" && isAPIClientMethod(declaration) && mutationTransport[declaration.Name.Name]) {
					inspectMutations(fset, name+":"+declaration.Name.Name, declaration.Body, found)
				}
			case *ast.GenDecl:
				for _, spec := range declaration.Specs {
					if value, ok := spec.(*ast.ValueSpec); ok && len(value.Names) > 0 {
						for _, expr := range value.Values {
							inspectMutations(fset, name+":"+value.Names[0].Name, expr, found)
						}
					}
				}
			}
		}
	}
	return found
}

func inspectMutations(fset *token.FileSet, owner string, root ast.Node, found map[string][]token.Position) {
	called := map[*ast.SelectorExpr]bool{}
	ast.Inspect(root, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.CallExpr:
			selector, ok := node.Fun.(*ast.SelectorExpr)
			if !ok || !mutationSenders[selector.Sel.Name] {
				return true
			}
			called[selector] = true
			if len(node.Args) < 3 || isMethodGet(node.Args[1]) {
				return true
			}
			key := owner + " " + selector.Sel.Name + " " + types.ExprString(node.Args[1]) + " " + types.ExprString(node.Args[2])
			found[key] = append(found[key], fset.Position(node.Pos()))
		case *ast.SelectorExpr:
			if mutationSenders[node.Sel.Name] && !called[node] {
				key := owner + " " + node.Sel.Name + " (method value)"
				found[key] = append(found[key], fset.Position(node.Pos()))
			}
		}
		return true
	})
}

func isAPIClientMethod(fn *ast.FuncDecl) bool {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return false
	}
	receiver := fn.Recv.List[0].Type
	if star, ok := receiver.(*ast.StarExpr); ok {
		receiver = star.X
	}
	ident, ok := receiver.(*ast.Ident)
	return ok && ident.Name == "apiClient"
}

func isMethodGet(expr ast.Expr) bool {
	text := types.ExprString(expr)
	return text == "http.MethodGet" || text == `"GET"`
}

// TestEveryCLIMutationTakesTheCommittedReadRecoveryStep fails when a CLI
// mutation skips mutate and is not on the allowlist (#1563), and when an
// allowlist entry no longer names a live call.
func TestEveryCLIMutationTakesTheCommittedReadRecoveryStep(t *testing.T) {
	t.Parallel()

	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	files := map[string]*ast.File{}
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, path, raw, 0)
		if err != nil {
			t.Fatal(err)
		}
		files[path] = file
	}
	if len(files) < 20 {
		t.Fatalf("incomplete CLI source inventory: %d files", len(files))
	}
	if _, ok := files["client.go"]; !ok {
		t.Fatal("client.go is missing from the CLI source inventory")
	}

	found := unrecoveredMutations(fset, files)
	keys := make([]string, 0, len(found))
	for key := range found {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, ok := unrecoveredMutationAllowlist[key]; !ok {
			t.Errorf("%s sends a mutation without mutate at %v: send it through mutate with its postChangeRecovery plan, "+
				"or add it to unrecoveredMutationAllowlist with its route and why no recovery is needed", key, found[key])
		} else if len(found[key]) != 1 {
			t.Errorf("allowlist entry %s matches %d calls at %v; each entry covers exactly one call", key, len(found[key]), found[key])
		}
	}
	for key, entry := range unrecoveredMutationAllowlist {
		if entry.route == "" || entry.reason == "" {
			t.Errorf("allowlist entry %s needs a route and a reason", key)
		}
		if _, ok := found[key]; !ok {
			t.Errorf("allowlist entry %s (%s) names no mutation call; remove it", key, entry.route)
		}
	}
}

// The guard reports each way a new mutation can skip mutate, and passes
// mutate, a GET and the transport's own bodies.
func TestUnrecoveredMutationsFindsAPlainMutation(t *testing.T) {
	t.Parallel()

	const source = `package main

func (c *apiClient) do(ctx context.Context, method, path string) error {
	return c.doWithResponse(ctx, method, path)
}

func (c *apiClient) cancelThing(ctx context.Context) error {
	return c.do(ctx, http.MethodPost, "/v1/things/cancel")
}

func (c *apiClients) newMutation(ctx context.Context) error {
	return c.l1.do(ctx, http.MethodPost, "/v1/new")
}

func (c *apiClients) newVariableMethod(ctx context.Context, method string) error {
	_, err := c.l3.doWithResponse(ctx, method, "/v1/other")
	return err
}

func (c *apiClients) newMethodValue(ctx context.Context) error {
	send := c.l1.do
	return send(ctx, http.MethodPost, "/v1/hidden")
}

var registeredCommand = func(ctx context.Context, c *apiClients) error {
	return c.l1.do(ctx, http.MethodDelete, "/v1/registered")
}

func (c *apiClients) guarded(ctx context.Context) error {
	_, err := c.l1.mutate(ctx, resendWithKey, http.MethodPost, "/v1/jobs")
	return err
}

func (c *apiClients) read(ctx context.Context) error {
	if err := c.l1.do(ctx, "GET", "/v1/jobs"); err != nil {
		return err
	}
	return c.l1.do(ctx, http.MethodGet, "/v1/jobs")
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "client.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := unrecoveredMutations(fset, map[string]*ast.File{"client.go": file})
	keys := make([]string, 0, len(found))
	for key := range found {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	want := []string{
		`client.go:cancelThing do http.MethodPost "/v1/things/cancel"`,
		"client.go:newMethodValue do (method value)",
		`client.go:newMutation do http.MethodPost "/v1/new"`,
		`client.go:newVariableMethod doWithResponse method "/v1/other"`,
		`client.go:registeredCommand do http.MethodDelete "/v1/registered"`,
	}
	if got := strings.Join(keys, "\n"); got != strings.Join(want, "\n") {
		t.Fatalf("unrecovered mutations:\n%s\nwant:\n%s", got, strings.Join(want, "\n"))
	}
}
