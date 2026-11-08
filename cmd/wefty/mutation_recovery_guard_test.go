package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// unrecoveredMutationAllowlist names each production function that sends an
// L1 or L3 mutation without mutate (#1563). Every other mutation goes through
// mutate, so a committed change whose post-change read expired (#1511) takes
// its one bounded recovery step. L1 writes that answer only from
// writeChangedJob and appliedResourceReadError, and none of the handlers
// below reaches either; L3 passes it on only for a run cancel.
var unrecoveredMutationAllowlist = map[string]struct{ route, reason string }{
	"client.go:bootstrapAdmin": {"POST /v1/admin-bootstrap", "L1 bootstrapAdmin never sends mutation_applied"},
	"client.go:mutateAdmin":    {"PUT|DELETE /v1/admin-policy/admins/{user_id}", "L1 addAdmin and removeAdmin never send mutation_applied"},
	"client.go:mutateComputerGrant": {"PUT /v1/computers/{computer_id}/grants/{user_id}",
		"L1 mutateComputerGrant never sends mutation_applied; a bare snapshot expiry resends the same key"},
	"client.go:setNodeClaims": {"POST /v1/nodes/{node_id}/claims", "L1 setNodeClaims never sends mutation_applied"},
	"client.go:interruptRestartableAttempt": {"POST /v1/nodes/{node_id}/restartable-attempts/{attempt_id}/interrupt",
		"L1 interruptRestartableAttempt never sends mutation_applied"},
	"client.go:drainNode": {"POST /v1/nodes/{node_id}/drain", "L1 operatorDrainNode never sends mutation_applied"},
	"client.go:submitRun": {"POST /v1/runs (L3)",
		"L3 stores the keyed Run and dispatches it to L1 later, so no L1 post-change answer reaches this call"},
	"client.go:rerun": {"POST /v1/runs/{run_id}/rerun (L3)",
		"L3 stores the keyed Run and dispatches it to L1 later, so no L1 post-change answer reaches this call"},
	"client.go:pruneComputerBackup": {"POST /v1/computers/{computer_id}/backups/{backup_id}/prune",
		"L1 pruneComputerBackup never sends mutation_applied"},
	"client.go:exportComputerBackup": {"POST /v1/computers/{computer_id}/backups/{backup_id}/export",
		"L1 exportComputerBackup never sends mutation_applied"},
	"client.go:importComputerCustody": {"POST /v1/custody-exports/{export_id}/import",
		"L1 importComputerCustody never sends mutation_applied"},
	"client.go:mutateComputerSubmission": {"PUT /v1/computers/{computer_id}/submission",
		"L1 mutateComputerSubmission never sends mutation_applied"},
	"cp.go:reportCopyOutcome": {"POST /v1/seats/{seat_id}/copy-outcome", "L1 reportSeatCopyOutcome never sends mutation_applied"},
	"seat_logs_command.go:executeSeatLogs": {"POST /v1/seats/{seat_id}/logs/read",
		"L1 markSeatLogRead never sends mutation_applied"},
	"seat_release_command.go:executeSeatRelease": {"POST /v1/seats/{seat_id}/release",
		"L1 releaseSeat never sends mutation_applied; a bare snapshot expiry resends the release"},
	"seat_request_command.go:executeSeatRequest": {"POST /v1/seat-requests", "L1 createSeatCall never sends mutation_applied"},
	"seat_request_command.go:waitSeatCall":       {"POST /v1/seat-requests", "L1 createSeatCall never sends mutation_applied"},
	"seat_window_command.go:executeSeat": {"POST /v1/seat-approval-windows/close",
		"L1 closeSeatApprovalWindows never sends mutation_applied"},
}

// unrecoveredMutations lists, by "file:function", every call that sends a
// non-GET request through the apiClient senders that skip mutate. The
// senders' own bodies are the implementation, not call sites.
func unrecoveredMutations(fset *token.FileSet, files map[string]*ast.File) map[string][]token.Position {
	senders := map[string]bool{"do": true, "doWithResponse": true, "resendOnBareSnapshotExpiry": true}
	found := map[string][]token.Position{}
	for name, file := range files {
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Body == nil || isAPIClientMethod(fn) {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok || len(call.Args) < 2 {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || !senders[selector.Sel.Name] || isMethodGet(call.Args[1]) {
					return true
				}
				key := name + ":" + fn.Name.Name
				found[key] = append(found[key], fset.Position(call.Pos()))
				return true
			})
		}
	}
	return found
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
	selector, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	return ok && pkg.Name == "http" && selector.Sel.Name == "MethodGet"
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

// The guard reports a new plain mutation, and passes mutate, a GET and the
// senders' own bodies.
func TestUnrecoveredMutationsFindsAPlainMutation(t *testing.T) {
	t.Parallel()

	const source = `package main

func (c *apiClient) do(ctx context.Context, method, path string) error {
	return c.doWithResponse(ctx, method, path)
}

func (c *apiClients) newMutation(ctx context.Context) error {
	return c.l1.do(ctx, http.MethodPost, "/v1/new")
}

func (c *apiClients) newVariableMethod(ctx context.Context, method string) error {
	_, err := c.l3.doWithResponse(ctx, method, "/v1/other")
	return err
}

func (c *apiClients) guarded(ctx context.Context) error {
	_, err := c.l1.mutate(ctx, resendWithKey, http.MethodPost, "/v1/jobs")
	return err
}

func (c *apiClients) read(ctx context.Context) error {
	return c.l1.do(ctx, http.MethodGet, "/v1/jobs")
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "new.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := unrecoveredMutations(fset, map[string]*ast.File{"new.go": file})
	keys := make([]string, 0, len(found))
	for key := range found {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if got := strings.Join(keys, ","); got != "new.go:newMutation,new.go:newVariableMethod" {
		t.Fatalf("unrecovered mutations = %s", got)
	}
}
