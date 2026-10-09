package l1

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// Resolve method/function aliases by compiler identity. Identifier spelling
// alone must neither admit a disguised client nor refuse a harmless lookalike.
type boundaryCalls struct {
	info     *types.Info
	bodies   map[*types.Func]*ast.BlockStmt
	bindings map[types.Object]ast.Expr
}

func newBoundaryCalls(files []*ast.File, info *types.Info) *boundaryCalls {
	calls := &boundaryCalls{info: info, bodies: map[*types.Func]*ast.BlockStmt{}, bindings: map[types.Object]ast.Expr{}}
	for _, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.FuncDecl:
				if fn, ok := info.Defs[n.Name].(*types.Func); ok {
					calls.bodies[fn] = n.Body
				}
			case *ast.AssignStmt:
				if len(n.Lhs) == len(n.Rhs) {
					for i, lhs := range n.Lhs {
						if id, ok := lhs.(*ast.Ident); ok {
							obj := info.ObjectOf(id)
							calls.bindings[obj] = n.Rhs[i]
						}
					}
				}
			case *ast.ValueSpec:
				if len(n.Names) == len(n.Values) {
					for i, id := range n.Names {
						calls.bindings[info.Defs[id]] = n.Values[i]
					}
				}
			}
			return true
		})
	}
	return calls
}

func (c *boundaryCalls) resolve(expr ast.Expr) ast.Expr {
	seen := map[types.Object]bool{}
	for {
		if paren, ok := expr.(*ast.ParenExpr); ok {
			expr = paren.X
			continue
		}
		id, ok := expr.(*ast.Ident)
		if !ok {
			return expr
		}
		obj := c.info.ObjectOf(id)
		binding := c.bindings[obj]
		if binding == nil || seen[obj] {
			return expr
		}
		seen[obj] = true
		expr = binding
	}
}

func (c *boundaryCalls) function(expr ast.Expr) *types.Func {
	switch n := c.resolve(expr).(type) {
	case *ast.Ident:
		fn, _ := c.info.ObjectOf(n).(*types.Func)
		return fn
	case *ast.SelectorExpr:
		if selection := c.info.Selections[n]; selection != nil {
			fn, _ := selection.Obj().(*types.Func)
			return fn
		}
		fn, _ := c.info.ObjectOf(n.Sel).(*types.Func)
		return fn
	}
	return nil
}

func boundaryNamed(typ types.Type) *types.Named {
	if typ == nil {
		return nil
	}
	typ = types.Unalias(typ)
	if ptr, ok := typ.(*types.Pointer); ok {
		typ = types.Unalias(ptr.Elem())
	}
	named, _ := typ.(*types.Named)
	return named
}

func boundaryStoreDoor(fn *types.Func, names ...string) bool {
	if fn == nil {
		return false
	}
	recv := fn.Type().(*types.Signature).Recv()
	if recv == nil {
		return false
	}
	named := boundaryNamed(recv.Type())
	if named == nil || named.Obj().Name() != "Store" {
		return false
	}
	// The fixture's package models the exact L1 receiver, without SQL or
	// network dependencies. Production doors must belong to L1.
	path := named.Obj().Pkg().Path()
	if path != "github.com/Derek-X-Wang/wefty/l1" && path != "fixture" {
		return false
	}
	for _, name := range names {
		if fn.Name() == name {
			return true
		}
	}
	return false
}

func boundaryExternal(fn *types.Func) bool {
	if fn == nil || fn.Pkg() == nil {
		return false
	}
	path, name := fn.Pkg().Path(), fn.Name()
	if strings.HasPrefix(path, "github.com/Derek-X-Wang/wefty/l3") {
		return true
	}
	if recv := fn.Type().(*types.Signature).Recv(); recv != nil {
		if named := boundaryNamed(recv.Type()); named != nil {
			obj := named.Obj()
			if (obj.Pkg().Path() == "github.com/Derek-X-Wang/wefty/l1" || obj.Pkg().Path() == "fixture") &&
				(obj.Name() == "ComputerTokenRevoker" || obj.Name() == "ComputerTokenRevocationClient") {
				return true
			}
		}
	}
	if strings.HasPrefix(path, "github.com/Derek-X-Wang/wefty/fabric") {
		switch name {
		case "Dial", "Listen", "WhoIs", "Provision", "Deprovision":
			return true
		}
	}
	switch path {
	case "net/http":
		switch name {
		case "Do", "Get", "Head", "Post", "PostForm", "RoundTrip", "Serve", "ServeTLS", "ListenAndServe", "ListenAndServeTLS", "Write", "WriteHeader", "Flush", "Hijack":
			return true
		}
	case "net/rpc", "net/smtp":
		return true
	case "net", "crypto/tls":
		// Parsing and address formatting are local. These calls contact peers,
		// resolve names, or operate an established network connection.
		return strings.HasPrefix(name, "Dial") || strings.HasPrefix(name, "Listen") ||
			strings.HasPrefix(name, "Lookup") || strings.HasPrefix(name, "Read") || strings.HasPrefix(name, "Write") ||
			strings.HasPrefix(name, "Accept") || name == "Call" || name == "Go" || name == "SendMail" || name == "Handshake" || name == "HandshakeContext"
	}
	return false
}

// Generic I/O helpers can also touch the network through a typed connection
// argument. Follow local aliases so io.Writer(c) does not disguise net.Conn.
func (c *boundaryCalls) networkArgument(expr ast.Expr) bool {
	expr = c.resolve(expr)
	if named := boundaryNamed(c.info.TypeOf(expr)); named != nil && named.Obj().Pkg() != nil {
		path, name := named.Obj().Pkg().Path(), named.Obj().Name()
		if path == "net" && (strings.HasSuffix(name, "Conn") || strings.HasSuffix(name, "Listener")) ||
			path == "crypto/tls" && name == "Conn" || path == "net/http" && name == "ResponseWriter" {
			return true
		}
	}
	if cast, ok := expr.(*ast.CallExpr); ok && c.info.Types[cast.Fun].IsType() {
		for _, arg := range cast.Args {
			if c.networkArgument(arg) {
				return true
			}
		}
	}
	return false
}

func (c *boundaryCalls) genericNetworkIO(call *ast.CallExpr, fn *types.Func) bool {
	for _, arg := range call.Args {
		if c.networkArgument(arg) {
			return true
		}
	}
	return false
}

// Follow local helpers, method aliases and concrete implementations of local
// interfaces (notably readModel). This is deliberately conservative: every
// implementation reachable through an interface must respect the boundary.
func (c *boundaryCalls) externalCalls(root ast.Node) []*ast.CallExpr {
	var violations []*ast.CallExpr
	reported := map[token.Pos]bool{}
	seen := map[*types.Func]bool{}
	var visit func(ast.Node)
	visit = func(node ast.Node) {
		if node == nil {
			return
		}
		ast.Inspect(node, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			fn := c.function(call.Fun)
			if boundaryExternal(fn) || c.genericNetworkIO(call, fn) {
				if !reported[call.Pos()] {
					reported[call.Pos()] = true
					violations = append(violations, call)
				}
				return true
			}
			if literal, ok := c.resolve(call.Fun).(*ast.FuncLit); ok {
				visit(literal.Body)
			}
			if fn == nil || seen[fn] {
				return true
			}
			seen[fn] = true
			if body := c.bodies[fn]; body != nil {
				visit(body)
			} else if recv := fn.Type().(*types.Signature).Recv(); recv != nil {
				if iface, ok := recv.Type().Underlying().(*types.Interface); ok {
					for candidate, body := range c.bodies {
						sig := candidate.Type().(*types.Signature)
						if candidate.Name() == fn.Name() && sig.Recv() != nil && types.Implements(sig.Recv().Type(), iface) && !seen[candidate] {
							seen[candidate] = true
							visit(body)
						}
					}
				}
			}
			return true
		})
	}
	visit(root)
	return violations
}

func boundaryAuthorityParams(params *ast.FieldList, info *types.Info, includeBegin bool) bool {
	if params == nil {
		return false
	}
	for _, param := range params.List {
		if named := boundaryNamed(info.TypeOf(param.Type)); named != nil {
			obj := named.Obj()
			if obj.Pkg() != nil && ((obj.Pkg().Path() == boundaryPkgPath || obj.Pkg().Path() == "fixture") &&
				(!includeBegin && obj.Name() == "readModel" || obj.Name() == "writeTransaction" || includeBegin && obj.Name() == "writeTransactionBegin") ||
				obj.Pkg().Path() == "database/sql" && obj.Name() == "Tx") {
				return true
			}
		}
	}
	return false
}

func boundaryWriteOwner(fn *ast.FuncDecl, calls *boundaryCalls) bool {
	if boundaryAuthorityParams(fn.Type.Params, calls.info, true) {
		return true
	}
	owns := false
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok && boundaryStoreDoor(calls.function(call.Fun), "beginWriteTransaction", "beginSettlementWriteTransaction") {
			owns = true
		}
		return true
	})
	return owns
}

func snapshotCallbackSites(fset *token.FileSet, files []*ast.File, info *types.Info) []string {
	calls := newBoundaryCalls(files, info)
	var sites []string
	reported := map[token.Pos]bool{}
	report := func(call *ast.CallExpr, kind string) {
		if !reported[call.Pos()] {
			reported[call.Pos()] = true
			sites = append(sites, fmt.Sprintf("%s: %s", fset.Position(call.Pos()), kind))
		}
	}
	scan := func(body *ast.BlockStmt) {
		ast.Inspect(body, func(n ast.Node) bool {
			if nested, ok := n.(*ast.CallExpr); ok && boundaryStoreDoor(calls.function(nested.Fun), "withReadSnapshot", "withAgentReadSnapshot") {
				report(nested, "nested snapshot")
			}
			return true
		})
		for _, external := range calls.externalCalls(body) {
			report(external, "external call")
		}
	}
	for _, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			// Forwarded callbacks and settlement literals hold authority even when
			// their enclosing owner has neither a door call nor a transaction parameter.
			if literal, ok := node.(*ast.FuncLit); ok && boundaryAuthorityParams(literal.Type.Params, info, false) {
				scan(literal.Body)
			}
			call, ok := node.(*ast.CallExpr)
			if !ok || !boundaryStoreDoor(calls.function(call.Fun), "withReadSnapshot", "withAgentReadSnapshot") {
				return true
			}
			for _, arg := range call.Args {
				if literal, ok := calls.resolve(arg).(*ast.FuncLit); ok {
					scan(literal.Body)
				}
			}
			return true
		})
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || boundaryStoreDoor(calls.function(fn.Name), "beginWriteTransaction", "beginSettlementWriteTransaction") {
				continue
			}
			if boundaryWriteOwner(fn, calls) {
				for _, external := range calls.externalCalls(fn.Body) {
					report(external, "external call")
				}
			}
		}
	}
	sort.Strings(sites)
	return sites
}

func requireBoundaryInfo(t *testing.T, info *types.Info) {
	t.Helper()
	if len(info.Defs) == 0 || len(info.Uses) == 0 || len(info.Selections) == 0 {
		t.Fatalf("boundary type resolution empty: Defs=%d Uses=%d Selections=%d", len(info.Defs), len(info.Uses), len(info.Selections))
	}
}

func TestReadSnapshotCallbackGuard(t *testing.T) {
	fset, files, info := readBoundaryTypes(t)
	requireBoundaryInfo(t, info)
	calls := newBoundaryCalls(files, info)
	snapshots, owners := 0, 0
	for _, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok && boundaryStoreDoor(calls.function(call.Fun), "withReadSnapshot", "withAgentReadSnapshot") {
				snapshots++
			}
			return true
		})
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil && boundaryWriteOwner(fn, calls) {
				owners++
			}
		}
	}
	// Floors pin known snapshot and write-authority coverage without requiring
	// unrelated additions to update counts.
	t.Logf("resolved bodies=%d snapshots=%d write owners=%d", len(calls.bodies), snapshots, owners)
	if len(calls.bodies) == 0 || snapshots < 50 || owners < 100 {
		t.Fatalf("callback scan lost coverage: bodies=%d snapshots=%d write owners=%d", len(calls.bodies), snapshots, owners)
	}
	if sites := snapshotCallbackSites(fset, files, info); len(sites) != 0 {
		t.Fatalf("read boundary callbacks:\n%s", strings.Join(sites, "\n"))
	}
}

func TestReadSnapshotGuardDetectsCallbackEscapes(t *testing.T) {
	source := `package fixture
 import (web "net/http"; network "net"; "io"; "database/sql"; "encoding/json")
 type Store struct{}
 type writeTransaction struct{}
 type readModel interface{}
 type ComputerTokenRevoker interface {CountComputerInflight()}
 type Harmless struct{}
 func (Harmless) CountComputerInflight() {}
 func (*Store) withReadSnapshot(use func()) {}
 func (*Store) withAgentReadSnapshot(use func()) {}
 func (*Store) beginWriteTransaction() *writeTransaction {return nil}
 func helper(c ComputerTokenRevoker) { call:=c.CountComputerInflight; call() }
 func nested(s *Store) {s.withReadSnapshot(func(){ s.withReadSnapshot(func(){}) })}
 func indirect(s *Store, c ComputerTokenRevoker) {s.withReadSnapshot(func(){helper(c)})}
 func agent(s *Store, c ComputerTokenRevoker) {s.withAgentReadSnapshot(func(){c.CountComputerInflight()})}
 func write(s *Store, c ComputerTokenRevoker) {_=s.beginWriteTransaction(); use:=func(){c.CountComputerInflight()}; use()}
 func networkIO(s *Store, c network.Conn, h *web.Client) {s.withReadSnapshot(func(){ _,_=c.Write(nil); _,_=h.Get("http://example.invalid"); writer:=io.Writer(c); _,_=io.WriteString(writer, "test") })}
 func settle(use func(*sql.Tx)) {}
 func settlement() {settle(func(tx *sql.Tx){ _,_=web.Get("http://example.invalid") })}
 func forward(use func(readModel)) {}
 func forwardedWrite(w web.ResponseWriter) {forward(func(reads readModel){ _,_=w.Write(nil) })}
 func forwardedNested(s *Store) {forward(func(reads readModel){s.withReadSnapshot(func(){})})}
 func forwardWriteAuthority(use func(*writeTransaction)) {}
 func writeLiteral() {forwardWriteAuthority(func(write *writeTransaction){_,_=web.Get("http://example.invalid")})}
 func writeJSON(w web.ResponseWriter, value any) { _=json.NewEncoder(w).Encode(value) }
 func response(s *Store, w web.ResponseWriter) {s.withReadSnapshot(func(){writeJSON(w, nil)})}
 func encoding(s *Store, w web.ResponseWriter) {s.withReadSnapshot(func(){_=json.NewEncoder(w).Encode(nil)})}
 func header(s *Store, w web.ResponseWriter) {s.withReadSnapshot(func(){w.WriteHeader(200)})}
 func harmless(s *Store) {s.withReadSnapshot(func(){Harmless{}.CountComputerInflight()})}
 `
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	info := newBoundaryInfo()
	_, _, production := readBoundaryTypes(t)
	packages := map[string]*types.Package{}
	for _, obj := range production.Uses {
		if pkg, ok := obj.(*types.PkgName); ok {
			packages[pkg.Imported().Path()] = pkg.Imported()
		}
	}
	if _, err := (&types.Config{Importer: boundaryPackageImporter(packages)}).Check("fixture", fset, []*ast.File{file}, info); err != nil {
		t.Fatal(err)
	}
	requireBoundaryInfo(t, info)
	sites := snapshotCallbackSites(fset, []*ast.File{file}, info)
	expected := []string{
		"fixture.go:12:71: external call", // helper reached by indirect callback
		"fixture.go:13:52: nested snapshot",
		"fixture.go:15:79: external call",   // agent callback
		"fixture.go:16:89: external call",   // write owner
		"fixture.go:17:90: external call",   // Conn.Write
		"fixture.go:17:108: external call",  // Client.Get
		"fixture.go:17:149: external call",  // io.Writer conversion of Conn
		"fixture.go:17:167: external call",  // io.WriteString
		"fixture.go:19:50: external call",   // settlement literal with *sql.Tx
		"fixture.go:21:80: external call",   // forwarded readModel literal: Write
		"fixture.go:22:64: nested snapshot", // forwarded readModel literal: snapshot
		"fixture.go:24:79: external call",   // writeTransaction literal
		"fixture.go:26:75: external call",   // writeJSON(w, ...)
		"fixture.go:27:77: external call",   // json.NewEncoder(w)
		"fixture.go:28:73: external call",   // WriteHeader
	}
	sort.Strings(expected)
	if !reflect.DeepEqual(sites, expected) {
		t.Fatalf("callback sites=%v; want exactly %v", sites, expected)
	}

}

func TestReadSnapshotExceptionClasses(t *testing.T) {
	for _, slice := range []string{"permanent", "agent-protocol"} {
		if !validReadSlice(slice) {
			t.Errorf("rejects permanent class %q", slice)
		}
	}
	for _, slice := range []string{"", "#748", "#749", "#750", "#751", "#752", "legacy", "agent-protocol-new"} {
		if validReadSlice(slice) {
			t.Errorf("accepts undocumented class %q", slice)
		}
	}
}

type boundaryPackageImporter map[string]*types.Package

func (i boundaryPackageImporter) Import(path string) (*types.Package, error) {
	if pkg := i[path]; pkg != nil {
		return pkg, nil
	}
	return nil, fmt.Errorf("missing production import %s", path)
}
