package l1

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/constant"
	"go/format"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

type rawPoolException struct {
	Site   string `json:"site"`
	Count  int    `json:"count"`
	Reason string `json:"reason"`
	Slice  string `json:"slice"`
}

// Load compiler export data rather than guessing selector names. This catches
// aliases, raw pool/connection arguments, returns, assignment and method use.
// go list is a sequential child of this test's Go job (and its shared lock).
func readBoundaryTypes(t *testing.T) (*token.FileSet, []*ast.File, *types.Info) {
	t.Helper()
	cmd := exec.Command("go", "list", "-export", "-deps", "-json", ".")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("read boundary export data: %v", err)
	}
	exports := map[string]string{}
	var sources []string
	decoder := json.NewDecoder(bytes.NewReader(out))
	for {
		var pkg struct {
			ImportPath, Export, Dir string
			GoFiles                 []string
		}
		err := decoder.Decode(&pkg)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		exports[pkg.ImportPath] = pkg.Export
		if pkg.ImportPath == "github.com/Derek-X-Wang/wefty/l1" {
			for _, name := range pkg.GoFiles {
				sources = append(sources, filepath.Join(pkg.Dir, name))
			}
		}
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, source := range sources {
		file, err := parser.ParseFile(fset, source, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
	}
	if len(files) == 0 {
		t.Fatal("no l1 production sources")
	}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Selections: map[*ast.SelectorExpr]*types.Selection{}, Uses: map[*ast.Ident]types.Object{}}
	imp := importer.ForCompiler(fset, "gc", func(path string) (io.ReadCloser, error) { return os.Open(exports[path]) })
	config := types.Config{Importer: imp}
	if _, err := config.Check("github.com/Derek-X-Wang/wefty/l1", fset, files, info); err != nil {
		t.Fatal(err)
	}
	return fset, files, info
}

func rawSQLHandle(typ types.Type) bool {
	pointer, ok := types.Unalias(typ).(*types.Pointer)
	if !ok {
		return false
	}
	named, ok := types.Unalias(pointer.Elem()).(*types.Named)
	if !ok {
		return false
	}
	obj := named.Obj()
	return obj.Pkg() != nil && obj.Pkg().Path() == "database/sql" && (obj.Name() == "DB" || obj.Name() == "Conn")
}
func rawPoolSites(fset *token.FileSet, files []*ast.File, info *types.Info) map[string]int {
	sites := map[string]int{}
	for _, file := range files {
		name := filepath.Base(fset.Position(file.Pos()).Filename)
		for _, decl := range file.Decls {
			owner := "package"
			var body ast.Node = decl
			if fn, ok := decl.(*ast.FuncDecl); ok {
				owner = fn.Name.Name
				if fn.Recv != nil {
					var receiver bytes.Buffer
					_ = format.Node(&receiver, fset, fn.Recv.List[0].Type)
					owner = receiver.String() + "." + owner
				}
				body = fn.Body
				if body == nil {
					continue
				}
			}
			ast.Inspect(body, func(node ast.Node) bool {
				expr, ok := node.(ast.Expr)
				if !ok {
					return true
				}
				value, ok := info.Types[expr]
				if !ok || value.IsType() || !rawSQLHandle(value.Type) {
					return true
				}
				var rendered bytes.Buffer
				_ = format.Node(&rendered, fset, expr)
				sites[name+":"+owner+" | "+rendered.String()]++
				return true
			})
		}
	}
	return sites
}

// Constants and aliases are checked by value, so hiding a pragma behind a
// named string does not bypass the connection-creation rule.
func queryOnlySites(fset *token.FileSet, files []*ast.File, info *types.Info) []string {
	var sites []string
	for _, file := range files {
		for _, decl := range file.Decls {
			owner := "package"
			if fn, ok := decl.(*ast.FuncDecl); ok {
				owner = fn.Name.Name
			}
			if filepath.Base(fset.Position(file.Pos()).Filename) == "store.go" && owner == "OpenStore" {
				continue
			}
			ast.Inspect(decl, func(node ast.Node) bool {
				expr, ok := node.(ast.Expr)
				if !ok {
					return true
				}
				value := info.Types[expr].Value
				if value != nil && value.Kind() == constant.String && strings.Contains(strings.ToLower(constant.StringVal(value)), "query_only") {
					sites = append(sites, fmt.Sprintf("%s:%s", fset.Position(expr.Pos()), owner))
				}
				return true
			})
		}
	}
	return sites
}

func TestReadSnapshotQueryOnlyPragmaGuard(t *testing.T) {
	fset, files, info := readBoundaryTypes(t)
	if sites := queryOnlySites(fset, files, info); len(sites) != 0 {
		t.Fatalf("query_only outside OpenStore: %v", sites)
	}
}

func TestReadSnapshotGuardDetectsQueryOnlyPragma(t *testing.T) {
	source := `package fixture
 const reset = "PRAGMA QUERY_ONLY=OFF"
 func leak() string {return reset}
 func OpenStore() string {return "query_only(1)"}
 `
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "store.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}}
	config := types.Config{}
	if _, err := config.Check("fixture", fset, []*ast.File{file}, info); err != nil {
		t.Fatal(err)
	}
	sites := queryOnlySites(fset, []*ast.File{file}, info)
	if len(sites) < 2 {
		t.Fatalf("guard missed pragma declaration/use: %v", sites)
	}
	for _, site := range sites {
		if strings.HasSuffix(site, ":OpenStore") {
			t.Fatalf("guard refused allowed constructor: %v", sites)
		}
	}
}

func validReadSlice(slice string) bool {
	switch slice {
	case "#748", "#749", "#750", "#751", "#752", "permanent":
		return true
	}
	return false
}

func TestReadSnapshotRawPoolRatchet(t *testing.T) {
	fset, files, info := readBoundaryTypes(t)
	actual := rawPoolSites(fset, files, info)
	data, err := os.ReadFile("read_boundary_exceptions.json")
	if err != nil {
		t.Fatal(err)
	}
	var exceptions []rawPoolException
	if err = json.Unmarshal(data, &exceptions); err != nil {
		t.Fatal(err)
	}
	allowed := map[string]int{}
	for _, exception := range exceptions {
		if exception.Reason == "" || !validReadSlice(exception.Slice) || exception.Count <= 0 || allowed[exception.Site] != 0 {
			t.Fatalf("invalid exception: %+v", exception)
		}
		allowed[exception.Site] = exception.Count
	}
	var violations []string
	for site, count := range actual {
		if count != allowed[site] {
			violations = append(violations, fmt.Sprintf("%s: got %d raw uses, allowed %d", site, count, allowed[site]))
		}
	}
	for site, count := range allowed {
		if actual[site] == 0 {
			violations = append(violations, fmt.Sprintf("remove obsolete exception %s (%d)", site, count))
		}
	}
	sort.Strings(violations)
	if len(violations) > 0 {
		// A diagnostic inventory is never read as an allowlist. It makes the initial
		// documented ratchet auditable without accepting a newly planted use.
		var inventory []rawPoolException
		var keys []string
		for site := range actual {
			keys = append(keys, site)
		}
		sort.Strings(keys)
		for _, site := range keys {
			inventory = append(inventory, rawPoolException{Site: site, Count: actual[site], Reason: "legacy acquisition/use; migrate in #748-#752"})
		}
		dump, _ := json.MarshalIndent(inventory, "", "  ")
		t.Logf("INVENTORY\n%s\nEND INVENTORY", dump)
		t.Fatalf("read boundary ratchet:\n%s", strings.Join(violations, "\n"))
	}
}

func TestReadSnapshotGuardDetectsRawHandleEscapes(t *testing.T) {
	// These are type checked fixtures, not substring checks. A field named db
	// with a harmless type is fine; aliases of real DB/Conn remain forbidden.
	source := `package fixture
 import ("context"; dbsql "database/sql")
 type Pool = dbsql.DB
 type Connection = dbsql.Conn
 type Store struct { pool *Pool; db string }
 type writeTransaction struct { tx *dbsql.Tx }
 func (s *Store) beginWriteTransaction(context.Context) *writeTransaction { return nil }
 func migrated(ctx context.Context, s *Store) { w:=s.beginWriteTransaction(ctx); _,_=w.tx.ExecContext(ctx,"INSERT INTO x VALUES(1)") }
 func accept(any) {}
 func leak(s *Store,c *Connection) *Pool {
  accept(s.pool)
  p:=s.pool
  accept(p)
  accept(c)
  _=s.db
  return p
 }
 func read(ctx context.Context,s *Store) { _,_=s.pool.QueryContext(ctx,"SELECT 1") }
 `
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}}
	// Reuse compiler importer export data from the production type check. Its
	// loaded sql package is obtained with a tiny separate importer below.
	_, _, production := readBoundaryTypes(t)
	var sqlPackage *types.Package
	for _, value := range production.Types {
		if rawSQLHandle(value.Type) {
			sqlPackage = types.Unalias(value.Type).(*types.Pointer).Elem().(*types.Named).Obj().Pkg()
			break
		}
	}
	config := types.Config{Importer: boundaryFixtureImporter{sql: sqlPackage, base: importer.Default()}}
	if _, err := config.Check("fixture", fset, []*ast.File{file}, info); err != nil {
		t.Fatal(err)
	}
	sites := rawPoolSites(fset, []*ast.File{file}, info)
	for site, count := range map[string]int{"fixture.go:leak | s.pool": 2, "fixture.go:leak | p": 2, "fixture.go:leak | c": 1, "fixture.go:read | s.pool": 1} {
		if sites[site] != count {
			t.Fatalf("%s: got %d want %d; all=%v", site, sites[site], count, sites)
		}
	}
	if len(sites) != 4 {
		t.Fatalf("unexpected raw handles: %v", sites)
	}
}

type boundaryFixtureImporter struct {
	sql  *types.Package
	base types.Importer
}

func (i boundaryFixtureImporter) Import(path string) (*types.Package, error) {
	if path == "database/sql" {
		return i.sql, nil
	}
	if path == "context" {
		for _, pkg := range i.sql.Imports() {
			if pkg.Path() == path {
				return pkg, nil
			}
		}
		return nil, fmt.Errorf("context missing from sql imports")
	}
	return i.base.Import(path)
}

// The agent-protocol exception chain is closed by construction. withAgentReadSnapshot is reachable
// only inside writeAgentComputer, and each of the two is reachable only
// from the agent acknowledgement route handlers the agent mux registers.
// The rule matches every selector resolving to one of the methods — calls,
// method values and method expressions, including package-level var
// initialisers — so there is no bypass shape it misses.
var agentReadSnapshotTargets = []string{"withAgentReadSnapshot", "writeAgentComputer"}

const boundaryPkgPath = "github.com/Derek-X-Wang/wefty/l1"

// boundaryMethodObject resolves a selector identifier to the l1 (or fixture)
// target method, whether the selector is a call, a method value, or a method
// expression.
func boundaryMethodObject(info *types.Info, selector *ast.SelectorExpr, pkgPath string) bool {
	if selection := info.Selections[selector]; selection != nil && selection.Obj() != nil &&
		selection.Obj().Pkg() != nil && selection.Obj().Pkg().Path() == pkgPath &&
		hasString(agentReadSnapshotTargets, selection.Obj().Name()) {
		return true
	}
	if use := info.Uses[selector.Sel]; use != nil && use.Name() != "" &&
		use.Pkg() != nil && use.Pkg().Path() == pkgPath && hasString(agentReadSnapshotTargets, use.Name()) {
		return true
	}
	return false
}

func hasString(list []string, want string) bool {
	for _, value := range list {
		if value == want {
			return true
		}
	}
	return false
}

// boundaryOwnerSites walks every declaration in a package-level file body.
// Function declarations report `file:Receiver.Name`; package-level var
// initialisers report `file:var <name>`, so an aggregator there cannot hide
// behind a call at another site.
func boundaryOwnerSites(fset *token.FileSet, files []*ast.File, info *types.Info, pkgPath string) []string {
	var sites []string
	for _, file := range files {
		name := filepath.Base(fset.Position(file.Pos()).Filename)
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok {
				owner := fn.Name.Name
				if fn.Recv != nil {
					var receiver bytes.Buffer
					_ = format.Node(&receiver, fset, fn.Recv.List[0].Type)
					owner = receiver.String() + "." + owner
				}
				sites = append(sites, boundarySelectors(fset, fn, name+":"+owner, info, pkgPath)...)
				continue
			}
			genDecl, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range genDecl.Specs {
				valueSpec, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for _, identifier := range valueSpec.Names {
					if res := boundarySelectors(fset, valueSpec, name+":var "+identifier.Name, info, pkgPath); len(res) > 0 {
						sites = append(sites, res...)
					}
				}
			}
		}
	}
	return sites
}

func boundarySelectors(fset *token.FileSet, node ast.Node, site string, info *types.Info, pkgPath string) []string {
	var sites []string
	ast.Inspect(node, func(inside ast.Node) bool {
		selector, ok := inside.(*ast.SelectorExpr)
		if !ok || !boundaryMethodObject(info, selector, pkgPath) {
			return true
		}
		sites = append(sites, site)
		return true
	})
	return sites
}

// agentAcknowledgementSites is the exact site set the chain may live in: the
// two loader declarations after the agent acknowledgement Store methods, and
// the agent-mux acknowledgement handlers that write the reloaded Computer
// view. It is asserted by set equality, so a vanished handler or type check
// fails the guard rather than silently relaxing the boundary.
var agentAcknowledgementSites = map[string]bool{
	"computer_operator.go:*Server.writeAgentComputer":        true,
	"server.go:*Server.acknowledgeComputerBackup":            true,
	"server.go:*Server.acknowledgeComputerStorageReset":      true,
	"server.go:*Server.acknowledgeComputerStorageGrow":       true,
	"server.go:*Server.acknowledgeComputerReimagePreflight":  true,
	"server.go:*Server.acknowledgeComputerStorageRetirement": true,
	"server.go:*Server.acknowledgeComputerStorageCopy":       true,
	"server.go:*Server.acknowledgeComputerRestoreRetirement": true,
}

func TestReadSnapshotAgentGuardDetectsBypassShapes(t *testing.T) {
	// These are typed-checked fixtures, not substring checks. Every bypass form
	// the old CallExpr-only matcher missed must be reported under its owner.
	source := `package fixture
	import "context"
	type model int
	type Store struct{}
	func (s *Store) withAgentReadSnapshot(ctx context.Context, use func(context.Context, model) error) error { _ = ctx; _ = use; return nil }
	type Server struct{ store *Store }
	func (s *Server) writeAgentComputer(args ...any) { s.store.withAgentReadSnapshot(context.Background(), nil) }
	func (s *Server) acknowledgeComputerBackup() { s.store.withAgentReadSnapshot(context.Background(), nil) }
	func handlerCallingWrapper(s *Server) { s.writeAgentComputer() }
	func bypassMethodValue(s *Store) { f := s.withAgentReadSnapshot; _ = f }
	func bypassMethodExpr(s *Store) { _ = (*Store).withAgentReadSnapshot }
	var bypassPackageVar = func(s *Store) { s.withAgentReadSnapshot(context.Background(), nil) }
	`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Selections: map[*ast.SelectorExpr]*types.Selection{}, Uses: map[*ast.Ident]types.Object{}}
	_, _, production := readBoundaryTypes(t)
	config := types.Config{Importer: boundaryFixtureImporter{sql: snapshotSQLPackage(t, production), base: importer.Default()}}
	if _, err := config.Check("fixture", fset, []*ast.File{file}, info); err != nil {
		t.Fatal(err)
	}
	sites := boundaryOwnerSites(fset, []*ast.File{file}, info, "fixture")
	expected := map[string]bool{
		"fixture.go:*Server.writeAgentComputer":        true,
		"fixture.go:*Server.acknowledgeComputerBackup": true,
		"fixture.go:handlerCallingWrapper":             true,
		"fixture.go:bypassMethodValue":                 true,
		"fixture.go:bypassMethodExpr":                  true,
		"fixture.go:var bypassPackageVar":              true,
	}
	if len(sites) != len(expected) {
		t.Fatalf("sites=%v, want exactly %v", sites, expected)
	}
	for _, site := range sites {
		if !expected[site] {
			t.Fatalf("unexpected site %s; all=%v", site, sites)
		}
	}
}

func snapshotSQLPackage(t *testing.T, info *types.Info) *types.Package {
	t.Helper()
	for _, value := range info.Types {
		if rawSQLHandle(value.Type) {
			return types.Unalias(value.Type).(*types.Pointer).Elem().(*types.Named).Obj().Pkg()
		}
	}
	t.Fatal("no sql handle in production type check")
	return nil
}

func TestAgentReadSnapshotIsAgentOnly(t *testing.T) {
	fset, files, info := readBoundaryTypes(t)
	actual := map[string]bool{}
	for _, site := range boundaryOwnerSites(fset, files, info, boundaryPkgPath) {
		actual[site] = true
	}
	// Positive control: the chain must be exactly where the agent mux routes
	// it. A vanished type check, a moved helper, or a disappeared handler all
	// fail here instead of silently relaxing the boundary.
	if !reflect.DeepEqual(actual, agentAcknowledgementSites) {
		var extra, missing []string
		for site := range actual {
			if !agentAcknowledgementSites[site] {
				extra = append(extra, site)
			}
		}
		for site := range agentAcknowledgementSites {
			if !actual[site] {
				missing = append(missing, site)
			}
		}
		sort.Strings(extra)
		sort.Strings(missing)
		t.Fatalf("agent acknowledgement chain mismatch; extra=%v missing=%v", extra, missing)
	}
}
