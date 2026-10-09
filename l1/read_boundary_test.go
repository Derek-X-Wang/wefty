package l1

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

type rawPoolException struct {
	Site   string `json:"site"`
	Count  int    `json:"count"`
	Reason string `json:"reason"`
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
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}}
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
		if exception.Reason == "" || exception.Count <= 0 || allowed[exception.Site] != 0 {
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
