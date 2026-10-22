package main

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/constant"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func seatApprovalNodePrincipalExpressions(info *types.Info) []token.Pos {
	var found []token.Pos
	for expr, value := range info.Types {
		if value.Value != nil && value.Value.Kind() == constant.String && constant.StringVal(value.Value) == "tag:wefty-agent" {
			found = append(found, expr.Pos())
		}
	}
	return found
}

// This invariant examines all compiled CLI expressions, including constants
// and aliases inside if dispatch, flags and helpers. It does not classify
// commands by their English names. CI runs it on both supported platforms.
func TestSeatApprovalCLIPrincipalStructure(t *testing.T) {
	output, err := exec.Command("go", "list", "-export", "-deps", "-json", ".").Output()
	if err != nil {
		t.Fatal(err)
	}
	exports := map[string]string{}
	var sources []string
	decoder := json.NewDecoder(bytes.NewReader(output))
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
		if pkg.ImportPath == "github.com/Derek-X-Wang/wefty/cmd/wefty" {
			for _, file := range pkg.GoFiles {
				sources = append(sources, filepath.Join(pkg.Dir, file))
			}
		}
	}
	if len(sources) < 20 {
		t.Fatalf("incomplete CLI source inventory: %d", len(sources))
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, path := range sources {
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
	}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}}
	config := types.Config{Importer: importer.ForCompiler(fset, "gc", func(path string) (io.ReadCloser, error) { return os.Open(exports[path]) })}
	if _, err := config.Check("github.com/Derek-X-Wang/wefty/cmd/wefty", fset, files, info); err != nil {
		t.Fatal(err)
	}
	for _, position := range seatApprovalNodePrincipalExpressions(info) {
		t.Errorf("CLI builds the node principal at %s", fset.Position(position))
	}
}

func TestSeatApprovalPrincipalGuardFindsIfDispatchAndAliases(t *testing.T) {
	for _, tag := range []string{`"tag:wefty-client"`, `"tag:wefty-"+"agent"`} {
		source := `package fixture; const role=` + tag + `;const alias=role; type identity struct{Tags []string};func dispatch(args []string)identity{var value identity;if args[0]=="grant"{value.Tags=append(value.Tags,alias)};return value}`
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, "fixture.go", source, 0)
		if err != nil {
			t.Fatal(err)
		}
		info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}}
		if _, err := (&types.Config{}).Check("fixture", fset, []*ast.File{file}, info); err != nil {
			t.Fatal(err)
		}
		found := seatApprovalNodePrincipalExpressions(info)
		if (len(found) > 0) != (tag != `"tag:wefty-client"`) {
			t.Fatalf("principal guard for %s: %v", tag, found)
		}
	}
}
