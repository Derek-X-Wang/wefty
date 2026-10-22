package main

import (
	"bytes"
	"encoding/json"
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

// Inspect production CLI source with compiler-resolved types and constants.
// Helper implementations live outside this inventory: their node-role entry
// points are guarded here, as are all Fabric identity literals in the CLI.
// CI runs this inventory on both supported platforms.
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
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Uses: map[*ast.Ident]types.Object{}}
	config := types.Config{Importer: importer.ForCompiler(fset, "gc", func(path string) (io.ReadCloser, error) { return os.Open(exports[path]) })}
	if _, err := config.Check("github.com/Derek-X-Wang/wefty/cmd/wefty", fset, files, info); err != nil {
		t.Fatal(err)
	}
	for _, position := range seatApprovalNodePrincipalExpressions(info) {
		t.Errorf("CLI builds the node principal at %s", fset.Position(position))
	}
	for position, reason := range seatApprovalCLIPrincipalViolations(fset, files, info) {
		t.Errorf("%s: %s", fset.Position(position), reason)
	}
}

func seatApprovalCLIPrincipalViolations(fset *token.FileSet, files []*ast.File, info *types.Info) map[token.Pos]string {
	violations := map[token.Pos]string{}
	// These untagged literals represent a person or placeholders subsequently
	// bound by SavedService. Pin their exact source and owning function; they
	// do not exempt other identity literals or any explicit Tags value.
	untagged := map[string]map[string]bool{
		"participant_choice.go:commandFabricConfig": {
			"fabric.Identity{NodeID: participant.NodeID, DeviceID: participant.NodeID, UserID: participant.UserID}": true,
		},
		"client_service.go:executeClientServiceWithOpener": {
			"fabric.Identity{NodeID: options.plainIdentity}": true,
			"fabric.Identity{}": true,
		},
	}
	stringValue := func(expr ast.Expr) (string, bool) {
		value := info.Types[expr].Value
		if value == nil || value.Kind() != constant.String {
			return "", false
		}
		return constant.StringVal(value), true
	}
	for _, file := range files {
		for _, decl := range file.Decls {
			owner := filepath.Base(fset.Position(decl.Pos()).Filename) + ":"
			if fn, ok := decl.(*ast.FuncDecl); ok {
				owner += fn.Name.Name
			}
			// Only direct, reviewed helper calls are allowed. Taking a helper
			// as a value is refused too, so aliases cannot hide a node role.
			safeHelpers := map[*ast.Ident]bool{}
			ast.Inspect(decl, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				var id *ast.Ident
				switch fun := call.Fun.(type) {
				case *ast.SelectorExpr:
					id = fun.Sel
				case *ast.Ident:
					id = fun
				}
				fn, ok := info.Uses[id].(*types.Func)
				if !ok || fn.Pkg() == nil || fn.Pkg().Path() != "github.com/Derek-X-Wang/wefty/internal/connprofile" ||
					(fn.Name() != "ParticipantRoleTags" && fn.Name() != "ServiceParticipant") {
					return true
				}
				if owner == "setup_enrolment.go:setupParticipantPolicy" {
					safeHelpers[id] = true
				} else if len(call.Args) > 0 {
					role, constantRole := stringValue(call.Args[len(call.Args)-1])
					if constantRole && (role == "client" || role == "person" || role == "l1" || role == "l3") {
						safeHelpers[id] = true
					}
				}
				return true
			})
			ast.Inspect(decl, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok {
					if fn, ok := info.Uses[id].(*types.Func); ok && fn.Pkg() != nil &&
						fn.Pkg().Path() == "github.com/Derek-X-Wang/wefty/internal/connprofile" &&
						(fn.Name() == "ParticipantRoleTags" || fn.Name() == "ServiceParticipant") && !safeHelpers[id] {
						violations[id.Pos()] = "CLI role helper may select the node principal outside setupParticipantPolicy"
					}
				}
				literal, ok := n.(*ast.CompositeLit)
				if !ok {
					return true
				}
				named, ok := types.Unalias(info.TypeOf(literal)).(*types.Named)
				if !ok || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != "github.com/Derek-X-Wang/wefty/fabric" || named.Obj().Name() != "Identity" {
					return true
				}
				var tags ast.Expr
				for _, element := range literal.Elts {
					if field, ok := element.(*ast.KeyValueExpr); ok {
						if key, ok := field.Key.(*ast.Ident); ok && key.Name == "Tags" {
							tags = field.Value
						}
					}
				}
				if tags == nil {
					var source bytes.Buffer
					if err := format.Node(&source, fset, literal); err == nil && untagged[owner][source.String()] {
						return true
					}
				} else if slice, ok := tags.(*ast.CompositeLit); ok && len(slice.Elts) > 0 {
					allowed := true
					for _, tag := range slice.Elts {
						value, constantTag := stringValue(tag)
						allowed = allowed && constantTag && value == "tag:wefty-client"
					}
					if allowed {
						return true
					}
				}
				violations[literal.Pos()] = "CLI Fabric identity must use explicit client/caller tags or a reviewed untagged site"
				return true
			})
		}
	}
	return violations
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
