package lima

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Keep host VM deletion at the one serialized guard boundary. Cover direct
// limactl commands and the command-runner forms used by Lima lifecycle code.
func TestHostVMDeleteOnlyAtGuardedBoundary(t *testing.T) {
	root := filepath.Join("..", "..")
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" || entry.Name() == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		positions, err := limaDeleteCalls(path, source)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if filepath.ToSlash(rel) != "runner/lima/supervisor.go" {
			for _, position := range positions {
				t.Errorf("Lima delete bypasses the recreate guard: %s", position)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestHostVMDeleteSourceGuardRecognizesCommandForms(t *testing.T) {
	for _, test := range []struct {
		call string
		want int
	}{
		{`supervisor.runBounded(ctx,"delete","--force",instance)`, 1},
		{`supervisor.runWithinRecoveryBudget(ctx,"delete",instance)`, 1},
		{`exec.Command("limactl","delete",instance)`, 1},
		{`exec.Command("limactl","delete","shell")`, 1},
		{`exec.CommandContext(ctx,"/opt/homebrew/bin/limactl","delete",instance)`, 1},
		{`run(ctx,config.Limactl,"delete",instance)`, 1},
		{`runCommand(ctx,limactl,"delete",instance)`, 1},
		{`runCommand(ctx,limactl,"shell",instance,"ip","link","delete",link)`, 0},
		{`exec.Command("other-tool","delete",resource)`, 0},
		{`supervisor.runBounded(ctx,"list",instance)`, 0},
	} {
		positions, err := limaDeleteCalls("fixture.go", []byte("package fixture\nfunc check(){"+test.call+"}\n"))
		if err != nil || len(positions) != test.want {
			t.Errorf("%s: positions=%v err=%v", test.call, positions, err)
		}
	}
}

func limaDeleteCalls(path string, source []byte) ([]token.Position, error) {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, source, 0)
	if err != nil {
		return nil, err
	}
	var positions []token.Position
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		deletes, lima, guestShell := false, false, false
		if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
			lima = selector.Sel.Name == "runBounded" || selector.Sel.Name == "runWithinRecoveryBudget"
		}
		for _, arg := range call.Args {
			if literal, ok := arg.(*ast.BasicLit); ok && literal.Kind == token.STRING {
				value, err := strconv.Unquote(literal.Value)
				if err == nil {
					guestShell = guestShell || value == "shell" && !deletes
					deletes = deletes || value == "delete"
					lima = lima || filepath.Base(value) == "limactl"
				}
			}
			ast.Inspect(arg, func(node ast.Node) bool {
				if id, ok := node.(*ast.Ident); ok && strings.EqualFold(id.Name, "limactl") {
					lima = true
				}
				return true
			})
		}
		if deletes && lima && !guestShell {
			positions = append(positions, set.Position(call.Pos()))
		}
		return true
	})
	return positions, nil
}
