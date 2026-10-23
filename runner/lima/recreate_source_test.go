package lima

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// Keep every destructive Lima command at the one serialized guard boundary.
// Scan literals, not just calls, to cover command tables and argument slices.
func TestHostVMDeleteOnlyAtGuardedBoundary(t *testing.T) {
	root := filepath.Join("..", "..")
	var positions []limaDeleteLiteral
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
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		found, err := limaDeleteCalls(filepath.ToSlash(rel), source)
		if err != nil {
			return err
		}
		positions = append(positions, found...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := limaDeleteBoundaryError(positions); err != nil {
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
		{`commands := [][]string{{config.Limactl, "delete", "--force", config.Instance}}`, 1},
		{`args := []string{"delete", "--force", instance}; run(ctx, limactl, args...)`, 1},
		{`run(ctx, limactl, "rm", instance)`, 1},
		{`run(ctx, limactl, "remove", instance)`, 1},
		{`run(ctx, limactl, "factory-reset", instance)`, 1},
		{`run(ctx, limactlPath, "delete", instance)`, 1},
		{`args := []string{"delete", instance}; run(ctx, limactl, args...); fmt.Printf("shell", args)`, 1},
		{`args := []string{"delete", instance}; run(ctx, limactl, args...); run(ctx, limactl, "shell", instance, args...)`, 1},
		{`runCommand(ctx,limactl,"shell",instance,"ip","link","delete",link)`, 0},
		{`exec.Command("other-tool","delete",resource)`, 0},
		{`supervisor.runBounded(ctx,"list",instance)`, 0},
		{`runBounded(ctx, "delete", instance)`, 1},
		{`run := supervisor.runBounded; args := []string{"delete", instance}; run(ctx, args...)`, 1},
		{`shell := []string{"--tty=false", "shell", "--workdir=/", instance, "sudo"}; run(ctx, config.Limactl, append(shell, "rm", "-f", GuestHelperPath)...)`, 0},
		{`flags := flag.NewFlagSet("bootstrap", flag.ContinueOnError); flags.Bool("remove", false, "remove bootstrap"); run(ctx, limactl, "list")`, 0},
		{`exec.Command("sh", "-c", "limactl delete --force wefty-oci")`, 1},
		{`command := fmt.Sprintf("limactl rm %s", instance)`, 1},
		{`exec.Command("/bin/sh", "-c", "/opt/homebrew/bin/limactl --tty=false factory-reset "+instance)`, 1},
		{`runCommand(ctx, limactl, "shell", instance, "sh", "-c", "limactl delete other")`, 1},
		{`command := fmt.Sprintf("limactl list %s", instance)`, 0},
		{`log.Print("limactl shell, then delete nothing")`, 0},
	} {
		positions, err := limaDeleteCalls("fixture.go", []byte("package fixture\nfunc check(){"+test.call+"}\n"))
		if err != nil || len(positions) != test.want {
			t.Errorf("%s: positions=%v err=%v", test.call, positions, err)
		}
	}
}

// A Lima-related file must not hide a destructive command in a literal, even
// when its binary or arguments reach the runner indirectly. Deliberately scan
// the whole file rather than trying to prove arbitrary Go data flow.
var limaDeleteCommandString = regexp.MustCompile(`(?i)\blimactl(?:\s+-\S+)*\s+(?:delete|rm|remove|factory-reset)\b`)

type limaDeleteLiteral struct {
	token.Position
	guarded bool
}

func limaDeleteCalls(path string, source []byte) ([]limaDeleteLiteral, error) {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, source, 0)
	if err != nil {
		return nil, err
	}
	// Include Lima package/import references as well as limactl spellings, so
	// introducing a wrapper does not remove a file from the scan.
	lima := strings.Contains(strings.ToLower(string(source)), "lima")
	ignored := make(map[token.Pos]bool)
	ast.Inspect(file, func(node ast.Node) bool {
		// A runner reference also counts when assigned to another variable,
		// or when the runner is called without a receiver.
		if identifier, ok := node.(*ast.Ident); ok {
			lima = lima || identifier.Name == "runBounded" || identifier.Name == "runWithinRecoveryBudget"
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		limaCall := false
		if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
			limaCall = selector.Sel.Name == "runBounded" || selector.Sel.Name == "runWithinRecoveryBudget"
			lima = lima || limaCall
			// The bootstrap's --remove flag is not a Lima command. Limit the
			// exemption to Bool's name argument on a flag.NewFlagSet result.
			if selector.Sel.Name == "Bool" && len(call.Args) > 0 {
				if receiver, ok := selector.X.(*ast.Ident); ok && receiver.Obj != nil {
					if declaration, ok := receiver.Obj.Decl.(*ast.AssignStmt); ok && len(declaration.Rhs) == 1 {
						if factory, ok := declaration.Rhs[0].(*ast.CallExpr); ok {
							if constructor, ok := factory.Fun.(*ast.SelectorExpr); ok {
								if pkg, ok := constructor.X.(*ast.Ident); ok && pkg.Name == "flag" && constructor.Sel.Name == "NewFlagSet" {
									ignored[call.Args[0].Pos()] = true
								}
							}
						}
					}
				}
			}
		}
		for _, arg := range call.Args {
			ast.Inspect(arg, func(node ast.Node) bool {
				switch node := node.(type) {
				case *ast.Ident:
					limaCall = limaCall || strings.Contains(strings.ToLower(node.Name), "limactl")
				case *ast.BasicLit:
					if node.Kind == token.STRING {
						value, _ := strconv.Unquote(node.Value)
						limaCall = limaCall || filepath.Base(value) == "limactl"
					}
				}
				return true
			})
		}
		if !limaCall {
			return true
		}
		// Guest commands such as `limactl shell ... rm` remove guest files,
		// not the host disk. Exempt only literals after an explicit shell
		// subcommand. Resolve static append prefixes used by helper removal;
		// unknown/dynamic prefixes receive no exemption.
		var args []ast.Expr
		for _, arg := range call.Args {
			args = append(args, limaStaticArguments(arg, 0)...)
		}
		guestShell, commandSeen := false, false
		for _, arg := range args {
			literal, ok := arg.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				continue
			}
			value, _ := strconv.Unquote(literal.Value)
			if !commandSeen {
				if strings.HasPrefix(value, "-") || filepath.Base(value) == "limactl" {
					continue
				}
				commandSeen, guestShell = true, value == "shell"
			} else if guestShell && literal.Pos() >= call.Pos() && literal.End() <= call.End() {
				// A referenced slice can also be used by a host command. Only
				// exempt literals physically written inside this guest call.
				ignored[literal.Pos()] = true
			}
		}
		return true
	})
	if !lima {
		return nil, nil
	}
	var positions []limaDeleteLiteral
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		value, _ := strconv.Unquote(literal.Value)
		// A whole command in one string, such as an sh -c script, names
		// limactl itself, so the guest-shell exemption never applies to it.
		if !limaDeleteCommandString.MatchString(value) {
			if ignored[literal.Pos()] {
				return true
			}
			switch value {
			case "delete", "rm", "remove", "factory-reset":
			default:
				return true
			}
		}
		guarded := false
		if path == "runner/lima/supervisor.go" {
			for _, declaration := range file.Decls {
				function, ok := declaration.(*ast.FuncDecl)
				if !ok || function.Name.Name != "Recreate" || function.Recv == nil || len(function.Recv.List) != 1 || function.Body == nil || literal.Pos() < function.Body.Pos() || literal.End() > function.Body.End() {
					continue
				}
				if pointer, ok := function.Recv.List[0].Type.(*ast.StarExpr); ok {
					receiver, ok := pointer.X.(*ast.Ident)
					guarded = ok && receiver.Name == "SupervisedBootBarrier"
				}
			}
		}
		positions = append(positions, limaDeleteLiteral{Position: set.Position(literal.Pos()), guarded: guarded})
		return true
	})
	return positions, nil
}

// Expand only static slice literals and append prefixes, with a bound for
// cyclic or unusual initializers. This is solely a narrow guest-shell exemption.
func limaStaticArguments(expression ast.Expr, depth int) []ast.Expr {
	if depth > 8 {
		return []ast.Expr{expression}
	}
	switch expression := expression.(type) {
	case *ast.CompositeLit:
		return expression.Elts
	case *ast.CallExpr:
		if function, ok := expression.Fun.(*ast.Ident); ok && function.Name == "append" {
			var args []ast.Expr
			for _, arg := range expression.Args {
				args = append(args, limaStaticArguments(arg, depth+1)...)
			}
			return args
		}
	case *ast.Ident:
		if expression.Obj != nil {
			if declaration, ok := expression.Obj.Decl.(*ast.AssignStmt); ok && len(declaration.Rhs) == 1 {
				switch declaration.Rhs[0].(type) {
				case *ast.CompositeLit, *ast.CallExpr:
					return limaStaticArguments(declaration.Rhs[0], depth+1)
				}
			}
		}
	}
	return []ast.Expr{expression}
}

func TestHostVMDeleteSourceGuardLimitsAllowedFunctionAndCount(t *testing.T) {
	for _, test := range []struct {
		name, path, receiver, function string
		count                          int
		wantAllowed                    bool
	}{
		{"guarded delete", "runner/lima/supervisor.go", "SupervisedBootBarrier", "Recreate", 1, true},
		{"another function", "runner/lima/supervisor.go", "SupervisedBootBarrier", "Install", 1, false},
		{"another receiver", "runner/lima/supervisor.go", "Installer", "Recreate", 1, false},
		{"another file", "runner/lima/install.go", "SupervisedBootBarrier", "Recreate", 1, false},
		{"two deletes in recreate", "runner/lima/supervisor.go", "SupervisedBootBarrier", "Recreate", 2, false},
		{"no guarded delete", "runner/lima/supervisor.go", "SupervisedBootBarrier", "Recreate", 0, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := fmt.Sprintf("package fixture\nfunc (barrier *%s) %s() { %s }", test.receiver, test.function,
				strings.Repeat(`barrier.Supervisor.runBounded(ctx,"delete",instance);`, test.count))
			positions, err := limaDeleteCalls(test.path, []byte(source))
			if err != nil || len(positions) != test.count {
				t.Fatalf("positions=%v err=%v", positions, err)
			}
			if allowed := limaDeleteBoundaryError(positions) == nil; allowed != test.wantAllowed {
				t.Fatalf("allowed=%t, want %t: %v", allowed, test.wantAllowed, positions)
			}
		})
	}
}

func limaDeleteBoundaryError(positions []limaDeleteLiteral) error {
	for _, position := range positions {
		if !position.guarded {
			return fmt.Errorf("destructive Lima command bypasses the recreate guard: %s", position.Position)
		}
	}
	if len(positions) != 1 {
		return fmt.Errorf("want exactly one guarded Lima delete, found %v", positions)
	}
	return nil
}
