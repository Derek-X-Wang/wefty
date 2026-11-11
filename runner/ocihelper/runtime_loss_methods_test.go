package ocihelper

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// engineFailureLossByMethod is the reviewed answer for every Method: does an
// engine_failure answered for it prove runtime loss (#1606)? A new Method has
// no row until someone decides, and the test below fails until they do.
var engineFailureLossByMethod = map[Method]bool{
	// Session and whole-namespace operations.
	MethodAcquireSession: true,
	MethodHeartbeat:      true,
	MethodSweep:          true,
	MethodVerify:         true,
	// Attempt lifecycle. Run and Delete have conditional rows below.
	MethodRun:    true,
	MethodSignal: true,
	MethodWatch:  true,
	MethodDelete: true,
	// Operation-scoped: the caller owns the failure.
	MethodReleaseAttemptPin:      false,
	MethodEnsureImage:            false,
	MethodReconcileImagePins:     false,
	MethodReleaseImagePin:        false,
	MethodImageCacheStatus:       false,
	MethodDoctorStatus:           false,
	MethodDeleteVolume:           false,
	MethodInventoryRemoval:       false,
	MethodAttestRemoval:          false,
	MethodResetStorage:           false,
	MethodGrowStorage:            false,
	MethodPreflightReimage:       false,
	MethodCreateBackup:           false,
	MethodDeleteBackup:           false,
	MethodCopyStorage:            false,
	MethodExportCustody:          false,
	MethodDialAttemptPort:        false,
	MethodDialHostBridge:         false,
	MethodSetComputerControl:     false,
	MethodSetComputerToken:       false,
	MethodListRunMailbox:         false,
	MethodReadRunMailbox:         false,
	MethodRemoveRunMailbox:       false,
	MethodInventoryHandoffs:      false,
	MethodSetComputerMemoryLimit: false,
	MethodCopyComputerSeat:       false,
	MethodSetSeatDriver:          false,
	MethodDialSeatEndpoint:       false,
	MethodExecComputerSeat:       false,
	MethodApplyComputerSeats:     false,
	MethodObserveComputerSeats:   false,
}

// declaredMethods reads every Method constant from this package's source, for
// every GOOS, so a constant behind a build tag still needs a row.
func declaredMethods(t *testing.T) map[Method]string {
	t.Helper()
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	declared := map[Method]string{}
	files := token.NewFileSet()
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(files, path, source, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		found, err := methodConstants(file)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		for method, name := range found {
			declared[method] = name
		}
	}
	if len(declared) == 0 {
		t.Fatal("found no Method constants; the source scan is broken")
	}
	return declared
}

// methodConstants returns the Method constants one file declares. It accepts
// the two forms Go infers as Method -- `Name Method = "x"` and
// `Name = Method("x")` -- and refuses any form it cannot read as a literal, so
// a constant can never pass by being invisible to the scan.
func methodConstants(file *ast.File) (map[Method]string, error) {
	found := map[Method]string{}
	for _, decl := range file.Decls {
		block, ok := decl.(*ast.GenDecl)
		if !ok || block.Tok != token.CONST {
			continue
		}
		previousMethod := false
		for _, spec := range block.Specs {
			value := spec.(*ast.ValueSpec)
			typed := false
			if kind, ok := value.Type.(*ast.Ident); ok && kind.Name == "Method" {
				typed = true
			}
			if len(value.Values) == 0 {
				// An implicit repetition copies the previous spec's type.
				if previousMethod {
					return nil, fmt.Errorf("Method constant %s repeats the previous expression; give it its own literal", value.Names[0].Name)
				}
				continue
			}
			previousMethod = false
			for i, name := range value.Names {
				expression := value.Values[i]
				if !typed {
					call, ok := expression.(*ast.CallExpr)
					if !ok {
						continue
					}
					if fun, ok := call.Fun.(*ast.Ident); !ok || fun.Name != "Method" || len(call.Args) != 1 {
						continue
					}
					expression = call.Args[0]
				}
				previousMethod = true
				literal, ok := expression.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return nil, fmt.Errorf("Method constant %s is not a string literal", name.Name)
				}
				method, err := strconv.Unquote(literal.Value)
				if err != nil {
					return nil, err
				}
				found[Method(method)] = name.Name
			}
		}
	}
	return found, nil
}

func TestMethodConstantScanSeesEveryForm(t *testing.T) {
	parse := func(source string) (map[Method]string, error) {
		file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", "package ocihelper\n"+source, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		return methodConstants(file)
	}
	found, err := parse(`const (
	MethodTyped Method = "Typed"
	MethodInferred = Method("Inferred")
	unrelated = "Unrelated"
	unrelatedTyped Code = "Code"
)
const MethodSingle = Method("Single")`)
	if err != nil {
		t.Fatal(err)
	}
	want := map[Method]string{"Typed": "MethodTyped", "Inferred": "MethodInferred", "Single": "MethodSingle"}
	if fmt.Sprint(found) != fmt.Sprint(want) {
		t.Fatalf("scan = %v, want %v", found, want)
	}
	for _, source := range []string{
		`const MethodComputed Method = prefix + "x"`,
		`const MethodComputed = Method(prefix + "x")`,
		"const (\n\tMethodA Method = \"A\"\n\tMethodB\n)",
	} {
		if found, err := parse(source); err == nil {
			t.Fatalf("scan accepted an unreadable Method constant %q: %v", source, found)
		}
	}
}

func engineFailure(fact *EngineFailureFact) *RPCError {
	return &RPCError{Code: CodeEngineFailure, Message: "OCI engine operation failed", EngineFailure: fact}
}

func TestEngineFailureRuntimeLossCoversEveryMethod(t *testing.T) {
	declared := declaredMethods(t)
	var missing, stale []string
	for method, name := range declared {
		if _, ok := engineFailureLossByMethod[method]; !ok {
			missing = append(missing, name)
		}
	}
	for method := range engineFailureLossByMethod {
		if _, ok := declared[method]; !ok {
			stale = append(stale, string(method))
		}
	}
	slices.Sort(missing)
	slices.Sort(stale)
	if len(missing) > 0 || len(stale) > 0 {
		t.Fatalf("decide whether an engine_failure for each new Method proves runtime loss: missing rows %v, rows for no Method %v", missing, stale)
	}

	for method, want := range engineFailureLossByMethod {
		for _, reason := range []EngineFailureReason{EngineFailureOperationFailed, EngineFailurePermissionDenied} {
			got := rpcErrorProvesRuntimeLoss(engineFailure(&EngineFailureFact{Operation: method, Reason: reason}))
			if got != want {
				t.Errorf("%s engine_failure (%s) proves runtime loss = %t, want %t", method, reason, got, want)
			}
		}
	}

	for _, tc := range []struct {
		name string
		fact *EngineFailureFact
		want bool
	}{
		{"attempt-scoped Run refusal", &EngineFailureFact{Operation: MethodRun, Reason: EngineFailureOperationFailed, AttemptScoped: true}, false},
		{"Delete deadline", &EngineFailureFact{Operation: MethodDelete, Reason: EngineFailureDeadlineExceeded}, false},
		{"Delete cancellation", &EngineFailureFact{Operation: MethodDelete, Reason: EngineFailureCanceled}, false},
		{"Watch deadline", &EngineFailureFact{Operation: MethodWatch, Reason: EngineFailureDeadlineExceeded}, true},
		{"no fact", nil, true},
		{"no operation", &EngineFailureFact{Reason: EngineFailureOperationFailed}, true},
	} {
		if got := rpcErrorProvesRuntimeLoss(engineFailure(tc.fact)); got != tc.want {
			t.Errorf("%s: proves runtime loss = %t, want %t", tc.name, got, tc.want)
		}
	}
}

// The six Computer Storage and removal methods #1606 found unexempt, and the
// pre-Run attempt pin release: one refusal used to close the session and end
// every OCI attempt on the Node.
// It now returns to its caller, and the session stays healthy.
func TestScopedEngineFailureKeepsTheSessionHealthy(t *testing.T) {
	client, stop := startTestServer(t, newFakeEngine(), ServerConfig{HeartbeatTimeout: 5 * time.Second})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	for _, method := range []Method{MethodResetStorage, MethodGrowStorage, MethodExportCustody, MethodInventoryRemoval, MethodAttestRemoval, MethodPreflightReimage, MethodReleaseAttemptPin} {
		refusal := engineFailure(&EngineFailureFact{Operation: method, Reason: EngineFailureOperationFailed})
		var loss *RuntimeLossError
		if err := session.markOperationFailure(t.Context(), refusal); err != refusal || errors.As(err, &loss) {
			t.Fatalf("%s refusal was rewritten to %v", method, err)
		}
		if err := session.HealthError(); err != nil {
			t.Fatalf("%s refusal marked the session lost: %v", method, err)
		}
	}
	sweep := engineFailure(&EngineFailureFact{Operation: MethodSweep, Reason: EngineFailureOperationFailed})
	var loss *RuntimeLossError
	if err := session.markOperationFailure(t.Context(), sweep); !errors.As(err, &loss) {
		t.Fatalf("Sweep engine_failure = %v, want runtime loss", err)
	}
	if session.HealthError() == nil {
		t.Fatal("Sweep engine_failure left the session healthy")
	}
}
