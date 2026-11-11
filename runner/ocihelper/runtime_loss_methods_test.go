package ocihelper

import (
	"errors"
	"fmt"
	"go/ast"
	"go/constant"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"slices"
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
	files := token.NewFileSet()
	var parsed []*ast.File
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(files, path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		parsed = append(parsed, file)
	}
	declared, err := methodConstants(files, parsed)
	if err != nil {
		t.Fatal(err)
	}
	if len(declared) == 0 {
		t.Fatal("found no Method constants; the source scan is broken")
	}
	return declared
}

type noImports struct{}

func (noImports) Import(path string) (*types.Package, error) {
	return nil, fmt.Errorf("import %s is not loaded by the Method scan", path)
}

// methodConstants type-checks the package's own source and returns every
// package-level constant whose type is its Method, however it is written:
// typed, converted, parenthesized, computed, or an implicit repetition.
// Imports are not loaded; the errors that causes are ignored, since a Method
// constant's value never depends on another package. An untyped string
// constant converts to Method silently, so a constant named Method* must have
// the Method type.
func methodConstants(files *token.FileSet, parsed []*ast.File) (map[Method]string, error) {
	config := types.Config{Importer: noImports{}, Error: func(error) {}}
	pkg, _ := config.Check("ocihelper", files, parsed, nil)
	if pkg == nil {
		return nil, errors.New("type-check produced no package")
	}
	found := map[Method]string{}
	for _, name := range pkg.Scope().Names() {
		object, ok := pkg.Scope().Lookup(name).(*types.Const)
		if !ok {
			continue
		}
		named, ok := object.Type().(*types.Named)
		if !ok || named.Obj().Name() != "Method" || named.Obj().Pkg() != pkg {
			if strings.HasPrefix(name, "Method") {
				return nil, fmt.Errorf("constant %s is named like a Method but has type %s", name, object.Type())
			}
			continue
		}
		if object.Val().Kind() != constant.String {
			return nil, fmt.Errorf("Method constant %s has no string value", name)
		}
		found[Method(constant.StringVal(object.Val()))] = name
	}
	return found, nil
}

func TestMethodConstantScanSeesEveryForm(t *testing.T) {
	scan := func(source string) (map[Method]string, error) {
		files := token.NewFileSet()
		file, err := parser.ParseFile(files, "fixture.go", "package ocihelper\ntype Method string\n"+source, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		return methodConstants(files, []*ast.File{file})
	}
	found, err := scan(`const prefix = "Pre"
const (
	MethodTyped Method = "Typed"
	MethodInferred = Method("Inferred")
	MethodParen = (Method(("Paren")))
	MethodComputed = Method(prefix + "Computed")
	MethodSuffixed = Method("New") + "Suffix"
	unrelated = "Unrelated"
)
const (
	hiddenTyped (Method) = "ParenType"
	hiddenRepeated
)
const (
	first Method = "First" + iota
)`)
	if err == nil {
		t.Fatalf("scan accepted a non-string iota Method constant: %v", found)
	}
	found, err = scan(`const prefix = "Pre"
const (
	MethodTyped Method = "Typed"
	MethodInferred = Method("Inferred")
	MethodParen = (Method(("Paren")))
	MethodComputed = Method(prefix + "Computed")
	MethodSuffixed = Method("New") + "Suffix"
	unrelated = "Unrelated"
)
const (
	hiddenTyped (Method) = "ParenType"
	hiddenRepeated
)`)
	if err != nil {
		t.Fatal(err)
	}
	want := map[Method]string{"Typed": "MethodTyped", "Inferred": "MethodInferred", "Paren": "MethodParen",
		"PreComputed": "MethodComputed", "NewSuffix": "MethodSuffixed", "ParenType": "hiddenTyped"}
	// hiddenRepeated repeats "ParenType"; either name may own the value.
	if found["ParenType"] == "hiddenRepeated" {
		want["ParenType"] = "hiddenRepeated"
	}
	if fmt.Sprint(found) != fmt.Sprint(want) {
		t.Fatalf("scan = %v, want %v", found, want)
	}
	for _, source := range []string{
		`const MethodUntyped = "Untyped"`,
		"const (\n\tother = \"x\"\n\tMethodHidden\n)",
	} {
		if found, err := scan(source); err == nil {
			t.Fatalf("scan accepted an untyped Method-named constant %q: %v", source, found)
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
