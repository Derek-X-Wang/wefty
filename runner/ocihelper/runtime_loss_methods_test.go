package ocihelper

import (
	"errors"
	"fmt"
	"go/ast"
	"go/build"
	"go/constant"
	"go/parser"
	"go/token"
	"go/types"
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

// declaredMethods reads every Method constant from this package's source for
// each platform the package builds on, so a constant behind a build tag, or a
// platform-specific value under the same name, still needs a row.
func declaredMethods(t *testing.T) map[Method]string {
	t.Helper()
	declared := map[Method]string{}
	for _, goos := range []string{"linux", "darwin"} {
		context := build.Default
		context.GOOS = goos
		pkg, err := context.ImportDir(".", 0)
		if err != nil {
			t.Fatal(err)
		}
		files := token.NewFileSet()
		var parsed []*ast.File
		for _, path := range pkg.GoFiles {
			file, err := parser.ParseFile(files, path, nil, parser.SkipObjectResolution)
			if err != nil {
				t.Fatal(err)
			}
			parsed = append(parsed, file)
		}
		found, err := methodConstants(files, parsed)
		if err != nil {
			t.Fatalf("%s: %v", goos, err)
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

type noImports struct{}

func (noImports) Import(path string) (*types.Package, error) {
	return nil, fmt.Errorf("import %s is not loaded by the Method scan", path)
}

// methodConstants type-checks one platform's file set and returns every
// constant definition whose type is the package's Method, however it is
// written (typed, converted, parenthesized, computed, an implicit repetition)
// and wherever it is declared, package level or inside a function. Imports
// are not loaded, and only the errors that causes are tolerated: any other
// checker error, such as a redeclaration, fails the scan. A constant named
// Method* must have the Method type, because an untyped string converts to
// Method silently.
//
// Limit: a Method constant whose value comes from another package cannot be
// typed without loading it. Protocol method names are string literals in this
// package; one built from an import would show here as an invalid constant.
func methodConstants(files *token.FileSet, parsed []*ast.File) (map[Method]string, error) {
	var checkErrors []error
	config := types.Config{Importer: noImports{}, Error: func(err error) { checkErrors = append(checkErrors, err) }}
	info := &types.Info{Defs: map[*ast.Ident]types.Object{}}
	pkg, _ := config.Check("ocihelper", files, parsed, info)
	if pkg == nil {
		return nil, errors.New("type-check produced no package")
	}
	for _, err := range checkErrors {
		if typeError, ok := err.(types.Error); ok && typeError.Soft {
			continue
		}
		message := err.Error()
		if strings.Contains(message, "could not import") || strings.Contains(message, "undefined:") ||
			strings.Contains(message, "invalid operation") || strings.Contains(message, "cannot use") ||
			strings.Contains(message, "not a type") || strings.Contains(message, "invalid argument") {
			continue
		}
		return nil, fmt.Errorf("Method scan type-check: %w", err)
	}
	found := map[Method]string{}
	for ident, object := range info.Defs {
		defined, ok := object.(*types.Const)
		if !ok {
			continue
		}
		named, isNamed := defined.Type().(*types.Named)
		isMethod := isNamed && named.Obj().Name() == "Method" && named.Obj().Pkg() == pkg
		if !isMethod {
			if strings.HasPrefix(ident.Name, "Method") {
				return nil, fmt.Errorf("constant %s is named like a Method but has type %s", ident.Name, defined.Type())
			}
			continue
		}
		if defined.Val().Kind() != constant.String {
			return nil, fmt.Errorf("Method constant %s has no string value", ident.Name)
		}
		method := Method(constant.StringVal(defined.Val()))
		if previous, exists := found[method]; exists && previous != ident.Name {
			return nil, fmt.Errorf("Method %q is declared as both %s and %s", method, previous, ident.Name)
		}
		found[method] = ident.Name
	}
	return found, nil
}

func TestMethodConstantScanSeesEveryForm(t *testing.T) {
	scan := func(sources ...string) (map[Method]string, error) {
		files := token.NewFileSet()
		var parsed []*ast.File
		for i, source := range sources {
			body := "package ocihelper\n" + source + "\n"
			if i == 0 {
				body += "type Method string\n"
			}
			file, err := parser.ParseFile(files, fmt.Sprintf("fixture%d.go", i), body, parser.SkipObjectResolution)
			if err != nil {
				t.Fatal(err)
			}
			parsed = append(parsed, file)
		}
		return methodConstants(files, parsed)
	}
	found, err := scan(`import "example.invalid/unloaded"
var _ = unloaded.Value
const prefix = "Pre"
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
)
func call() Method {
	const local Method = "Local"
	return local
}`)
	if err != nil {
		t.Fatal(err)
	}
	want := map[Method]string{"Typed": "MethodTyped", "Inferred": "MethodInferred", "Paren": "MethodParen",
		"PreComputed": "MethodComputed", "NewSuffix": "MethodSuffixed", "ParenType": "hiddenTyped", "Local": "local"}
	if fmt.Sprint(found) != fmt.Sprint(want) {
		t.Fatalf("scan = %v, want %v", found, want)
	}
	for name, sources := range map[string][]string{
		"untyped Method-named constant": {`const MethodUntyped = "Untyped"`},
		"untyped implicit repetition":   {"const (\n\tother = \"x\"\n\tMethodHidden\n)"},
		"repeated Method value":         {"const (\n\tfirst Method = \"Same\"\n\tsecond\n)"},
		"non-string Method value":       {`const first Method = "First" + iota`},
		"redeclared across files":       {`const MethodA Method = "A"`, `const MethodA Method = "B"`},
		"same value under two names":    {`const MethodA Method = "A"`, `const MethodB Method = "A"`},
		"Method-named local untyped":    {"func call() {\n\tconst MethodLocal = \"x\"\n\t_ = MethodLocal\n}"},
	} {
		if found, err := scan(sources...); err == nil {
			t.Fatalf("scan accepted %s: %v", name, found)
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
