//go:build service_acceptance && (darwin || linux)

package agent

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/agent/managedroot"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

func TestServiceAcceptanceAgentClassAndHandoffContract(t *testing.T) {
	resolvedTemporaryRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stateRoot := filepath.Join(resolvedTemporaryRoot, "state")
	resource, err := initializeManagedResource(stateRoot, "node-1", "boot-1")
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("service executable")
	digest := sha256.Sum256(content)
	runner := &serviceContractRunner{t: t}
	agent := &Agent{
		runner:          runner,
		managedResource: resource,
		handoffs:        newHandoffManager(t.TempDir(), DefaultHandoffRetention),
	}
	workingDirectory := t.TempDir()
	operatorFile := filepath.Join(workingDirectory, "operator-owned")
	if err := os.WriteFile(operatorFile, []byte("untouched"), 0o600); err != nil {
		t.Fatal(err)
	}
	claim := l1.Claim{
		Job: l1.Job{JobID: "service-job", Spec: contract.JobSpec{
			Kind:  "process",
			Class: contract.JobClassService,
			Execution: contract.ExecutionSpec{
				Executable: contract.ExecutableSpec{
					InlineBase64: base64.StdEncoding.EncodeToString(content),
					SHA256:       hex.EncodeToString(digest[:]),
				},
				Argv: []string{"service"}, Env: map[string]string{
					contract.EnvServiceDir: "/untrusted/public",
				}, SensitiveEnv: map[string]string{
					contract.EnvServiceDir: "/untrusted/secret",
				}, WorkingDirectory: workingDirectory,
			},
		}},
		Lease: l1.AttemptLease{AttemptID: "service-contract-attempt"},
	}
	beforeTemp := executableTempDirectories(t, claim.Lease.AttemptID)
	result, err := agent.runProcess(context.Background(), claim)
	if err != nil || result.ExitCode == nil || *result.ExitCode != 0 || !runner.called {
		t.Fatalf("service runProcess() = (%#v, %v), runner called=%v", result, err, runner.called)
	}
	paths := servicePaths(t, stateRoot, claim.Job.JobID)
	if runner.serviceDirectory != paths.Data {
		t.Fatalf("payload %s = %q, want %q", contract.EnvServiceDir, runner.serviceDirectory, paths.Data)
	}
	if !pathWithin(runner.executablePath, paths.Runtime) {
		t.Fatalf("inline executable = %q, want under managed runtime %q", runner.executablePath, paths.Runtime)
	}
	if payload, err := os.ReadFile(operatorFile); err != nil || string(payload) != "untouched" {
		t.Fatalf("working directory changed: payload=%q err=%v", payload, err)
	}
	assertDirectoryEmpty(t, paths.Attempts)
	assertDirectoryEmpty(t, paths.Runtime)
	if afterTemp := executableTempDirectories(t, claim.Lease.AttemptID); !sameStringSet(beforeTemp, afterTemp) {
		t.Fatalf("wefty executable temp directories changed across service run: before=%v after=%v", beforeTemp, afterTemp)
	}
	restartedResource, err := initializeManagedResource(stateRoot, "node-1", "boot-2")
	if err != nil {
		t.Fatal(err)
	}
	agent.managedResource = restartedResource
	claim.Lease.AttemptID = "service-contract-restart"
	result, err = agent.runProcess(context.Background(), claim)
	if err != nil || result.ExitCode == nil || *result.ExitCode != 0 || runner.calls != 2 {
		t.Fatalf("restarted service runProcess() = (%#v, %v), calls=%d", result, err, runner.calls)
	}
	assertDirectoryEmpty(t, paths.Attempts)
	assertDirectoryEmpty(t, paths.Runtime)
	var ownership managedroot.OwnershipManifest
	readJSON(t, filepath.Join(paths.Root, managedroot.OwnershipManifestName), &ownership)
	if ownership.JobID != claim.Job.JobID || ownership.RemovalGeneration != initialServiceRemovalGeneration {
		t.Fatalf("ownership manifest = %#v", ownership)
	}

	claim.Job.Spec.Class = "scheduled"
	result, err = agent.runProcess(context.Background(), claim)
	if err == nil || result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailureUnsupportedClass {
		t.Fatalf("unknown-class runProcess() = (%#v, %v)", result, err)
	}
}

type serviceContractRunner struct {
	t                *testing.T
	called           bool
	calls            int
	executablePath   string
	serviceDirectory string
}

func (runner *serviceContractRunner) Run(_ context.Context, request processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
	runner.called = true
	runner.calls++
	runner.executablePath = request.Execution.Executable.Path
	runner.serviceDirectory = request.Execution.Env[contract.EnvServiceDir]
	if _, exists := request.Execution.SensitiveEnv[contract.EnvServiceDir]; exists {
		runner.t.Fatalf("%s remained in SensitiveEnv", contract.EnvServiceDir)
	}
	if payload, err := os.ReadFile(request.Execution.Executable.Path); err != nil || string(payload) != "service executable" {
		runner.t.Fatalf("materialized executable payload=%q err=%v", payload, err)
	}
	persistent := filepath.Join(runner.serviceDirectory, "survives-restart")
	if runner.calls == 1 {
		if err := os.WriteFile(persistent, []byte("durable"), 0o600); err != nil {
			runner.t.Fatal(err)
		}
	} else if payload, err := os.ReadFile(persistent); err != nil || string(payload) != "durable" {
		runner.t.Fatalf("service data did not survive restart: payload=%q err=%v", payload, err)
	}
	exitCode := 0
	return contract.ProcessResult{ExitCode: &exitCode}, nil
}

func servicePaths(t *testing.T, stateRoot, jobID string) managedroot.ServicePaths {
	t.Helper()
	manager, err := managedroot.Open(managedroot.Config{Root: stateRoot, NodeID: "node-1", BootSessionID: "boot-1"})
	if err != nil {
		t.Fatal(err)
	}
	paths, err := manager.ServicePaths(jobID)
	if err != nil {
		t.Fatal(err)
	}
	return paths
}

func executableTempDirectories(t *testing.T, attemptID string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), "wefty-executable-"+attemptID+"-*"))
	if err != nil {
		t.Fatal(err)
	}
	return matches
}

func pathWithin(path, root string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != "." && relative != ".." && !filepath.IsAbs(relative) &&
		!strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func assertDirectoryEmpty(t *testing.T, path string) {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("directory %q is not empty: %#v", path, entries)
	}
}

func readJSON(t *testing.T, path string, target any) {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(payload, target); err != nil {
		t.Fatal(err)
	}
}

func sameStringSet(left, right []string) bool {
	return slices.Equal(left, right)
}
