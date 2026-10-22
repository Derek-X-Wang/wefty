package process

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	wr "github.com/Derek-X-Wang/wefty/runner"
)

func TestRestartableRequiresGuardian(t *testing.T) {
	for _, path := range []string{"", filepath.Join(t.TempDir(), "missing")} {
		runner := New(Config{GuardianExecutable: path})
		request := Request{RequireGuardian: true, Guarded: true, AttemptID: "attempt", Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", "-c", "exit 0"}, WorkingDirectory: t.TempDir()}}
		result, err := runner.Run(context.Background(), request, nil)
		if err == nil || result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailureRuntimeUnavailable {
			t.Fatalf("run without Guardian: %+v %v", result, err)
		}
		adapter := NewAdapter(runner)
		_, result2, err := adapter.Preflight(context.Background(), wr.Request{RequireGuardian: true, Authority: wr.AttemptAuthority{NodeID: "n", BootSessionID: "b", JobID: "j", AttemptID: "a", FencingToken: "f"}})
		if err == nil || result2.Outcome.SpawnError == nil || result2.Outcome.SpawnError.Code != contract.SpawnFailureRuntimeUnavailable {
			t.Fatalf("preflight without Guardian: %+v %v", result2, err)
		}
	}
}
