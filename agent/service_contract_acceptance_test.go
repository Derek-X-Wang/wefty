//go:build service_acceptance && (darwin || linux)

package agent

import (
	"context"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

func TestServiceAcceptanceAgentClassAndHandoffContract(t *testing.T) {
	runner := &serviceContractRunner{}
	agent := &Agent{
		runner:   runner,
		handoffs: newHandoffManager(t.TempDir(), DefaultHandoffRetention),
	}
	claim := l1.Claim{
		Job: l1.Job{Spec: contract.JobSpec{
			Kind:  "process",
			Class: contract.JobClassService,
			Execution: contract.ExecutionSpec{
				Executable: contract.ExecutableSpec{Path: "/bin/true"},
				Argv:       []string{"true"}, WorkingDirectory: t.TempDir(),
			},
		}},
		Lease: l1.AttemptLease{AttemptID: "service-contract-attempt"},
	}
	result, err := agent.runProcess(context.Background(), claim)
	if err != nil || result.ExitCode == nil || *result.ExitCode != 0 || !runner.called {
		t.Fatalf("service runProcess() = (%#v, %v), runner called=%v", result, err, runner.called)
	}

	claim.Job.Spec.Class = "scheduled"
	result, err = agent.runProcess(context.Background(), claim)
	if err == nil || result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailureUnsupportedClass {
		t.Fatalf("unknown-class runProcess() = (%#v, %v)", result, err)
	}
}

type serviceContractRunner struct {
	called bool
}

func (runner *serviceContractRunner) Run(context.Context, processrunner.Request, processrunner.OutputSink) (contract.ProcessResult, error) {
	runner.called = true
	exitCode := 0
	return contract.ProcessResult{ExitCode: &exitCode}, nil
}
