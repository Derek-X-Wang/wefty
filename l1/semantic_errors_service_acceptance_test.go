//go:build service_acceptance

package l1

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestServiceAcceptanceSemanticAgentAuthorityErrors(t *testing.T) {
	assertSemanticAgentAuthorityErrors(t)
}

func TestServiceAcceptanceJobSpecAndFailurePolicy(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	port := 8080
	maxRestarts := 3
	spec := validJobSpec("service-acceptance-contract", nil)
	spec.Class = contract.JobClassService
	spec.Execution.HandoffDirectory = ""
	spec.PublishedPort = &port
	spec.Restart = contract.RestartAlways
	spec.MaxRestartStreak = &maxRestarts

	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusCreated {
		t.Fatalf("submit service status = %d body=%s", status, body)
	}
	var job Job
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	if job.Spec.Class != contract.JobClassService || job.Spec.Execution.HandoffDirectory != "" || job.Spec.PublishedPort == nil || *job.Spec.PublishedPort != port {
		t.Fatalf("persisted service contract = %#v", job.Spec)
	}
	if !IsRestartableSpawnFailure(contract.SpawnFailureStartupReadinessTimeout) {
		t.Fatal("startup readiness timeout must be restartable")
	}
	if IsRestartableSpawnFailure(contract.SpawnFailureCode("unknown")) {
		t.Fatal("unknown spawn failure must default terminal")
	}
	if err := validateProcessResult(ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseGuardian}); err != nil {
		t.Fatalf("structured guardian termination rejected: %v", err)
	}
	if err := validateProcessResult(ProcessResult{RuntimeFailure: &contract.RuntimeFailure{
		Code: contract.RuntimeFailureUnavailable, Message: "engine lost",
	}, OOM: true}); err != nil {
		t.Fatalf("runtime failure with additive OOM rejected: %v", err)
	}
	if got := classifyRuntimeFailure(contract.RuntimeFailureCode("unknown")); got != spawnFailureTerminal {
		t.Fatalf("unknown runtime failure classification = %d, want terminal", got)
	}
}
