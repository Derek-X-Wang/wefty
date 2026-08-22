package contract

import (
	"encoding/json"
	"testing"
)

func TestProcessResultPreservesExitZero(t *testing.T) {
	exitCode := 0
	encoded, err := json.Marshal(ProcessResult{ExitCode: &exitCode})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(encoded), `{"exit_code":0}`; got != want {
		t.Fatalf("ProcessResult JSON = %s, want %s", got, want)
	}
}

func TestProcessResultSerializesOutputFailure(t *testing.T) {
	encoded, err := json.Marshal(ProcessResult{OutputError: "durable log flush failed"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(encoded), `{"output_error":"durable log flush failed"}`; got != want {
		t.Fatalf("ProcessResult JSON = %s, want %s", got, want)
	}
}

func TestProcessResultSerializesCodedSpawnFailure(t *testing.T) {
	encoded, err := json.Marshal(ProcessResult{SpawnError: &SpawnFailure{
		Code: SpawnFailureExecutableMaterialization, Message: "digest mismatch",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(encoded), `{"spawn_error":{"code":"executable_materialization_failed","message":"digest mismatch"}}`; got != want {
		t.Fatalf("ProcessResult JSON = %s, want %s", got, want)
	}
}

func TestProcessResultSerializesTerminationCause(t *testing.T) {
	encoded, err := json.Marshal(ProcessResult{Signal: "terminated", TerminationCause: TerminationCauseGuardian})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(encoded), `{"signal":"terminated","termination_cause":"guardian"}`; got != want {
		t.Fatalf("ProcessResult JSON = %s, want %s", got, want)
	}
}

func TestProcessResultSerializesRuntimeFailureAndAdditiveOOM(t *testing.T) {
	encoded, err := json.Marshal(ProcessResult{
		RuntimeFailure: &RuntimeFailure{Code: RuntimeFailureUnavailable, Message: "helper lost"}, OOM: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(encoded), `{"runtime_failure":{"code":"runtime_unavailable","message":"helper lost"},"oom":true}`; got != want {
		t.Fatalf("ProcessResult JSON = %s, want %s", got, want)
	}
}
