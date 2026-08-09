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
