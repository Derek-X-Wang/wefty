package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestSubmitRejectsOverflowingRuntimeSeconds(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := executeSubmit(t.Context(), ledgerStub(t, nil), true, []string{"--image=example/tool:latest", "--max-runtime=9223372037"}, &stdout, &stderr)
	if commandExitCode(err) != exitUsage || !strings.Contains(err.Error(), "max_runtime_seconds") {
		t.Fatalf("overflow err=%v code=%d", err, commandExitCode(err))
	}
}

func TestWaitJSONPreservesReachedLimitReasonAndExitTen(t *testing.T) {
	for _, reason := range []string{"maximum runtime reached", "idle limit reached"} {
		t.Run(reason, func(t *testing.T) {
			clients := ledgerStub(t, map[string]any{"/v1/runs/run-limit": contract.RunRecord{RunID: "run-limit", Status: contract.RunFailed, FailureReason: reason}})
			var out bytes.Buffer
			err := executeWait(t.Context(), clients, true, []string{"run-limit"}, &out, &bytes.Buffer{})
			var record contract.RunRecord
			if decodeErr := json.Unmarshal(out.Bytes(), &record); decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if commandExitCode(err) != 10 || record.Status != contract.RunFailed || record.FailureReason != reason {
				t.Fatalf("wait code=%d record=%+v err=%v", commandExitCode(err), record, err)
			}
		})
	}
}
