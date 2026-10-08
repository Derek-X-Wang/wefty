package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

// Keep stdout's result and stderr's error separate: accepted mutations can
// legitimately emit both, whereas Run verdicts must emit only their result.
func runPolishBinary(t *testing.T, binary string, args ...string) (int, []byte, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, args...)
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, l1AddressEnv+"=") && !strings.HasPrefix(entry, l3AddressEnv+"=") {
			command.Env = append(command.Env, entry)
		}
	}
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	if ctx.Err() != nil {
		t.Fatalf("binary timed out: %v stdout=%s stderr=%s", args, stdout.String(), stderr.String())
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return exit.ExitCode(), stdout.Bytes(), stderr.Bytes()
	}
	if err != nil {
		t.Fatal(err)
	}
	return 0, stdout.Bytes(), stderr.Bytes()
}

func TestPolishMutationWaitTimeoutFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	var slowReads atomic.Int32
	address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/computers/computer-1":
			_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: "computer-1", IntentRevision: 2, StorageID: "storage-1", StorageGeneration: 1})
		case "/v1/computers/computer-1/backups":
			if r.Method == http.MethodPost {
				var request l1.ComputerBackupCreateRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				if request.IdempotencyKey == "replay" {
					w.Header().Set("Idempotent-Replay", "true")
				}
				w.Header().Set("Backup-Id", "backup-1")
				w.Header().Set("Backup-Operation-Revision", "3")
				w.WriteHeader(http.StatusAccepted)
				_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: "computer-1", IntentRevision: 3, StorageID: "storage-1", StorageGeneration: 1})
			} else {
				_ = json.NewEncoder(w).Encode(l1.BackupList{Backups: []l1.Backup{}, Operation: &l1.ComputerBackupOperationOutcome{BackupID: "backup-1", OperationRevision: 3, Status: "planned"}})
			}
		case "/v1/computers/computer-1/storage-provenance":
			_ = json.NewEncoder(w).Encode(l1.ComputerStorageProvenance{})
		case "/v1/jobs/starting-service", "/v1/jobs/starting-service/desired-state":
			if r.Method != http.MethodGet {
				w.WriteHeader(http.StatusAccepted)
			}
			_ = json.NewEncoder(w).Encode(l1.Job{JobID: "starting-service", State: contract.JobStopped, ServiceJob: &l1.ServiceJob{DesiredState: contract.ServiceDesiredStopped}})
		case "/v1/jobs/slow-service", "/v1/jobs/slow-service/desired-state":
			if r.Method == http.MethodGet && slowReads.Add(1) > 1 {
				<-r.Context().Done()
				return
			}
			if r.Method != http.MethodGet {
				w.WriteHeader(http.StatusAccepted)
			}
			_ = json.NewEncoder(w).Encode(l1.Job{JobID: "slow-service", State: contract.JobRunning, ServiceJob: &l1.ServiceJob{DesiredState: contract.ServiceDesiredRunning}})
		case "/v1/jobs/service-1", "/v1/jobs/service-1/desired-state", "/v1/jobs/service-1/remove":
			if r.Method != http.MethodGet {
				w.WriteHeader(http.StatusAccepted)
			}
			_ = json.NewEncoder(w).Encode(l1.Job{JobID: "service-1", State: contract.JobRunning, ServiceJob: &l1.ServiceJob{DesiredState: contract.ServiceDesiredRunning}})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			http.Error(w, "unexpected request", 404)
		}
	})
	for _, args := range [][]string{
		{"services", "backup", "create", "computer-1", "--idempotency-key=k", "--intent-revision=2", "--storage-id=storage-1", "--storage-generation=1"},
		{"services", "backup", "create", "computer-1", "--idempotency-key=replay", "--intent-revision=2", "--storage-id=storage-1", "--storage-generation=1"},
		{"services", "start", "starting-service"},
		{"services", "stop", "service-1"},
		{"services", "stop", "slow-service"},
		{"services", "remove", "service-1"},
	} {
		t.Run(strings.Join(args[:3], "/"), func(t *testing.T) {
			command := append([]string{"--json", "--l1=" + address}, args...)
			command = append(command, "--wait=30ms", "--poll-interval=1ms")
			code, stdout, stderr := runPolishBinary(t, binary, command...)
			var envelope contract.ErrorResponse
			wantReplay := hasArg(args, "--idempotency-key=replay")
			wantApplied := !wantReplay
			var wantEvidence any
			if args[1] == "backup" {
				wantEvidence = wantApplied
			}
			if err := json.Unmarshal(stderr, &envelope); err != nil || code != 14 || envelope.Error.Code != "wait_timeout" || envelope.Error.Details["mutation_applied"] != wantEvidence || envelope.Error.Retryable || envelope.Error.RequestID != "" {
				t.Errorf("exit=%d stdout=%s stderr=%s decode=%v", code, stdout, stderr, err)
			}
			if _, present := envelope.Error.Details["mutation_applied"]; !present {
				t.Errorf("mutation_applied omitted: %s", stderr)
			}
			if args[1] == "backup" {
				var result storageMutationOutput
				if err := json.Unmarshal(stdout, &result); err != nil || result.MutationApplied != wantApplied || result.IdempotentReplay != wantReplay || result.Observation == nil || result.Observation.Status != "failed" {
					t.Errorf("accepted mutation result=%s decode=%v", stdout, err)
				}
			}
		})
	}
}

func TestPolishDerivedFailureReasonFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, recorded := range []string{"", "recorded reason"} {
		t.Run(recorded, func(t *testing.T) {
			address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if strings.HasSuffix(r.URL.Path, "/execution") {
					if recorded != "" {
						t.Error("recorded reason should not require execution lookup")
					}
					_ = json.NewEncoder(w).Encode(l3.RunExecution{RunID: "failed", Job: &l1.Job{
						JobID: "job-1", State: contract.JobFailed, Attempts: []l1.Attempt{{State: contract.AttemptFailed,
							Result: &l1.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}}},
					}})
					return
				}
				_ = json.NewEncoder(w).Encode(contract.RunRecord{RunID: "failed", Status: contract.RunFailed, FailureReason: recorded})
			})
			code, stdout, stderr := runPolishBinary(t, binary, "--json", "--l3="+address, "wait", "failed")
			want := recorded
			if want == "" {
				want = "signal terminated (agent)"
			}
			var result contract.RunRecord
			if err := json.Unmarshal(stdout, &result); err != nil || code != 10 || result.FailureReason != want || len(stderr) != 0 {
				t.Fatalf("exit=%d stdout=%s stderr=%s decode=%v", code, stdout, stderr, err)
			}
		})
	}
}

func TestPolishLimaJSONFlagValuesFromRealBinary(t *testing.T) {
	if runtime.GOOS != "darwin" {
		// Linux setup-oci registers its own flags and no Lima sizing flags;
		// the parser's value-flag list is covered on every OS by json_flags_test.
		t.Skip("Lima sizing flags are registered only by the macOS node setup")
	}
	binary := buildWefty(t)
	missing := filepath.Join(t.TempDir(), "absent-config.json")
	for _, name := range []string{"vm-memory", "vm-cpus", "vm-disk"} {
		code, _, stderr := runPolishBinary(t, binary, "--node-config="+missing, "node", "setup-oci", "--"+name, "--json")
		if code != 1 || json.Valid(stderr) || !strings.Contains(string(stderr), missing) {
			t.Errorf("literal --json became global for %s: exit=%d stderr=%s", name, code, stderr)
		}
		for _, value := range []string{"--json", "--json=nope", "--json=false"} {
			t.Run(name+"/"+value, func(t *testing.T) {
				code, stdout, stderr := runPolishBinary(t, binary, "--node-config="+missing, "node", "setup-oci", "--"+name, value, "--json")
				var envelope contract.ErrorResponse
				if err := json.Unmarshal(stderr, &envelope); err != nil || code != 1 || envelope.Error.Code != contract.ErrorInternal || !strings.Contains(envelope.Error.Message, missing) || len(stdout) != 0 {
					t.Fatalf("flag value changed global parsing: exit=%d stdout=%s stderr=%s decode=%v", code, stdout, stderr, err)
				}
			})
		}
	}
}

func TestPolishUnavailableEnvelopeFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, test := range []struct {
		code         contract.ErrorCode
		retryable    bool
		status, exit int
	}{
		{contract.ErrorInternal, true, 503, 13},
		{contract.ErrorRunLedgerUnavailable, true, 503, 13},
		{contract.ErrorInternal, false, 503, 1},
		{contract.ErrorRunLedgerUnavailable, false, 503, 1},
		{contract.ErrorInternal, true, 400, 1},
		{contract.ErrorForbidden, true, 503, 3},
		{contract.ErrorCode("future_code"), true, 503, 1},
	} {
		t.Run(string(test.code)+"/"+http.StatusText(test.status)+"/"+map[bool]string{true: "retryable", false: "permanent"}[test.retryable], func(t *testing.T) {
			address := startStubLedger(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(test.status)
				_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
					Code: test.code, Message: "server refusal", Retryable: test.retryable,
					RequestID: "request-711", Details: map[string]any{"reason": "preserved"},
				}})
			})
			code, stdout, stderr := runPolishBinary(t, binary, "--json", "--l3="+address, "inspect", "run-1")
			var envelope contract.ErrorResponse
			if err := json.Unmarshal(stderr, &envelope); err != nil || code != test.exit || envelope.Error.Code != test.code || envelope.Error.Retryable != test.retryable || envelope.Error.RequestID != "request-711" || envelope.Error.Details["reason"] != "preserved" || len(stdout) != 0 {
				t.Errorf("exit=%d want=%d stdout=%s stderr=%s decode=%v", code, test.exit, stdout, stderr, err)
			}
		})
	}
}

func TestPolishBareDeadlineIsNotUnavailable(t *testing.T) {
	var stderr bytes.Buffer
	writeCommandError(&stderr, context.DeadlineExceeded, true)
	var envelope contract.ErrorResponse
	if err := json.Unmarshal(stderr.Bytes(), &envelope); err != nil || commandExitCode(context.DeadlineExceeded) != exitFailure || envelope.Error.Code != contract.ErrorInternal || envelope.Error.Retryable {
		t.Fatalf("bare deadline misclassified: %s decode=%v", stderr.String(), err)
	}
}
