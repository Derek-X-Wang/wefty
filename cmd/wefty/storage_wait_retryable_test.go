package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// A retryable 503 inside a --wait window is one bad poll, not a verdict
// (#763). These checks pin that across every wait loop, the provenance walk,
// and the process boundary; one-shot reads stay exit 13.

// writeRetryableUnavailable answers with the defect's shape: 503 unavailable,
// retryable=true, naming the read snapshot reason.
func writeRetryableUnavailable(w http.ResponseWriter, reason string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
		Code: contract.ErrorUnavailable, Message: "read snapshot unavailable", Retryable: true,
		Details: map[string]any{"reason": reason},
	}})
}

func writeForbiddenResponse(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusForbidden)
	_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
		Code: contract.ErrorForbidden, Message: "no", Retryable: false,
	}})
}

// retryable503 is the same answer in the API error type the CLI decodes.
func retryable503() *apiResponseError {
	return &apiResponseError{Service: "L1", StatusCode: http.StatusServiceUnavailable, APIError: contract.APIError{
		Code: contract.ErrorUnavailable, Message: "read snapshot unavailable", Retryable: true,
		Details: map[string]any{"reason": "read_snapshot_expired"},
	}}
}

// retryableWaitFlags is comfortably longer than the transient fakes need, so a
// wait that only ended when the deadline expired would outrun every assertion
// here even on a loaded runner; every fake below decides by count.
var retryableWaitFlags = storageWaitFlags{timeout: 3 * time.Second, pollInterval: time.Millisecond}

func TestPollStorageObservationContinuesPastRetryableAnswers(t *testing.T) {
	t.Run("retryable answers mid-wait do not end the wait", func(t *testing.T) {
		var answers atomic.Int32
		observation, err := pollStorageObservation(t.Context(), retryableWaitFlags, func(context.Context) (bool, error) {
			switch answers.Add(1) {
			case 1, 2:
				return false, retryable503()
			}
			return true, nil
		})
		if err != nil || answers.Load() != 3 || observation.Status != "observed" || observation.Error != "" {
			t.Fatalf("wait ended early after %d retryable answers: %#v %v", answers.Load(), observation, err)
		}
	})
	t.Run("a non-retryable answer still ends the wait at once", func(t *testing.T) {
		var answers atomic.Int32
		started := time.Now()
		observation, err := pollStorageObservation(t.Context(), retryableWaitFlags, func(context.Context) (bool, error) {
			if answers.Add(1) == 1 {
				return false, retryable503()
			}
			return false, &apiResponseError{StatusCode: http.StatusForbidden, APIError: contract.APIError{Code: contract.ErrorForbidden}}
		})
		if commandExitCode(err) != exitUnauthorized || answers.Load() != 2 ||
			time.Since(started) > time.Second || observation.Status != "failed" {
			t.Fatalf("non-retryable answer did not end the wait promptly: %v (%d answers) %s", err, answers.Load(), time.Since(started))
		}
	})
	t.Run("only retryable answers until the deadline is the wait-timeout outcome", func(t *testing.T) {
		var answers atomic.Int32
		wait := storageWaitFlags{timeout: time.Second, pollInterval: time.Millisecond}
		observation, err := pollStorageObservation(t.Context(), wait, func(context.Context) (bool, error) {
			answers.Add(1)
			return false, retryable503()
		})
		if answers.Load() < 2 || commandExitCode(err) != exitMutationWaitTimeout || observation.Status != "failed" {
			t.Fatalf("retryable-only deadline: %d answers, exit %d, want wait_timeout exit 14: %v", answers.Load(), commandExitCode(err), err)
		}
		var timeout *mutationWaitTimeoutError
		if !errors.As(err, &timeout) {
			t.Fatalf("want mutationWaitTimeoutError, got %T %v", err, err)
		}
		if !strings.Contains(observation.Error, "read_snapshot_expired") {
			t.Fatalf("last retryable answer missing from observation detail: %q", observation.Error)
		}
	})
}

// TestEveryStorageWaitToleratesARetryableAnswer drives every wait loop —
// backup, prune, clone, restore, custody import and export, grow and removal —
// through the real HTTP client with a retryable 503 on the first observation
// read and the terminal answer on the next.
func TestEveryStorageWaitToleratesARetryableAnswer(t *testing.T) {
	for _, test := range []struct {
		name string
		wait func(context.Context, *apiClients) error
	}{
		{"backup", func(ctx context.Context, clients *apiClients) error {
			_, _, err := waitForBackupOperation(ctx, clients, "computer-1", "backup-1", retryableWaitFlags)
			return err
		}},
		{"prune", func(ctx context.Context, clients *apiClients) error {
			_, _, err := waitForBackupPrune(ctx, clients, "computer-1", "backup-1", retryableWaitFlags)
			return err
		}},
		{"clone", func(ctx context.Context, clients *apiClients) error {
			_, _, err := waitForComputerClone(ctx, clients, cloneComputerOperation{computerID: "computer-1", operationRevision: 1}, retryableWaitFlags)
			return err
		}},
		{"restore", func(ctx context.Context, clients *apiClients) error {
			_, _, err := waitForComputerRestore(ctx, clients, "computer-1", 1, retryableWaitFlags)
			return err
		}},
		{"custody-import", func(ctx context.Context, clients *apiClients) error {
			_, _, _, err := waitForCustodyImport(ctx, clients, "import-1", 1, retryableWaitFlags)
			return err
		}},
		{"custody-export", func(ctx context.Context, clients *apiClients) error {
			_, _, err := waitForCustodyExport(ctx, clients, "computer-1", "export-1", retryableWaitFlags)
			return err
		}},
		{"grow", func(ctx context.Context, clients *apiClients) error {
			_, _, err := waitForComputerGrowRevision(ctx, clients, "computer-1", 1, retryableWaitFlags)
			return err
		}},
		{"removal", func(ctx context.Context, clients *apiClients) error {
			_, _, err := waitForComputerRemoval(ctx, clients, "computer-1", retryableWaitFlags)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var reads atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case strings.HasSuffix(r.URL.Path, "/storage-provenance"):
					_ = json.NewEncoder(w).Encode(l1.ComputerStorageProvenance{})
				case r.URL.Path == "/v1/custody-imports/import-1":
					if reads.Add(1) == 1 {
						writeRetryableUnavailable(w, "read_snapshot_expired")
						return
					}
					_ = json.NewEncoder(w).Encode(l1.ComputerCustodyImportObservation{OperationRevision: 1, Status: "complete"})
				case r.URL.Path == "/v1/computers/import-1":
					_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: "import-1", AppliedRevision: 1, ReconfigurationPhase: l1.ComputerReconfigurationStable})
				case r.URL.Path == "/v1/computers/computer-1/custody-exports":
					if reads.Add(1) == 1 {
						writeRetryableUnavailable(w, "read_snapshot_expired")
						return
					}
					_ = json.NewEncoder(w).Encode([]l1.ComputerCustodyExport{{ExportID: "export-1", Status: "available"}})
				case r.URL.Path == "/v1/computers/computer-1/backups":
					if reads.Add(1) == 1 {
						writeRetryableUnavailable(w, "read_snapshot_expired")
						return
					}
					list := l1.BackupList{Backups: []l1.Backup{{BackupID: "backup-1", Status: "pruned"}}}
					if r.URL.Query().Get("backup_id") == "backup-1" {
						list.Operation = &l1.ComputerBackupOperationOutcome{BackupID: "backup-1", Status: "published"}
					}
					_ = json.NewEncoder(w).Encode(list)
				case r.URL.Path == "/v1/computers/computer-1":
					if reads.Add(1) == 1 {
						writeRetryableUnavailable(w, "read_snapshot_expired")
						return
					}
					computer := l1.Computer{ComputerID: "computer-1", IntentRevision: 2, StorageID: "storage-1",
						StorageGeneration: 1, AppliedRevision: 1, ReconfigurationPhase: l1.ComputerReconfigurationStable,
						CurrentJob: l1.Job{State: contract.JobRemovedVerified}, RemovalOutcome: "removed_verified"}
					switch {
					case r.URL.Query().Get("clone_operation_revision") == "1":
						computer.CloneOperation = &l1.ComputerCloneOperation{OperationRevision: 1, Status: "complete"}
					case r.URL.Query().Get("restore_operation_revision") == "1":
						computer.RestoreOperation = &l1.ComputerRestoreOperation{OperationRevision: 1, Status: "retired"}
					}
					_ = json.NewEncoder(w).Encode(computer)
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			client := &apiClient{name: "L1", client: server.Client()}
			client.client.Transport = rewriteWaitTestTransport{base: server.Client().Transport, url: server.URL}
			if err := test.wait(ctx, &apiClients{l1: client}); err != nil {
				t.Fatalf("wait with one retryable 503 mid-wait: %v", err)
			}
			if reads.Load() != 2 {
				t.Fatalf("observation reads = %d, want 2 after one retryable answer", reads.Load())
			}
		})
	}
}

func TestWaitForServiceToleratesARetryableAnswer(t *testing.T) {
	predicate := func(job l1.Job) bool { return job.State == contract.JobStopped }
	t.Run("retryable 503 mid-wait", func(t *testing.T) {
		var reads atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if reads.Add(1) == 1 {
				writeRetryableUnavailable(w, "read_snapshot_expired")
				return
			}
			_ = json.NewEncoder(w).Encode(l1.Job{JobID: "svc", State: contract.JobStopped})
		}))
		defer server.Close()
		client := &apiClient{name: "L1", client: server.Client()}
		client.client.Transport = rewriteWaitTestTransport{base: server.Client().Transport, url: server.URL}
		job, err := waitForService(t.Context(), &apiClients{l1: client}, l1.Job{JobID: "svc", State: contract.JobRunning},
			retryableWaitFlags.timeout, retryableWaitFlags.pollInterval, "stopped", predicate)
		if err != nil || reads.Load() != 2 || job.State != contract.JobStopped {
			t.Fatalf("service wait with one retryable 503: job=%#v reads=%d err=%v", job, reads.Load(), err)
		}
	})
	t.Run("non-retryable answer ends the wait at once", func(t *testing.T) {
		var reads atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			reads.Add(1)
			writeForbiddenResponse(w)
		}))
		defer server.Close()
		client := &apiClient{name: "L1", client: server.Client()}
		client.client.Transport = rewriteWaitTestTransport{base: server.Client().Transport, url: server.URL}
		started := time.Now()
		_, err := waitForService(t.Context(), &apiClients{l1: client}, l1.Job{JobID: "svc", State: contract.JobRunning},
			retryableWaitFlags.timeout, retryableWaitFlags.pollInterval, "stopped", predicate)
		if commandExitCode(err) != exitUnauthorized || reads.Load() != 1 || time.Since(started) > time.Second {
			t.Fatalf("non-retryable read did not end the service wait promptly: %v (%d reads) %s", err, reads.Load(), time.Since(started))
		}
	})
	t.Run("only retryable answers until the deadline", func(t *testing.T) {
		var reads atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			reads.Add(1)
			writeRetryableUnavailable(w, "read_snapshot_expired")
		}))
		defer server.Close()
		client := &apiClient{name: "L1", client: server.Client()}
		client.client.Transport = rewriteWaitTestTransport{base: server.Client().Transport, url: server.URL}
		job, err := waitForService(t.Context(), &apiClients{l1: client}, l1.Job{JobID: "svc", State: contract.JobRunning},
			time.Second, time.Millisecond, "stopped", predicate)
		if reads.Load() < 2 || commandExitCode(err) != exitMutationWaitTimeout || job.JobID != "" {
			t.Fatalf("retryable-only service wait: reads=%d exit=%d err=%v", reads.Load(), commandExitCode(err), err)
		}
		if !strings.Contains(err.Error(), "read_snapshot_expired") {
			t.Fatalf("last retryable answer missing from service wait timeout: %v", err)
		}
	})
}

// The process boundary. The typed exit already exists for exhausted waits;
// these prove a transient 503 no longer ends a wait there, that exhausting the
// window on retryable answers alone still reaches exit 14 with the last
// answer recorded, and that one-shot reads remain exit 13. Waits are seconds,
// fakes decide by count, so slow runners cannot flip the outcome.
func TestRetryableAnswerWaitsReachTheProcessBoundaryFromRealBinary(t *testing.T) {
	t.Parallel()
	binary := buildWefty(t)
	for _, count := range []int32{1, 2} {
		t.Run(fmt.Sprintf("mid-wait-%d", count), func(t *testing.T) {
			var reads atomic.Int32
			address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/v1/computers/computer-1":
					_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: "computer-1", IntentRevision: 2, StorageID: "storage-1", StorageGeneration: 1})
				case "/v1/computers/computer-1/backups":
					if r.Method == http.MethodPost {
						w.Header().Set("Backup-Id", "backup-1")
						w.WriteHeader(http.StatusAccepted)
						_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: "computer-1", IntentRevision: 3})
						return
					}
					if reads.Add(1) <= count {
						writeRetryableUnavailable(w, "read_snapshot_expired")
						return
					}
					_ = json.NewEncoder(w).Encode(l1.BackupList{Operation: &l1.ComputerBackupOperationOutcome{BackupID: "backup-1", Status: "published"}})
				case "/v1/computers/computer-1/storage-provenance":
					_ = json.NewEncoder(w).Encode(l1.ComputerStorageProvenance{})
				default:
					w.WriteHeader(http.StatusNotFound)
					_ = json.NewEncoder(w).Encode(contract.APIError{Code: contract.ErrorNotFound, Message: r.URL.Path})
				}
			})
			code, output := runWefty(t, binary, 30*time.Second, "--json", "--l1="+address,
				"services", "backup", "create", "computer-1", "--idempotency-key=k",
				"--intent-revision=2", "--storage-id=storage-1", "--storage-generation=1",
				"--wait=5s", "--poll-interval=20ms")
			var result storageMutationOutput
			if err := json.Unmarshal([]byte(output), &result); err != nil {
				t.Fatalf("mutation output: %v: %s", err, output)
			}
			if code != 0 || result.Observation == nil || result.Observation.Status != "observed" || reads.Load() != count+1 {
				t.Fatalf("wait with %d retryable 503(s) exited %d after %d reads: %s", count, code, reads.Load(), output)
			}
		})
	}
}

func TestRetryableOnlyAnswersExhaustTheWaitTimeoutFromRealBinary(t *testing.T) {
	t.Parallel()
	binary := buildWefty(t)
	var reads atomic.Int32
	address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/computers/computer-1":
			_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: "computer-1", IntentRevision: 2, StorageID: "storage-1", StorageGeneration: 1})
		case "/v1/computers/computer-1/backups":
			if r.Method == http.MethodPost {
				w.Header().Set("Backup-Id", "backup-1")
				w.WriteHeader(http.StatusAccepted)
				_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: "computer-1", IntentRevision: 3})
				return
			}
			reads.Add(1)
			writeRetryableUnavailable(w, "read_snapshot_expired")
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(contract.APIError{Code: contract.ErrorNotFound, Message: r.URL.Path})
		}
	})
	code, output := runWefty(t, binary, 30*time.Second, "--json", "--l1="+address,
		"services", "backup", "create", "computer-1", "--idempotency-key=k",
		"--intent-revision=2", "--storage-id=storage-1", "--storage-generation=1", "--allow-power-off",
		"--wait=2s", "--poll-interval=20ms")
	decoder := json.NewDecoder(strings.NewReader(output))
	var result storageMutationOutput
	var envelope contract.ErrorResponse
	if err := decoder.Decode(&result); err != nil {
		t.Fatalf("mutation output: %v: %s", err, output)
	}
	if err := decoder.Decode(&envelope); err != nil {
		t.Fatalf("error output: %v: %s", err, output)
	}
	if code != exitMutationWaitTimeout || envelope.Error.Code != errorWaitTimeout || envelope.Error.Retryable ||
		envelope.Error.Details["mutation_applied"] != true || !result.MutationApplied ||
		result.Observation == nil || result.Observation.Status != "failed" ||
		!strings.Contains(result.Observation.Error, "read_snapshot_expired") ||
		reads.Load() < 2 {
		t.Fatalf("retryable-only wait exited %d with %d reads, want 14 / wait_timeout with the last answer recorded: %s", code, reads.Load(), output)
	}
}

func TestOneShotReadStillSurfacesRetryable503FromRealBinary(t *testing.T) {
	t.Parallel()
	binary := buildWefty(t)
	var reads atomic.Int32
	address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/computers/computer-1":
			_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: "computer-1", IntentRevision: 2, StorageID: "storage-1", StorageGeneration: 1})
		case "/v1/computers/computer-1/backups":
			reads.Add(1)
			writeRetryableUnavailable(w, "read_snapshot_expired")
		default:
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(contract.APIError{Code: contract.ErrorNotFound, Message: r.URL.Path})
		}
	})
	code, output := runWefty(t, binary, 10*time.Second, "--json", "--l1="+address,
		"services", "backup", "list", "computer-1")
	decoder := json.NewDecoder(strings.NewReader(output))
	var envelope contract.ErrorResponse
	_ = decoder.Decode(&envelope)
	if code != exitUnavailable || envelope.Error.Code != contract.ErrorUnavailable || !envelope.Error.Retryable ||
		reads.Load() != 1 {
		t.Fatalf("one-shot read exited %d with %d reads, want 13 without any wait retry: %s", code, reads.Load(), output)
	}
}
