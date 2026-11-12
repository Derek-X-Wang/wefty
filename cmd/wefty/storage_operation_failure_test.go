package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

// A restore whose predecessor could not be deleted within the failure budget
// ends `cleanup_owed` (#1608). The data switch happened, so --wait treats it
// as done, with a warning that names the owed cleanup, and not as a failure.
func TestRestoreWaitEndsOnCleanupOwedWithAWarning(t *testing.T) {
	firstFailed := time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/v1/computers/computer-1" || r.URL.Query().Get("restore_operation_revision") != "4" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: "computer-1", ReconfigurationPhase: l1.ComputerReconfigurationStable,
			RestoreOperation: &l1.ComputerRestoreOperation{OperationRevision: 4, Status: "cleanup_owed",
				FailureCode: "engine_failure", Failure: &l1.ComputerStorageOperationFailure{
					Stage: l1.ComputerStorageOperationRetire, Class: l1.ComputerStorageOperationFailureEngine,
					Code: "engine_failure", FailedAttempts: 6, FirstFailedAt: &firstFailed,
					LastFailedAt: firstFailed.Add(11 * time.Minute)}}})
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	observed, _, err := waitForComputerRestore(ctx, &apiClients{l1: waitStubClient(t, server)}, "computer-1", 4, retryableWaitFlags)
	if err != nil {
		t.Fatalf("a cleanup_owed restore did not end the wait: %v", err)
	}
	if err := awaitedComputerRestoreFailure(observed); err != nil {
		t.Fatalf("a cleanup_owed restore is done, got %v", err)
	}
	var stderr bytes.Buffer
	warnComputerRestoreCleanupOwed(&stderr, observed)
	if warning := stderr.String(); !strings.Contains(warning, "restore 4 applied") || !strings.Contains(warning, "engine_failure") {
		t.Fatalf("warning = %q", warning)
	}
}

// A Storage operation the budget ended carries its count and window in the
// typed wait error, so the caller sees why it ended.
func TestEndedStorageOperationWaitErrorCarriesTheFailureBudget(t *testing.T) {
	firstFailed := time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)
	failure := &l1.ComputerStorageOperationFailure{Stage: l1.ComputerStorageOperationPrepare,
		Class: l1.ComputerStorageOperationFailureEngine, Code: "engine_failure", FailedAttempts: 5,
		FirstFailedAt: &firstFailed, LastFailedAt: firstFailed.Add(10 * time.Minute)}
	for name, err := range map[string]error{
		"restore": awaitedComputerRestoreFailure(l1.Computer{ComputerID: "computer-1",
			RestoreOperation: &l1.ComputerRestoreOperation{OperationRevision: 4, Status: "failed",
				FailureCode: "engine_failure", Failure: failure}}),
		"clone": awaitedComputerCloneFailure(l1.Computer{ComputerID: "computer-1",
			CurrentJob: l1.Job{ServiceJob: &l1.ServiceJob{}},
			CloneOperation: &l1.ComputerCloneOperation{OperationRevision: 1, Status: "failed",
				FailureCode: "engine_failure", Failure: failure}}),
		"backup": awaitedBackupFailure("computer-1", l1.BackupList{Operation: &l1.ComputerBackupOperationOutcome{
			BackupID: "backup-1", OperationRevision: 2, Status: "failed", FailureCode: "engine_failure", Failure: failure}}),
	} {
		var typed *apiResponseError
		if !errors.As(err, &typed) || typed.APIError.Details["failure"] != failure {
			t.Fatalf("%s wait error = %#v, want the failure budget in its details", name, err)
		}
	}
}

// The failure budget ends a quarantined Custody import at once, so the wait
// sees `failed` before any preparation outcome; it keeps the quarantine exit.
func TestCustodyImportEndedByQuarantineKeepsItsExitStatus(t *testing.T) {
	for code, want := range map[string]custodyImportOutcome{
		l1.ComputerStoragePreparationQuarantined: custodyImportQuarantined,
		"engine_failure":                         custodyImportFailed,
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(l1.ComputerCustodyImportObservation{OperationRevision: 1, Status: "failed", FailureCode: code})
		}))
		_, _, _, err := waitForCustodyImport(t.Context(), &apiClients{l1: waitStubClient(t, server)}, "import-1", 1, retryableWaitFlags)
		server.Close()
		var outcome *custodyImportOutcomeError
		if !errors.As(err, &outcome) || outcome.outcome != want || outcome.detail != code {
			t.Fatalf("%s import wait = %#v, want %s", code, err, want)
		}
	}
}
