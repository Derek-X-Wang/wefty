package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

type storageCopyTestCopier struct {
	err error
}

func (copier storageCopyTestCopier) CopyComputerStorage(context.Context, workloadrunner.ComputerStorageCopyRequest) (workloadrunner.ComputerStorageCopyReceipt, error) {
	return workloadrunner.ComputerStorageCopyReceipt{}, copier.err
}

func TestStorageCopyControllerMapsPreparationOutcomeToL1(t *testing.T) {
	recordedAt := time.Date(2026, 9, 4, 12, 34, 56, 789, time.UTC)
	droppedAt := recordedAt.Add(time.Hour)
	outcome := workloadrunner.ComputerStoragePreparationOutcome{
		Code: workloadrunner.ComputerStoragePreparationResumeDeferred,
		Storage: workloadrunner.ComputerStorage{ComputerID: "computer-import", StorageID: "storage-import",
			StorageGeneration: 3, IntentRevision: 7, DiskBytes: 8 << 30},
		HelperGeneration: 11, SweepEpoch: "sweep-import", DiskName: "disk-import",
		Operation: "computer_storage_copy", Reason: "operational_failure", DeferredReason: "context_expired",
		Attempts: 4, FirstDeferredAt: &recordedAt, PayloadDroppedAt: &droppedAt,
		RecordedAt: recordedAt,
	}
	received := make(chan l1.ComputerStorageCopyAcknowledgementRequest, 1)
	client := newRoundTripClient(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.EscapedPath() != "/v1/agent/computers/computer-import/storage-copy-acknowledgement" {
			t.Errorf("acknowledgement path = %q", request.URL.EscapedPath())
			response.WriteHeader(http.StatusNotFound)
			return
		}
		var acknowledgement l1.ComputerStorageCopyAcknowledgementRequest
		if err := json.NewDecoder(request.Body).Decode(&acknowledgement); err != nil {
			t.Errorf("decode preparation acknowledgement: %v", err)
		}
		received <- acknowledgement
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{}`))
	}))
	controller := newStorageCopyController(client, storageCopyTestCopier{err: &workloadrunner.ComputerStoragePreparationError{Outcome: outcome}},
		nil, "node-import", "boot-import", "root-import", nil)
	directive := l1.ComputerStorageCopyDirective{Operation: "import", BoundNodeID: "node-import", RootInstanceID: "root-import",
		DestinationComputerID: "computer-import", DestinationStorageID: "storage-import", DestinationGeneration: 3,
		DestinationSize: 8 << 30, OperationRevision: 7}
	if err := controller.process(t.Context(), directive); err != nil {
		t.Fatal(err)
	}
	acknowledgement := <-received
	wantKey := "preparation-computer_storage_resume_deferred-11-sweep-import"
	if acknowledgement.IdempotencyKey != wantKey || acknowledgement.NodeID != "node-import" ||
		acknowledgement.BootSessionID != "boot-import" || acknowledgement.PreparationOutcome == nil {
		t.Fatalf("preparation acknowledgement = %#v, want key %q", acknowledgement, wantKey)
	}
	got := acknowledgement.PreparationOutcome
	if got.Code != outcome.Code || got.DestinationComputerID != outcome.Storage.ComputerID ||
		got.DestinationStorageID != outcome.Storage.StorageID || got.DestinationGeneration != outcome.Storage.StorageGeneration ||
		got.IntentRevision != outcome.Storage.IntentRevision || got.DiskBytes != outcome.Storage.DiskBytes ||
		got.HelperGeneration != outcome.HelperGeneration || got.SweepEpoch != outcome.SweepEpoch ||
		got.DiskName != outcome.DiskName || got.Operation != outcome.Operation || got.Reason != outcome.Reason ||
		got.DeferredReason != outcome.DeferredReason || got.Attempts != outcome.Attempts ||
		got.FirstDeferredAt == nil || !got.FirstDeferredAt.Equal(*outcome.FirstDeferredAt) ||
		got.PayloadDroppedAt == nil || !got.PayloadDroppedAt.Equal(*outcome.PayloadDroppedAt) || got.RecordedAt == nil || !got.RecordedAt.Equal(outcome.RecordedAt) {
		t.Fatalf("mapped preparation outcome = %#v, want %#v", got, outcome)
	}
}

// A restore's destination is a live Computer whose predecessor still owns the
// bytes, so it has no never-attached generation to close and its preparation
// failures stay ordinary errors. Only clone and import acknowledge.
func TestStorageCopyControllerDoesNotMisrouteRestorePreparationError(t *testing.T) {
	requests := 0
	client := newRoundTripClient(t, http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		requests++
		response.WriteHeader(http.StatusInternalServerError)
	}))
	preparation := &workloadrunner.ComputerStoragePreparationError{Outcome: workloadrunner.ComputerStoragePreparationOutcome{
		Code: workloadrunner.ComputerStoragePreparationResumeDeferred,
	}}
	controller := newStorageCopyController(client, storageCopyTestCopier{err: preparation}, nil,
		"node-copy", "boot-copy", "root-copy", nil)
	err := controller.process(t.Context(), l1.ComputerStorageCopyDirective{
		Operation: "restore", BoundNodeID: "node-copy", RootInstanceID: "root-copy",
	})
	if !errors.Is(err, preparation) || requests != 0 {
		t.Fatalf("restore preparation error = %v, acknowledgement requests=%d", err, requests)
	}
}

// The helper's own classification of a half-copied clone destination is the
// acknowledgement L1 needs; without it the clone directive came back every
// sweep forever (#526).
func TestStorageCopyControllerAcknowledgesQuarantinedCloneDestination(t *testing.T) {
	recordedAt := time.Date(2026, 9, 22, 4, 5, 6, 0, time.UTC)
	outcome := workloadrunner.ComputerStoragePreparationOutcome{
		Code: workloadrunner.ComputerStoragePreparationQuarantined,
		Storage: workloadrunner.ComputerStorage{ComputerID: "computer-clone", StorageID: "storage-clone",
			StorageGeneration: 1, IntentRevision: 1, DiskBytes: 128 << 20},
		HelperGeneration: 21, SweepEpoch: "sweep-clone", DiskName: "disk-clone",
		Operation: "computer_storage_copy", Reason: "computer_disk_anomaly_quarantined",
		RecordedAt: recordedAt,
	}
	received := make(chan l1.ComputerStorageCopyAcknowledgementRequest, 1)
	client := newRoundTripClient(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.EscapedPath() != "/v1/agent/computers/computer-clone/storage-copy-acknowledgement" {
			t.Errorf("acknowledgement path = %q", request.URL.EscapedPath())
			response.WriteHeader(http.StatusNotFound)
			return
		}
		var acknowledgement l1.ComputerStorageCopyAcknowledgementRequest
		if err := json.NewDecoder(request.Body).Decode(&acknowledgement); err != nil {
			t.Errorf("decode quarantined clone acknowledgement: %v", err)
		}
		received <- acknowledgement
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{}`))
	}))
	controller := newStorageCopyController(client, storageCopyTestCopier{err: &workloadrunner.ComputerStoragePreparationError{Outcome: outcome}},
		nil, "node-clone", "boot-clone", "root-clone", nil)
	directive := l1.ComputerStorageCopyDirective{Operation: "clone", BoundNodeID: "node-clone", RootInstanceID: "root-clone",
		DestinationComputerID: "computer-clone", DestinationStorageID: "storage-clone", DestinationGeneration: 1,
		DestinationSize: 128 << 20, OperationRevision: 1}
	if err := controller.process(t.Context(), directive); err != nil {
		t.Fatal(err)
	}
	acknowledgement := <-received
	got := acknowledgement.PreparationOutcome
	if acknowledgement.IdempotencyKey != "preparation-computer_storage_quarantined-21-sweep-clone" || got == nil ||
		got.Code != l1.ComputerStoragePreparationQuarantined || got.DestinationComputerID != "computer-clone" ||
		got.DestinationStorageID != "storage-clone" || got.DestinationGeneration != 1 || got.IntentRevision != 1 ||
		got.DiskBytes != 128<<20 || got.HelperGeneration != 21 || got.SweepEpoch != "sweep-clone" ||
		got.DiskName != "disk-clone" || got.Operation != "computer_storage_copy" ||
		got.Reason != "computer_disk_anomaly_quarantined" || got.RecordedAt == nil || !got.RecordedAt.Equal(recordedAt) {
		t.Fatalf("quarantined clone acknowledgement = %#v outcome=%#v", acknowledgement, got)
	}
}

func TestStorageCopyControllerRecordsCloneRuntimeLossAsInterruptedPreparation(t *testing.T) {
	received := make(chan l1.ComputerStorageCopyAcknowledgementRequest, 1)
	client := newRoundTripClient(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var acknowledgement l1.ComputerStorageCopyAcknowledgementRequest
		if err := json.NewDecoder(request.Body).Decode(&acknowledgement); err != nil {
			t.Errorf("decode interrupted clone acknowledgement: %v", err)
		}
		received <- acknowledgement
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{}`))
	}))
	loss := &workloadrunner.RuntimeLossError{
		Generation: workloadrunner.RuntimeGeneration{InstanceID: "helper-clone", Generation: 9},
		Err:        errors.New("oci helper engine_failure operation_failed: EOF"),
	}
	controller := newStorageCopyController(client, storageCopyTestCopier{err: loss}, nil,
		"node-clone", "boot-clone", "root-clone", nil)
	directive := l1.ComputerStorageCopyDirective{Operation: "clone", BoundNodeID: "node-clone", RootInstanceID: "root-clone",
		DestinationComputerID: "computer-clone", DestinationStorageID: "storage-clone", DestinationGeneration: 1,
		DestinationSize: 128 << 20, OperationRevision: 1}
	if err := controller.process(t.Context(), directive); err != nil {
		t.Fatal(err)
	}
	acknowledgement := <-received
	if acknowledgement.IdempotencyKey != "preparation-computer_storage_preparation_interrupted-9-" ||
		acknowledgement.PreparationOutcome == nil ||
		acknowledgement.PreparationOutcome.Code != l1.ComputerStoragePreparationInterrupted ||
		acknowledgement.PreparationOutcome.DestinationComputerID != directive.DestinationComputerID ||
		acknowledgement.PreparationOutcome.DestinationGeneration != directive.DestinationGeneration ||
		acknowledgement.PreparationOutcome.IntentRevision != directive.OperationRevision ||
		acknowledgement.PreparationOutcome.DiskBytes != directive.DestinationSize ||
		acknowledgement.PreparationOutcome.HelperGeneration != 9 ||
		acknowledgement.PreparationOutcome.Operation != "computer_storage_copy" ||
		acknowledgement.PreparationOutcome.Reason != "runtime_loss" ||
		acknowledgement.PreparationOutcome.RecordedAt == nil {
		t.Fatalf("interrupted clone acknowledgement = %#v", acknowledgement)
	}
}

func TestStorageCopyControllerRecordsImportRuntimeLossAsInterruptedPreparation(t *testing.T) {
	received := make(chan l1.ComputerStorageCopyAcknowledgementRequest, 1)
	client := newRoundTripClient(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var acknowledgement l1.ComputerStorageCopyAcknowledgementRequest
		if err := json.NewDecoder(request.Body).Decode(&acknowledgement); err != nil {
			t.Errorf("decode interrupted preparation acknowledgement: %v", err)
		}
		received <- acknowledgement
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{}`))
	}))
	loss := &workloadrunner.RuntimeLossError{
		Generation: workloadrunner.RuntimeGeneration{InstanceID: "helper-import", Generation: 13},
		Err:        errors.New("oci helper engine_failure operation_failed: EOF"),
	}
	controller := newStorageCopyController(client, storageCopyTestCopier{err: loss}, nil,
		"node-import", "boot-import", "root-import", nil)
	directive := l1.ComputerStorageCopyDirective{Operation: "import", BoundNodeID: "node-import", RootInstanceID: "root-import",
		DestinationComputerID: "computer-import", DestinationStorageID: "storage-import", DestinationGeneration: 2,
		DestinationSize: 4 << 30, OperationRevision: 5}
	if err := controller.process(t.Context(), directive); err != nil {
		t.Fatal(err)
	}
	acknowledgement := <-received
	if acknowledgement.IdempotencyKey != "preparation-computer_storage_preparation_interrupted-13-" ||
		acknowledgement.PreparationOutcome == nil ||
		acknowledgement.PreparationOutcome.Code != l1.ComputerStoragePreparationInterrupted ||
		acknowledgement.PreparationOutcome.DestinationComputerID != directive.DestinationComputerID ||
		acknowledgement.PreparationOutcome.DestinationStorageID != directive.DestinationStorageID ||
		acknowledgement.PreparationOutcome.DestinationGeneration != directive.DestinationGeneration ||
		acknowledgement.PreparationOutcome.IntentRevision != directive.OperationRevision ||
		acknowledgement.PreparationOutcome.DiskBytes != directive.DestinationSize ||
		acknowledgement.PreparationOutcome.HelperGeneration != 13 ||
		acknowledgement.PreparationOutcome.Operation != "computer_storage_copy" ||
		acknowledgement.PreparationOutcome.Reason != "runtime_loss" ||
		acknowledgement.PreparationOutcome.RecordedAt == nil {
		t.Fatalf("interrupted preparation acknowledgement = %#v", acknowledgement)
	}
}

type countingStorageCopyCopier struct {
	calls *int
	errs  []error
}

func (copier countingStorageCopyCopier) CopyComputerStorage(context.Context, workloadrunner.ComputerStorageCopyRequest) (workloadrunner.ComputerStorageCopyReceipt, error) {
	call := *copier.calls
	*copier.calls++
	if call < len(copier.errs) && copier.errs[call] != nil {
		return workloadrunner.ComputerStorageCopyReceipt{}, copier.errs[call]
	}
	return workloadrunner.ComputerStorageCopyReceipt{Kind: "computer_storage_copy_verified", ReceiptID: "late-success", HelperGeneration: 13}, nil
}

// An engine_failure outcome ends the copy (#1581). When L1 does not accept its
// acknowledgement, the next reconciliation sends the same acknowledgement
// again and never calls the copy again: a second copy that succeeded would
// complete a copy that has already failed for good.
func TestStorageCopyControllerRetriesOnlyTheTerminalEngineFailureAcknowledgement(t *testing.T) {
	recordedAt := time.Date(2026, 11, 10, 18, 30, 0, 0, time.UTC)
	outcome := workloadrunner.ComputerStoragePreparationOutcome{
		Code: workloadrunner.ComputerStoragePreparationInterrupted,
		Storage: workloadrunner.ComputerStorage{ComputerID: "computer-clone", StorageID: "storage-clone",
			StorageGeneration: 2, IntentRevision: 5, DiskBytes: 4 << 30},
		HelperGeneration: 13, Operation: "computer_storage_copy",
		Reason: workloadrunner.ComputerStoragePreparationReasonEngineFailure, RecordedAt: recordedAt,
	}
	var acknowledgements []l1.ComputerStorageCopyAcknowledgementRequest
	client := newRoundTripClient(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var acknowledgement l1.ComputerStorageCopyAcknowledgementRequest
		if err := json.NewDecoder(request.Body).Decode(&acknowledgement); err != nil {
			t.Errorf("decode acknowledgement: %v", err)
		}
		acknowledgements = append(acknowledgements, acknowledgement)
		response.Header().Set("Content-Type", "application/json")
		if len(acknowledgements) == 1 {
			response.WriteHeader(http.StatusServiceUnavailable)
			_, _ = response.Write([]byte(`{"code":"internal","message":"L1 unavailable","retryable":true}`))
			return
		}
		_, _ = response.Write([]byte(`{}`))
	}))
	calls := 0
	controller := newStorageCopyController(client, countingStorageCopyCopier{calls: &calls,
		errs: []error{&workloadrunner.ComputerStoragePreparationError{Outcome: outcome}}}, nil,
		"node-clone", "boot-clone", "root-clone", nil)
	directive := l1.ComputerStorageCopyDirective{Operation: "clone", BoundNodeID: "node-clone", RootInstanceID: "root-clone",
		DestinationComputerID: "computer-clone", DestinationStorageID: "storage-clone", DestinationGeneration: 2,
		DestinationSize: 4 << 30, OperationRevision: 5}
	if err := controller.process(t.Context(), directive); err == nil {
		t.Fatal("first acknowledgement failure was not reported")
	}
	if err := controller.process(t.Context(), directive); err != nil {
		t.Fatalf("retried acknowledgement: %v", err)
	}
	if calls != 1 {
		t.Fatalf("copier calls = %d, want 1: the terminal copy ran again", calls)
	}
	if len(acknowledgements) != 2 || acknowledgements[0].IdempotencyKey != acknowledgements[1].IdempotencyKey ||
		acknowledgements[1].PreparationOutcome == nil ||
		acknowledgements[1].PreparationOutcome.Reason != l1.ComputerStoragePreparationReasonEngineFailure ||
		!acknowledgements[1].PreparationOutcome.RecordedAt.Equal(recordedAt) {
		t.Fatalf("acknowledgements = %#v", acknowledgements)
	}
	// Once L1 has accepted it, the outcome is no longer held.
	if len(controller.terminal) != 0 {
		t.Fatalf("accepted terminal outcome is still held: %#v", controller.terminal)
	}
}
