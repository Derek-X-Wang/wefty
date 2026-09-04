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
	outcome := workloadrunner.ComputerStoragePreparationOutcome{
		Code: workloadrunner.ComputerStoragePreparationResumeDeferred,
		Storage: workloadrunner.ComputerStorage{ComputerID: "computer-import", StorageID: "storage-import",
			StorageGeneration: 3, IntentRevision: 7, DiskBytes: 8 << 30},
		HelperGeneration: 11, SweepEpoch: "sweep-import", DiskName: "disk-import",
		Operation: "computer_storage_copy", Reason: "operational_failure", DeferredReason: "context_expired",
		Attempts: 4, FirstDeferredAt: &recordedAt, PayloadDroppedAt: recordedAt.Add(time.Hour).Format(time.RFC3339Nano),
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
		got.PayloadDroppedAt != outcome.PayloadDroppedAt || got.RecordedAt == nil || !got.RecordedAt.Equal(outcome.RecordedAt) {
		t.Fatalf("mapped preparation outcome = %#v, want %#v", got, outcome)
	}
}

func TestStorageCopyControllerDoesNotMisrouteNonImportPreparationError(t *testing.T) {
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
		Operation: "clone", BoundNodeID: "node-copy", RootInstanceID: "root-copy",
	})
	if !errors.Is(err, preparation) || requests != 0 {
		t.Fatalf("non-import preparation error = %v, acknowledgement requests=%d", err, requests)
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
