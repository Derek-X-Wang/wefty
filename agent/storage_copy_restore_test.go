package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

type restoreCopierFunc func() error

func (copy restoreCopierFunc) CopyComputerStorage(context.Context, workloadrunner.ComputerStorageCopyRequest) (workloadrunner.ComputerStorageCopyReceipt, error) {
	if err := copy(); err != nil {
		return workloadrunner.ComputerStorageCopyReceipt{}, err
	}
	return workloadrunner.ComputerStorageCopyReceipt{Kind: "computer_storage_copy_verified", ReceiptID: "restore-copied", HelperGeneration: 13}, nil
}

type restoreBoundFixture struct {
	controller       *storageCopyController
	directive        l1.ComputerStorageCopyDirective
	now              time.Time
	copies           int
	acknowledgements []l1.ComputerStorageCopyAcknowledgementRequest
	logs             []string
}

// newRestoreBoundFixture runs a restore copy whose result each call takes from
// next. L1 answers acknowledgements with the status unavailable() returns.
func newRestoreBoundFixture(t *testing.T, next func(call int) error, unavailable func(call int) bool) *restoreBoundFixture {
	t.Helper()
	fixture := &restoreBoundFixture{now: time.Date(2026, 11, 10, 20, 0, 0, 0, time.UTC),
		directive: l1.ComputerStorageCopyDirective{Operation: "restore", BoundNodeID: "node-restore",
			RootInstanceID: "root-restore", DestinationComputerID: "computer-restore", DestinationStorageID: "storage-restore",
			OldGeneration: 1, DestinationGeneration: 2, DestinationSize: 4 << 30, OperationRevision: 7, JobID: "job-restore"}}
	client := newRoundTripClient(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var acknowledgement l1.ComputerStorageCopyAcknowledgementRequest
		if err := json.NewDecoder(request.Body).Decode(&acknowledgement); err != nil {
			t.Errorf("decode acknowledgement: %v", err)
		}
		fixture.acknowledgements = append(fixture.acknowledgements, acknowledgement)
		response.Header().Set("Content-Type", "application/json")
		if unavailable != nil && unavailable(len(fixture.acknowledgements)) {
			response.WriteHeader(http.StatusServiceUnavailable)
			_, _ = response.Write([]byte(`{"code":"internal","message":"L1 unavailable","retryable":true}`))
			return
		}
		_, _ = response.Write([]byte(`{}`))
	}))
	copier := restoreCopierFunc(func() error {
		fixture.copies++
		return next(fixture.copies)
	})
	fixture.controller = newStorageCopyController(client, copier, nil, "node-restore", "boot-restore", "root-restore",
		func(format string, args ...any) { fixture.logs = append(fixture.logs, fmt.Sprintf(format, args...)) })
	fixture.controller.now = func() time.Time { return fixture.now }
	return fixture
}

func (fixture *restoreBoundFixture) engineFailure() error {
	return &workloadrunner.ComputerStoragePreparationError{Outcome: workloadrunner.ComputerStoragePreparationOutcome{
		Code: workloadrunner.ComputerStoragePreparationInterrupted,
		Storage: workloadrunner.ComputerStorage{ComputerID: fixture.directive.DestinationComputerID,
			StorageID: fixture.directive.DestinationStorageID, StorageGeneration: fixture.directive.DestinationGeneration,
			IntentRevision: fixture.directive.OperationRevision, DiskBytes: fixture.directive.DestinationSize},
		HelperGeneration: 13, Operation: "computer_storage_copy",
		Reason: workloadrunner.ComputerStoragePreparationReasonEngineFailure, RecordedAt: fixture.now,
	}}
}

// sweep runs one reconciliation of the restore directive, then advances the
// clock by one heartbeat.
func (fixture *restoreBoundFixture) sweep(t *testing.T) error {
	t.Helper()
	err := fixture.controller.process(t.Context(), fixture.directive)
	fixture.now = fixture.now.Add(15 * time.Second)
	return err
}

func (fixture *restoreBoundFixture) preparationAcknowledgements() int {
	count := 0
	for _, acknowledgement := range fixture.acknowledgements {
		if acknowledgement.PreparationOutcome != nil {
			count++
		}
	}
	return count
}

// A transient engine failure on a live session clears by itself on a later
// sweep (#1591). Inside the bound the restore stays an ordinary error, and the
// copy that then succeeds publishes as usual.
func TestRestoreEngineFailureRecoversInsideTheBound(t *testing.T) {
	var fixture *restoreBoundFixture
	fixture = newRestoreBoundFixture(t, func(call int) error {
		if call <= 3 {
			return fixture.engineFailure()
		}
		return nil
	}, nil)
	for sweep := 1; sweep <= 3; sweep++ {
		if err := fixture.sweep(t); err == nil {
			t.Fatalf("sweep %d: engine failure was not reported", sweep)
		}
	}
	if err := fixture.sweep(t); err != nil {
		t.Fatalf("recovered copy: %v", err)
	}
	if fixture.preparationAcknowledgements() != 0 || len(fixture.acknowledgements) != 1 ||
		fixture.acknowledgements[0].Receipt.ReceiptID != "restore-copied" {
		t.Fatalf("acknowledgements = %#v", fixture.acknowledgements)
	}
	if len(fixture.controller.restoreFailures) != 0 {
		t.Fatalf("success kept the failure count: %#v", fixture.controller.restoreFailures)
	}
}

// A persistent engine failure ends the restore once both halves of the bound
// are spent, in one agent lifetime and without a restart: five failures are
// not enough before ten minutes have passed since the first.
func TestRestoreEngineFailureAbortsOnceTheBoundIsSpent(t *testing.T) {
	var fixture *restoreBoundFixture
	fixture = newRestoreBoundFixture(t, func(int) error { return fixture.engineFailure() }, nil)
	first := fixture.now
	for fixture.now.Sub(first) < restoreEngineFailureWindow {
		err := fixture.sweep(t)
		var preparation *workloadrunner.ComputerStoragePreparationError
		if !errors.As(err, &preparation) {
			t.Fatalf("failure %d inside the bound = %v", fixture.copies, err)
		}
		if fixture.preparationAcknowledgements() != 0 {
			t.Fatalf("restore aborted after %d failures and %s", fixture.copies, fixture.now.Sub(first))
		}
	}
	if err := fixture.sweep(t); err != nil {
		t.Fatalf("abort acknowledgement: %v", err)
	}
	// 41 calls at the 15 s heartbeat: the first at zero, the last at ten minutes.
	if fixture.copies != 41 || fixture.preparationAcknowledgements() != 1 {
		t.Fatalf("copies=%d acknowledgements=%#v", fixture.copies, fixture.acknowledgements)
	}
	outcome := fixture.acknowledgements[0].PreparationOutcome
	if outcome.Code != l1.ComputerStoragePreparationInterrupted ||
		outcome.Reason != l1.ComputerStoragePreparationReasonEngineFailure ||
		outcome.IntentRevision != fixture.directive.OperationRevision ||
		outcome.DestinationGeneration != fixture.directive.DestinationGeneration {
		t.Fatalf("abort outcome = %#v", outcome)
	}
	if len(fixture.logs) != 1 || !strings.Contains(fixture.logs[0], "storage_copy_engine_failure") ||
		!strings.Contains(fixture.logs[0], "computer=computer-restore") || !strings.Contains(fixture.logs[0], "revision=7") ||
		!strings.Contains(fixture.logs[0], "generation=2") || !strings.Contains(fixture.logs[0], "failures=41") ||
		!strings.Contains(fixture.logs[0], "elapsed=10m0s") {
		t.Fatalf("terminal log = %q", fixture.logs)
	}
	if len(fixture.controller.restoreFailures) != 0 || len(fixture.controller.terminal) != 0 {
		t.Fatalf("accepted abort is still held: %#v %#v", fixture.controller.restoreFailures, fixture.controller.terminal)
	}
}

// When L1 does not accept the abort, the next sweep sends the same
// acknowledgement again and never calls the copy again.
func TestRestoreEngineFailureAbortRetriesOnlyTheAcknowledgement(t *testing.T) {
	var fixture *restoreBoundFixture
	fixture = newRestoreBoundFixture(t, func(int) error { return fixture.engineFailure() },
		func(call int) bool { return call == 1 })
	for fixture.preparationAcknowledgements() == 0 {
		_ = fixture.sweep(t)
	}
	copies := fixture.copies
	if err := fixture.sweep(t); err != nil {
		t.Fatalf("retried acknowledgement: %v", err)
	}
	if fixture.copies != copies || len(fixture.acknowledgements) != 2 ||
		fixture.acknowledgements[0].IdempotencyKey != fixture.acknowledgements[1].IdempotencyKey ||
		!fixture.acknowledgements[0].PreparationOutcome.RecordedAt.Equal(*fixture.acknowledgements[1].PreparationOutcome.RecordedAt) {
		t.Fatalf("copies %d -> %d, acknowledgements = %#v", copies, fixture.copies, fixture.acknowledgements)
	}
}

// Other errors between engine failures neither count nor reset the bound, so
// alternating error classes cannot keep a restore retrying forever.
func TestRestoreEngineFailureBoundSurvivesOtherErrors(t *testing.T) {
	var fixture *restoreBoundFixture
	other := errors.New("transport hiccup")
	fixture = newRestoreBoundFixture(t, func(call int) error {
		if call%2 == 0 {
			return other
		}
		return fixture.engineFailure()
	}, nil)
	first := fixture.now
	for fixture.preparationAcknowledgements() == 0 {
		if fixture.now.Sub(first) > 2*restoreEngineFailureWindow {
			t.Fatalf("no abort after %d copies", fixture.copies)
		}
		_ = fixture.sweep(t)
	}
	// The window closes at the 41st copy, the 21st engine failure, exactly as
	// if the other errors had never happened.
	if engineFailures := (fixture.copies + 1) / 2; fixture.copies != 41 || engineFailures != 21 {
		t.Fatalf("aborted after %d engine failures in %d copies", engineFailures, fixture.copies)
	}
}

// A restore's other preparation errors stay ordinary errors and never count.
func TestRestoreRuntimeLossDoesNotCountTowardTheBound(t *testing.T) {
	var fixture *restoreBoundFixture
	fixture = newRestoreBoundFixture(t, func(int) error {
		return &workloadrunner.RuntimeLossError{Generation: workloadrunner.RuntimeGeneration{Generation: 13}, Err: errors.New("lost")}
	}, nil)
	for sweep := 0; sweep < 60; sweep++ {
		if err := fixture.sweep(t); err == nil {
			t.Fatal("runtime loss was not reported")
		}
	}
	if len(fixture.acknowledgements) != 0 || len(fixture.controller.restoreFailures) != 0 {
		t.Fatalf("runtime loss counted: acknowledgements=%d failures=%#v", len(fixture.acknowledgements), fixture.controller.restoreFailures)
	}
}
