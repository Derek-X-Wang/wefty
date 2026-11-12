package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// failureBudgetL1 is a fake L1 that validates every failure report it
// receives the way L1's own boundary does, so a malformed report fails the
// test instead of being answered (a mute fixture once hid a deadlock).
type failureBudgetL1 struct {
	t        *testing.T
	mu       sync.Mutex
	reports  []l1.ComputerStorageOperationFailureRequest
	paths    []string
	acks     []string
	seen     map[string]bool
	answer   func(report l1.ComputerStorageOperationFailureRequest, count int) (int, string)
	ackError func(path string) (int, string)
}

var (
	testReportIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
	testReportCode      = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)
)

func newFailureBudgetL1(t *testing.T) (*failureBudgetL1, *Client) {
	t.Helper()
	fake := &failureBudgetL1{t: t, seen: make(map[string]bool)}
	return fake, newRoundTripClient(t, fake)
}

func (fake *failureBudgetL1) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "application/json")
	path := request.URL.EscapedPath()
	if !strings.HasSuffix(path, "/storage-operation-failure") {
		fake.mu.Lock()
		fake.acks = append(fake.acks, path)
		ackError := fake.ackError
		fake.mu.Unlock()
		if ackError != nil {
			if status, body := ackError(path); status != 0 {
				response.WriteHeader(status)
				_, _ = response.Write([]byte(body))
				return
			}
		}
		_, _ = response.Write([]byte(`{}`))
		return
	}
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var report l1.ComputerStorageOperationFailureRequest
	if err := decoder.Decode(&report); err != nil {
		fake.t.Errorf("decode failure report: %v", err)
	}
	fake.validate(path, report)
	fake.mu.Lock()
	fake.reports = append(fake.reports, report)
	fake.paths = append(fake.paths, path)
	count := len(fake.reports)
	answer := fake.answer
	fake.mu.Unlock()
	status, body := http.StatusOK, fmt.Sprintf(`{"disposition":"counted","failed_attempts":%d,"retry_after_seconds":15,"operation_status":"reserved"}`, count)
	if answer != nil {
		status, body = answer(report, count)
	}
	response.WriteHeader(status)
	_, _ = response.Write([]byte(body))
}

func (fake *failureBudgetL1) validate(path string, report l1.ComputerStorageOperationFailureRequest) {
	t := fake.t
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if !testReportIDPattern.MatchString(report.ReportID) || fake.seen[report.ReportID] {
		t.Errorf("report_id %q is malformed or reused", report.ReportID)
	}
	fake.seen[report.ReportID] = true
	if report.NodeID == "" || report.BootSessionID == "" || report.Fence == "" || report.OperationRevision <= 0 {
		t.Errorf("incomplete failure report %#v", report)
	}
	switch report.Verb {
	case l1.ComputerStorageOperationRestore, l1.ComputerStorageOperationReset:
		if report.Stage != l1.ComputerStorageOperationPrepare && report.Stage != l1.ComputerStorageOperationRetire {
			t.Errorf("stage %q", report.Stage)
		}
	case l1.ComputerStorageOperationImport, l1.ComputerStorageOperationClone, l1.ComputerStorageOperationBackup, l1.ComputerStorageOperationGrow:
		if report.Stage != l1.ComputerStorageOperationPrepare {
			t.Errorf("%s stage %q", report.Verb, report.Stage)
		}
	default:
		t.Errorf("verb %q", report.Verb)
	}
	switch report.Class {
	case l1.ComputerStorageOperationFailureEngine, l1.ComputerStorageOperationFailureRuntimeLoss,
		l1.ComputerStorageOperationFailureQuarantined, l1.ComputerStorageOperationFailureResumeDeferred,
		l1.ComputerStorageOperationFailureAcknowledgementRefused, l1.ComputerStorageOperationFailureAgentRefused:
	default:
		t.Errorf("class %q", report.Class)
	}
	if !testReportCode.MatchString(report.Code) {
		t.Errorf("code %q", report.Code)
	}
	if len(report.Detail) > l1.MaximumStorageOperationFailureDetail || !utf8.ValidString(report.Detail) {
		t.Errorf("detail of %d bytes, valid UTF-8 %v", len(report.Detail), utf8.ValidString(report.Detail))
	}
	if report.PreparationOutcome != nil && (report.Stage != l1.ComputerStorageOperationPrepare ||
		(report.Verb != l1.ComputerStorageOperationImport && report.Verb != l1.ComputerStorageOperationClone &&
			report.Verb != l1.ComputerStorageOperationRestore)) {
		t.Errorf("%s %s carries a preparation outcome", report.Verb, report.Stage)
	}
	if want := "/v1/agent/computers/"; !strings.HasPrefix(path, want) {
		t.Errorf("report path %q", path)
	}
}

func (fake *failureBudgetL1) sent() ([]l1.ComputerStorageOperationFailureRequest, []string, []string) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([]l1.ComputerStorageOperationFailureRequest(nil), fake.reports...),
		append([]string(nil), fake.paths...), append([]string(nil), fake.acks...)
}

type failingCopier struct {
	mu    sync.Mutex
	calls int
	err   func(call int) error
}

func (copier *failingCopier) CopyComputerStorage(context.Context, workloadrunner.ComputerStorageCopyRequest) (workloadrunner.ComputerStorageCopyReceipt, error) {
	copier.mu.Lock()
	copier.calls++
	call := copier.calls
	copier.mu.Unlock()
	if err := copier.err(call); err != nil {
		return workloadrunner.ComputerStorageCopyReceipt{}, err
	}
	return workloadrunner.ComputerStorageCopyReceipt{Kind: "computer_storage_copy_verified", ReceiptID: "copied", HelperGeneration: 13}, nil
}

func (copier *failingCopier) count() int {
	copier.mu.Lock()
	defer copier.mu.Unlock()
	return copier.calls
}

type failingBackupper struct{ err error }

func (backupper failingBackupper) CreateComputerBackup(context.Context, workloadrunner.ComputerBackupRequest) (workloadrunner.ComputerBackupCopyReceipt, error) {
	return workloadrunner.ComputerBackupCopyReceipt{}, backupper.err
}

func (failingBackupper) DeleteComputerBackupCopy(context.Context, workloadrunner.ComputerBackupCopyRemovalRequest) (workloadrunner.ComputerBackupCopyRemovalReceipt, error) {
	return workloadrunner.ComputerBackupCopyRemovalReceipt{}, errors.New("not under test")
}

type failingResetter struct{ err error }

func (resetter failingResetter) ResetComputerStorage(context.Context, workloadrunner.ComputerStorageResetRequest) (workloadrunner.ComputerStorageResetReceipt, error) {
	return workloadrunner.ComputerStorageResetReceipt{}, resetter.err
}

type failingGrower struct{ err error }

func (grower failingGrower) GrowComputerStorage(context.Context, workloadrunner.ComputerStorageGrowRequest) (workloadrunner.ComputerStorageGrowReceipt, error) {
	return workloadrunner.ComputerStorageGrowReceipt{}, grower.err
}

func helperEngineFailure() error {
	return &ocihelper.RPCError{Code: ocihelper.CodeEngineFailure, Message: "the engine refused the call"}
}

func restoreDirective() l1.ComputerStorageCopyDirective {
	return l1.ComputerStorageCopyDirective{Operation: "restore", BoundNodeID: "node-1", RootInstanceID: "root-1",
		DestinationComputerID: "computer-restore", DestinationStorageID: "storage-restore", OldGeneration: 1,
		DestinationGeneration: 2, DestinationSize: 4 << 30, OperationRevision: 7, JobID: "job-restore",
		CleanupFence: "restore-fence"}
}

func copyPreparationError(directive l1.ComputerStorageCopyDirective, code, reason string, helperGeneration uint64, recordedAt time.Time) error {
	outcome := workloadrunner.ComputerStoragePreparationOutcome{Code: code,
		Storage: workloadrunner.ComputerStorage{ComputerID: directive.DestinationComputerID,
			StorageID: directive.DestinationStorageID, StorageGeneration: directive.DestinationGeneration,
			IntentRevision: directive.OperationRevision, DiskBytes: directive.DestinationSize},
		HelperGeneration: helperGeneration, Operation: "computer_storage_copy", Reason: reason, RecordedAt: recordedAt}
	if code != workloadrunner.ComputerStoragePreparationInterrupted {
		outcome.SweepEpoch, outcome.DiskName = "sweep-1", "disk-1"
	}
	return &workloadrunner.ComputerStoragePreparationError{Outcome: outcome}
}

// Each error the agent can see maps to one closed class, or to nothing when
// it says nothing about the operation itself (#1608).
func TestStorageOperationFailureClassification(t *testing.T) {
	directive := restoreDirective()
	now := time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)
	refused := func(status int, code contract.ErrorCode, retryable bool) error {
		return fmt.Errorf("acknowledge: %w", &ProtocolError{StatusCode: status,
			APIError: contract.APIError{Code: code, Message: "refused", Retryable: retryable}})
	}
	for _, row := range []struct {
		name        string
		err         error
		class       l1.ComputerStorageOperationFailureClass
		code        string
		generation  uint64
		preparation bool
	}{
		{"preparation quarantined", copyPreparationError(directive, workloadrunner.ComputerStoragePreparationQuarantined, "computer_disk_anomaly_quarantined", 11, now),
			l1.ComputerStorageOperationFailureQuarantined, "computer_storage_quarantined", 11, true},
		{"preparation deferred", copyPreparationError(directive, workloadrunner.ComputerStoragePreparationResumeDeferred, "operational_failure", 11, now),
			l1.ComputerStorageOperationFailureResumeDeferred, "computer_storage_resume_deferred", 11, true},
		{"preparation engine failure", copyPreparationError(directive, workloadrunner.ComputerStoragePreparationInterrupted, workloadrunner.ComputerStoragePreparationReasonEngineFailure, 13, now),
			l1.ComputerStorageOperationFailureEngine, "computer_storage_preparation_interrupted", 13, true},
		{"preparation runtime loss", copyPreparationError(directive, workloadrunner.ComputerStoragePreparationInterrupted, workloadrunner.ComputerStoragePreparationReasonRuntimeLoss, 9, now),
			l1.ComputerStorageOperationFailureRuntimeLoss, "computer_storage_preparation_interrupted", 9, true},
		{"adapter runtime loss", fmt.Errorf("copy: %w", &workloadrunner.RuntimeLossError{Generation: workloadrunner.RuntimeGeneration{Generation: 5}, Err: errors.New("EOF")}),
			l1.ComputerStorageOperationFailureRuntimeLoss, "runtime_loss", 5, false},
		{"helper runtime loss", &ocihelper.RuntimeLossError{Cause: errors.New("EOF")},
			l1.ComputerStorageOperationFailureRuntimeLoss, "runtime_loss", 0, false},
		{"helper engine failure", fmt.Errorf("reset: %w", helperEngineFailure()),
			l1.ComputerStorageOperationFailureEngine, "engine_failure", 0, false},
		{"helper quarantine", &ocihelper.RPCError{Code: ocihelper.CodeComputerStorageQuarantined},
			l1.ComputerStorageOperationFailureQuarantined, "computer_storage_quarantined", 0, false},
		{"helper deferral", &ocihelper.RPCError{Code: ocihelper.CodeComputerStorageResumeDeferred},
			l1.ComputerStorageOperationFailureResumeDeferred, "computer_storage_resume_deferred", 0, false},
		{"acknowledgement refused", refused(http.StatusConflict, contract.ErrorConflict, false),
			l1.ComputerStorageOperationFailureAcknowledgementRefused, "conflict", 0, false},
		{"agent refusal", refuseStorageOperation("managed_root_instance_mismatch", errors.New("other root")),
			l1.ComputerStorageOperationFailureAgentRefused, "managed_root_instance_mismatch", 0, false},
	} {
		t.Run(row.name, func(t *testing.T) {
			failure, reportable := classifyStorageOperationFailure(row.err)
			if !reportable || failure.class != row.class || failure.code != row.code ||
				failure.helperGeneration != row.generation || (failure.preparation != nil) != row.preparation {
				t.Fatalf("classification = %+v reportable=%v, want %s/%s generation %d preparation %v",
					failure, reportable, row.class, row.code, row.generation, row.preparation)
			}
		})
	}
	for _, row := range []struct {
		name string
		err  error
	}{
		{"cancelled", fmt.Errorf("copy: %w", context.Canceled)},
		{"deadline", context.DeadlineExceeded},
		{"L1 unavailable", refused(http.StatusServiceUnavailable, contract.ErrorInternal, true)},
		{"L1 retryable conflict", refused(http.StatusConflict, contract.ErrorConflict, true)},
		{"stale fence", refused(http.StatusConflict, contract.ErrorStaleFence, false)},
		{"node dead", refused(http.StatusConflict, contract.ErrorNodeDead, false)},
		{"no live helper session", errors.New("OCI helper session is not configured")},
		{"helper connection limit", &ocihelper.RPCError{Code: ocihelper.CodeConnectionLimit}},
		{"helper sweep required", fmt.Errorf("copy: %w", &ocihelper.RPCError{Code: ocihelper.CodeSweepRequired})},
		{"helper session busy", &ocihelper.RPCError{Code: ocihelper.CodeSessionBusy}},
		{"Storage held by another attempt", &ocihelper.RPCError{Code: ocihelper.CodeComputerStorageBusy, Detail: "admission_contention"}},
		{"cleanup quarantine", errors.Join(errors.New("delete"), &workloadrunner.ManagedVolumeCleanupQuarantinedError{})},
	} {
		t.Run("not reported/"+row.name, func(t *testing.T) {
			if failure, reportable := classifyStorageOperationFailure(row.err); reportable {
				t.Fatalf("classification = %+v, want not reported", failure)
			}
		})
	}
}

// Every verb and stage reports its failure against its own operation, with
// the directive's revision and fence, and treats L1's answer as final for the
// attempt.
func TestEveryStorageVerbReportsItsFailure(t *testing.T) {
	now := time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)
	retirement := func(controller *storageCopyController) {
		controller.finalizeVolumes = func(context.Context, workloadrunner.ManagedVolumeFinalizationRequest) error {
			return helperEngineFailure()
		}
		controller.attestRuntimeRemoval = func(context.Context, workloadrunner.RuntimeRemovalProofRequest) (workloadrunner.RuntimeRemovalAttestation, error) {
			return workloadrunner.RuntimeRemovalAttestation{}, errors.New("unreachable")
		}
	}
	type want struct {
		computer string
		verb     l1.ComputerStorageOperationVerb
		revision int64
		stage    l1.ComputerStorageOperationStage
		fence    string
		class    l1.ComputerStorageOperationFailureClass
	}
	for _, row := range []struct {
		name string
		run  func(t *testing.T, client *Client) error
		want want
	}{
		{"restore prepare", func(t *testing.T, client *Client) error {
			directive := restoreDirective()
			controller := newStorageCopyController(client, &failingCopier{err: func(int) error {
				return copyPreparationError(directive, workloadrunner.ComputerStoragePreparationInterrupted, workloadrunner.ComputerStoragePreparationReasonEngineFailure, 13, now)
			}}, nil, "node-1", "boot-1", "root-1", t.Logf)
			return controller.process(t.Context(), directive)
		}, want{"computer-restore", l1.ComputerStorageOperationRestore, 7, l1.ComputerStorageOperationPrepare, "restore-fence", l1.ComputerStorageOperationFailureEngine}},
		{"restore retire", func(t *testing.T, client *Client) error {
			directive := restoreDirective()
			directive.Phase = "published"
			controller := newStorageCopyController(client, &failingCopier{err: func(int) error { return nil }}, nil, "node-1", "boot-1", "root-1", t.Logf)
			retirement(controller)
			return controller.process(t.Context(), directive)
		}, want{"computer-restore", l1.ComputerStorageOperationRestore, 7, l1.ComputerStorageOperationRetire, "restore-fence", l1.ComputerStorageOperationFailureEngine}},
		{"clone", func(t *testing.T, client *Client) error {
			directive := restoreDirective()
			directive.Operation, directive.DestinationComputerID, directive.OperationRevision = "clone", "computer-clone", 1
			controller := newStorageCopyController(client, &failingCopier{err: func(int) error {
				return copyPreparationError(directive, workloadrunner.ComputerStoragePreparationQuarantined, "computer_disk_anomaly_quarantined", 21, now)
			}}, nil, "node-1", "boot-1", "root-1", t.Logf)
			return controller.process(t.Context(), directive)
		}, want{"computer-clone", l1.ComputerStorageOperationClone, 1, l1.ComputerStorageOperationPrepare, "restore-fence", l1.ComputerStorageOperationFailureQuarantined}},
		{"import", func(t *testing.T, client *Client) error {
			directive := restoreDirective()
			directive.Operation, directive.DestinationComputerID, directive.OperationRevision = "import", "computer-import", 3
			controller := newStorageCopyController(client, &failingCopier{err: func(int) error {
				return copyPreparationError(directive, workloadrunner.ComputerStoragePreparationResumeDeferred, "operational_failure", 11, now)
			}}, nil, "node-1", "boot-1", "root-1", t.Logf)
			return controller.process(t.Context(), directive)
		}, want{"computer-import", l1.ComputerStorageOperationImport, 3, l1.ComputerStorageOperationPrepare, "restore-fence", l1.ComputerStorageOperationFailureResumeDeferred}},
		{"backup", func(t *testing.T, client *Client) error {
			controller := newBackupController(client, nil, failingBackupper{err: helperEngineFailure()}, "node-1", "boot-1", "root-1", t.Logf)
			controller.enqueueCreate(t.Context(), l1.ComputerBackupDirective{BackupID: "backup-1", CopyID: "copy-1",
				ComputerID: "computer-backup", StorageID: "storage-1", StorageGeneration: 1, BoundNodeID: "node-1",
				RootInstanceID: "root-1", JobID: "job-1", OperationRevision: 4, CleanupFence: "backup-fence"}, nil)
			controller.wait()
			return nil
		}, want{"computer-backup", l1.ComputerStorageOperationBackup, 4, l1.ComputerStorageOperationPrepare, "backup-fence", l1.ComputerStorageOperationFailureEngine}},
		{"reset prepare", func(t *testing.T, client *Client) error {
			controller := newStorageResetController(client, nil, failingResetter{err: helperEngineFailure()}, "node-1", "boot-1", t.Logf)
			return controller.process(t.Context(), l1.ComputerStorageResetDirective{ComputerID: "computer-reset",
				BoundNodeID: "node-1", RootInstanceID: "root-1", JobID: "job-1", IntentRevision: 5, StorageID: "storage-1",
				OldGeneration: 1, NewGeneration: 2, DiskBytes: 1 << 30, CleanupFence: "reset-fence"})
		}, want{"computer-reset", l1.ComputerStorageOperationReset, 5, l1.ComputerStorageOperationPrepare, "reset-fence", l1.ComputerStorageOperationFailureEngine}},
		{"reset retire", func(t *testing.T, client *Client) error {
			controller := newStorageResetController(client, nil, failingResetter{}, "node-1", "boot-1", t.Logf)
			controller.finalizeVolumes = func(context.Context, workloadrunner.ManagedVolumeFinalizationRequest) error {
				return &ocihelper.RuntimeLossError{Cause: errors.New("EOF")}
			}
			controller.attestRuntimeRemoval = func(context.Context, workloadrunner.RuntimeRemovalProofRequest) (workloadrunner.RuntimeRemovalAttestation, error) {
				return workloadrunner.RuntimeRemovalAttestation{}, errors.New("unreachable")
			}
			return controller.process(t.Context(), l1.ComputerStorageResetDirective{ComputerID: "computer-reset",
				BoundNodeID: "node-1", RootInstanceID: "root-1", JobID: "job-1", IntentRevision: 5, StorageID: "storage-1",
				OldGeneration: 1, NewGeneration: 2, DiskBytes: 1 << 30, CleanupFence: "reset-fence", Phase: "published"})
		}, want{"computer-reset", l1.ComputerStorageOperationReset, 5, l1.ComputerStorageOperationRetire, "reset-fence", l1.ComputerStorageOperationFailureRuntimeLoss}},
		{"grow", func(t *testing.T, client *Client) error {
			controller := newStorageGrowController(client, failingGrower{err: helperEngineFailure()}, "node-1", "boot-1", "root-1", t.Logf)
			return controller.process(t.Context(), l1.ComputerStorageGrowDirective{ComputerID: "computer-grow",
				StorageID: "storage-1", StorageGeneration: 1, OldDiskBytes: 1 << 30, NewDiskBytes: 2 << 30,
				BoundNodeID: "node-1", RootInstanceID: "root-1", JobID: "job-1", OperationRevision: 6, OperationFence: "grow-fence"})
		}, want{"computer-grow", l1.ComputerStorageOperationGrow, 6, l1.ComputerStorageOperationPrepare, "grow-fence", l1.ComputerStorageOperationFailureEngine}},
	} {
		t.Run(row.name, func(t *testing.T) {
			fake, client := newFailureBudgetL1(t)
			if err := row.run(t, client); err != nil {
				t.Fatalf("a reported failure is final for its attempt, got %v", err)
			}
			reports, paths, acks := fake.sent()
			if len(reports) != 1 || len(acks) != 0 {
				t.Fatalf("reports = %#v acknowledgements = %v, want exactly one report", reports, acks)
			}
			got := reports[0]
			if paths[0] != "/v1/agent/computers/"+row.want.computer+"/storage-operation-failure" ||
				got.NodeID != "node-1" || got.BootSessionID != "boot-1" || got.Verb != row.want.verb ||
				got.OperationRevision != row.want.revision || got.Stage != row.want.stage ||
				got.Fence != row.want.fence || got.Class != row.want.class || got.Detail == "" {
				t.Fatalf("report %s = %#v, want %+v", paths[0], got, row.want)
			}
		})
	}
}

// L1's retry_after_seconds is the only pace: inside it nothing runs, after it
// the next attempt runs and its failure is a new report.
func TestStorageOperationRetryAfterPacesTheNextAttempt(t *testing.T) {
	fake, client := newFailureBudgetL1(t)
	fake.answer = func(_ l1.ComputerStorageOperationFailureRequest, count int) (int, string) {
		return http.StatusOK, fmt.Sprintf(`{"disposition":"counted","failed_attempts":%d,"retry_after_seconds":60,"operation_status":"reserved"}`, count)
	}
	directive := restoreDirective()
	now := time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)
	copier := &failingCopier{err: func(int) error {
		return copyPreparationError(directive, workloadrunner.ComputerStoragePreparationInterrupted, workloadrunner.ComputerStoragePreparationReasonEngineFailure, 13, now)
	}}
	controller := newStorageCopyController(client, copier, nil, "node-1", "boot-1", "root-1", t.Logf)
	controller.failures.now = func() time.Time { return now }
	sweep := func() {
		controller.enqueue(t.Context(), directive, nil)
		controller.wait()
	}
	sweep()
	for range 3 {
		now = now.Add(15 * time.Second)
		sweep()
	}
	if calls := copier.count(); calls != 1 {
		t.Fatalf("copies inside L1's retry_after = %d, want 1", calls)
	}
	now = now.Add(15 * time.Second)
	sweep()
	reports, _, _ := fake.sent()
	if calls := copier.count(); calls != 2 || len(reports) != 2 || reports[0].ReportID == reports[1].ReportID {
		t.Fatalf("copies=%d reports=%#v, want a second attempt with a fresh report_id", calls, reports)
	}

	// A copy that then succeeds clears the pace: the next directive runs.
	copier.err = func(int) error { return nil }
	now = now.Add(time.Minute)
	sweep()
	if _, _, acks := fake.sent(); len(acks) != 1 {
		t.Fatalf("acknowledgements after success = %v", acks)
	}
	if !controller.failures.due(storageCopyOperation(directive)) {
		t.Fatal("a succeeded attempt kept its pace")
	}
}

// Recovery passes call the reset and grow controllers directly, so the pace
// holds there too.
func TestStorageOperationPaceHoldsOnDirectRecoveryCalls(t *testing.T) {
	fake, client := newFailureBudgetL1(t)
	now := time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)
	controller := newStorageGrowController(client, failingGrower{err: helperEngineFailure()}, "node-1", "boot-1", "root-1", t.Logf)
	controller.failures.now = func() time.Time { return now }
	directive := l1.ComputerStorageGrowDirective{ComputerID: "computer-grow", StorageID: "storage-1", StorageGeneration: 1,
		OldDiskBytes: 1 << 30, NewDiskBytes: 2 << 30, BoundNodeID: "node-1", RootInstanceID: "root-1",
		JobID: "job-1", OperationRevision: 6, OperationFence: "grow-fence"}
	for range 3 {
		if err := controller.process(t.Context(), directive); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(15 * time.Second)
	if err := controller.process(t.Context(), directive); err != nil {
		t.Fatal(err)
	}
	if reports, _, _ := fake.sent(); len(reports) != 2 {
		t.Fatalf("reports = %d, want 2 (one per retry_after)", len(reports))
	}
}

// Nothing is reported for a cancelled pass or for an L1 answer about
// availability or authority; those failures stay ordinary errors. A failure
// L1 did not answer is paced locally on the same curve.
func TestStorageOperationFailuresThatAreNotReported(t *testing.T) {
	now := time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)
	t.Run("cancelled pass", func(t *testing.T) {
		fake, client := newFailureBudgetL1(t)
		ctx, cancel := context.WithCancel(t.Context())
		directive := restoreDirective()
		controller := newStorageCopyController(client, &failingCopier{err: func(int) error {
			cancel()
			return fmt.Errorf("copy: %w", context.Canceled)
		}}, nil, "node-1", "boot-1", "root-1", t.Logf)
		if err := controller.process(ctx, directive); err == nil {
			t.Fatal("a cancelled attempt was settled")
		}
		if reports, _, _ := fake.sent(); len(reports) != 0 {
			t.Fatalf("a cancelled pass was reported: %#v", reports)
		}
		if !controller.failures.due(storageCopyOperation(directive)) {
			t.Fatal("a cancelled pass paced the operation")
		}
	})
	for _, row := range []struct {
		name   string
		status int
		body   string
	}{
		{"L1 unavailable", http.StatusServiceUnavailable, `{"error":{"code":"internal","message":"L1 unavailable","retryable":true}}`},
		{"stale fence", http.StatusConflict, `{"error":{"code":"stale_fence","message":"fence moved","retryable":false}}`},
		{"node dead", http.StatusConflict, `{"error":{"code":"node_dead","message":"node dead","retryable":false}}`},
	} {
		t.Run("acknowledgement "+row.name, func(t *testing.T) {
			fake, client := newFailureBudgetL1(t)
			fake.ackError = func(string) (int, string) { return row.status, row.body }
			directive := restoreDirective()
			controller := newStorageCopyController(client, &failingCopier{err: func(int) error { return nil }}, nil,
				"node-1", "boot-1", "root-1", t.Logf)
			controller.failures.now = func() time.Time { return now }
			if err := controller.process(t.Context(), directive); err == nil {
				t.Fatal("an unreported acknowledgement failure was swallowed")
			}
			if reports, _, _ := fake.sent(); len(reports) != 0 {
				t.Fatalf("reports = %#v, want none", reports)
			}
			operation := storageCopyOperation(directive)
			if controller.failures.due(operation) {
				t.Fatal("an unreported failure was not paced")
			}
			controller.failures.now = func() time.Time { return now.Add(storageOperationRetryBase) }
			if !controller.failures.due(operation) {
				t.Fatal("the local pace is longer than one heartbeat")
			}
		})
	}
	t.Run("report itself fails", func(t *testing.T) {
		fake, client := newFailureBudgetL1(t)
		fake.answer = func(l1.ComputerStorageOperationFailureRequest, int) (int, string) {
			return http.StatusServiceUnavailable, `{"error":{"code":"internal","message":"L1 unavailable","retryable":true}}`
		}
		controller := newStorageGrowController(client, failingGrower{err: helperEngineFailure()}, "node-1", "boot-1", "root-1", t.Logf)
		controller.failures.now = func() time.Time { return now }
		err := controller.process(t.Context(), l1.ComputerStorageGrowDirective{ComputerID: "computer-grow",
			BoundNodeID: "node-1", RootInstanceID: "root-1", OperationRevision: 6, OperationFence: "grow-fence"})
		var rpc *ocihelper.RPCError
		if !errors.As(err, &rpc) || !strings.Contains(err.Error(), "report Computer grow failure") {
			t.Fatalf("unanswered report = %v, want the helper error and the report error", err)
		}
	})
}

// L1 holds the whole budget: a restarted agent keeps nothing and simply
// reports the next failure under a fresh report_id.
func TestRestartedAgentReportsWithoutLocalState(t *testing.T) {
	fake, client := newFailureBudgetL1(t)
	directive := restoreDirective()
	now := time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)
	failure := func(int) error {
		return copyPreparationError(directive, workloadrunner.ComputerStoragePreparationInterrupted, workloadrunner.ComputerStoragePreparationReasonEngineFailure, 13, now)
	}
	first := newStorageCopyController(client, &failingCopier{err: failure}, nil, "node-1", "boot-1", "root-1", t.Logf)
	if err := first.process(t.Context(), directive); err != nil {
		t.Fatal(err)
	}
	restarted := newStorageCopyController(client, &failingCopier{err: failure}, nil, "node-1", "boot-2", "root-1", t.Logf)
	if !restarted.failures.due(storageCopyOperation(directive)) {
		t.Fatal("a restarted agent inherited a pace")
	}
	if err := restarted.process(t.Context(), directive); err != nil {
		t.Fatal(err)
	}
	reports, _, _ := fake.sent()
	if len(reports) != 2 || reports[0].ReportID == reports[1].ReportID || reports[1].BootSessionID != "boot-2" {
		t.Fatalf("reports = %#v", reports)
	}
}

// A restore used to return a quarantined staging generation, a deferral and a
// runtime loss as plain errors and retry them every heartbeat (#1591). Each
// is now reported, and the helper's outcome rides along.
func TestRestoreReportsQuarantineDeferralAndRuntimeLoss(t *testing.T) {
	directive := restoreDirective()
	now := time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)
	for _, row := range []struct {
		name        string
		err         error
		class       l1.ComputerStorageOperationFailureClass
		preparation string
	}{
		{"quarantined", copyPreparationError(directive, workloadrunner.ComputerStoragePreparationQuarantined, "computer_disk_anomaly_quarantined", 11, now),
			l1.ComputerStorageOperationFailureQuarantined, l1.ComputerStoragePreparationQuarantined},
		{"deferred", copyPreparationError(directive, workloadrunner.ComputerStoragePreparationResumeDeferred, "operational_failure", 11, now),
			l1.ComputerStorageOperationFailureResumeDeferred, l1.ComputerStoragePreparationResumeDeferred},
		{"helper runtime loss", &ocihelper.RuntimeLossError{Cause: errors.New("EOF")},
			l1.ComputerStorageOperationFailureRuntimeLoss, ""},
	} {
		t.Run(row.name, func(t *testing.T) {
			fake, client := newFailureBudgetL1(t)
			controller := newStorageCopyController(client, &failingCopier{err: func(int) error { return row.err }}, nil,
				"node-1", "boot-1", "root-1", t.Logf)
			if err := controller.process(t.Context(), directive); err != nil {
				t.Fatal(err)
			}
			reports, _, _ := fake.sent()
			if len(reports) != 1 || reports[0].Class != row.class {
				t.Fatalf("reports = %#v", reports)
			}
			got := reports[0].PreparationOutcome
			if row.preparation == "" {
				if got != nil {
					t.Fatalf("runtime loss without a generation carried an outcome: %#v", got)
				}
				return
			}
			if got == nil || got.Code != row.preparation || got.DestinationComputerID != directive.DestinationComputerID ||
				got.DestinationGeneration != directive.DestinationGeneration || got.IntentRevision != directive.OperationRevision ||
				got.DiskBytes != directive.DestinationSize || got.HelperGeneration != 11 || got.SweepEpoch != "sweep-1" ||
				got.RecordedAt == nil || !got.RecordedAt.Equal(now) {
				t.Fatalf("preparation outcome = %#v", got)
			}
		})
	}
}

// Runtime loss of a session that copied into a fresh destination generation
// is the generation-bound outcome L1 shows on the operation.
func TestCopyRuntimeLossCarriesTheInterruptedOutcome(t *testing.T) {
	fake, client := newFailureBudgetL1(t)
	directive := restoreDirective()
	directive.Operation, directive.DestinationComputerID, directive.OperationRevision = "clone", "computer-clone", 1
	controller := newStorageCopyController(client, &failingCopier{err: func(int) error {
		return &workloadrunner.RuntimeLossError{Generation: workloadrunner.RuntimeGeneration{InstanceID: "helper", Generation: 9},
			Err: errors.New("oci helper engine_failure operation_failed: EOF")}
	}}, nil, "node-1", "boot-1", "root-1", t.Logf)
	if err := controller.process(t.Context(), directive); err != nil {
		t.Fatal(err)
	}
	reports, _, _ := fake.sent()
	if len(reports) != 1 {
		t.Fatalf("reports = %#v", reports)
	}
	got := reports[0]
	if got.Class != l1.ComputerStorageOperationFailureRuntimeLoss || got.HelperGeneration != 9 || got.PreparationOutcome == nil ||
		got.PreparationOutcome.Code != l1.ComputerStoragePreparationInterrupted ||
		got.PreparationOutcome.Reason != l1.ComputerStoragePreparationReasonRuntimeLoss ||
		got.PreparationOutcome.Operation != "computer_storage_copy" || got.PreparationOutcome.HelperGeneration != 9 ||
		got.PreparationOutcome.DestinationGeneration != directive.DestinationGeneration || got.PreparationOutcome.RecordedAt == nil {
		t.Fatalf("runtime loss report = %#v outcome=%#v", got, got.PreparationOutcome)
	}
}

// The reimage shape: the helper call succeeds and L1 refuses its
// acknowledgement the same way every sweep. That refusal is a counted
// failure, not a log line forever.
func TestRefusedAcknowledgementIsReported(t *testing.T) {
	fake, client := newFailureBudgetL1(t)
	fake.ackError = func(string) (int, string) {
		return http.StatusConflict, `{"error":{"code":"storage_reference_conflict","message":"receipt names another generation","retryable":false}}`
	}
	controller := newStorageResetController(client, nil, resetReceiptFunc(func() workloadrunner.ComputerStorageResetReceipt {
		return workloadrunner.ComputerStorageResetReceipt{Kind: "computer_storage_reset_prepared", ReceiptID: "reset-receipt"}
	}), "node-1", "boot-1", t.Logf)
	if err := controller.process(t.Context(), l1.ComputerStorageResetDirective{ComputerID: "computer-reset",
		BoundNodeID: "node-1", IntentRevision: 5, CleanupFence: "reset-fence"}); err != nil {
		t.Fatal(err)
	}
	reports, _, acks := fake.sent()
	if len(acks) != 1 || len(reports) != 1 || reports[0].Class != l1.ComputerStorageOperationFailureAcknowledgementRefused ||
		reports[0].Code != "storage_reference_conflict" {
		t.Fatalf("acknowledgements=%v reports=%#v", acks, reports)
	}
}

type resetReceiptFunc func() workloadrunner.ComputerStorageResetReceipt

func (receipt resetReceiptFunc) ResetComputerStorage(context.Context, workloadrunner.ComputerStorageResetRequest) (workloadrunner.ComputerStorageResetReceipt, error) {
	return receipt(), nil
}

// A directive the agent refuses locally is reported, not logged every 15 s.
func TestLocalRefusalIsReported(t *testing.T) {
	fake, client := newFailureBudgetL1(t)
	directive := restoreDirective()
	directive.RootInstanceID = "another-root"
	copier := &failingCopier{err: func(int) error { return nil }}
	controller := newStorageCopyController(client, copier, nil, "node-1", "boot-1", "root-1", t.Logf)
	if err := controller.process(t.Context(), directive); err != nil {
		t.Fatal(err)
	}
	reports, _, _ := fake.sent()
	if copier.count() != 0 || len(reports) != 1 || reports[0].Class != l1.ComputerStorageOperationFailureAgentRefused ||
		reports[0].Code != "managed_root_instance_mismatch" {
		t.Fatalf("copies=%d reports=%#v", copier.count(), reports)
	}
}

func TestStorageOperationFailureDetailIsBounded(t *testing.T) {
	long := errors.New(strings.Repeat("é", l1.MaximumStorageOperationFailureDetail))
	detail := storageOperationFailureDetail(long)
	if len(detail) > l1.MaximumStorageOperationFailureDetail || len(detail) < l1.MaximumStorageOperationFailureDetail-1 ||
		!utf8.ValidString(detail) {
		t.Fatalf("detail of %d bytes, valid %v", len(detail), utf8.ValidString(detail))
	}
	if got := storageOperationFailureDetail(errors.New("bad \xff byte")); !utf8.ValidString(got) {
		t.Fatalf("detail %q is not valid UTF-8", got)
	}
}

// Every field of the helper's preparation outcome reaches L1 unchanged.
func TestPreparationOutcomeMapsEveryField(t *testing.T) {
	recordedAt := time.Date(2026, 9, 4, 12, 34, 56, 789, time.UTC)
	droppedAt := recordedAt.Add(time.Hour)
	outcome := workloadrunner.ComputerStoragePreparationOutcome{
		Code: workloadrunner.ComputerStoragePreparationResumeDeferred,
		Storage: workloadrunner.ComputerStorage{ComputerID: "computer-import", StorageID: "storage-import",
			StorageGeneration: 3, IntentRevision: 7, DiskBytes: 8 << 30},
		HelperGeneration: 11, SweepEpoch: "sweep-import", DiskName: "disk-import",
		Operation: "computer_storage_copy", Reason: "operational_failure", DeferredReason: "context_expired",
		Attempts: 4, FirstDeferredAt: &recordedAt, PayloadDroppedAt: &droppedAt, RecordedAt: recordedAt,
	}
	got := l1StoragePreparationOutcome(outcome)
	if got.Code != outcome.Code || got.DestinationComputerID != outcome.Storage.ComputerID ||
		got.DestinationStorageID != outcome.Storage.StorageID || got.DestinationGeneration != outcome.Storage.StorageGeneration ||
		got.IntentRevision != outcome.Storage.IntentRevision || got.DiskBytes != outcome.Storage.DiskBytes ||
		got.HelperGeneration != outcome.HelperGeneration || got.SweepEpoch != outcome.SweepEpoch ||
		got.DiskName != outcome.DiskName || got.Operation != outcome.Operation || got.Reason != outcome.Reason ||
		got.DeferredReason != outcome.DeferredReason || got.Attempts != outcome.Attempts ||
		got.FirstDeferredAt == nil || !got.FirstDeferredAt.Equal(*outcome.FirstDeferredAt) ||
		got.PayloadDroppedAt == nil || !got.PayloadDroppedAt.Equal(*outcome.PayloadDroppedAt) ||
		got.RecordedAt == nil || !got.RecordedAt.Equal(outcome.RecordedAt) {
		t.Fatalf("mapped preparation outcome = %#v, want %#v", got, outcome)
	}
}

// A recovery pass and a heartbeat share one reservation: while a heartbeat's
// attempt is in flight, a direct recovery call runs nothing.
func TestRecoveryCallWaitsForTheInFlightAttempt(t *testing.T) {
	fake, client := newFailureBudgetL1(t)
	entered, release := make(chan struct{}), make(chan struct{})
	controller := newStorageResetController(client, nil, blockingResetter{entered: entered, release: release}, "node-1", "boot-1", t.Logf)
	directive := l1.ComputerStorageResetDirective{ComputerID: "computer-reset", BoundNodeID: "node-1",
		IntentRevision: 5, CleanupFence: "reset-fence"}
	controller.enqueue(t.Context(), directive, make(chan destinationError, 1))
	<-entered
	if err := controller.process(t.Context(), directive); err != nil {
		t.Fatal(err)
	}
	close(release)
	controller.wait()
	if reports, _, _ := fake.sent(); len(reports) != 1 {
		t.Fatalf("reports = %d, want 1: the recovery call ran beside the heartbeat's attempt", len(reports))
	}
}

type blockingResetter struct{ entered, release chan struct{} }

func (resetter blockingResetter) ResetComputerStorage(context.Context, workloadrunner.ComputerStorageResetRequest) (workloadrunner.ComputerStorageResetReceipt, error) {
	close(resetter.entered)
	<-resetter.release
	return workloadrunner.ComputerStorageResetReceipt{}, helperEngineFailure()
}

// When the report itself is answered with a node-session error, that answer
// routes the pass, not the operation's own failure.
func TestReportNodeSessionErrorRoutesThePass(t *testing.T) {
	fake, client := newFailureBudgetL1(t)
	fake.answer = func(l1.ComputerStorageOperationFailureRequest, int) (int, string) {
		return http.StatusConflict, `{"error":{"code":"node_session_replaced","message":"replaced","retryable":false}}`
	}
	fake.ackError = func(string) (int, string) {
		return http.StatusConflict, `{"error":{"code":"storage_reference_conflict","message":"other generation","retryable":false}}`
	}
	controller := newStorageResetController(client, nil, resetReceiptFunc(func() workloadrunner.ComputerStorageResetReceipt {
		return workloadrunner.ComputerStorageResetReceipt{Kind: "computer_storage_reset_prepared", ReceiptID: "reset-receipt"}
	}), "node-1", "boot-1", t.Logf)
	failures := make(chan destinationError, 1)
	controller.enqueue(t.Context(), l1.ComputerStorageResetDirective{ComputerID: "computer-reset", BoundNodeID: "node-1",
		IntentRevision: 5, CleanupFence: "reset-fence"}, failures)
	controller.wait()
	select {
	case routed := <-failures:
		if routed.destination != errorDestinationNodeSession {
			t.Fatalf("routed to %v", routed.destination)
		}
	default:
		t.Fatal("the report's node_session_replaced did not reach the node session")
	}
}
