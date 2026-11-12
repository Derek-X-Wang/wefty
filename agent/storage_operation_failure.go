package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// storageOperationRef names one stage of one Storage operation directive: the
// unit L1's failure budget counts (#1608).
type storageOperationRef struct {
	computerID string
	verb       l1.ComputerStorageOperationVerb
	revision   int64
	stage      l1.ComputerStorageOperationStage
	fence      string
}

func (ref storageOperationRef) key() string {
	return fmt.Sprintf("%s\x00%s\x00%d\x00%s", ref.computerID, ref.verb, ref.revision, ref.stage)
}

// storageOperationRefusal is a directive the agent refused locally, before
// or instead of calling the helper: a managed-root instance it does not run,
// mechanics it was not built with, or helper evidence that does not match
// the request. It is reported as `agent_refused`.
type storageOperationRefusal struct {
	code string
	err  error
}

func (refusal *storageOperationRefusal) Error() string { return refusal.err.Error() }
func (refusal *storageOperationRefusal) Unwrap() error { return refusal.err }

func refuseStorageOperation(code string, err error) error {
	return &storageOperationRefusal{code: code, err: err}
}

// Local pacing for an attempt whose failure L1 did not answer: one default
// heartbeat, doubling to five minutes, the same curve L1 answers with. A
// failure that cannot clear is then not retried at whatever rate heartbeats
// and recovery passes arrive (#558), even while L1 is unreachable.
const (
	storageOperationRetryBase    = DefaultHeartbeatInterval
	storageOperationRetryCeiling = 5 * time.Minute
)

// storageFailureReporter sends each failed Storage operation attempt to L1,
// which owns the budget and decides when the operation ends (#1608). The
// agent keeps no count of its own. It keeps only a "not before" time per
// operation stage in memory, from L1's retry_after_seconds; losing it on a
// restart costs one early retry, never a wrong outcome.
type storageFailureReporter struct {
	client        *Client
	nodeID        string
	bootSessionID string
	logf          func(string, ...any)
	now           func() time.Time
	newReportID   func() (string, error)

	mu   sync.Mutex
	pace map[string]storageOperationPace
}

type storageOperationPace struct {
	failures  int
	notBefore time.Time
}

func newStorageFailureReporter(client *Client, nodeID, bootSessionID string, logf func(string, ...any)) *storageFailureReporter {
	return &storageFailureReporter{client: client, nodeID: nodeID, bootSessionID: bootSessionID, logf: logf,
		now: time.Now, newReportID: newStorageOperationReportID, pace: make(map[string]storageOperationPace)}
}

func newStorageOperationReportID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", fmt.Errorf("mint Storage operation failure report id: %w", err)
	}
	return hex.EncodeToString(id[:]), nil
}

// due reports whether ref may be attempted now. Callers hold their own
// controller lock while they ask, together with the in-flight reservation,
// so a failing attempt records its pace before it releases the reservation.
func (reporter *storageFailureReporter) due(ref storageOperationRef) bool {
	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	pace, failed := reporter.pace[ref.key()]
	return !failed || !reporter.now().Before(pace.notBefore)
}

// settle closes one attempt of ref. A success clears its pace. A failure the
// agent can classify is reported to L1; once L1 has answered, the attempt is
// done and settle returns nil, because L1's answer is final for it. Any other
// failure is returned for the caller to route or log, and is paced locally.
func (reporter *storageFailureReporter) settle(ctx context.Context, ref storageOperationRef, err error) error {
	if err == nil {
		reporter.mu.Lock()
		delete(reporter.pace, ref.key())
		reporter.mu.Unlock()
		return nil
	}
	// A cancelled pass teaches nothing about this operation.
	if ctx.Err() != nil {
		return err
	}
	failure, reportable := classifyStorageOperationFailure(err)
	if !reportable {
		reporter.paceLocally(ref)
		return err
	}
	reportID, mintErr := reporter.newReportID()
	if mintErr != nil {
		reporter.paceLocally(ref)
		return errors.Join(err, mintErr)
	}
	response, reportErr := reporter.client.ReportComputerStorageOperationFailure(ctx, ref.computerID,
		l1.ComputerStorageOperationFailureRequest{NodeID: reporter.nodeID, BootSessionID: reporter.bootSessionID,
			ReportID: reportID, Verb: ref.verb, OperationRevision: ref.revision, Stage: ref.stage, Fence: ref.fence,
			Class: failure.class, Code: failure.code, Detail: storageOperationFailureDetail(err),
			HelperGeneration: failure.helperGeneration, PreparationOutcome: failure.preparation})
	if reportErr != nil {
		reporter.paceLocally(ref)
		// The report's own error leads, so an L1 answer about the node
		// session on the report routes the pass the way it routes any other.
		return errors.Join(fmt.Errorf("report Computer %s failure: %w", ref.verb, reportErr), err)
	}
	reporter.mu.Lock()
	if response.RetryAfterSeconds > 0 {
		pace := reporter.pace[ref.key()]
		pace.failures++
		pace.notBefore = reporter.now().Add(time.Duration(response.RetryAfterSeconds) * time.Second)
		reporter.pace[ref.key()] = pace
	} else {
		delete(reporter.pace, ref.key())
	}
	reporter.mu.Unlock()
	if reporter.logf != nil {
		reporter.logf("agent: Computer %s %s computer=%s revision=%d %s/%s: %s (failed_attempts=%d operation_status=%s retry_after=%ds): %v",
			ref.verb, ref.stage, ref.computerID, ref.revision, failure.class, failure.code, response.Disposition,
			response.FailedAttempts, response.OperationStatus, response.RetryAfterSeconds, err)
	}
	return nil
}

func (reporter *storageFailureReporter) paceLocally(ref storageOperationRef) {
	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	pace := reporter.pace[ref.key()]
	pace.failures++
	pace.notBefore = reporter.now().Add(doublingDelay(storageOperationRetryBase, storageOperationRetryCeiling, pace.failures))
	reporter.pace[ref.key()] = pace
}

type storageOperationFailure struct {
	class            l1.ComputerStorageOperationFailureClass
	code             string
	helperGeneration uint64
	preparation      *l1.ComputerStoragePreparationOutcome
}

var storageOperationFailureCode = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)

// classifyStorageOperationFailure maps what the agent saw to one closed class
// (#1608). It does not interpret further: L1 decides what the class means.
// It reports false for what says nothing about the operation itself: a
// cancelled or unreachable L1, an L1 answer about the node session or the
// attempt's authority, a helper with no live session to answer or one that
// answered about its session, and a cleanup quarantine, which has its own
// receipt path.
func classifyStorageOperationFailure(err error) (storageOperationFailure, bool) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return storageOperationFailure{}, false
	}
	var refusal *storageOperationRefusal
	if errors.As(err, &refusal) {
		return storageOperationFailure{class: l1.ComputerStorageOperationFailureAgentRefused, code: refusal.code}, true
	}
	var cleanupQuarantine *workloadrunner.ManagedVolumeCleanupQuarantinedError
	if errors.As(err, &cleanupQuarantine) {
		return storageOperationFailure{}, false
	}
	var preparation *workloadrunner.ComputerStoragePreparationError
	if errors.As(err, &preparation) {
		outcome := preparation.Outcome
		failure := storageOperationFailure{helperGeneration: outcome.HelperGeneration,
			preparation: l1StoragePreparationOutcome(outcome)}
		switch outcome.Code {
		case workloadrunner.ComputerStoragePreparationQuarantined:
			failure.class, failure.code = l1.ComputerStorageOperationFailureQuarantined, outcome.Code
		case workloadrunner.ComputerStoragePreparationResumeDeferred:
			failure.class, failure.code = l1.ComputerStorageOperationFailureResumeDeferred, outcome.Code
		default:
			failure.class, failure.code = l1.ComputerStorageOperationFailureEngine, outcome.Code
			if outcome.Reason == workloadrunner.ComputerStoragePreparationReasonRuntimeLoss {
				failure.class = l1.ComputerStorageOperationFailureRuntimeLoss
			}
		}
		failure.code = storageOperationCode(failure.code, string(failure.class))
		return failure, true
	}
	var runtimeLoss *workloadrunner.RuntimeLossError
	if errors.As(err, &runtimeLoss) {
		return storageOperationFailure{class: l1.ComputerStorageOperationFailureRuntimeLoss, code: "runtime_loss",
			helperGeneration: runtimeLoss.Generation.Generation}, true
	}
	var helperLoss *ocihelper.RuntimeLossError
	if errors.As(err, &helperLoss) {
		return storageOperationFailure{class: l1.ComputerStorageOperationFailureRuntimeLoss, code: "runtime_loss"}, true
	}
	var rpc *ocihelper.RPCError
	if errors.As(err, &rpc) {
		switch rpc.Code {
		case ocihelper.CodeSessionBusy, ocihelper.CodeSessionStale, ocihelper.CodeSweepRequired, ocihelper.CodeComputerStorageBusy,
			ocihelper.CodeConnectionLimit, ocihelper.CodeStartupBoundTripped, ocihelper.CodeDiagnosticFailure,
			ocihelper.CodePeerUnauthenticated, ocihelper.CodeVersionMismatch, ocihelper.CodeChecksumMismatch:
			// The helper answered about availability, not about this
			// operation: a busy or unswept session, a connection slot, a
			// handshake, or Storage another attempt still holds. The same
			// call succeeds once that clears.
			return storageOperationFailure{}, false
		case ocihelper.CodeComputerStorageQuarantined:
			return storageOperationFailure{class: l1.ComputerStorageOperationFailureQuarantined, code: string(rpc.Code)}, true
		case ocihelper.CodeComputerStorageResumeDeferred:
			return storageOperationFailure{class: l1.ComputerStorageOperationFailureResumeDeferred, code: string(rpc.Code)}, true
		}
		return storageOperationFailure{class: l1.ComputerStorageOperationFailureEngine,
			code: storageOperationCode(string(rpc.Code), "engine_failure")}, true
	}
	var protocol *ProtocolError
	if errors.As(err, &protocol) && acknowledgementRefused(protocol) {
		return storageOperationFailure{class: l1.ComputerStorageOperationFailureAcknowledgementRefused,
			code: storageOperationCode(string(protocol.APIError.Code), "acknowledgement_refused")}, true
	}
	return storageOperationFailure{}, false
}

// acknowledgementRefused reports an L1 answer that will come back the same
// on every sweep: a client error that is not retryable and that the agent's
// protocol classifier does not route to the node session or to the attempt's
// authority. classifyAgentProtocolError calls such a code transient, because
// for the generic completion path it is; for an acknowledgement of the same
// receipt it is the reimage shape (#1608).
func acknowledgementRefused(protocol *ProtocolError) bool {
	if protocol.StatusCode < http.StatusBadRequest || protocol.StatusCode >= http.StatusInternalServerError ||
		protocol.APIError.Retryable {
		return false
	}
	_, classified := agentProtocolErrorClassifications[protocol.APIError.Code]
	return !classified
}

func storageOperationCode(code, fallback string) string {
	if storageOperationFailureCode.MatchString(code) {
		return code
	}
	return fallback
}

// storageOperationFailureDetail is the error text, cut to L1's bound on a
// rune boundary.
func storageOperationFailureDetail(err error) string {
	detail := strings.ToValidUTF8(err.Error(), "?")
	if len(detail) <= l1.MaximumStorageOperationFailureDetail {
		return detail
	}
	detail = detail[:l1.MaximumStorageOperationFailureDetail]
	for !utf8.ValidString(detail) {
		detail = detail[:len(detail)-1]
	}
	return detail
}

func l1StoragePreparationOutcome(outcome workloadrunner.ComputerStoragePreparationOutcome) *l1.ComputerStoragePreparationOutcome {
	recordedAt := outcome.RecordedAt
	return &l1.ComputerStoragePreparationOutcome{
		Code: outcome.Code, DestinationComputerID: outcome.Storage.ComputerID,
		DestinationStorageID: outcome.Storage.StorageID, DestinationGeneration: outcome.Storage.StorageGeneration,
		IntentRevision: outcome.Storage.IntentRevision, DiskBytes: outcome.Storage.DiskBytes,
		HelperGeneration: outcome.HelperGeneration, SweepEpoch: outcome.SweepEpoch, DiskName: outcome.DiskName,
		Operation: outcome.Operation, Reason: outcome.Reason, DeferredReason: outcome.DeferredReason,
		Attempts: outcome.Attempts, FirstDeferredAt: outcome.FirstDeferredAt, PayloadDroppedAt: outcome.PayloadDroppedAt,
		RecordedAt: &recordedAt,
	}
}
