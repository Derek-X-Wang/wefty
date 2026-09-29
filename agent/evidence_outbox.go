package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

const (
	maxEvidenceRecoveryWorkers         = 8
	maxEvidenceLogReplayBatchesPerPass = 8
	// maxEvidenceRecoveryBackoff is the ceiling of the per-attempt and spool
	// scan backoff. A failure that keeps recurring is retried at most this
	// often, so an L1 that answers every replay with a transient error sees one
	// request per attempt per ceiling rather than one per retry interval.
	maxEvidenceRecoveryBackoff = 30 * time.Second
	// A refused attempt is re-checked on its own schedule, starting at
	// evidenceRecoveryRefusalRecheck and doubling to
	// maxEvidenceRecoveryRefusalRecheck, for as long as the evidence is
	// pending. It never stops: node_session_replaced also answers a
	// completion L1 has not accepted while the replaced registration's lease
	// is still running, and that turns into lease_expired -- the result kept
	// as late evidence -- only once the lease runs out, and only within L1's
	// late-evidence window (48 h by default, configurable down to minutes;
	// after it L1 keeps a gap in place of the result). The spool does not keep
	// the lease an attempt was claimed with, so the schedule cannot be derived
	// from it. It starts short and doubles instead: every wait is the time
	// already waited plus the first step, so the first re-check after the
	// lease runs out comes at most the lease's remainder plus 10 s later, and
	// never more than the hourly cap later. That lands the result as itself on
	// any L1 whose late-evidence window is longer than its lease plus 10 s. The price is a
	// few silent requests in the first minutes; the cap keeps a completion
	// that can never land to one request an hour.
	evidenceRecoveryRefusalRecheck    = 10 * time.Second
	maxEvidenceRecoveryRefusalRecheck = time.Hour
)

// evidenceRecoveryRefusal is a typed L1 refusal of a replay that repeating
// on a retry interval cannot clear: the node session the request speaks for
// has been replaced, or the node's identity is refused. Recovery keeps
// re-checking the attempt on the slow refusal schedule, logging only the first
// refusal, and its durable evidence stays exactly as it is on disk; nothing
// about the refusal says the evidence is wrong, only that this session cannot
// deliver it now.
type evidenceRecoveryRefusal struct {
	code contract.ErrorCode
	err  error
}

func (refusal *evidenceRecoveryRefusal) Error() string { return refusal.err.Error() }

func (refusal *evidenceRecoveryRefusal) Unwrap() error { return refusal.err }

// recoveryRefusal wraps err when L1 refused it for a reason no retry from this
// session can change. Every other error is returned as is.
func recoveryRefusal(err error, code contract.ErrorCode, classification agentProtocolErrorClassification) error {
	if classification.destination == errorDestinationNodeSession &&
		classification.nodeSessionReaction == nodeSessionStopRecordAndEscalate {
		return &evidenceRecoveryRefusal{code: code, err: err}
	}
	return err
}

// evidenceRecoveryBackoff is the wait before retry number failures of the same
// recovery work: the configured retry interval, doubled per consecutive
// failure, capped at maxEvidenceRecoveryBackoff (or the interval itself, when
// that is configured larger).
func evidenceRecoveryBackoff(interval time.Duration, failures int) time.Duration {
	return doublingDelay(interval, max(maxEvidenceRecoveryBackoff, interval), failures)
}

// evidenceRecoveryRefusalDelay is the wait before re-checking an attempt L1
// has refused refusals times in a row.
func evidenceRecoveryRefusalDelay(refusals int) time.Duration {
	return doublingDelay(evidenceRecoveryRefusalRecheck, maxEvidenceRecoveryRefusalRecheck, refusals)
}

// doublingDelay is base doubled once per step after the first, capped at
// ceiling.
func doublingDelay(base, ceiling time.Duration, step int) time.Duration {
	if base <= 0 {
		return base
	}
	delay := base
	for i := 1; i < step && delay < ceiling; i++ {
		delay *= 2
	}
	return min(delay, ceiling)
}

// evidenceOutbox owns durable evidence for the lifetime of the agent process.
// Sessions borrow it; ending or replacing a session must not discard evidence
// that still needs delivery.
type evidenceOutbox struct {
	spool         *logSpool
	clock         Clock
	batchSize     int
	flushInterval time.Duration
	retryInterval time.Duration
	ociIntentGate *ociIntentCompletionGate
	// completionStored is a test seam for ordering cancellation against the
	// durable commit edge. Production construction leaves it nil.
	completionStored func()
	// recoveryAttemptFinished is a test seam for ordering a wake against the
	// scheduler's active-attempt retirement edge. Production leaves it nil.
	recoveryAttemptFinished func(string)

	recoveryMu     sync.Mutex
	recoveryCancel context.CancelFunc
	recoveryWG     sync.WaitGroup
	recoveryWake   chan struct{}
	ownershipMu    sync.RWMutex
	liveAttempts   map[string]struct{}
	lateEvents     map[string]int
	lateMu         sync.Mutex
	lateContext    context.Context
	lateCancel     context.CancelFunc
	lateWG         sync.WaitGroup
	lateClosed     bool
}

func newEvidenceOutbox(directory, nodeID string, maxBytes int64, clock Clock, batchSize int, flushInterval, retryInterval time.Duration) (*evidenceOutbox, error) {
	spool, err := openLogSpool(directory, nodeID, maxBytes)
	if err != nil {
		return nil, err
	}
	lateContext, lateCancel := context.WithCancel(context.Background())
	return &evidenceOutbox{
		spool: spool, clock: clock, batchSize: batchSize,
		flushInterval: flushInterval, retryInterval: retryInterval,
		recoveryWake: make(chan struct{}, 1),
		liveAttempts: make(map[string]struct{}),
		lateEvents:   make(map[string]int),
		lateContext:  lateContext,
		lateCancel:   lateCancel,
	}, nil
}

func (outbox *evidenceOutbox) newLogSink(ctx context.Context, client *Client, claim l1.Claim) (*batchingLogSink, error) {
	sink, err := newBatchingLogSink(ctx, client, claim, outbox.spool, outbox.clock, outbox.batchSize, outbox.flushInterval, outbox.retryInterval)
	if err != nil {
		return nil, err
	}
	sink.retainLate = outbox.retainLateEvent
	return sink, nil
}

func (outbox *evidenceOutbox) ensureAttempt(ctx context.Context, claim l1.Claim) error {
	return outbox.spool.ensureAttempt(ctx, claim)
}

func (outbox *evidenceOutbox) storeCompletion(ctx context.Context, attemptID string, result l1.ProcessResult, finishedAt time.Time, evidence ...l1.RuntimeQuiescenceEvidence) error {
	if err := outbox.spool.storeCompletion(ctx, attemptID, result, finishedAt, evidence...); err != nil {
		return err
	}
	if outbox.completionStored != nil {
		outbox.completionStored()
	}
	return nil
}

func (outbox *evidenceOutbox) completionDelivered(ctx context.Context, attemptID string, revision ...uint64) error {
	var observed uint64
	if len(revision) > 0 {
		observed = revision[0]
	}
	return outbox.spool.completionDelivered(ctx, attemptID, observed)
}

func (outbox *evidenceOutbox) suppressCompletion(ctx context.Context, attemptID string, revision uint64) error {
	for {
		err := outbox.spool.recordCompletionDisposition(ctx, attemptID, "suppressed", "service_intent_stop", revision)
		if err == nil {
			return nil
		}
		timer := outbox.clock.NewTimer(outbox.retryInterval)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return errors.Join(err, ctx.Err())
		case <-timer.C():
		}
	}
}

func (outbox *evidenceOutbox) withholdCompletion(ctx context.Context, attemptID, reason string, revision uint64) error {
	return outbox.spool.recordCompletionDisposition(ctx, attemptID, "withheld", reason, revision)
}

func (outbox *evidenceOutbox) beginRemoval(ctx context.Context, removal localRemoval) error {
	return outbox.spool.beginRemoval(ctx, removal, outbox.clock.Now())
}

func (outbox *evidenceOutbox) storeRuntimeResourceManifest(ctx context.Context, manifest workloadrunner.RuntimeResourceManifest) error {
	return outbox.spool.storeRuntimeResourceManifest(ctx, manifest, outbox.clock.Now())
}

func (outbox *evidenceOutbox) runtimeRemoval(ctx context.Context, jobID string) (runtimeRemovalRecord, bool, error) {
	return outbox.spool.runtimeRemoval(ctx, jobID)
}

func (outbox *evidenceOutbox) storeReconstructedRuntimeRemoval(ctx context.Context, removal localRemoval, attempts []workloadrunner.RuntimeResourceManifest) error {
	return outbox.spool.storeReconstructedRuntimeRemoval(ctx, removal, attempts, outbox.clock.Now())
}

func (outbox *evidenceOutbox) pendingRuntimeRemovals(ctx context.Context) ([]runtimeRemovalRecord, error) {
	return outbox.spool.pendingRuntimeRemovals(ctx)
}

func (outbox *evidenceOutbox) recordRuntimeQuiesced(ctx context.Context, removal localRemoval, receipt workloadrunner.ReapReceipt) error {
	return outbox.spool.recordRuntimeQuiesced(ctx, removal, receipt, outbox.clock.Now())
}

func (outbox *evidenceOutbox) recordRuntimeRemovalFailure(ctx context.Context, removal localRemoval, refusalCode, refusalDetail, bootSessionID string) error {
	return outbox.spool.recordRuntimeRemovalFailure(ctx, removal, refusalCode, refusalDetail, bootSessionID, outbox.clock.Now())
}

func (outbox *evidenceOutbox) recordRuntimeRemovalUntypedFailure(ctx context.Context, removal localRemoval, bootSessionID string) error {
	return outbox.spool.recordRuntimeRemovalUntypedFailure(ctx, removal, bootSessionID, outbox.clock.Now())
}

func (outbox *evidenceOutbox) freezeRuntimeRemovalStallDeclaration(ctx context.Context, removal localRemoval,
	declaration []byte, key string) ([]byte, string, error) {
	return outbox.spool.freezeRuntimeRemovalStallDeclaration(ctx, removal, declaration, key)
}

func (outbox *evidenceOutbox) removalStartedAt(ctx context.Context, jobID string) (time.Time, error) {
	return outbox.spool.removalStartedAt(ctx, jobID)
}

func (outbox *evidenceOutbox) recordRuntimeRemovalStallDeclared(ctx context.Context, removal localRemoval) error {
	return outbox.spool.recordRuntimeRemovalStallDeclared(ctx, removal, outbox.clock.Now())
}

func (outbox *evidenceOutbox) recordRuntimeAttested(ctx context.Context, removal localRemoval, attestation workloadrunner.RuntimeRemovalAttestation) error {
	return outbox.spool.recordRuntimeAttested(ctx, removal, attestation, outbox.clock.Now())
}

func (outbox *evidenceOutbox) purgeJob(ctx context.Context, jobID string) error {
	return outbox.spool.purgeJob(ctx, jobID)
}

func (outbox *evidenceOutbox) removalIntent(ctx context.Context, jobID string) (localRemoval, bool, error) {
	return outbox.spool.removalIntent(ctx, jobID)
}

func (outbox *evidenceOutbox) completeRemoval(ctx context.Context, removal localRemoval) error {
	return outbox.spool.completeRemoval(ctx, removal)
}

func (outbox *evidenceOutbox) backupCopyRemovalAcknowledged(ctx context.Context, directive l1.ComputerBackupPruneDirective) (bool, error) {
	return outbox.spool.backupCopyRemovalAcknowledged(ctx, directive)
}

func (outbox *evidenceOutbox) recordBackupCopyRemovalAcknowledged(ctx context.Context, directive l1.ComputerBackupPruneDirective) error {
	return outbox.spool.recordBackupCopyRemovalAcknowledged(ctx, directive, outbox.clock.Now())
}

// startRecovery starts the durable replay scan before registration without
// waiting for any network call. Pending evidence is never a startup gate.
func (outbox *evidenceOutbox) startRecovery(ctx context.Context, client *Client, report func(error)) {
	outbox.recoveryMu.Lock()
	if outbox.recoveryCancel != nil {
		outbox.recoveryMu.Unlock()
		return
	}
	recoveryContext, cancel := context.WithCancel(ctx)
	outbox.recoveryCancel = cancel
	outbox.recoveryWG.Add(1)
	outbox.recoveryMu.Unlock()

	go func() {
		defer outbox.recoveryWG.Done()
		type recoveryResult struct {
			attemptID string
			err       error
		}
		active := make(map[string]struct{})
		retryAt := make(map[string]time.Time)
		// failures counts consecutive transient failures per attempt and
		// drives its backoff; refusals counts consecutive refusals and drives
		// the slow re-check, and refusalCode is the refusal already logged.
		// None of it survives the process: a restarted agent starts empty.
		failures := make(map[string]int)
		refusals := make(map[string]int)
		refusalCode := make(map[string]contract.ErrorCode)
		var scanRetryAt time.Time
		var scanFailures int
		var retryTimer Timer
		var retryWake <-chan time.Time
		finished := make(chan recoveryResult)
		armRetry := func() {
			var earliest time.Time
			if !scanRetryAt.IsZero() {
				earliest = scanRetryAt
			}
			for _, deadline := range retryAt {
				if earliest.IsZero() || deadline.Before(earliest) {
					earliest = deadline
				}
			}
			if retryTimer != nil {
				stopTimer(retryTimer)
				retryTimer = nil
				retryWake = nil
			}
			if earliest.IsZero() {
				return
			}
			delay := earliest.Sub(outbox.clock.Now())
			if delay < 0 {
				delay = 0
			}
			retryTimer = outbox.clock.NewTimer(delay)
			retryWake = retryTimer.C()
		}
		var launchPending func()
		launchPending = func() {
			now := outbox.clock.Now()
			if !scanRetryAt.IsZero() && now.Before(scanRetryAt) {
				armRetry()
				return
			}
			attempts, err := outbox.spool.pendingAttempts(recoveryContext)
			if err != nil {
				if recoveryContext.Err() == nil && report != nil {
					report(err)
				}
				scanFailures++
				scanRetryAt = now.Add(evidenceRecoveryBackoff(outbox.retryInterval, scanFailures))
				armRetry()
				return
			}
			scanRetryAt = time.Time{}
			scanFailures = 0
			pending := make(map[string]struct{}, len(attempts))
			for _, attempt := range attempts {
				pending[attempt.attemptID] = struct{}{}
			}
			for attemptID := range retryAt {
				if _, present := pending[attemptID]; !present {
					delete(retryAt, attemptID)
					delete(failures, attemptID)
				}
			}
			for attemptID := range refusals {
				if _, present := pending[attemptID]; !present {
					delete(refusals, attemptID)
					delete(refusalCode, attemptID)
				}
			}
			for _, attempt := range attempts {
				if _, running := active[attempt.attemptID]; running {
					continue
				}
				if deadline, waiting := retryAt[attempt.attemptID]; waiting && now.Before(deadline) {
					continue
				}
				if len(active) >= maxEvidenceRecoveryWorkers {
					break
				}
				if outbox.attemptIsLive(attempt.attemptID) {
					continue
				}
				delete(retryAt, attempt.attemptID)
				active[attempt.attemptID] = struct{}{}
				outbox.recoveryWG.Add(1)
				go func(attempt logSpoolAttempt) {
					defer outbox.recoveryWG.Done()
					result := recoveryResult{attemptID: attempt.attemptID, err: outbox.recoverAttempt(recoveryContext, client, attempt)}
					if outbox.recoveryAttemptFinished != nil {
						outbox.recoveryAttemptFinished(attempt.attemptID)
					}
					select {
					case finished <- result:
					case <-recoveryContext.Done():
					}
				}(attempt)
			}
			armRetry()
		}

		launchPending()
		for {
			select {
			case <-recoveryContext.Done():
				if retryTimer != nil {
					stopTimer(retryTimer)
				}
				return
			case <-outbox.recoveryWake:
				launchPending()
			case <-retryWake:
				retryTimer = nil
				retryWake = nil
				launchPending()
			case result := <-finished:
				delete(active, result.attemptID)
				reportErr := result.err
				var refusal *evidenceRecoveryRefusal
				switch {
				case errors.As(result.err, &refusal):
					delete(failures, result.attemptID)
					refusals[result.attemptID]++
					delay := evidenceRecoveryRefusalDelay(refusals[result.attemptID])
					retryAt[result.attemptID] = outbox.clock.Now().Add(delay)
					if refusalCode[result.attemptID] == refusal.code {
						// The same answer again: already logged.
						reportErr = nil
					} else {
						refusalCode[result.attemptID] = refusal.code
						reportErr = fmt.Errorf("%w; durable evidence left on disk, re-checking from %s backing off to hourly, further identical refusals not logged",
							result.err, delay)
					}
				default:
					if code, wasRefused := refusalCode[result.attemptID]; wasRefused {
						// The refusal is over, one way or the other.
						delete(refusals, result.attemptID)
						delete(refusalCode, result.attemptID)
						if result.err == nil {
							reportErr = fmt.Errorf("attempt %s: recovery of durable evidence L1 had refused (%s) now succeeded", result.attemptID, code)
						}
					}
					if result.err == nil {
						delete(retryAt, result.attemptID)
						delete(failures, result.attemptID)
						break
					}
					failures[result.attemptID]++
					retryAt[result.attemptID] = outbox.clock.Now().Add(
						evidenceRecoveryBackoff(outbox.retryInterval, failures[result.attemptID]))
				}
				if reportErr != nil && recoveryContext.Err() == nil && report != nil {
					if result.err == nil {
						report(reportErr)
					} else {
						report(fmt.Errorf("attempt %s: %w", result.attemptID, reportErr))
					}
				}
				// Fill the released worker slot immediately. A failed attempt is
				// skipped until its own injected-clock backoff expires, and a
				// refused one until its re-check is due.
				launchPending()
			}
		}
	}()
}

func (outbox *evidenceOutbox) ownAttempt(attemptID string) {
	if outbox == nil {
		return
	}
	outbox.ownershipMu.Lock()
	outbox.liveAttempts[attemptID] = struct{}{}
	outbox.ownershipMu.Unlock()
}

func (outbox *evidenceOutbox) releaseAttempt(attemptID string, reconcile bool) {
	if outbox == nil {
		return
	}
	outbox.ownershipMu.Lock()
	delete(outbox.liveAttempts, attemptID)
	outbox.ownershipMu.Unlock()
	if reconcile {
		outbox.scheduleRecovery()
	}
}

func (outbox *evidenceOutbox) attemptIsLive(attemptID string) bool {
	outbox.ownershipMu.RLock()
	_, live := outbox.liveAttempts[attemptID]
	live = live || outbox.lateEvents[attemptID] > 0
	outbox.ownershipMu.RUnlock()
	return live
}

// retainLateEvent transfers a redacted event whose first durable append was
// interrupted by the bounded finalization deadline to the process-lifetime
// outbox. The append is idempotent, and recovery treats the attempt as live
// until this retry finishes so it cannot deliver completion ahead of the log.
func (outbox *evidenceOutbox) retainLateEvent(event contract.LogEvent) {
	if outbox == nil || outbox.spool == nil {
		return
	}
	retained := event
	retained.Bytes = append([]byte(nil), event.Bytes...)
	if event.Gap != nil {
		gap := *event.Gap
		retained.Gap = &gap
	}
	outbox.lateMu.Lock()
	if outbox.lateClosed {
		outbox.lateMu.Unlock()
		return
	}
	outbox.lateWG.Add(1)
	outbox.ownershipMu.Lock()
	outbox.lateEvents[event.AttemptID]++
	outbox.ownershipMu.Unlock()
	lateContext := outbox.lateContext
	outbox.lateMu.Unlock()

	go func() {
		defer outbox.lateWG.Done()
		_ = outbox.spool.append(lateContext, retained)
		outbox.ownershipMu.Lock()
		outbox.lateEvents[event.AttemptID]--
		if outbox.lateEvents[event.AttemptID] == 0 {
			delete(outbox.lateEvents, event.AttemptID)
		}
		outbox.ownershipMu.Unlock()
		outbox.scheduleRecovery()
	}()
}

// scheduleRecovery wakes the process-lifetime outbox reconciler after the
// attempt lifecycle has decided that a durable completion is eligible for L1
// delivery. Persistence itself cannot wake recovery: OCI intent-stop may still
// suppress an outcome that raced the local stop boundary.
func (outbox *evidenceOutbox) scheduleRecovery() {
	if outbox == nil {
		return
	}
	select {
	case outbox.recoveryWake <- struct{}{}:
	default:
	}
}

func (outbox *evidenceOutbox) recoverAttempt(ctx context.Context, client *Client, attempt logSpoolAttempt) error {
	if err := outbox.recoverLogs(ctx, client, attempt); err != nil {
		return err
	}
	if err := outbox.recoverCompletion(ctx, client, attempt); err != nil {
		return err
	}
	return nil
}

func (outbox *evidenceOutbox) recoverLogs(ctx context.Context, client *Client, attempt logSpoolAttempt) error {
	lostBatches := 0
	for {
		if lostBatches >= maxEvidenceLogReplayBatchesPerPass {
			// L1 has positively reported AttemptLost. A bounded pass may yield to
			// completion because L1 treats it as late evidence and continues to
			// admit this attempt's logs on later passes.
			return nil
		}
		batch, err := outbox.spool.pendingBatch(ctx, attempt.attemptID, outbox.batchSize)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		events := make([]contract.LogEvent, 0, len(batch))
		for _, stored := range batch {
			events = append(events, stored.event)
		}
		response, err := client.AppendLogs(ctx, attempt.jobID, attempt.attemptID, l1.AppendLogsRequest{
			FencingToken: attempt.fencingToken,
			Events:       events,
		})
		if err == nil {
			if err := validateLogAcknowledgement(events, response.Acknowledged); err != nil {
				return err
			}
			if err := outbox.spool.acknowledge(ctx, attempt.attemptID, response.Acknowledged); err != nil {
				return err
			}
			if response.AttemptState == contract.AttemptLost {
				lostBatches++
			}
			continue
		}

		code := protocolErrorCode(err)
		if permanentEvidenceRejection(code) {
			if eventsContainGap(events) {
				return outbox.sealIncomplete(ctx, attempt.attemptID, "replacement gap was rejected", code)
			}
			if err := outbox.spool.replaceBatchWithReplayGaps(ctx, attempt.attemptID, batch); err != nil {
				return outbox.sealIncomplete(ctx, attempt.attemptID, "rejected replay could not be replaced with a gap", code)
			}
			continue
		}

		classification := classifyAgentProtocolError(err)
		switch classification.destination {
		case errorDestinationTransient:
			return err
		case errorDestinationAttemptAuthority:
			return outbox.sealIncomplete(ctx, attempt.attemptID, "attempt authority no longer accepts evidence", code)
		case errorDestinationNodeSession:
			return recoveryRefusal(err, code, classification)
		default:
			return err
		}
	}
}

func (outbox *evidenceOutbox) recoverCompletion(ctx context.Context, client *Client, attempt logSpoolAttempt) error {
	result, evidence, _, present, err := outbox.spool.completionWithEvidence(ctx, attempt.attemptID)
	if err != nil || !present {
		return err
	}
	var observation OCIIntentObservation
	var releaseIntent func()
	if requiresOCIIntentFence(attempt.kind, attempt.class) {
		disposition, _, dispositionRevision, dispositionErr := outbox.spool.completionDisposition(ctx, attempt.attemptID)
		if dispositionErr != nil {
			return dispositionErr
		}
		if disposition == "suppressed" {
			return nil
		}
		if outbox.ociIntentGate != nil {
			var intentErr error
			observation, releaseIntent, intentErr = outbox.ociIntentGate.beginCompletion(ctx)
			if intentErr != nil {
				if disposition == "withheld" && dispositionRevision == 0 {
					return intentErr
				}
				if receiptErr := outbox.withholdCompletion(context.WithoutCancel(ctx), attempt.attemptID, "intent_authority_unavailable", 0); receiptErr != nil {
					return errors.Join(intentErr, receiptErr)
				}
				return intentErr
			}
			if !outbox.ociIntentGate.allows(observation) {
				suppressionContext, cancelSuppression := outbox.ociIntentGate.beginSuppression(ctx)
				err := outbox.suppressCompletion(suppressionContext, attempt.attemptID, observation.Revision)
				cancelSuppression()
				err = outbox.ociIntentGate.finishSuppression(attempt.attemptID, observation, err)
				releaseIntent()
				return err
			}
		}
	}
	request := l1.CompletionRequest{
		FencingToken: attempt.fencingToken, IdempotencyKey: "completion:" + attempt.attemptID,
		Result: result, RuntimeQuiescenceEvidence: evidence,
	}
	_, err = client.Complete(ctx, attempt.jobID, attempt.attemptID, request)
	if releaseIntent != nil {
		releaseIntent()
	}
	if err == nil || protocolErrorCode(err) == contract.ErrorLeaseExpired {
		return outbox.spool.completionDelivered(ctx, attempt.attemptID, observation.Revision)
	}
	code := protocolErrorCode(err)
	if permanentEvidenceRejection(code) {
		return outbox.sealIncomplete(ctx, attempt.attemptID, "completion was permanently rejected", code)
	}
	classification := classifyAgentProtocolError(err)
	switch classification.destination {
	case errorDestinationTransient:
		return err
	case errorDestinationAttemptAuthority:
		return outbox.sealIncomplete(ctx, attempt.attemptID, "attempt authority no longer accepts completion evidence", code)
	case errorDestinationNodeSession:
		// node_session_replaced: the attempt belongs to an older registration
		// of this node and L1 has not accepted this completion. (One it had
		// accepted -- the #549 storm -- is answered as an already-recorded
		// replay and was delivered above, #553.) It clears only when the
		// attempt's lease expires at L1, after which the replay is answered
		// lease_expired and lands as late evidence -- what the slow re-checks
		// are for.
		return recoveryRefusal(err, code, classification)
	default:
		return err
	}
}

func (outbox *evidenceOutbox) sealIncomplete(ctx context.Context, attemptID, reason string, code contract.ErrorCode) error {
	if err := outbox.sealAttemptEvidence(ctx, attemptID, reason, code); err != nil {
		return err
	}
	return fmt.Errorf("durable evidence sealed incomplete: %s (%s)", reason, code)
}

func (outbox *evidenceOutbox) sealAttemptEvidence(ctx context.Context, attemptID, reason string, code contract.ErrorCode) error {
	return outbox.spool.sealIncomplete(ctx, attemptID, reason, code, outbox.clock.Now())
}

func protocolErrorCode(err error) contract.ErrorCode {
	var protocolErr *ProtocolError
	if errors.As(err, &protocolErr) {
		return protocolErr.APIError.Code
	}
	return ""
}

func permanentEvidenceRejection(code contract.ErrorCode) bool {
	switch code {
	case contract.ErrorInvalidRequest, contract.ErrorConflict, contract.ErrorIdempotencyConflict,
		contract.ErrorNotFound, contract.ErrorUnsupportedClass, contract.ErrorUnsupportedKind,
		contract.ErrorUnsupportedRuntimeHandler, contract.ErrorNotImplemented:
		return true
	default:
		return false
	}
}

func eventsContainGap(events []contract.LogEvent) bool {
	for _, event := range events {
		if event.Gap != nil {
			return true
		}
	}
	return false
}

func (outbox *evidenceOutbox) Close() error {
	if outbox == nil || outbox.spool == nil {
		return nil
	}
	outbox.lateMu.Lock()
	outbox.lateClosed = true
	outbox.lateCancel()
	outbox.lateMu.Unlock()
	outbox.lateWG.Wait()
	outbox.recoveryMu.Lock()
	if outbox.recoveryCancel != nil {
		outbox.recoveryCancel()
	}
	outbox.recoveryMu.Unlock()
	outbox.recoveryWG.Wait()
	return outbox.spool.Close()
}
