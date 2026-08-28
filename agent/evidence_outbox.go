package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// evidenceOutbox owns durable evidence for the lifetime of the agent process.
// Sessions borrow it; ending or replacing a session must not discard evidence
// that still needs delivery.
type evidenceOutbox struct {
	spool         *logSpool
	clock         Clock
	batchSize     int
	flushInterval time.Duration
	retryInterval time.Duration

	recoveryMu     sync.Mutex
	recoveryCancel context.CancelFunc
	recoveryWG     sync.WaitGroup
}

func newEvidenceOutbox(directory, nodeID string, maxBytes int64, clock Clock, batchSize int, flushInterval, retryInterval time.Duration) (*evidenceOutbox, error) {
	spool, err := openLogSpool(directory, nodeID, maxBytes)
	if err != nil {
		return nil, err
	}
	return &evidenceOutbox{
		spool: spool, clock: clock, batchSize: batchSize,
		flushInterval: flushInterval, retryInterval: retryInterval,
	}, nil
}

func (outbox *evidenceOutbox) newLogSink(ctx context.Context, client *Client, claim l1.Claim) (*batchingLogSink, error) {
	return newBatchingLogSink(ctx, client, claim, outbox.spool, outbox.clock, outbox.batchSize, outbox.flushInterval, outbox.retryInterval)
}

func (outbox *evidenceOutbox) ensureAttempt(ctx context.Context, claim l1.Claim) error {
	return outbox.spool.ensureAttempt(ctx, claim)
}

func (outbox *evidenceOutbox) storeCompletion(ctx context.Context, attemptID string, result l1.ProcessResult, finishedAt time.Time, evidence ...l1.RuntimeQuiescenceEvidence) error {
	return outbox.spool.storeCompletion(ctx, attemptID, result, finishedAt, evidence...)
}

func (outbox *evidenceOutbox) completionDelivered(ctx context.Context, attemptID string) error {
	return outbox.spool.completionDelivered(ctx, attemptID)
}

func (outbox *evidenceOutbox) beginRemoval(ctx context.Context, removal localRemoval) error {
	return outbox.spool.beginRemoval(ctx, removal, outbox.clock.Now())
}

func (outbox *evidenceOutbox) purgeJob(ctx context.Context, jobID string) error {
	return outbox.spool.purgeJob(ctx, jobID)
}

func (outbox *evidenceOutbox) completeRemoval(ctx context.Context, removal localRemoval) error {
	return outbox.spool.completeRemoval(ctx, removal)
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
		if err := outbox.recover(recoveryContext, client); err != nil && recoveryContext.Err() == nil && report != nil {
			report(err)
		}
	}()
}

// recover fans replay out by attempt. A transient or poisoned old attempt can
// therefore never prevent later outbox entries from being considered.
func (outbox *evidenceOutbox) recover(ctx context.Context, client *Client) error {
	attempts, err := outbox.spool.pendingAttempts(ctx)
	if err != nil {
		return err
	}
	results := make(chan error, len(attempts))
	var attemptsWG sync.WaitGroup
	for _, attempt := range attempts {
		attempt := attempt
		attemptsWG.Add(1)
		go func() {
			defer attemptsWG.Done()
			if err := outbox.recoverAttempt(ctx, client, attempt); err != nil {
				results <- fmt.Errorf("attempt %s: %w", attempt.attemptID, err)
			}
		}()
	}
	attemptsWG.Wait()
	close(results)
	var recoveredErrors []error
	for recoveredErr := range results {
		recoveredErrors = append(recoveredErrors, recoveredErr)
	}
	return errors.Join(recoveredErrors...)
}

func (outbox *evidenceOutbox) recoverAttempt(ctx context.Context, client *Client, attempt logSpoolAttempt) error {
	if err := outbox.recoverLogs(ctx, client, attempt); err != nil {
		return err
	}
	return outbox.recoverCompletion(ctx, client, attempt)
}

func (outbox *evidenceOutbox) recoverLogs(ctx context.Context, client *Client, attempt logSpoolAttempt) error {
	for {
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
			if err := outbox.waitRetry(ctx); err != nil {
				return err
			}
		case errorDestinationAttemptAuthority:
			return outbox.sealIncomplete(ctx, attempt.attemptID, "attempt authority no longer accepts evidence", code)
		case errorDestinationNodeSession:
			if classification.nodeSessionReaction == nodeSessionReregister {
				if err := outbox.waitRetry(ctx); err != nil {
					return err
				}
				continue
			}
			return err
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
	request := l1.CompletionRequest{
		FencingToken: attempt.fencingToken, IdempotencyKey: "completion:" + attempt.attemptID,
		Result: result, RuntimeQuiescenceEvidence: evidence,
	}
	for {
		_, err := client.Complete(ctx, attempt.jobID, attempt.attemptID, request)
		if err == nil || protocolErrorCode(err) == contract.ErrorLeaseExpired {
			return outbox.spool.completionDelivered(ctx, attempt.attemptID)
		}
		code := protocolErrorCode(err)
		if permanentEvidenceRejection(code) {
			return outbox.sealIncomplete(ctx, attempt.attemptID, "completion was permanently rejected", code)
		}
		classification := classifyAgentProtocolError(err)
		switch classification.destination {
		case errorDestinationTransient:
			if err := outbox.waitRetry(ctx); err != nil {
				return err
			}
		case errorDestinationAttemptAuthority:
			return outbox.sealIncomplete(ctx, attempt.attemptID, "attempt authority no longer accepts completion evidence", code)
		case errorDestinationNodeSession:
			if classification.nodeSessionReaction == nodeSessionReregister {
				if err := outbox.waitRetry(ctx); err != nil {
					return err
				}
				continue
			}
			return err
		default:
			return err
		}
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

func (outbox *evidenceOutbox) waitRetry(ctx context.Context) error {
	timer := outbox.clock.NewTimer(outbox.retryInterval)
	select {
	case <-ctx.Done():
		stopTimer(timer)
		return ctx.Err()
	case <-timer.C():
		return nil
	}
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
	outbox.recoveryMu.Lock()
	if outbox.recoveryCancel != nil {
		outbox.recoveryCancel()
	}
	outbox.recoveryMu.Unlock()
	outbox.recoveryWG.Wait()
	return outbox.spool.Close()
}
