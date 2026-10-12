package agent

import (
	"context"
	"errors"
	"fmt"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

type completionDeliveryMode uint8

const (
	completionDeliveryLive completionDeliveryMode = iota
	completionDeliveryReplay
)

type completionVerdictKind uint8

const (
	completionDelivered completionVerdictKind = iota
	completionSuppressed
	completionWithheld
	completionLocalFailure
	completionAbandoned
	completionRetry
	completionLedgerBackoff
	completionRefused
	completionPermanentRefusal
	completionSealed
)

type completionVerdict struct {
	kind        completionVerdictKind
	destination errorDestination
	err         error
}

// completionPersistenceError keeps local storage failure separate from the L1
// response, including when replay's error returns through inline shutdown.
type completionPersistenceError struct{ err error }

func (err *completionPersistenceError) Error() string { return err.err.Error() }
func (err *completionPersistenceError) Unwrap() error { return err.err }

// deliverCompletion holds the existing intent fence for one observation and
// one request (or bounded suppression). It owns the durable effects; callers
// own retries and scheduling. Live body refusals retain evidence for replay.
func (outbox *evidenceOutbox) deliverCompletion(ctx context.Context, client *Client, attempt logSpoolAttempt, request l1.CompletionRequest, mode completionDeliveryMode) completionVerdict {
	localFailure := func(err error) completionVerdict {
		return completionVerdict{kind: completionLocalFailure, err: &completionPersistenceError{err: err}}
	}
	persistenceContext := ctx
	if mode == completionDeliveryLive {
		persistenceContext = context.WithoutCancel(ctx)
	}
	var observation OCIIntentObservation
	var releaseIntent func()
	if requiresOCIIntentFence(attempt.kind, attempt.class) {
		var disposition string
		var dispositionRevision uint64
		if mode == completionDeliveryReplay && outbox.spool != nil {
			var err error
			disposition, _, dispositionRevision, err = outbox.spool.completionDisposition(persistenceContext, attempt.attemptID)
			if err != nil {
				return localFailure(err)
			}
			if disposition == "suppressed" {
				return completionVerdict{kind: completionSuppressed}
			}
		}
		if gate := outbox.ociIntentGate; gate != nil {
			var release func()
			var err error
			observation, release, err = gate.beginCompletion(persistenceContext)
			if err != nil {
				if outbox.spool != nil && !(disposition == "withheld" && dispositionRevision == 0) {
					if receiptErr := outbox.withholdCompletion(context.WithoutCancel(ctx), attempt.attemptID, "intent_authority_unavailable", 0); receiptErr != nil {
						return localFailure(errors.Join(err, receiptErr))
					}
				}
				return completionVerdict{kind: completionWithheld, err: err}
			}
			if !gate.allows(observation) {
				defer release()
				if outbox.spool != nil {
					suppressionContext, cancel := gate.beginSuppression(ctx)
					err := outbox.suppressCompletion(suppressionContext, attempt.attemptID, observation.Revision)
					cancel()
					if err := gate.finishSuppression(attempt.attemptID, observation, err); err != nil {
						return localFailure(err)
					}
				}
				return completionVerdict{kind: completionSuppressed}
			}
			releaseIntent = release
		}
	}
	_, err := client.Complete(ctx, attempt.jobID, attempt.attemptID, request)
	if releaseIntent != nil {
		releaseIntent()
	}
	// A cancellation cause may itself be a renewal ProtocolError. Only an
	// independent /complete response is an L1 verdict.
	var response *ProtocolError
	cause := context.Cause(ctx)
	if err != nil && cause != nil && !(errors.As(err, &response) && !errors.Is(err, cause)) {
		return completionVerdict{kind: completionAbandoned, err: &completionDeliveryAbandoned{cause: cause, err: err}}
	}
	code := protocolErrorCode(err)
	if err == nil || code == contract.ErrorLeaseExpired {
		if outbox.spool != nil {
			if receiptErr := outbox.completionDelivered(persistenceContext, attempt.attemptID, observation.Revision); receiptErr != nil {
				return localFailure(receiptErr)
			}
		}
		verdict := completionVerdict{kind: completionDelivered, err: err}
		if err != nil {
			verdict.destination = errorDestinationAttemptAuthority
		}
		return verdict
	}
	classification := classifyAgentProtocolError(err)
	verdict := completionVerdict{kind: completionRefused, destination: classification.destination, err: err}
	reason := "attempt authority no longer accepts completion evidence"
	if permanentEvidenceRejection(code) {
		verdict.kind = completionPermanentRefusal
		verdict.destination = errorDestinationAttemptAuthority
		if mode == completionDeliveryLive {
			return verdict
		}
		reason = "completion was permanently rejected"
	} else if classification.destination != errorDestinationAttemptAuthority {
		if classification.destination == errorDestinationTransient {
			verdict.kind = completionRetry
			if code == contract.ErrorRunLedgerUnavailable {
				verdict.kind = completionLedgerBackoff
			}
		}
		return verdict
	}
	if outbox.spool != nil {
		if sealErr := outbox.sealAttemptEvidence(persistenceContext, attempt.attemptID, reason, code); sealErr != nil {
			return localFailure(sealErr)
		}
	}
	verdict.kind = completionSealed
	if mode == completionDeliveryReplay {
		verdict.err = fmt.Errorf("durable evidence sealed incomplete: %s (%s)", reason, code)
	}
	return verdict
}
