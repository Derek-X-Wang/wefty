package agent

import (
	"context"
	"fmt"
	"time"

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

func (outbox *evidenceOutbox) recover(ctx context.Context, client *Client) error {
	attempts, err := outbox.spool.pendingAttempts(ctx)
	if err != nil {
		return err
	}
	for _, attempt := range attempts {
		claim := l1.Claim{
			Job:   l1.Job{JobID: attempt.jobID},
			Lease: l1.AttemptLease{AttemptID: attempt.attemptID, FencingToken: attempt.fencingToken},
		}
		uploader, err := outbox.newLogSink(ctx, client, claim)
		if err != nil {
			return err
		}
		if err := uploader.Close(); err != nil {
			return fmt.Errorf("attempt %s: %w", attempt.attemptID, err)
		}
	}
	return nil
}

func (outbox *evidenceOutbox) Close() error {
	if outbox == nil || outbox.spool == nil {
		return nil
	}
	return outbox.spool.Close()
}
