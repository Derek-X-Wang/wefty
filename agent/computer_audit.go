package agent

import (
	"context"
	"fmt"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

const computerAuditAttempts = 3

type computerAuditAppend func(context.Context, l1.ComputerTakeoverAuditEvent) (l1.ComputerTakeoverAuditReceipt, error)

func canonicalComputerAuditEvent(event l1.ComputerTakeoverAuditEvent) l1.ComputerTakeoverAuditEvent {
	event.OccurredAt = event.OccurredAt.UTC().Round(0)
	return event
}

func appendAssertedComputerAudit(ctx context.Context, appendEvent computerAuditAppend, event l1.ComputerTakeoverAuditEvent) error {
	event = canonicalComputerAuditEvent(event)
	var lastErr error
	for attempt := 0; attempt < computerAuditAttempts; attempt++ {
		receipt, err := appendEvent(ctx, event)
		if err == nil && computerAuditReceiptAsserts(receipt, event) {
			return nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("L1 receipt did not assert the requested event")
		}
		if ctx.Err() != nil {
			break
		}
	}
	return lastErr
}

func computerAuditReceiptAsserts(receipt l1.ComputerTakeoverAuditReceipt, requested l1.ComputerTakeoverAuditEvent) bool {
	asserted := receipt.Event
	if asserted.AuthorityGeneration < 1 || !asserted.OccurredAt.Equal(requested.OccurredAt) {
		return false
	}
	asserted.AuthorityGeneration = 0
	asserted.OccurredAt = time.Time{}
	requested.AuthorityGeneration = 0
	requested.OccurredAt = time.Time{}
	return asserted == requested
}
