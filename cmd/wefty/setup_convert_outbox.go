package main

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
)

const conversionOutboxTimeout = 30 * time.Second

func waitConversionOutbox(ctx context.Context, op upgradeHost, paths setupPaths, nodeID string, wait bool) error {
	deadline := op.now().Add(conversionOutboxTimeout)
	bounded, cancel := context.WithTimeout(ctx, conversionOutboxTimeout)
	defer cancel()
	for {
		probe, stop := context.WithTimeout(bounded, time.Second)
		n, err := agent.PendingEvidence(probe, filepath.Join(paths.data, "node-agent", "log-spool"), nodeID)
		stop()
		if err != nil {
			return fmt.Errorf("evidence outbox is unverified; keep the plain stack and retry after repair: %w", err)
		}
		if n == 0 {
			return nil
		}
		if !wait || !op.now().Before(deadline) {
			return &apiResponseError{APIError: contract.APIError{Code: contract.ErrorConflict, Message: fmt.Sprintf("conversion refused: %d pending evidence outbox entries remain; let the plain agent deliver them, then retry later", n)}}
		}
		if err := op.wait(bounded, min(time.Second, deadline.Sub(op.now()))); err != nil {
			return err
		}
	}
}
