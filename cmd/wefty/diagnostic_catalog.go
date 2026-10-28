package main

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
)

const diagnosticCatalogRetryBudget = 2500 * time.Millisecond
const diagnosticCatalogBusyDetail = "generation catalog was busy (an upgrade or agent may be writing it)"

func diagnosticCatalogBusy(err error) bool {
	return errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN)
}

// Keep the store's non-waiting lock contract. Only the CLI retries contention,
// within both a short retry window and the caller's remaining diagnosis budget.
func readDiagnosticCatalog(ctx context.Context, store agentinstall.Store) (agentinstall.Catalog, error) {
	ctx, cancel := context.WithTimeout(ctx, diagnosticCatalogRetryBudget)
	defer cancel()
	for {
		catalog, err := store.ReadDiagnostic()
		if !diagnosticCatalogBusy(err) {
			return catalog, err
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return catalog, fmt.Errorf("%s; rerun in a moment: %w", diagnosticCatalogBusyDetail, err)
		case <-timer.C:
		}
	}
}
