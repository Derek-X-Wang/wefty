package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Exec alone outlasts transient transport failures during a service restart.
// Each read has a bounded recovery window; successful reads reset the window.
// Keep the log cursor outside retries so already delivered output is not replayed.
func readExec[T any](caller context.Context, clients *apiClients, read func(context.Context) (T, error)) (T, error) {
	ctx, stop := context.WithTimeout(withoutClientRetry(caller), execReadRecoveryBudget)
	defer stop()
	for {
		value, err := read(ctx)
		if err == nil {
			return value, nil
		}
		if caller.Err() != nil {
			return value, caller.Err()
		}
		var transport *unavailableError
		var response *apiResponseError
		transient := errors.As(err, &transport) || errors.Is(err, context.DeadlineExceeded) ||
			(errors.As(err, &response) && response.StatusCode >= 500 && response.APIError.Retryable)
		if !transient || ctx.Err() != nil {
			return value, err
		}
		if e := clients.wait(ctx, execPollInterval); e != nil {
			return value, err
		}
	}
}

func observeExecRunOutput(ctx context.Context, clients *apiClients, runID string, jsonOutput bool, stdout, stderr io.Writer) (record contract.RunRecord, err error) {
	var jsonLogs execJSONLogWriter
	if jsonOutput {
		defer func() {
			if flushErr := jsonLogs.flush(stdout); flushErr != nil {
				err = flushErr
			}
		}()
	}
	cursor := ""
	var truncation truncationAnnouncer
	started := time.Now()
	announcedWaiting := false
	for {
		wasTerminal := runIsTerminal(record.Status)
		page, err := readExec(ctx, clients, func(readCtx context.Context) (l1.LogPage, error) {
			return clients.getRunLogs(readCtx, runID, cursor, l1.DefaultLogPageLimit)
		})
		if err != nil {
			return record, err
		}
		if err = truncation.announce(stderr, page.Truncation); err != nil {
			return record, err
		}
		if jsonOutput {
			err = jsonLogs.write(stdout, page.Events)
		} else {
			err = writeLogEvents(stdout, stderr, page.Events)
		}
		if err != nil {
			return record, err
		}
		cursor = page.NextCursor
		nextRecord, err := readExec(ctx, clients, func(readCtx context.Context) (contract.RunRecord, error) {
			return clients.getRun(readCtx, runID)
		})
		if err != nil {
			return record, err
		}
		record = nextRecord
		// Once JSON has a terminal projection, finish draining logs even if
		// a signal arrives. Preserve the plain observer's existing policy.
		if jsonOutput && runIsTerminal(record.Status) {
			ctx = context.WithoutCancel(ctx)
		}
		if runIsTerminal(record.Status) && len(page.Events) == 0 && (!jsonOutput || wasTerminal) {
			return record, nil
		}
		if !announcedWaiting && queuedRunIsWaiting(record.Status) && time.Since(started) >= followWaitingNoticeAfter {
			announcedWaiting = true
			if err = announceWaitingForNode(ctx, clients, stderr, runID, record.Status); err != nil {
				return record, err
			}
		}
		if err = clients.wait(ctx, execPollInterval); err != nil {
			return record, err
		}
	}
}

func lostExecObservation(stderr io.Writer, runID, storage string, cause error) error {
	fmt.Fprintf(stderr, "wefty: lost observation of run %s; the run continues\nwefty: follow with `wefty logs %s --follow`; cancel with `wefty cancel %s`\nwefty: snapshot retained at %s\n", runID, runID, runID, storage)
	return &unavailableError{cause: fmt.Errorf("could not observe run %s: %w", runID, cause)}
}

// Mailbox result delivery may trail terminal projection. Startup failures have
// their own result field, so they cannot be mistaken for the command's exit code.
func reportExecRun(ctx context.Context, clients *apiClients, record contract.RunRecord, stderr io.Writer) error {
	// Plain success keeps its existing policy: terminal projection is enough.
	if record.Status != contract.RunFailed {
		return reportTerminalRun(stderr, record, runFailureReason(context.WithoutCancel(ctx), clients, record), false, false)
	}
	event, result := resolveExecFinished(ctx, clients, record, execCapture{})
	if event.Outcome == "cancelled" {
		if _, err := fmt.Fprintln(stderr, "cancelled"); err != nil {
			return err
		}
		return result
	}
	var unavailable *unavailableError
	if errors.As(result, &unavailable) {
		if event.infrastructureFailure {
			fmt.Fprintf(stderr, "wefty: infrastructure failure: %s\n", event.Reason)
		}
		return result
	}
	return reportTerminalRun(stderr, record, event.Reason, false, false)
}
