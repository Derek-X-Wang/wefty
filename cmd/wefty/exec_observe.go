package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
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

func observeExecRun(ctx context.Context, clients *apiClients, runID string, stdout, stderr io.Writer) (contract.RunRecord, error) {
	cursor := ""
	var truncation truncationAnnouncer
	started := time.Now()
	announcedWaiting := false
	var record contract.RunRecord
	for {
		page, err := readExec(ctx, clients, func(readCtx context.Context) (l1.LogPage, error) {
			return clients.getRunLogs(readCtx, runID, cursor, l1.DefaultLogPageLimit)
		})
		if err != nil {
			return record, err
		}
		if err = truncation.announce(stderr, page.Truncation); err != nil {
			return record, err
		}
		if err = writeLogEvents(stdout, stderr, page.Events); err != nil {
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
		if runIsTerminal(record.Status) && len(page.Events) == 0 {
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
	// Once terminal state is known, a late signal cannot cancel that verdict.
	// Result delivery still has one bounded recovery window.
	ctx = context.WithoutCancel(ctx)
	if record.Status == contract.RunFailed {
		if record.CancelStatus != "" {
			if _, err := fmt.Fprintln(stderr, "cancelled"); err != nil {
				return err
			}
			return &runOutcomeError{runID: record.RunID, status: record.Status, reason: "cancelled"}
		}
		reason := runFailureReason(ctx, clients, record)
		if strings.HasPrefix(reason, "maximum runtime reached") || strings.HasPrefix(reason, "idle limit reached") {
			return reportTerminalRun(stderr, record, reason, false, false)
		}
		resultCtx, stop := context.WithTimeout(ctx, execReadRecoveryBudget)
		defer stop()
		for {
			result, err := readExec(resultCtx, clients, func(readCtx context.Context) (l3.RunResult, error) {
				return clients.getRunResult(readCtx, record.RunID)
			})
			if err == nil && len(result.Document) > 0 {
				var document struct {
					InfrastructureError string `json:"infrastructure_error"`
					ExitCode            *int   `json:"exit_code"`
				}
				if json.Unmarshal(result.Document, &document) == nil {
					if document.InfrastructureError != "" {
						fmt.Fprintf(stderr, "wefty: infrastructure failure: %s\n", document.InfrastructureError)
						return &unavailableError{cause: fmt.Errorf("run %s infrastructure failure: %s", record.RunID, document.InfrastructureError)}
					}
					if document.ExitCode != nil {
						return reportTerminalRun(stderr, record, reason, false, false)
					}
				}
				break
			}
			if err == nil && result.SkipReason != "" {
				break
			}
			var response *apiResponseError
			if err != nil && !(errors.As(err, &response) && response.APIError.Code == contract.ErrorNotFound) {
				return &unavailableError{cause: fmt.Errorf("run %s ended; could not read its result: %w", record.RunID, err)}
			}
			if clients.wait(resultCtx, 100*time.Millisecond) != nil {
				break
			}
		}
		// Without a result, an exit 1 may have been wrapper startup rather
		// than the command. Do not turn unavailable evidence into exit 10.
		return &unavailableError{cause: fmt.Errorf("run %s ended; command result unavailable; inspect with `wefty inspect %s --execution`", record.RunID, record.RunID)}
	}
	return reportTerminalRun(stderr, record, runFailureReason(ctx, clients, record), false, false)
}
