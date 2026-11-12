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

// Attached exec outlasts transient read failures. A successful read ends the
// retries. Keep the log cursor outside retries so already delivered output is
// not replayed.
//
// An unreachable service gets the restart window, counted from the first of
// its failures in a row. With stillFollowing, the read follows a live run, and
// a service that is there but busy (it answers a retryable server error, or
// nothing before the attempt's deadline) gets the longer overload window and a
// growing gap, so a loaded host is not asked again every second (#1629).
// stillFollowing runs once per read, when the next try starts past the restart
// window, so a silent follow does not look hung. Without it every failure shares the
// restart window: a known verdict is not held back for a busy service.
func readExec[T any](caller context.Context, clients *apiClients, stillFollowing func(), read func(context.Context) (T, error)) (T, error) {
	budget := execReadRecoveryBudget
	if stillFollowing != nil {
		budget = execReadOverloadBudget
	}
	ctx, stop := context.WithTimeout(withoutClientRetry(caller), budget)
	defer stop()
	started := time.Now()
	var unreachableSince time.Time
	busyGap := execPollInterval
	for {
		attempt, stopAttempt := context.WithTimeout(ctx, execReadRecoveryBudget)
		value, err := read(attempt)
		timedOut := attempt.Err() != nil && errors.Is(err, context.DeadlineExceeded)
		stopAttempt()
		if err == nil {
			return value, nil
		}
		if caller.Err() != nil {
			return value, caller.Err()
		}
		var transport *unavailableError
		var response *apiResponseError
		gap := execPollInterval
		if timedOut || (errors.As(err, &response) && response.StatusCode >= 500 && response.APIError.Retryable) {
			unreachableSince = time.Time{}
			gap, busyGap = busyGap, min(2*busyGap, execReadOverloadGapLimit*execPollInterval)
			if stillFollowing != nil && time.Since(started)+gap >= execReadRecoveryBudget {
				stillFollowing()
				stillFollowing = nil
			}
		} else if !errors.As(err, &transport) && !errors.Is(err, context.DeadlineExceeded) {
			return value, err
		} else if unreachableSince.IsZero() {
			unreachableSince = time.Now()
		} else if time.Since(unreachableSince) >= execReadRecoveryBudget {
			return value, err
		}
		if ctx.Err() != nil || clients.wait(ctx, gap) != nil {
			return value, err
		}
	}
}

func observeExecRunOutput(ctx context.Context, clients *apiClients, runID, nodeID string, jsonOutput bool, stdout, stderr io.Writer) (record contract.RunRecord, err error) {
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
	waiting := execWaitingAnnouncer{nodeID: nodeID}
	announcedBusy := false
	stillFollowing := func() {
		if !announcedBusy {
			announcedBusy = true
			fmt.Fprintf(stderr, "wefty: control plane busy; still following run %s\n", runID)
		}
	}
	for {
		wasTerminal := runIsTerminal(record.Status)
		if wasTerminal {
			// The verdict is known; only the drain is left.
			stillFollowing = nil
		}
		page, err := readExec(ctx, clients, stillFollowing, func(readCtx context.Context) (l1.LogPage, error) {
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
		nextRecord, err := readExec(ctx, clients, stillFollowing, func(readCtx context.Context) (contract.RunRecord, error) {
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
		if queuedRunIsWaiting(record.Status) && time.Since(started) >= followWaitingNoticeAfter {
			if err = waiting.announce(ctx, clients, stdout, stderr, runID, record.Status, jsonOutput, time.Now()); err != nil {
				return record, err
			}
		}
		if err = clients.wait(ctx, execPollInterval); err != nil {
			return record, err
		}
	}
}

func lostExecObservation(stderr io.Writer, runID, storage string, cause error) error {
	fmt.Fprintf(stderr, "wefty: lost observation of run %s; the run continues\nwefty: follow with `wefty logs %s --follow`; cancel with `wefty cancel %s`\n", runID, runID, runID)
	if storage != "" {
		fmt.Fprintf(stderr, "wefty: snapshot retained at %s\n", storage)
	}
	return &unavailableError{cause: fmt.Errorf("could not observe run %s: %w", runID, cause)}
}

// Mailbox result delivery may trail terminal projection. Startup failures have
// their own result field, so they cannot be mistaken for the command's exit code.
func reportExecRun(ctx context.Context, clients *apiClients, record contract.RunRecord, capture execCapture, stderr io.Writer) error {
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
	if message := seatExecLimitMessage(record, event.Reason); message != "" {
		if _, err := fmt.Fprintln(stderr, message); err != nil {
			return err
		}
		return result
	}
	writeExecPathHint(stderr, event)
	writeExecDependencyHints(ctx, stderr, event, capture)
	return reportTerminalRun(stderr, record, event.Reason, false, false)
}

// Check at most once a minute. Load fluctuations within a busy period are not
// changes of reason; only a different reason merits another waiting line.
type execWaitingAnnouncer struct {
	nodeID    string
	checkedAt time.Time
	reasonKey string
}

func (a *execWaitingAnnouncer) announce(ctx context.Context, clients *apiClients, stdout, stderr io.Writer, runID string, status contract.RunState, jsonOutput bool, now time.Time) error {
	if !a.checkedAt.IsZero() && now.Sub(a.checkedAt) < time.Minute {
		return nil
	}
	a.checkedAt = now
	reason := "waiting for a node to start it"
	key := reason
	if unavailable, _ := unschedulableReason(ctx, clients, runID); unavailable != "" {
		reason += " (no eligible node: " + unavailable + ")"
		key = reason
	} else if a.nodeID != "" {
		probeCtx, stop := context.WithTimeout(ctx, statusProbeBudget)
		node, err := clients.getNode(probeCtx, a.nodeID)
		stop()
		if err == nil && node.NodeID == a.nodeID && node.Capabilities["kind:process"] {
			load := readLocalNodeLoad(clients, node)
			if load.Holding {
				reason = fmt.Sprintf("this machine is busy (load %.2f, limit %.2f), so it starts when the load drops", *load.Average, load.Limit)
				key = "machine_busy"
			}
		}
	}
	if a.reasonKey == key {
		return nil
	}
	a.reasonKey = key
	line := fmt.Sprintf("wefty: run %s is %s; %s\n", runID, status, reason)
	if _, err := io.WriteString(stderr, line); err != nil {
		return err
	}
	if jsonOutput {
		return writeJSONLine(stdout, struct {
			Type   string             `json:"type"`
			Stream contract.LogStream `json:"stream"`
			Text   string             `json:"text"`
			Reason string             `json:"reason"`
		}{"heartbeat", contract.LogStderr, line, reason})
	}
	return nil
}
