package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l3"
)

// All accepted watching modes use the same final event, including cases where
// observation ended without a command verdict. A null exit code is deliberate.
type execFinished struct {
	Type              string            `json:"type"`
	Outcome           string            `json:"outcome"`
	ExitCode          *int              `json:"exit_code"`
	RunID             string            `json:"run_id"`
	Base              string            `json:"base_commit"`
	Dirty             bool              `json:"dirty"`
	Status            contract.RunState `json:"status"`
	Reason            string            `json:"reason,omitempty"`
	SeatID            string            `json:"seat_id,omitempty"`
	MaxRuntimeSeconds int               `json:"max_runtime_seconds,omitempty"`

	// Keep plain diagnostics specific to a published wrapper failure.
	infrastructureFailure bool
}

func execFinishedEvent(capture execCapture, record contract.RunRecord, outcome string) execFinished {
	event := execFinished{Type: "finished", Outcome: outcome, RunID: record.RunID, Base: capture.Base, Dirty: capture.Dirty, Status: record.Status, SeatID: record.SeatID}
	if record.SeatID != "" && record.Limits != nil {
		event.MaxRuntimeSeconds = record.Limits.MaxRuntimeSeconds
	}
	return event
}

var execHeartbeatLine = regexp.MustCompile(`^still (running|preparing the snapshot) \([0-9]+m\)\n$`)

// Raw reads can split a UTF-8 character. Keep only its incomplete suffix,
// independently for each stream; invalid bytes still use JSON's replacement rule.
type execJSONLogWriter struct {
	pending map[contract.LogStream][]byte
}

func (w *execJSONLogWriter) write(stdout io.Writer, events []contract.LogEvent) error {
	if w.pending == nil {
		w.pending = make(map[contract.LogStream][]byte)
	}
	for _, event := range events {
		data := append(w.pending[event.Stream], event.Bytes...)
		end := 0
		for end < len(data) && utf8.FullRune(data[end:]) {
			_, size := utf8.DecodeRune(data[end:])
			end += size
		}
		w.pending[event.Stream] = append([]byte(nil), data[end:]...)
		event.Bytes = data[:end]
		if err := writeExecJSONLogs(stdout, []contract.LogEvent{event}); err != nil {
			return err
		}
	}
	return nil
}

func (w *execJSONLogWriter) flush(stdout io.Writer) error {
	for _, stream := range []contract.LogStream{contract.LogStdout, contract.LogStderr} {
		if err := writeExecJSONLogs(stdout, []contract.LogEvent{{Stream: stream, Bytes: w.pending[stream]}}); err != nil {
			return err
		}
		delete(w.pending, stream)
	}
	return nil
}

func writeExecJSONLogs(stdout io.Writer, events []contract.LogEvent) error {
	for _, event := range events {
		// A raw event can contain several lines. Keep each text piece intact and
		// classify only complete wrapper liveness lines on stderr.
		for _, text := range strings.SplitAfter(string(event.Bytes), "\n") {
			if text == "" {
				continue
			}
			kind := "output"
			if event.Stream == contract.LogStderr && execHeartbeatLine.MatchString(text) {
				kind = "heartbeat"
			}
			if err := writeJSONLine(stdout, struct {
				Type   string             `json:"type"`
				Stream contract.LogStream `json:"stream"`
				Text   string             `json:"text"`
			}{kind, event.Stream, text}); err != nil {
				return err
			}
		}
	}
	return nil
}

// The wrapper exits with the command's code, so success proves exit 0.
// Only failed runs need to wait for the command result after terminal projection.
func resolveExecFinished(ctx context.Context, clients *apiClients, record contract.RunRecord, capture execCapture) (execFinished, error) {
	ctx = context.WithoutCancel(ctx)
	event := execFinishedEvent(capture, record, "pass")
	if record.Status == contract.RunSucceeded {
		code := 0
		event.ExitCode = &code
		return event, nil
	}
	if record.Status == contract.RunFailed && record.CancelStatus != "" {
		event.Outcome = "cancelled"
		event.Reason = "cancelled"
		return event, &runOutcomeError{runID: record.RunID, status: record.Status, reason: event.Reason}
	}
	if record.Restartable {
		execution, err := readExec(ctx, clients, func(readCtx context.Context) (l3.RunExecution, error) {
			return clients.getRunExecution(readCtx, record.RunID)
		})
		if err != nil || execution.Job == nil || execution.Job.RestartRecovery == nil {
			event.Outcome = "infrastructure_error"
			event.Reason = "restart evidence unavailable"
			cause := fmt.Errorf("run %s: %s", record.RunID, event.Reason)
			if err != nil {
				cause = fmt.Errorf("%w: %w", cause, err)
			}
			return event, &unavailableError{cause: cause}
		}
		if reason := execution.Job.RestartRecovery.TerminalReason; reason != nil {
			event.Outcome = "infrastructure_error"
			event.Reason = *reason
			return event, &unavailableError{cause: fmt.Errorf("run %s: %s", record.RunID, *reason)}
		}
	}
	event.Reason = runFailureReason(ctx, clients, record)
	if record.Status == contract.RunFailed {
		event.Outcome = "fail"
		if strings.HasPrefix(event.Reason, "maximum runtime reached") || strings.HasPrefix(event.Reason, "idle limit reached") {
			return event, &runOutcomeError{runID: record.RunID, status: record.Status, reason: event.Reason}
		}
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
					event.Outcome = "infrastructure_error"
					event.Reason = document.InfrastructureError
					event.infrastructureFailure = true
					return event, &unavailableError{cause: fmt.Errorf("run %s infrastructure failure: %s", record.RunID, document.InfrastructureError)}
				}
				if document.ExitCode != nil {
					event.ExitCode = document.ExitCode
					return event, &runOutcomeError{runID: record.RunID, status: record.Status, reason: event.Reason}
				}
			}
			break
		}
		if err == nil && result.SkipReason != "" {
			break
		}
		var response *apiResponseError
		if err != nil && !(errors.As(err, &response) && response.APIError.Code == contract.ErrorNotFound) {
			event.Outcome = "infrastructure_error"
			event.Reason = "command result unavailable"
			return event, &unavailableError{cause: fmt.Errorf("run %s ended; could not read its result: %w", record.RunID, err)}
		}
		if clients.wait(resultCtx, 100*time.Millisecond) != nil {
			break
		}
	}
	event.Outcome = "infrastructure_error"
	event.Reason = "command result unavailable"
	return event, &unavailableError{cause: fmt.Errorf("run %s ended; command result unavailable; inspect with `wefty inspect %s --execution`", record.RunID, record.RunID)}
}

func reportExecFinished(ctx context.Context, clients *apiClients, record contract.RunRecord, capture execCapture, jsonOutput bool, stdout, stderr io.Writer) error {
	// Preserve the original plain verdict and recovery policy.
	if !jsonOutput {
		return reportExecRun(ctx, clients, record, capture, stderr)
	}
	event, result := resolveExecFinished(ctx, clients, record, capture)
	if err := writeJSONLine(stdout, event); err != nil {
		return err
	}
	writeExecPathHint(stderr, event)
	writeExecDependencyHints(ctx, stderr, event, capture)
	return result
}

func writeExecPathHint(stderr io.Writer, event execFinished) {
	if event.Outcome == "fail" && event.ExitCode != nil && *event.ExitCode == 127 {
		if event.SeatID != "" {
			fmt.Fprintln(stderr, "wefty: exit 127 usually means a command was not found in the Seat's environment; install tools in the Seat or use an executable path.")
			return
		}
		fmt.Fprintln(stderr, "wefty: exit 127 usually means a command was not found on the run's PATH; runs get the node's login-shell PATH, not yours. Use `bun run`, `npx`, `pnpm exec`, `uv run` or a path such as ./node_modules/.bin/<tool>; `wefty exec -- sh -c 'echo $PATH'` shows it.")
	}
}

func endExecObservation(clients *apiClients, capture execCapture, record contract.RunRecord, jsonOutput bool, stdout, stderr io.Writer, cause error) error {
	// A stop immediately after acceptance has no last observed status. Read it
	// with a fresh bounded context, without tying the run to caller cancellation.
	if record.Status == "" {
		ctx, stop := context.WithTimeout(context.Background(), statusProbeBudget)
		known, err := clients.getRun(ctx, record.RunID)
		stop()
		if err == nil {
			record = known
		}
	}
	if runIsTerminal(record.Status) {
		// The verdict is known, but logs after the last read were not drained,
		// so JSON consumers get the same warning as the normal terminal path.
		if jsonOutput && cause != nil {
			fmt.Fprintf(stderr, "wefty: warning: could not drain logs for terminal run %s: %v\n", record.RunID, cause)
		}
		return reportExecFinished(context.Background(), clients, record, capture, jsonOutput, stdout, stderr)
	}
	if jsonOutput {
		outcome := "observation_lost"
		if queuedRunIsWaiting(record.Status) {
			outcome = "queued"
		} else if record.Status == contract.RunRunning {
			outcome = "running"
		}
		event := execFinishedEvent(capture, record, outcome)
		event.Reason = cause.Error()
		if err := writeJSONLine(stdout, event); err != nil {
			return lostExecObservation(stderr, record.RunID, execCaptureLocation(capture), err)
		}
	}
	return lostExecObservation(stderr, record.RunID, execCaptureLocation(capture), cause)
}

// Cancellation keeps the existing settlement and exit policy. JSON describes
// the accepted cancellation separately from a command's own exit code.
func cancelExecObservation(clients *apiClients, capture execCapture, runID string, jsonOutput bool, stdout, stderr io.Writer, cause error) error {
	result := cancelExecRun(clients, runID, capture.Storage, cause, stderr, execCaptureLocation(capture))
	if !jsonOutput {
		return result
	}
	ctx, stop := context.WithTimeout(context.Background(), statusProbeBudget)
	record, err := clients.getRun(ctx, runID)
	stop()
	outcome := "observation_lost"
	if err != nil {
		record = contract.RunRecord{RunID: runID}
	} else if record.CancelStatus != "" {
		outcome = "cancelled"
	}
	event := execFinishedEvent(capture, record, outcome)
	event.Reason = result.Error()
	if err := writeJSONLine(stdout, event); err != nil {
		return err
	}
	return result
}
