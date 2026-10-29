package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

func executeSeatExec(ctx context.Context, clients *apiClients, id string, argv []string, seconds int, keepRunning, detach, jsonOutput bool, stdout, stderr io.Writer) error {
	source := contract.SeatRunInput{SeatID: id, Argv: argv, MaxRuntimeSeconds: int64(seconds)}
	// Validate using the backend's vocabulary, but keep omission intact so L1
	// chooses its configured default rather than freezing the shipped default.
	check := source
	if err := check.Normalize(); err != nil {
		return usageError(err.Error())
	}
	secret, err := loadSeatCredential(id)
	if err != nil {
		return err
	}
	if secret == "" {
		return &apiResponseError{StatusCode: http.StatusForbidden, APIError: contract.APIError{
			Code: contract.ErrorForbidden, Message: fmt.Sprintf("Seat credential is unavailable; wait again with wefty seat wait %s", id), Details: map[string]any{"seat_id": id},
		}}
	}
	ctx, stopSignalContext := execSignalContext(ctx)
	defer stopSignalContext()
	source.WorkingDirectory, err = prepareSeatWorkingCopy(ctx, clients, id, secret)
	if err != nil {
		return err
	}
	key, err := ensureIdempotencyKey("")
	if err != nil {
		return err
	}
	submitCtx, stop := context.WithTimeout(ctx, 30*time.Second)
	// Send empty params explicitly: a nil RawMessage encodes as null,
	// which older L3 servers reject (#1332).
	request := l3.CreateRunRequest{SeatRun: &source, Params: json.RawMessage("{}")}
	accepted, uncertain, err := submitExecRun(submitCtx, clients, request, key, stderr, secret)
	stop()
	if err != nil && uncertain && ctx.Err() != nil && !keepRunning && !detach {
		// Recovery after a signal is normally read-only. This default-mode
		// exception is safe: the shared observer immediately cancels any Run
		// the replay finds or creates. Keep-running and detach must not replay.
		recoveryCtx, stopRecovery := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		recovered, recoveryErr := clients.submitRun(recoveryCtx, request, key, secret)
		stopRecovery()
		if recoveryErr == nil && recovered.RunID != "" {
			accepted, err = recovered, nil
		}
	}
	if err != nil {
		if uncertain {
			return &unavailableError{cause: fmt.Errorf("Seat command submission could not be confirmed (key %s); inspect `wefty runs list` before starting another command: %w", key, err)}
		}
		return err
	}
	defer writeExecSeatHint(ctx, clients, id, stderr)
	return observeSubmittedExecRun(ctx, clients, accepted.RunID, "", execCapture{}, keepRunning, detach, jsonOutput, stdout, stderr)
}

// Exec stdout belongs to the command or its JSON stream. Keep the shared Seat
// reply hint on stderr, including detach, whose JSON reply is only a Run ID.
// This original-caller read acknowledges lifecycle outcomes only; it neither
// sends the credential again nor renews Seat activity while watching.
func writeExecSeatHint(ctx context.Context, clients *apiClients, id string, stderr io.Writer) {
	ctx, stop := context.WithTimeout(context.WithoutCancel(ctx), statusProbeBudget)
	defer stop()
	var result l1.SeatCallResult
	if err := clients.l1.do(ctx, http.MethodGet, "/v1/seat-requests/"+id, nil, nil, &result, http.StatusOK); err != nil {
		fmt.Fprintf(stderr, "wefty: warning: could not read Seat events; check with `wefty logs %s`: %v\n", id, err)
		return
	}
	if result.SeatID != id {
		fmt.Fprintln(stderr, "wefty: warning: Seat event reply changed its ID")
		return
	}
	_ = withSeatReply(stderr, false, id, result.UnreadEvents, func(io.Writer) error { return nil })
}

func seatExecLimitMessage(record contract.RunRecord, reason string) string {
	if record.SeatID == "" || !strings.HasPrefix(reason, "maximum runtime reached") || record.Limits == nil || record.Limits.MaxRuntimeSeconds <= 0 {
		return ""
	}
	limit := time.Duration(record.Limits.MaxRuntimeSeconds) * time.Second
	text := limit.String()
	if limit%time.Hour == 0 {
		text = fmt.Sprintf("%dh", limit/time.Hour)
	} else if limit%time.Minute == 0 {
		text = fmt.Sprintf("%dm", limit/time.Minute)
	}
	return fmt.Sprintf("stopped: reached its %s limit. Run it again with a longer --max-runtime.", text)
}
