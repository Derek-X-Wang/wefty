package agent

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/seatcopy"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type seatCopyControl interface {
	AuthorizeSeatCopy(context.Context, string, l1.SeatCopyAuthorityInput) (l1.SeatCopyAuthority, error)
	RecordSeatCopy(context.Context, string, l1.SeatCopyReceipt) error
}
type seatCopyRuntime interface {
	CopyComputerSeat(context.Context, ocihelper.CopyComputerSeatRequest, io.Reader, func([]byte) error) (int64, error)
}

var seatCopyRoute = regexp.MustCompile(`^/v1/seats/(seat_[0-9a-f]{32})/copy$`)

func newSeatCopyHandler(runtime seatCopyRuntime, control seatCopyControl, authority workloadrunner.AttemptAuthority, computer string) http.Handler {
	return newSeatCopyHandlerWithClock(runtime, control, authority, computer, systemClock{})
}
func newSeatCopyHandlerWithClock(runtime seatCopyRuntime, control seatCopyControl, authority workloadrunner.AttemptAuthority, computer string, clock Clock) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		match := seatCopyRoute.FindStringSubmatch(r.URL.Path)
		if len(match) != 2 || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		// Copy IDs belong to the node. Never let a caller reserve the identity
		// under which the node's immutable transfer evidence will be recorded.
		copyID := rand.Text()
		sendError := func(err error) {
			api := contract.APIError{Code: contract.ErrorCode("seat_copy_failed"), Message: err.Error()}
			var protocol *ProtocolError
			if errors.As(err, &protocol) && protocol.APIError.Code != "" {
				api = protocol.APIError
			}
			_ = json.NewEncoder(w).Encode(l1.SeatCopyStreamEvent{CopyID: copyID, Error: &api})
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		direction, path := r.URL.Query().Get("direction"), r.URL.Query().Get("path")
		if runtime == nil || control == nil {
			sendError(errors.New("Seat copy path unavailable"))
			return
		}
		if (direction != "in" && direction != "out") || path == "" || len(path) > 4096 || strings.ContainsRune(path, 0) {
			sendError(errors.New("copy needs direction in or out and a path"))
			return
		}
		input := l1.SeatCopyAuthorityInput{ComputerSeatAttemptRequest: l1.ComputerSeatAttemptRequest{NodeID: authority.NodeID, BootSessionID: authority.BootSessionID, FencingToken: authority.FencingToken}, ComputerID: computer, JobID: authority.JobID, AttemptID: authority.AttemptID, Credential: r.Header.Get(l1.SeatCredentialHeader)}
		binding, err := control.AuthorizeSeatCopy(r.Context(), match[1], input)
		if err != nil {
			sendError(err)
			return
		}
		start := l1.SeatCopyReceipt{SeatCopyAuthorityInput: input, CopyID: copyID, Direction: direction, Path: path, Outcome: "sending"}
		// Bound the local lease from request issuance, never a late reply.
		leaseIssuedAt := clock.Now()
		if err := recordSeatCopyReceipt(r.Context(), control, match[1], start); err != nil {
			// The start may have committed before its acknowledgement failed.
			// Close that operation and release any hold without moving payload.
			start.Outcome, start.Reason = "failed", "start_failed"
			finishErr := recordSeatCopyReceipt(r.Context(), control, match[1], start)
			sendError(fmt.Errorf("copy start unconfirmed: %w", errors.Join(err, finishErr)))
			return
		}
		copyCtx, cancelCopy := context.WithCancel(r.Context())
		defer cancelCopy()
		// Closing the helper connection cancels its process. Also interrupt
		// blocked HTTP upload/download I/O when Seat authority is withdrawn.
		stopIO := context.AfterFunc(copyCtx, func() {
			response := http.NewResponseController(w)
			_ = response.SetReadDeadline(time.Now())
			_ = response.SetWriteDeadline(time.Now())
			_ = r.Body.Close()
		})
		defer stopIO()
		renewalDone := make(chan struct{})
		go func() {
			defer close(renewalDone)
			renewSeatCopyLease(copyCtx, control, match[1], start, clock, cancelCopy, leaseIssuedAt)
		}()
		// HTTP/1 must permit response chunks while upload still streams. HTTP/2
		// already has this behavior. There is no application-wide body limit.
		_ = http.NewResponseController(w).EnableFullDuplex()
		encode := json.NewEncoder(w)
		meter := seatcopy.NewMeter()
		var inputStream io.Reader = r.Body
		if direction == "in" {
			inputStream = io.TeeReader(r.Body, meter)
		}
		size, err := runtime.CopyComputerSeat(copyCtx, ocihelper.CopyComputerSeatRequest{Authority: ocihelper.AttemptAuthority{NodeID: authority.NodeID, BootSessionID: authority.BootSessionID, JobID: authority.JobID, AttemptID: authority.AttemptID, FencingToken: authority.FencingToken, Class: authority.WorkloadClass, RemovalGeneration: authority.RemovalGeneration}, Generation: binding.Generation, Place: binding.Place, Direction: direction, Path: path}, inputStream, func(data []byte) error {
			if err := encode.Encode(l1.SeatCopyStreamEvent{Data: data}); err != nil {
				return err
			}
			if err := http.NewResponseController(w).Flush(); err != nil {
				return err
			}
			_, _ = meter.Write(data)
			return nil
		})
		stopIO()
		cancelCopy()
		<-renewalDone
		size = max(size, meter.Close())
		outcome, reason := "completed", ""
		if direction == "out" {
			outcome = "sent"
		}
		if err != nil {
			outcome = "failed"
			reason = "copy_failed"
			if errors.Is(err, context.Canceled) {
				reason = "cancelled"
			}
		}
		receipt := l1.SeatCopyReceipt{SeatCopyAuthorityInput: input, CopyID: copyID, Direction: direction, Path: path, SizeBytes: size, Outcome: outcome, Reason: reason}
		recordErr := recordSeatCopyReceipt(r.Context(), control, match[1], receipt)
		if recordErr != nil {
			sendError(fmt.Errorf("copy outcome unconfirmed: %w", recordErr))
			return
		}
		if err != nil {
			sendError(err)
			return
		}
		_ = encode.Encode(l1.SeatCopyStreamEvent{CopyID: copyID, SizeBytes: &size})
	})
}

func (c *Client) AuthorizeSeatCopy(ctx context.Context, id string, r l1.SeatCopyAuthorityInput) (l1.SeatCopyAuthority, error) {
	var value l1.SeatCopyAuthority
	err := c.post(ctx, "/v1/agent/seats/"+id+"/copy-authority", r, &value)
	return value, err
}
func (c *Client) RecordSeatCopy(ctx context.Context, id string, r l1.SeatCopyReceipt) error {
	return c.post(ctx, "/v1/agent/seats/"+id+"/copy-receipt", r, nil)
}

// Three attempts share one five-second bound and one immutable CopyID. A
// disconnected caller cannot discard evidence for an already committed copy.
func recordSeatCopyReceipt(ctx context.Context, control seatCopyControl, id string, r l1.SeatCopyReceipt) error {
	return recordSeatCopyReceiptWithClock(ctx, control, id, r, systemClock{})
}

func recordSeatCopyReceiptWithClock(ctx context.Context, control seatCopyControl, id string, r l1.SeatCopyReceipt, clock Clock) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	var err error
	for attempt := range 3 {
		err = control.RecordSeatCopy(ctx, id, r)
		if err == nil {
			return nil
		}
		var protocol *ProtocolError
		if ctx.Err() != nil || errors.As(err, &protocol) && protocol.StatusCode < 500 && !protocol.APIError.Retryable {
			break
		}
		if attempt < 2 {
			timer := clock.NewTimer(time.Duration(attempt+1) * 100 * time.Millisecond)
			select {
			case <-ctx.Done():
				stopTimer(timer)
				return ctx.Err()
			case <-timer.C():
			}
			stopTimer(timer)
		}
	}
	return err
}

const seatCopyLeaseSafetyMargin = 5 * time.Second
const seatCopyRenewalRequestBudget = 5 * time.Second

// Transient outages consume the last acknowledged lease's remaining budget.
// Terminal authority refusals stop immediately. A failed/ambiguous receipt
// never extends the local deadline, even if L1 might have committed it.
func renewSeatCopyLease(ctx context.Context, control seatCopyControl, id string, start l1.SeatCopyReceipt, clock Clock, cancel context.CancelFunc, issuedAt time.Time) {
	safeUntil := issuedAt.Add(contract.SeatCopyHoldLease - seatCopyLeaseSafetyMargin)
	next := issuedAt.Add(contract.SeatCopyHoldRenewal)
	delay := 100 * time.Millisecond
	timer := clock.NewTimer(max(0, next.Sub(clock.Now())))
	defer stopTimer(timer)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C():
		}
		remaining := safeUntil.Sub(clock.Now())
		if remaining <= 0 {
			cancel()
			return
		}
		issuedAt = clock.Now()
		renewCtx, stop := context.WithTimeout(ctx, min(seatCopyRenewalRequestBudget, remaining))
		err := control.RecordSeatCopy(renewCtx, id, start)
		stop()
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			safeUntil = issuedAt.Add(contract.SeatCopyHoldLease - seatCopyLeaseSafetyMargin)
			next = issuedAt.Add(contract.SeatCopyHoldRenewal)
			delay = 100 * time.Millisecond
		} else {
			var protocol *ProtocolError
			if classifyAgentProtocolError(err).destination == errorDestinationAttemptAuthority || errors.As(err, &protocol) && (protocol.APIError.Code == "seat_ended" || protocol.StatusCode >= 400 && protocol.StatusCode < 500 && !protocol.APIError.Retryable) {
				cancel()
				return
			}
			next = clock.Now().Add(delay)
			delay = min(delay*2, time.Second)
		}
		remaining = safeUntil.Sub(clock.Now())
		if remaining <= 0 {
			cancel()
			return
		}
		timer.Reset(min(max(0, next.Sub(clock.Now())), remaining))
	}
}
