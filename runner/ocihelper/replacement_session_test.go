package ocihelper

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"
)

// ReapSession records its call before the server releases exclusive session
// ownership. Like the boot barrier, replacement callers must tolerate only
// session_busy while that release finishes, within a bounded acquisition window.
func openReplacementSession(ctx context.Context, client *Client, request AcquireSessionRequest) (*Session, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		session, err := client.OpenSession(ctx, request)
		if err == nil {
			return session, nil
		}
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, contextErr
		}
		var rpcErr *RPCError
		if !errors.As(err, &rpcErr) || rpcErr.Code != CodeSessionBusy {
			return nil, err
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func TestOpenReplacementSessionRetriesSessionBusy(t *testing.T) {
	handshake := validAcquireResponse("replacement-helper", time.Second)
	admitted := handshake
	admitted.SessionCapability = "replacement-capability"
	admitted.SessionGeneration = 7
	busy := scriptedAcquireAttempt{handshake: handshake, admissionError: &RPCError{Code: CodeSessionBusy, Message: "incumbent is reaping"}}
	client := scriptedAcquireClient(t, []scriptedAcquireAttempt{busy, busy, {handshake: handshake, admission: &admitted}})
	session, err := openReplacementSession(t.Context(), client, testSessionRequest())
	if session != nil {
		defer session.Close()
	}
	if err != nil || session == nil || client.scriptedDials() != 3 {
		t.Fatalf("replacement session=%v err=%v dials=%d, want success after two busy refusals", session, err, client.scriptedDials())
	}
	if got := session.Handshake(); got.SessionGeneration != admitted.SessionGeneration || session.capability != admitted.SessionCapability {
		t.Fatalf("replacement handshake = %+v, want %+v", got, admitted)
	}
}

func TestOpenReplacementSessionBoundsSessionBusy(t *testing.T) {
	// The parent is only a safety net: the helper must stop on its own bound.
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	attempts := make([]scriptedAcquireAttempt, 1000)
	for i := range attempts {
		attempts[i] = scriptedAcquireAttempt{
			handshake:      validAcquireResponse("busy-helper", time.Second),
			admissionError: &RPCError{Code: CodeSessionBusy, Message: "incumbent is still reaping"},
		}
	}
	client := scriptedAcquireClient(t, attempts)
	session, err := openReplacementSession(ctx, client, testSessionRequest())
	if session != nil {
		defer session.Close()
	}
	// Connection deadlines can fire just before the context timer publishes
	// Done; either timeout is evidence that the acquisition bound was enforced.
	timedOut := errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded)
	if session != nil || !timedOut || ctx.Err() != nil || client.scriptedDials() < 2 {
		t.Fatalf("busy bound: session=%v err=%v parent=%v dials=%d, want bounded retries", session, err, ctx.Err(), client.scriptedDials())
	}
}

func TestOpenReplacementSessionStopsOnOtherErrors(t *testing.T) {
	transportErr := errors.New("session_busy text is not an RPC refusal")
	for _, test := range []struct {
		name  string
		code  ErrorCode
		cause error
	}{
		{name: "invalid request", code: CodeInvalidRequest},
		{name: "stale session", code: CodeSessionStale},
		{name: "version mismatch", code: CodeVersionMismatch},
		{name: "checksum mismatch", code: CodeChecksumMismatch},
		{name: "unknown RPC code", code: "unknown_session_error"},
		{name: "transport error", cause: transportErr},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Refuse immediately both on the first dial and after a busy retry.
			for _, busyCount := range []int{0, 1} {
				handshake := validAcquireResponse("refusing-helper", time.Second)
				admitted := handshake
				admitted.SessionCapability = "must-not-be-acquired"
				admitted.SessionGeneration = 1
				var attempts []scriptedAcquireAttempt
				if busyCount != 0 {
					attempts = append(attempts, scriptedAcquireAttempt{handshake: handshake, admissionError: &RPCError{Code: CodeSessionBusy}})
				}
				attempts = append(attempts,
					scriptedAcquireAttempt{handshake: handshake, admissionError: &RPCError{Code: test.code, Message: "session_busy text must not cause a retry"}},
					scriptedAcquireAttempt{handshake: handshake, admission: &admitted},
				)
				client := scriptedAcquireClient(t, attempts)
				dials := 0
				dial := client.Dial
				client.Dial = func(ctx context.Context) (net.Conn, error) {
					dials++
					if test.cause != nil && dials == busyCount+1 {
						return nil, test.cause
					}
					return dial(ctx)
				}
				session, err := openReplacementSession(t.Context(), client, testSessionRequest())
				if session != nil {
					_ = session.Close()
				}
				var rpcErr *RPCError
				wantErr := errors.As(err, &rpcErr) && rpcErr.Code == test.code
				if test.cause != nil {
					wantErr = errors.Is(err, test.cause)
				}
				if session != nil || !wantErr || dials != busyCount+1 {
					t.Fatalf("after %d busy refusals: session=%v err=%v dials=%d, want immediate original refusal", busyCount, session, err, dials)
				}
			}
		})
	}
}
