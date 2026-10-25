package ocihelper

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"testing/synctest"
	"time"
)

type seatExecDeadlineEngine struct {
	*fakeEngine
	run func(context.Context, func(SeatCommandEvent) error) error
}

func (e *seatExecDeadlineEngine) ExecComputerSeat(ctx context.Context, _ ExecComputerSeatRequest, emit func(SeatCommandEvent) error) error {
	return e.run(ctx, emit)
}

func TestSeatExecDeadlineIsCommandScopedWithInjectedClock(t *testing.T) {
	for _, stalled := range []bool{false, true} {
		t.Run(map[bool]string{false: "idle_command", true: "nonreading_client"}[stalled], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				authority := testAuthority()
				engine := &seatExecDeadlineEngine{fakeEngine: newFakeEngine(), run: func(ctx context.Context, emit func(SeatCommandEvent) error) error {
					if stalled {
						if err := emit(SeatCommandEvent{Stream: "stdout", Data: []byte("out")}); err != nil {
							return err
						}
					}
					<-ctx.Done()
					return ctx.Err()
				}}
				server := &Server{engine: engine, dataStreams: make(chan struct{}, 56)}
				session := &serverSession{identity: SessionIdentity{NodeID: authority.NodeID, BootSessionID: authority.BootSessionID}, attempts: map[string]*serverAttempt{authority.key(): {authority: authority, state: attemptLive, computer: true}}}
				serving, client := net.Pipe()
				defer client.Close()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				operation := &sessionOperation{ctx: ctx, cancel: cancel, session: session, conn: serving}
				body, err := json.Marshal(ExecComputerSeatRequest{authority, 1, 1, []string{"true"}, time.Hour, ""})
				if err != nil {
					t.Fatal(err)
				}
				done := make(chan struct{})
				go func() {
					defer close(done)
					defer serving.Close()
					server.dispatchExecComputerSeat(operation, newFramedConn(serving), frame{Method: MethodExecComputerSeat, Body: body})
				}()
				if stalled {
					time.Sleep(time.Hour)
					synctest.Wait()
				}
				var result frame
				if err := newFramedConn(client).read(&result); err != nil {
					t.Fatal(err)
				}
				if stalled && result.OK {
					if err := newFramedConn(client).read(&result); err != nil {
						t.Fatal(err)
					}
				}
				if result.Error == nil || result.Error.Code != CodeDiagnosticFailure || result.Error.Detail != DetailSeatCommandRuntimeLimit {
					t.Fatalf("command deadline was not a scoped diagnostic: %+v", result)
				}
				<-done
				if _, err := session.authorizeAttempt(authority); err != nil {
					t.Fatalf("command deadline lost attempt authority: %v", err)
				}
			})
		})
	}
}
