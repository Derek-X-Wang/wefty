package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type seatExecProtocolEngine struct {
	*seatProtocolEngine
	mu        sync.Mutex
	block     bool
	started   chan struct{}
	cancelled chan struct{}
}

func (e *seatExecProtocolEngine) ExecComputerSeat(ctx context.Context, request ExecComputerSeatRequest, emit func(SeatCommandEvent) error) error {
	if _, err := e.seats.command(request); err != nil {
		return err
	}
	e.mu.Lock()
	block := e.block
	e.mu.Unlock()
	if block {
		e.started <- struct{}{}
		<-ctx.Done()
		e.cancelled <- struct{}{}
		return ctx.Err()
	}
	code := 23
	for _, event := range []SeatCommandEvent{{Stream: "stdout", Data: []byte("out")}, {Stream: "stderr", Data: []byte("err")}, {Exit: &SeatCommandExit{Code: &code}}} {
		if err := emit(event); err != nil {
			return err
		}
	}
	return nil
}
func seatExecSession(t *testing.T) (*Session, *seatExecProtocolEngine, AttemptAuthority) {
	t.Helper()
	e := &seatExecProtocolEngine{seatProtocolEngine: &seatProtocolEngine{newFakeEngine(), newComputerSeats(nil, t.TempDir())}, started: make(chan struct{}, 8), cancelled: make(chan struct{}, 8)}
	client, stop := startTestServer(t, e, ServerConfig{})
	t.Cleanup(stop)
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	requireSweep(t, session)
	authority := testAuthority()
	authority.Class = "service"
	request := testRunRequest(authority, time.Minute)
	request.Workload.Computer = true
	request.EnableHostBridgeFallback = true
	request.ActivateHostBridgeFallback = true
	request.AllocateEndpoints = []string{"view", "control"}
	request.Workload.Limits.MemoryBytes = 1 << 30
	request.Workload.ManagedVolumes = []ManagedVolumeDescriptor{{Kind: ManagedVolumeComputerDisk, ComputerStorage: &ComputerStorageReference{ComputerID: "computer", StorageID: "storage", StorageGeneration: 1, IntentRevision: 1, DiskBytes: 1 << 30}}}
	e.runResponse.Endpoints = map[string]uint16{"view": 42011, "control": 42012}
	if _, err := session.Run(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := session.ApplyComputerSeats(t.Context(), ApplyComputerSeatsRequest{authority, []ComputerSeat{{1, "seat1", 7102, 7103}}}); err != nil {
		t.Fatal(err)
	}
	return session, e, authority
}
func TestSeatExecProtocolAuthorityReadinessAndOutput(t *testing.T) {
	session, e, authority := seatExecSession(t)
	req := ExecComputerSeatRequest{authority, 1, 1, []string{"true"}, time.Hour}
	check := func(code ErrorCode, detail string) {
		t.Helper()
		err := session.ExecComputerSeat(t.Context(), req, nil)
		var rpc *RPCError
		if !errors.As(err, &rpc) || rpc.Code != code || string(rpc.Detail) != detail {
			t.Fatalf("got %v, want %s/%s", err, code, detail)
		}
	}
	check(CodeSeatOperationRefused, "seat_not_ready")
	req.Authority.FencingToken = "wrong"
	check(CodeAttemptOutsideSession, "")
	req.Authority = authority
	other := testAuthority()
	other.AttemptID = "ordinary"
	e.runResponse.Endpoints = nil
	if _, err := session.Run(t.Context(), testRunRequest(other, time.Minute)); err != nil {
		t.Fatal(err)
	}
	req.Authority = other
	check(CodeUnauthorizedAttempt, "")
	req.Authority = authority
	req.Generation = 2
	check(CodeSeatOperationRefused, "generation_mismatch")
	req.Generation = 1
	req.MaxRuntime = 0
	check(CodeInvalidRequest, "")
	req.MaxRuntime = time.Hour
	valid := req
	for _, invalid := range []ExecComputerSeatRequest{
		{authority, 1, 1, nil, time.Hour},
		{authority, 1, 1, []string{"bad\x00command"}, time.Hour},
		{authority, 1, 0, []string{"true"}, time.Hour},
		{authority, 0, 1, []string{"true"}, time.Hour},
		{authority, 1, 1, []string{strings.Repeat("a", 128<<10)}, time.Hour},
	} {
		req = invalid
		check(CodeInvalidRequest, "")
	}
	req = valid
	errUnknown := session.call(t.Context(), MethodExecComputerSeat, map[string]any{
		"authority": authority, "generation": 1, "place": 1, "args": []string{"true"}, "max_runtime": int64(time.Hour), "environment": map[string]string{"HOME": "/root"},
	}, &struct{}{})
	var unknown *RPCError
	if !errors.As(errUnknown, &unknown) || unknown.Code != CodeInvalidRequest {
		t.Fatalf("accepted caller environment: %v", errUnknown)
	}
	e.seats.entries[1].ready = true
	e.seats.entries[1].readyGeneration = 1
	var events []SeatCommandEvent
	err := session.ExecComputerSeat(t.Context(), req, func(event SeatCommandEvent) error { events = append(events, event); return nil })
	if err != nil || len(events) != 3 || string(events[0].Data) != "out" || string(events[1].Data) != "err" || *events[2].Exit.Code != 23 {
		t.Fatalf("events %+v, %v", events, err)
	}
	// A failed consumer ends only this stream; unrelated runtime stays usable.
	sentinel := errors.New("consumer closed")
	if err := session.ExecComputerSeat(t.Context(), req, func(SeatCommandEvent) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if _, err := session.ObserveComputerSeats(t.Context(), ObserveComputerSeatsRequest{Authority: authority, Generation: 1}); err != nil {
		t.Fatal(err)
	}
}
func TestSeatExecClientDisconnectCancelsOnlyCommand(t *testing.T) {
	session, e, authority := seatExecSession(t)
	e.seats.entries[1].ready = true
	e.seats.entries[1].readyGeneration = 1
	e.mu.Lock()
	e.block = true
	e.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- session.ExecComputerSeat(ctx, ExecComputerSeatRequest{authority, 1, 1, []string{"sleep", "60"}, time.Hour}, nil)
	}()
	select {
	case <-e.started:
	case <-time.After(5 * time.Second):
		t.Fatal("command never started")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	select {
	case <-e.cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("disconnected command was not cancelled")
	}
	if _, err := session.ObserveComputerSeats(t.Context(), ObserveComputerSeatsRequest{Authority: authority, Generation: 1}); err != nil {
		t.Fatal(err)
	}
}

func TestSeatExecPerSeatLimitLeavesOtherSeatWorking(t *testing.T) {
	session, e, authority := seatExecSession(t)
	for place := 1; place <= 2; place++ {
		e.seats.entries[place] = &computerSeatEntry{config: ComputerSeat{Place: place, User: fmt.Sprintf("seat%d", place)}, wanted: true, ready: true, readyGeneration: 1}
	}
	e.mu.Lock()
	e.block = true
	e.mu.Unlock()
	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	req := ExecComputerSeatRequest{authority, 1, 1, []string{"sleep", "60"}, time.Hour}
	done := make(chan error, 3)
	for i := 0; i < 2; i++ {
		go func() { done <- session.ExecComputerSeat(ctx, req, nil) }()
		select {
		case <-e.started:
		case <-time.After(5 * time.Second):
			t.Fatal("command did not start")
		}
	}
	refuseCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	err := session.ExecComputerSeat(refuseCtx, req, nil)
	var rpc *RPCError
	if !errors.As(err, &rpc) || rpc.Code != CodeConnectionLimit || rpc.Detail != "seat_command_limit" {
		t.Fatalf("Seat consumed unlimited exec slots: %v", err)
	}
	other := req
	other.Place = 2
	go func() { done <- session.ExecComputerSeat(ctx, other, nil) }()
	select {
	case <-e.started:
	case <-time.After(5 * time.Second):
		t.Fatal("another Seat was starved")
	}
	stop()
	for i := 0; i < 3; i++ {
		<-done
	}
}
