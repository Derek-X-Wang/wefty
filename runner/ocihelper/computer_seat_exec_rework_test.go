package ocihelper

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"testing"
	"testing/synctest"
	"time"
)

func TestSeatExecLimitFinishesPartlyConsumedFrame(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		authority := testAuthority()
		engine := &seatExecDeadlineEngine{fakeEngine: newFakeEngine(), run: func(ctx context.Context, emit func(SeatCommandEvent) error) error {
			if err := emit(SeatCommandEvent{Stream: "stdout", Data: bytes.Repeat([]byte("x"), 4096)}); err != nil {
				return err
			}
			<-ctx.Done()
			return ctx.Err()
		}}
		server := &Server{engine: engine, dataStreams: make(chan struct{}, 56)}
		session := &serverSession{identity: SessionIdentity{authority.NodeID, authority.BootSessionID}, attempts: map[string]*serverAttempt{authority.key(): {authority: authority, state: attemptLive, computer: true}}}
		serving, client := net.Pipe()
		defer client.Close()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		body, _ := json.Marshal(ExecComputerSeatRequest{authority, 1, 1, []string{"true"}, time.Hour, ""})
		done := make(chan struct{})
		go func() {
			defer close(done)
			defer serving.Close()
			server.dispatchExecComputerSeat(&sessionOperation{ctx: ctx, cancel: cancel, session: session, conn: serving}, newFramedConn(serving), frame{Method: MethodExecComputerSeat, Body: body})
		}()
		var header [4]byte
		if _, err := io.ReadFull(client, header[:]); err != nil {
			t.Fatal(err)
		}
		payload := make([]byte, binary.BigEndian.Uint32(header[:]))
		if _, err := io.ReadFull(client, payload[:100]); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Hour)
		synctest.Wait()
		if _, err := io.ReadFull(client, payload[100:]); err != nil {
			t.Fatalf("runtime limit cut the in-flight frame: %v", err)
		}
		var eventFrame frame
		if err := json.Unmarshal(payload, &eventFrame); err != nil {
			t.Fatalf("frame mixed with final diagnostic: %v", err)
		}
		var event SeatCommandEvent
		if err := decodeBody(eventFrame.Body, &event); err != nil || len(event.Data) != 4096 {
			t.Fatalf("lost output: %v %+v", err, event)
		}
		var final frame
		if err := newFramedConn(client).read(&final); err != nil {
			t.Fatal(err)
		}
		if final.Error == nil || final.Error.Code != CodeDiagnosticFailure || final.Error.Detail != "runtime_limit" {
			t.Fatalf("runtime limit not typed: %+v", final)
		}
		<-done
	})
}
func TestSeatExecBrokenTransportCannotLoseSession(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := net.Pipe()
		control, controlPeer := net.Pipe()
		defer control.Close()
		defer controlPeer.Close()
		session := &Session{client: &Client{Dial: func(context.Context) (net.Conn, error) { return client, nil }}, capability: "test", control: control}
		go func() {
			defer server.Close()
			var request frame
			_ = newFramedConn(server).read(&request)
			_ = writeAll(server, []byte{0, 0, 1, 0, '{'})
		}()
		err := session.ExecComputerSeat(t.Context(), ExecComputerSeatRequest{}, nil)
		var loss *RuntimeLossError
		if err == nil || errors.As(err, &loss) || session.HealthError() != nil {
			t.Fatalf("command stream lost helper session: %v health=%v", err, session.HealthError())
		}
	})
}

type delayedSeatExecReader struct {
	net.Conn
	read    int
	delayed bool
}

func (c *delayedSeatExecReader) Read(data []byte) (int, error) {
	if c.read >= 104 && !c.delayed {
		c.delayed = true
		time.Sleep(time.Hour + 6*time.Second)
	}
	if c.read < 104 && len(data) > 104-c.read {
		data = data[:104-c.read]
	}
	n, err := c.Conn.Read(data)
	c.read += n
	return n, err
}
func TestSeatExecStalledPartialFrameEndsLocallyAfterGrace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		authority := testAuthority()
		engine := &seatExecDeadlineEngine{fakeEngine: newFakeEngine(), run: func(ctx context.Context, emit func(SeatCommandEvent) error) error {
			return emit(SeatCommandEvent{Stream: "stdout", Data: bytes.Repeat([]byte("x"), 4096)})
		}}
		helper := &Server{engine: engine, dataStreams: make(chan struct{}, 56)}
		authoritySession := &serverSession{identity: SessionIdentity{authority.NodeID, authority.BootSessionID}, attempts: map[string]*serverAttempt{authority.key(): {authority: authority, state: attemptLive, computer: true}}}
		client, server := net.Pipe()
		control, controlPeer := net.Pipe()
		defer control.Close()
		defer controlPeer.Close()
		session := &Session{client: &Client{Dial: func(context.Context) (net.Conn, error) { return &delayedSeatExecReader{Conn: client}, nil }}, capability: "test", control: control}
		done := make(chan struct{})
		go func() {
			defer close(done)
			defer server.Close()
			wire := newFramedConn(server)
			var request frame
			if err := wire.read(&request); err != nil {
				return
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			helper.dispatchExecComputerSeat(&sessionOperation{ctx: ctx, cancel: cancel, session: authoritySession, conn: server}, wire, request)
		}()
		err := session.ExecComputerSeat(t.Context(), ExecComputerSeatRequest{authority, 1, 1, []string{"true"}, time.Hour, ""}, nil)
		var loss *RuntimeLossError
		if err == nil || errors.As(err, &loss) || session.HealthError() != nil {
			t.Fatalf("stalled frame escaped command scope: %v health=%v", err, session.HealthError())
		}
		select {
		case <-done:
		default:
			t.Fatal("stalled frame retained helper operation past grace")
		}
	})
}

func TestSeatExecRuntimeLimitDoesNotMaskMechanicsFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		authority := testAuthority()
		engine := &seatExecDeadlineEngine{fakeEngine: newFakeEngine(), run: func(ctx context.Context, _ func(SeatCommandEvent) error) error {
			<-ctx.Done()
			return errors.New("containerd failed independently")
		}}
		helper := &Server{engine: engine, dataStreams: make(chan struct{}, 56)}
		session := &serverSession{identity: SessionIdentity{authority.NodeID, authority.BootSessionID}, attempts: map[string]*serverAttempt{authority.key(): {authority: authority, state: attemptLive, computer: true}}}
		serving, client := net.Pipe()
		defer client.Close()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		body, _ := json.Marshal(ExecComputerSeatRequest{authority, 1, 1, []string{"true"}, time.Hour, ""})
		go func() {
			defer serving.Close()
			helper.dispatchExecComputerSeat(&sessionOperation{ctx: ctx, cancel: cancel, session: session, conn: serving}, newFramedConn(serving), frame{Method: MethodExecComputerSeat, Body: body})
		}()
		var result frame
		if err := newFramedConn(client).read(&result); err != nil {
			t.Fatal(err)
		}
		if result.Error == nil || result.Error.Code != CodeDiagnosticFailure || result.Error.Detail != "" {
			t.Fatalf("real mechanics failure masked as limit: %+v", result)
		}
	})
}

// A bounded containerd IO wait may return while its copy goroutine is still
// delivering an event. The final failure must serialize with that whole frame.
func TestSeatExecCleanupTimeoutFinishesInFlightFrame(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	authority := testAuthority()
	finishEngine := make(chan struct{})
	emitDone := make(chan error, 1)
	engine := &seatExecDeadlineEngine{fakeEngine: newFakeEngine(), run: func(_ context.Context, emit func(SeatCommandEvent) error) error {
		go func() { emitDone <- emit(SeatCommandEvent{Stream: "stdout", Data: bytes.Repeat([]byte("x"), 4096)}) }()
		<-finishEngine
		return &seatCommandCleanupError{errors.New("fixture output drain expired")}
	}}
	helper := &Server{engine: engine, dataStreams: make(chan struct{}, 56)}
	session := &serverSession{identity: SessionIdentity{authority.NodeID, authority.BootSessionID}, attempts: map[string]*serverAttempt{authority.key(): {authority: authority, state: attemptLive, computer: true}}}
	serving, client := net.Pipe()
	defer serving.Close()
	defer client.Close()
	if err := client.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(ExecComputerSeatRequest{authority, 1, 1, []string{"true"}, time.Hour, ""})
	done := make(chan struct{})
	go func() {
		defer close(done)
		helper.dispatchExecComputerSeat(&sessionOperation{ctx: ctx, cancel: cancel, session: session, conn: serving}, newFramedConn(serving), frame{Method: MethodExecComputerSeat, Body: body})
	}()
	var header [4]byte
	if _, err := io.ReadFull(client, header[:]); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, binary.BigEndian.Uint32(header[:]))
	if _, err := io.ReadFull(client, payload[:100]); err != nil {
		t.Fatal(err)
	}
	close(finishEngine)
	if _, err := io.ReadFull(client, payload[100:]); err != nil {
		t.Fatalf("cleanup cut event frame: %v", err)
	}
	var eventFrame frame
	if err := json.Unmarshal(payload, &eventFrame); err != nil {
		t.Fatal(err)
	}
	var event SeatCommandEvent
	if err := decodeBody(eventFrame.Body, &event); err != nil || len(event.Data) != 4096 {
		t.Fatalf("lost in-flight output: %v %+v", err, event)
	}
	var final frame
	if err := newFramedConn(client).read(&final); err != nil {
		t.Fatal(err)
	}
	if final.Error == nil || final.Error.Code != CodeDiagnosticFailure || final.Error.Detail != "" {
		t.Fatalf("cleanup failure lost: %+v", final)
	}
	if err := <-emitDone; err != nil {
		t.Fatal(err)
	}
	<-done
}
