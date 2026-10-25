package ocihelper

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func (e *seatExecProtocolEngine) CopyComputerSeat(ctx context.Context, r CopyComputerSeatRequest, input io.Reader, emit func([]byte) error) (int64, error) {
	if _, err := e.seats.command(r.command()); err != nil {
		return 0, err
	}
	if r.Direction == "in" {
		return io.Copy(io.Discard, input)
	}
	if err := emit([]byte("streamed copy")); err != nil {
		return 0, err
	}
	return 13, nil
}
func TestSeatCopyProtocolStreamsAndFences(t *testing.T) {
	session, e, authority := seatExecSession(t)
	request := CopyComputerSeatRequest{Authority: authority, Generation: 1, Place: 1, Direction: "out", Path: "report"}
	if _, err := session.CopyComputerSeat(t.Context(), request, nil, nil); err == nil {
		t.Fatal("unready copy accepted")
	}
	e.seats.entries[1].ready = true
	e.seats.entries[1].readyGeneration = 1
	var got bytes.Buffer
	size, err := session.CopyComputerSeat(t.Context(), request, nil, func(data []byte) error { _, e := got.Write(data); return e })
	if err != nil || size != 13 || got.String() != "streamed copy" {
		t.Fatalf("size=%d data=%q err=%v", size, got.String(), err)
	}
	request.Direction = "in"
	size, err = session.CopyComputerSeat(t.Context(), request, bytes.NewBufferString("copy in"), nil)
	if err != nil || size != 7 {
		t.Fatalf("copy in=%d %v", size, err)
	}
	request.Generation++
	_, err = session.CopyComputerSeat(t.Context(), request, bytes.NewReader(nil), nil)
	var rpc *RPCError
	if !errors.As(err, &rpc) || rpc.Code != CodeSeatOperationRefused || rpc.Detail != "generation_mismatch" {
		t.Fatalf("stale generation: %v", err)
	}
	request.Generation--
	request.Authority.FencingToken = "wrong"
	if _, err = session.CopyComputerSeat(t.Context(), request, nil, nil); err == nil {
		t.Fatal("wrong fence accepted")
	}
}

func TestSeatCopyStdinWrapperRejectsChangedTemplate(t *testing.T) {
	wrapper, err := seatCommandWithInput(seatCommandWrapper)
	if err != nil || !strings.Contains(wrapper, "stdin=sys.stdin") || strings.Contains(wrapper, "stdin=subprocess.DEVNULL") {
		t.Fatalf("stdin wrapper=%q %v", wrapper, err)
	}
	for _, broken := range []string{"changed", seatCommandWrapper + "\nstdin=subprocess.DEVNULL"} {
		if _, err = seatCommandWithInput(broken); err == nil {
			t.Fatal("unguarded wrapper replacement")
		}
	}
}

// gatedCopyConn holds every read after the first reply frame until the
// client has finished a half-close or a full close. That forces the losing
// order from #1279: the helper refuses and closes, the client's upload write
// fails, the client reacts, and only then does the read loop look for the
// typed refusal. The gate opens only after the inner close returns, so a full
// close always wins against the read.
type gatedCopyConn struct {
	net.Conn
	read        []byte
	armed       bool
	release     chan struct{}
	once        sync.Once
	writeFailed atomic.Bool
}

// seatCopyGateHangGuard only stops a broken test from hanging the package. The
// gate normally opens within milliseconds, so CI load cannot reach it.
const seatCopyGateHangGuard = 2 * time.Minute

func (c *gatedCopyConn) open() { c.once.Do(func() { close(c.release) }) }
func (c *gatedCopyConn) Read(p []byte) (int, error) {
	if c.armed {
		select {
		case <-c.release:
		case <-time.After(seatCopyGateHangGuard):
			return 0, errors.New("test gate: the client never closed or half-closed")
		}
		return c.Conn.Read(p)
	}
	n, err := c.Conn.Read(p)
	c.read = append(c.read, p[:n]...)
	if len(c.read) >= 4 && len(c.read) >= 4+int(binary.BigEndian.Uint32(c.read[:4])) {
		c.armed = true
	}
	return n, err
}
func (c *gatedCopyConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if err != nil {
		c.writeFailed.Store(true)
	}
	return n, err
}
func (c *gatedCopyConn) Close() error {
	defer c.open()
	return c.Conn.Close()
}
func (c *gatedCopyConn) CloseWrite() error {
	defer c.open()
	return c.Conn.(interface{ CloseWrite() error }).CloseWrite()
}

type endlessCopyInput struct{}

func (endlessCopyInput) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

func TestSeatCopyRefusalSurvivesFailedUploadWrite(t *testing.T) {
	session, e, authority := seatExecSession(t)
	e.seats.entries[1].ready = true
	e.seats.entries[1].readyGeneration = 1
	var gate *gatedCopyConn
	dial := session.client.Dial
	session.client.Dial = func(ctx context.Context) (net.Conn, error) {
		conn, err := dial(ctx)
		if err != nil {
			return nil, err
		}
		gate = &gatedCopyConn{Conn: conn, release: make(chan struct{})}
		return gate, nil
	}
	request := CopyComputerSeatRequest{Authority: authority, Generation: 2, Place: 1, Direction: "in", Path: "report"}
	_, err := session.CopyComputerSeat(t.Context(), request, endlessCopyInput{}, nil)
	if gate == nil || !gate.writeFailed.Load() {
		t.Fatalf("losing order not reached: err=%v", err)
	}
	var rpc *RPCError
	if !errors.As(err, &rpc) || rpc.Code != CodeSeatOperationRefused || rpc.Detail != "generation_mismatch" {
		t.Fatalf("stale generation after failed upload write: %v", err)
	}
}

// brokenUploadConn fails every write after the first reply frame and cannot
// half-close, while the helper stays alive waiting for upload input.
type brokenUploadConn struct {
	net.Conn
	read  []byte
	armed bool
}

func (c *brokenUploadConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if !c.armed {
		c.read = append(c.read, p[:n]...)
		c.armed = len(c.read) >= 4 && len(c.read) >= 4+int(binary.BigEndian.Uint32(c.read[:4]))
	}
	return n, err
}
func (c *brokenUploadConn) Write(p []byte) (int, error) {
	if c.armed {
		return 0, errors.New("injected upload write failure")
	}
	return c.Conn.Write(p)
}
func (c *brokenUploadConn) CloseWrite() error { return errors.New("injected: no half-close") }

func TestSeatCopyFailedUploadWriteWithoutHalfCloseDoesNotHang(t *testing.T) {
	grace := seatCopyReplyGrace
	seatCopyReplyGrace = 100 * time.Millisecond
	t.Cleanup(func() { seatCopyReplyGrace = grace })
	session, e, authority := seatExecSession(t)
	e.seats.entries[1].ready = true
	e.seats.entries[1].readyGeneration = 1
	dial := session.client.Dial
	session.client.Dial = func(ctx context.Context) (net.Conn, error) {
		conn, err := dial(ctx)
		if err != nil {
			return nil, err
		}
		return &brokenUploadConn{Conn: conn}, nil
	}
	// The context is only a hang guard. The helper never sees the end of
	// input, so only the client's reply grace may end this copy. Without the
	// grace, the copy ends later through a helper-side bound (EOF) instead.
	ctx, cancel := context.WithTimeout(t.Context(), seatCopyGateHangGuard)
	defer cancel()
	request := CopyComputerSeatRequest{Authority: authority, Generation: 1, Place: 1, Direction: "in", Path: "report"}
	_, err := session.CopyComputerSeat(ctx, request, endlessCopyInput{}, nil)
	if !errors.Is(err, os.ErrDeadlineExceeded) || ctx.Err() != nil {
		t.Fatalf("copy with a broken upload: err=%v ctx=%v", err, ctx.Err())
	}
}
