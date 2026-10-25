package ocihelper

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
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

// gatedCopyConn holds every read after the first reply frame until a frame
// write fails or the connection closes. That forces the losing order from
// #1279: the helper refuses and closes, the client's upload write fails, and
// only then does the read loop look for the typed refusal.
type gatedCopyConn struct {
	net.Conn
	read        []byte
	armed       bool
	release     chan struct{}
	once        sync.Once
	writeFailed atomic.Bool
}

func (c *gatedCopyConn) open() { c.once.Do(func() { close(c.release) }) }
func (c *gatedCopyConn) Read(p []byte) (int, error) {
	if c.armed {
		select {
		case <-c.release:
		case <-time.After(10 * time.Second):
			return 0, errors.New("test gate: no write failure or close")
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
		c.open()
	}
	return n, err
}
func (c *gatedCopyConn) Close() error { c.open(); return c.Conn.Close() }
func (c *gatedCopyConn) CloseWrite() error {
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
