package ocihelper

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
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
