package ocihelper

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

const MethodCopyComputerSeat Method = "CopyComputerSeat"
const SeatCopyChunkBytes = 32 << 10

type CopyComputerSeatRequest struct {
	Authority  AttemptAuthority `json:"authority"`
	Generation uint64           `json:"generation"`
	Place      int              `json:"place"`
	Direction  string           `json:"direction"`
	Path       string           `json:"path"`
}
type SeatCopyEvent struct {
	Data      []byte `json:"data,omitempty"`
	SizeBytes *int64 `json:"size_bytes,omitempty"`
}
type seatCopyInput struct {
	Data []byte `json:"data,omitempty"`
	End  bool   `json:"end,omitempty"`
}
type ComputerSeatCopyEngine interface {
	CopyComputerSeat(context.Context, CopyComputerSeatRequest, io.Reader, func([]byte) error) (int64, error)
}

//go:embed computer_seat_copy.py
var seatCopyProgram string

func (r CopyComputerSeatRequest) command() ExecComputerSeatRequest {
	return ExecComputerSeatRequest{r.Authority, r.Generation, r.Place, []string{"/usr/bin/python3", "-I", "-S", "-c", seatCopyProgram, r.Direction, r.Path}, time.Duration(1<<63 - 1)}
}
func (r CopyComputerSeatRequest) validate() error {
	if r.Direction != "in" && r.Direction != "out" {
		return errors.New("copy direction must be in or out")
	}
	if r.Path == "" || len(r.Path) > 4096 || strings.ContainsRune(r.Path, 0) {
		return errors.New("copy needs a path without NUL, at most 4096 bytes")
	}
	return r.command().validate()
}

func (s *Session) CopyComputerSeat(ctx context.Context, r CopyComputerSeatRequest, input io.Reader, emit func([]byte) error) (int64, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	conn, wire, err := s.dialRequestInternal(ctx, MethodCopyComputerSeat, r, false)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	if err = decodeResponse(wire, &struct{}{}); err != nil {
		return 0, err
	}
	done := make(chan error, 1)
	go func() {
		var sendErr error
		inputFailed := false
		if input != nil && r.Direction == "in" {
			buf := make([]byte, SeatCopyChunkBytes)
			for {
				n, e := input.Read(buf)
				if n > 0 {
					sendErr = wire.write(seatCopyInput{Data: buf[:n]})
					if sendErr != nil {
						break
					}
				}
				if e != nil {
					if e != io.EOF {
						sendErr, inputFailed = e, true
					}
					break
				}
			}
		}
		if sendErr == nil {
			sendErr = wire.write(seatCopyInput{End: true})
		}
		switch {
		case inputFailed:
			// Local input broke: abort the copy and the helper's process.
			conn.Close()
		case sendErr != nil:
			// A failed frame write usually means the helper already answered
			// and closed, for example with a stale-generation refusal. A full
			// close here would discard that typed reply before the read loop
			// sees it (#1279). Half-close so the helper still sees EOF.
			_ = closeWrite(conn)
		}
		done <- sendErr
	}()
	var size *int64
	for {
		var response frame
		if err = wire.read(&response); err != nil {
			return 0, fmt.Errorf("copy transport failed: %w", err)
		}
		if response.Version != s.client.protocolVersion() {
			return 0, errors.New("copy protocol version mismatch")
		}
		if response.Error != nil {
			return 0, response.Error
		}
		if !response.OK {
			return 0, errors.New("invalid copy response")
		}
		if len(response.Body) == 0 {
			if size == nil {
				return 0, errors.New("copy ended without completion evidence")
			}
			if err = <-done; err != nil {
				return 0, err
			}
			return *size, nil
		}
		var event SeatCopyEvent
		if err = decodeBody(response.Body, &event); err != nil {
			return 0, err
		}
		if size != nil {
			return 0, errors.New("copy output after completion")
		}
		if event.SizeBytes != nil {
			if *event.SizeBytes < 0 || len(event.Data) != 0 {
				return 0, errors.New("invalid copy size")
			}
			size = event.SizeBytes
		} else {
			if len(event.Data) == 0 || len(event.Data) > SeatCommandChunkBytes {
				return 0, errors.New("invalid copy chunk")
			}
			if emit != nil {
				if err = emit(event.Data); err != nil {
					return 0, err
				}
			}
		}
	}
}

func seatCommandWithInput(wrapper string) (string, error) {
	const placeholder = "stdin=subprocess.DEVNULL"
	if strings.Count(wrapper, placeholder) != 1 {
		return "", errors.New("Seat command stdin template changed; refusing copy")
	}
	return strings.Replace(wrapper, placeholder, "stdin=sys.stdin", 1), nil
}
