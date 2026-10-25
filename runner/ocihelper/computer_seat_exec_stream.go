package ocihelper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// One frame, one synchronous consumer call. No output queue grows with the Run.
type seatCommandStream struct {
	ctx      context.Context
	emit     func(SeatCommandEvent) error
	mu       sync.Mutex
	pending  []byte
	terminal bool
	refusal  error
	err      error
}

func newSeatCommandStream(ctx context.Context, emit func(SeatCommandEvent) error) *seatCommandStream {
	return &seatCommandStream{ctx: ctx, emit: emit}
}
func (w *seatCommandStream) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(data)
	if w.ctx.Err() != nil {
		return n, nil
	}
	if w.err != nil {
		return 0, w.err
	}
	for len(data) > 0 {
		index := bytes.IndexByte(data, '\n')
		length := len(data)
		if index >= 0 {
			length = index
		}
		if len(w.pending)+length > seatCommandFrameBytes {
			w.err = errors.New("Seat command frame exceeds bound")
			return 0, w.err
		}
		w.pending = append(w.pending, data[:length]...)
		data = data[length:]
		if index < 0 {
			break
		}
		data = data[1:]
		var event SeatCommandEvent
		err := decodeSeatDocument(w.pending, &event)
		if err == nil {
			err = event.validate()
		}
		if err == nil && w.terminal {
			err = errors.New("Seat command output after exit")
		}
		if err == nil {
			if w.emit != nil {
				err = w.emit(event)
			}
			if event.Exit != nil || event.Refusal != nil {
				w.terminal = true
			}
			if event.Refusal != nil {
				w.refusal = fmt.Errorf("%s: %w", event.Refusal.Message, &SeatOperationRefusalError{Reason: event.Refusal.Reason})
			}
		}
		w.pending = w.pending[:0]
		if err != nil {
			w.err = err
			return 0, err
		}
	}
	return n, nil
}
func (w *seatCommandStream) finish() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	if w.ctx.Err() != nil {
		return w.ctx.Err()
	}
	if len(w.pending) != 0 || !w.terminal {
		return errors.New("Seat command has no complete exit evidence")
	}
	return w.refusal
}

// Containerd must keep draining after a failed consumer so a wrapper blocked
// in write can handle cancellation. The context cancels the command, not I/O.
type seatCommandDrain struct {
	stream *seatCommandStream
	cancel context.CancelFunc
}

func (w seatCommandDrain) Write(data []byte) (int, error) {
	if _, err := w.stream.Write(data); err != nil {
		w.cancel()
	}
	return len(data), nil
}

func marshalSeatCommandEvent(event SeatCommandEvent) ([]byte, error) {
	data, err := json.Marshal(event)
	return append(data, '\n'), err
}
