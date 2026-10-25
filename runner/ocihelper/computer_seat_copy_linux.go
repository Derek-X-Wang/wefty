//go:build linux

package ocihelper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

func (e *ContainerdEngine) CopyComputerSeat(ctx context.Context, r CopyComputerSeatRequest, input io.Reader, emit func([]byte) error) (int64, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	clock := e.config.Clock
	if clock == nil {
		clock = systemClock{}
	}
	ctx, progress, stop := watchSeatCopy(ctx, clock)
	defer stop()
	var diagnostic bytes.Buffer
	var exited bool
	receive := func(event SeatCommandEvent) error {
		progress()
		if event.Exit != nil {
			if event.Exit.Code == nil || *event.Exit.Code != 0 {
				message := strings.TrimSpace(diagnostic.String())
				if message == "" {
					message = "copy process failed"
				}
				return errors.New(message)
			}
			exited = true
			return nil
		}
		if event.Stream == "stderr" {
			if diagnostic.Len()+len(event.Data) > 8192 {
				return errors.New("Seat copy diagnostic exceeds limit")
			}
			_, err := diagnostic.Write(event.Data)
			return err
		}
		if r.Direction != "out" {
			return errors.New("unexpected copy-in output")
		}
		return emit(event.Data)
	}
	var inputs []io.Reader
	if r.Direction == "in" {
		if input == nil {
			return 0, errors.New("copy-in input missing")
		}
		inputs = []io.Reader{seatCopyReader{io.MultiReader(input, strings.NewReader("\nWEFTY_COPY_COMMIT_V1\n")), progress}}
	}
	if err := e.execComputerSeat(ctx, r.command(), receive, confirmSeatCommandReady, inputs...); err != nil {
		return 0, errors.Join(err, context.Cause(ctx))
	}
	var result struct {
		SizeBytes *int64 `json:"size_bytes"`
	}
	if err := json.Unmarshal(diagnostic.Bytes(), &result); err != nil || !exited || result.SizeBytes == nil || *result.SizeBytes < 0 {
		return 0, errors.New("Seat copy has no completion evidence")
	}
	return *result.SizeBytes, nil
}
