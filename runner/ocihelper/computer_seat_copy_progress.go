package ocihelper

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

const seatCopyIdleTimeout = 60 * time.Second

var errSeatCopyStalled = errors.New("Seat copy stalled: no stream progress for 60 seconds")

func watchSeatCopy(ctx context.Context, clock Clock) (context.Context, func(), func()) {
	ctx, cancel := context.WithCancelCause(ctx)
	var mu sync.Mutex
	last := clock.Now()
	timer := clock.NewTimerAt(last.Add(seatCopyIdleTimeout))
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C():
				mu.Lock()
				deadline := last.Add(seatCopyIdleTimeout)
				if !clock.Now().Before(deadline) {
					cancel(errSeatCopyStalled)
					mu.Unlock()
					return
				}
				timer.ResetAt(deadline)
				mu.Unlock()
			}
		}
	}()
	progress := func() { mu.Lock(); last = clock.Now(); mu.Unlock() }
	return ctx, progress, func() { cancel(context.Canceled); <-done }
}

type seatCopyReader struct {
	reader   io.Reader
	progress func()
}

func (r seatCopyReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.progress()
	}
	return n, err
}
