package ocihelper

import (
	"context"
	"errors"
	"io"
	"net"
)

// Relay joins two authorized byte streams. Cancellation closes both sides;
// normal EOF half-closes the destination write side and drains the peer.
func Relay(ctx context.Context, left, right io.ReadWriteCloser) error {
	completed := make(chan error, 2)
	copyOne := func(destination io.Writer, source io.Reader) {
		_, err := io.Copy(destination, source)
		if closer, ok := destination.(interface{ CloseWrite() error }); ok {
			err = errors.Join(err, closer.CloseWrite())
		}
		completed <- err
	}
	go copyOne(right, left)
	go copyOne(left, right)
	var failures []error
	for count := 0; count < 2; count++ {
		select {
		case <-ctx.Done():
			_ = left.Close()
			_ = right.Close()
			failures = append(failures, ctx.Err())
			for ; count < 2; count++ {
				<-completed
			}
			return errors.Join(failures...)
		case err := <-completed:
			if err != nil && !errors.Is(err, net.ErrClosed) {
				failures = append(failures, err)
			}
		}
	}
	return errors.Join(failures...)
}
