package ocihelper

import (
	"context"
	"time"
)

func publishTerminalAfterTaskRelease(timeout time.Duration, release func(context.Context) error, ready chan struct{}) error {
	if timeout <= 0 {
		timeout = time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var err error
	if release != nil {
		err = release(ctx)
	}
	close(ready)
	return err
}
