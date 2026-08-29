package ocihelper

import (
	"context"
	"time"
)

// DefaultTaskReleaseTimeout bounds exited-task deletion and binary-v2 logger
// pipe sealing before terminal publication.
const DefaultTaskReleaseTimeout = 5 * time.Second

func publishTerminalAfterTaskRelease(timeout time.Duration, release func(context.Context) error, ready chan struct{}) error {
	if timeout <= 0 {
		timeout = DefaultTaskReleaseTimeout
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
