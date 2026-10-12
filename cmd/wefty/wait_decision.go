package main

import (
	"context"
	"errors"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

type waitVerdict uint8

const (
	waitRetry waitVerdict = iota
	waitCallerStopped
	waitWindowExpired
	waitFailed
)

// Each observer owns its cadence; disable the client's separate GET retry.
func openWaitWindow(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	window, cancel := context.WithTimeout(ctx, d)
	return withoutClientRetry(window), cancel
}

// decideWaitFailure handles failed reads and interrupted sleeps. A nil error
// records a successful read, clearing the last transient answer; the observer
// still decides when to accept that answer. A stalled read keeps the last
// answer, and a permanent refusal keeps its classification at the deadline.
func decideWaitFailure(caller, window context.Context, err error, last *error) waitVerdict {
	if err == nil {
		*last = nil
		return waitRetry
	}
	if caller.Err() != nil || errors.Is(window.Err(), context.Canceled) {
		return waitCallerStopped
	}
	retryable := isRetryableL1Answer(err)
	if retryable {
		*last = err
	}
	if window.Err() == context.DeadlineExceeded && (errors.Is(err, context.DeadlineExceeded) || retryable) {
		return waitWindowExpired
	}
	if retryable && window.Err() == nil {
		return waitRetry
	}
	return waitFailed
}

// isRetryableL1Answer reports whether L1 answered a read with a transient
// refusal a wait window may outlast (#763): the `unavailable` code with
// retryable=true, such as a read snapshot that expired while the node was busy
// copying a clone's disk. Inside a --wait window such an answer is one poll's
// bad luck, not a verdict; outside one it still surfaces as exit 13.
func isRetryableL1Answer(err error) bool {
	var response *apiResponseError
	return errors.As(err, &response) && response.APIError.Code == contract.ErrorUnavailable &&
		response.APIError.Retryable
}
