package ocihelper

import (
	"context"
	"sync"
	"time"
)

type imageOperationGroup struct {
	mu      sync.Mutex
	flights map[imageOperationKey]*imageOperationFlight
}

type imageOperationKey struct {
	Namespace   string
	Digest      string
	Platform    string
	Snapshotter string
}

type imageOperationFlight struct {
	done    chan struct{}
	cancel  context.CancelFunc
	result  EnsureImageResponse
	err     error
	waiters int
	cleanup func()
	closed  bool
}

func newImageOperationGroup() *imageOperationGroup {
	return &imageOperationGroup{flights: make(map[imageOperationKey]*imageOperationFlight)}
}

// Do shares one mechanics operation without inheriting the first waiter's
// cancellation. Operation timeout is caller policy carried into the helper;
// CancelAll remains the helper-session authority boundary.
func (group *imageOperationGroup) Do(
	waiter context.Context,
	key imageOperationKey,
	timeout time.Duration,
	run func(context.Context) (EnsureImageResponse, error),
) (EnsureImageResponse, error) {
	return group.DoPreparedWithAttach(waiter, key, timeout, func(context.Context) (func(context.Context) (EnsureImageResponse, error), func(), error) {
		return run, func() {}, nil
	}, nil)
}

// DoPrepared establishes any leader-owned resource before its waiter may
// cancel. This lets a streamed archive be unlinked by the canceled caller
// while the shared operation safely continues through its already-open file.
func (group *imageOperationGroup) DoPrepared(
	waiter context.Context,
	key imageOperationKey,
	timeout time.Duration,
	prepare func(context.Context) (func(context.Context) (EnsureImageResponse, error), func(), error),
) (EnsureImageResponse, error) {
	return group.DoPreparedWithAttach(waiter, key, timeout, prepare, nil)
}

// DoPreparedWithAttach keeps the operation lease alive until every waiter
// that observed success has attached its own longer-lived hold.
func (group *imageOperationGroup) DoPreparedWithAttach(
	waiter context.Context,
	key imageOperationKey,
	timeout time.Duration,
	prepare func(context.Context) (func(context.Context) (EnsureImageResponse, error), func(), error),
	attach func(context.Context, EnsureImageResponse) error,
) (EnsureImageResponse, error) {
	group.mu.Lock()
	flight := group.flights[key]
	if flight == nil {
		operationContext, cancel := context.WithTimeout(context.Background(), timeout)
		flight = &imageOperationFlight{done: make(chan struct{}), cancel: cancel}
		group.flights[key] = flight
		go func() {
			run, cleanup, prepareErr := prepare(operationContext)
			var result EnsureImageResponse
			runErr := prepareErr
			if prepareErr == nil {
				result, runErr = run(operationContext)
			}
			cancel()
			group.mu.Lock()
			flight.result = result
			flight.err = runErr
			flight.cleanup = cleanup
			flight.closed = true
			close(flight.done)
			if flight.waiters == 0 && group.flights[key] == flight {
				delete(group.flights, key)
				cleanup = flight.cleanup
				flight.cleanup = nil
				group.mu.Unlock()
				if cleanup != nil {
					cleanup()
				}
				return
			}
			group.mu.Unlock()
		}()
	}
	flight.waiters++
	group.mu.Unlock()

	select {
	case <-waiter.Done():
		group.detach(key, flight)
		return EnsureImageResponse{}, waiter.Err()
	case <-flight.done:
		result, err := flight.result, flight.err
		if err == nil && attach != nil {
			err = attach(waiter, result)
		}
		group.detach(key, flight)
		return result, err
	}
}

func (group *imageOperationGroup) detach(key imageOperationKey, flight *imageOperationFlight) {
	group.mu.Lock()
	flight.waiters--
	var cleanup func()
	if flight.waiters == 0 && flight.closed && group.flights[key] == flight {
		delete(group.flights, key)
		cleanup = flight.cleanup
		flight.cleanup = nil
	}
	group.mu.Unlock()
	if cleanup != nil {
		cleanup()
	}
}

func (group *imageOperationGroup) ActiveKeys() map[imageOperationKey]struct{} {
	if group == nil {
		return nil
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	active := make(map[imageOperationKey]struct{}, len(group.flights))
	for key := range group.flights {
		active[key] = struct{}{}
	}
	return active
}

func (group *imageOperationGroup) CancelAll(ctx context.Context) error {
	group.mu.Lock()
	flights := make([]*imageOperationFlight, 0, len(group.flights))
	for _, flight := range group.flights {
		flight.cancel()
		flights = append(flights, flight)
	}
	group.mu.Unlock()
	for _, flight := range flights {
		select {
		case <-flight.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
