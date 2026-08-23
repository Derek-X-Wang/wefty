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
	return group.DoPrepared(waiter, key, timeout, func() (func(context.Context) (EnsureImageResponse, error), func(), error) {
		return run, func() {}, nil
	})
}

// DoPrepared establishes any leader-owned resource before its waiter may
// cancel. This lets a streamed archive be unlinked by the canceled caller
// while the shared operation safely continues through its already-open file.
func (group *imageOperationGroup) DoPrepared(
	waiter context.Context,
	key imageOperationKey,
	timeout time.Duration,
	prepare func() (func(context.Context) (EnsureImageResponse, error), func(), error),
) (EnsureImageResponse, error) {
	group.mu.Lock()
	flight := group.flights[key]
	if flight == nil {
		run, cleanup, err := prepare()
		if err != nil {
			group.mu.Unlock()
			return EnsureImageResponse{}, err
		}
		operationContext, cancel := context.WithTimeout(context.Background(), timeout)
		flight = &imageOperationFlight{done: make(chan struct{}), cancel: cancel}
		group.flights[key] = flight
		go func() {
			defer cleanup()
			flight.result, flight.err = run(operationContext)
			cancel()
			close(flight.done)
			group.mu.Lock()
			if flight.waiters == 0 && group.flights[key] == flight {
				delete(group.flights, key)
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
		group.detach(key, flight)
		return result, err
	}
}

func (group *imageOperationGroup) detach(key imageOperationKey, flight *imageOperationFlight) {
	group.mu.Lock()
	defer group.mu.Unlock()
	flight.waiters--
	if flight.waiters == 0 {
		select {
		case <-flight.done:
			if group.flights[key] == flight {
				delete(group.flights, key)
			}
		default:
		}
	}
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
