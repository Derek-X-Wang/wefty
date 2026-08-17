package agent

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestPublicationControllerSerializesAbsoluteStateWithAsymmetricHysteresis(t *testing.T) {
	assertPublicationControllerSerializesAbsoluteStateWithAsymmetricHysteresis(t)
}

func assertPublicationControllerSerializesAbsoluteStateWithAsymmetricHysteresis(t *testing.T) {
	t.Helper()
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	actions := make(chan string, 16)
	controller := newPublicationController(
		clock,
		DefaultPublicationRecoveryWindow,
		DefaultPublicationRetryInterval,
		func(_ context.Context, ready bool) error {
			actions <- "publish:" + boolString(ready)
			return nil
		},
		func(ready bool) { actions <- "forward:" + boolString(ready) },
	)
	done := make(chan error, 1)
	go func() { done <- controller.Run(context.Background()) }()
	wantNoPublicationAction(t, actions)

	controller.Observe(true)
	wantPublicationAction(t, actions, "publish:true")
	wantPublicationAction(t, actions, "forward:true")
	controller.Observe(true)
	wantNoPublicationAction(t, actions)

	controller.Observe(false)
	wantPublicationAction(t, actions, "forward:false")
	wantPublicationAction(t, actions, "publish:false")
	controller.Observe(false)
	wantNoPublicationAction(t, actions)

	controller.Observe(true)
	firstRecoveryDeadline := clock.Now().Add(DefaultPublicationRecoveryWindow)
	clock.waitForDeadline(t, firstRecoveryDeadline)
	clock.Advance(9 * time.Second)
	wantNoPublicationAction(t, actions)

	controller.Observe(false)
	wantPublicationAction(t, actions, "forward:false")
	controller.Observe(true)
	resetRecoveryDeadline := clock.Now().Add(DefaultPublicationRecoveryWindow)
	clock.waitForDeadline(t, resetRecoveryDeadline)
	clock.Advance(time.Second)
	wantNoPublicationAction(t, actions)
	clock.Advance(9 * time.Second)
	wantPublicationAction(t, actions, "publish:true")
	wantPublicationAction(t, actions, "forward:true")
	wantNoPublicationAction(t, actions)

	controller.Stop()
	wantPublicationAction(t, actions, "forward:false")
	wantPublicationAction(t, actions, "publish:false")
	if err := waitPublicationDone(t, done); err != nil {
		t.Fatalf("publication controller stop: %v", err)
	}
}

func TestPublicationControllerNeverHasTwoRequestsInFlight(t *testing.T) {
	assertPublicationControllerNeverHasTwoRequestsInFlight(t)
}

func assertPublicationControllerNeverHasTwoRequestsInFlight(t *testing.T) {
	t.Helper()
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	firstRelease := make(chan struct{})
	requests := make(chan bool, 4)
	var calls atomic.Int32
	var active atomic.Int32
	var maximum atomic.Int32
	controller := newPublicationController(
		clock,
		DefaultPublicationRecoveryWindow,
		DefaultPublicationRetryInterval,
		func(_ context.Context, ready bool) error {
			call := calls.Add(1)
			current := active.Add(1)
			for current > maximum.Load() && !maximum.CompareAndSwap(maximum.Load(), current) {
			}
			defer active.Add(-1)
			requests <- ready
			if call == 1 {
				<-firstRelease
			}
			return nil
		},
		nil,
	)
	done := make(chan error, 1)
	go func() { done <- controller.Run(context.Background()) }()

	controller.Observe(true)
	if ready := waitPublicationRequest(t, requests); !ready {
		t.Fatal("initial publication was not absolute ready=true")
	}
	controller.Observe(false)
	controller.Observe(true)
	wantNoPublicationRequest(t, requests)
	close(firstRelease)
	if ready := waitPublicationRequest(t, requests); ready {
		t.Fatal("withdrawal queued behind in-flight publish was not absolute ready=false")
	}
	if maximum.Load() != 1 {
		t.Fatalf("maximum concurrent publication requests = %d, want 1", maximum.Load())
	}

	controller.Stop()
	if err := waitPublicationDone(t, done); err != nil {
		t.Fatalf("publication controller stop: %v", err)
	}
}

func TestPublicationControllerRetriesTransientAndStopsOnAuthorityLoss(t *testing.T) {
	assertPublicationControllerRetriesTransientAndStopsOnAuthorityLoss(t)
}

func assertPublicationControllerRetriesTransientAndStopsOnAuthorityLoss(t *testing.T) {
	t.Helper()
	t.Run("transient uncertainty", func(t *testing.T) {
		clock := newManualClock(time.Unix(1_700_000_000, 0))
		attempts := make(chan int, 4)
		forwarded := make(chan bool, 2)
		var calls atomic.Int32
		controller := newPublicationController(
			clock,
			DefaultPublicationRecoveryWindow,
			DefaultPublicationRetryInterval,
			func(_ context.Context, _ bool) error {
				call := int(calls.Add(1))
				attempts <- call
				switch call {
				case 1:
					return errors.New("transport uncertainty")
				case 2:
					return &ProtocolError{
						StatusCode: http.StatusServiceUnavailable,
						APIError:   contract.APIError{Code: contract.ErrorInternal},
					}
				default:
					return nil
				}
			},
			func(ready bool) { forwarded <- ready },
		)
		done := make(chan error, 1)
		go func() { done <- controller.Run(context.Background()) }()
		controller.Observe(true)
		wantPublicationAttempt(t, attempts, 1)
		clock.waitForDeadline(t, clock.Now().Add(DefaultPublicationRetryInterval))
		clock.Advance(DefaultPublicationRetryInterval)
		wantPublicationAttempt(t, attempts, 2)
		clock.waitForDeadline(t, clock.Now().Add(DefaultPublicationRetryInterval))
		clock.Advance(DefaultPublicationRetryInterval)
		wantPublicationAttempt(t, attempts, 3)
		if ready := waitPublicationRequest(t, forwarded); !ready {
			t.Fatal("successful retry did not enable forwarding")
		}
		controller.Stop()
		if ready := waitPublicationRequest(t, forwarded); ready {
			t.Fatal("stop did not disable forwarding")
		}
		if err := waitPublicationDone(t, done); err != nil {
			t.Fatalf("publication controller stop: %v", err)
		}
	})

	t.Run("attempt authority", func(t *testing.T) {
		forwarded := make(chan bool, 2)
		controller := newPublicationController(
			systemClock{},
			DefaultPublicationRecoveryWindow,
			DefaultPublicationRetryInterval,
			func(_ context.Context, _ bool) error {
				return &ProtocolError{
					StatusCode: http.StatusConflict,
					APIError:   contract.APIError{Code: contract.ErrorStaleFence},
				}
			},
			func(ready bool) { forwarded <- ready },
		)
		done := make(chan error, 1)
		go func() { done <- controller.Run(context.Background()) }()
		controller.Observe(true)
		err := waitPublicationDone(t, done)
		var routed *routedDestinationError
		if !errors.As(err, &routed) || routed.destination != errorDestinationAttemptAuthority {
			t.Fatalf("authority rejection = %v, want attempt-authority routing", err)
		}
		if ready := waitPublicationRequest(t, forwarded); ready {
			t.Fatal("authority loss did not leave forwarding disabled")
		}
	})
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func wantPublicationAction(t *testing.T, actions <-chan string, want string) {
	t.Helper()
	select {
	case got := <-actions:
		if got != want {
			t.Fatalf("publication action = %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for publication action %q", want)
	}
}

func wantNoPublicationAction(t *testing.T, actions <-chan string) {
	t.Helper()
	select {
	case action := <-actions:
		t.Fatalf("unexpected stable-state publication action %q", action)
	case <-time.After(25 * time.Millisecond):
	}
}

func waitPublicationRequest(t *testing.T, requests <-chan bool) bool {
	t.Helper()
	select {
	case ready := <-requests:
		return ready
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for publication request")
		return false
	}
}

func wantNoPublicationRequest(t *testing.T, requests <-chan bool) {
	t.Helper()
	select {
	case ready := <-requests:
		t.Fatalf("concurrent publication request ready=%t", ready)
	case <-time.After(25 * time.Millisecond):
	}
}

func wantPublicationAttempt(t *testing.T, attempts <-chan int, want int) {
	t.Helper()
	select {
	case got := <-attempts:
		if got != want {
			t.Fatalf("publication attempt = %d, want %d", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for publication attempt %d", want)
	}
}

func waitPublicationDone(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for publication controller")
		return nil
	}
}
