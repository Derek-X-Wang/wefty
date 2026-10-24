// Package testwait provides load-tolerant waits for tests. Production code must not import it.
package testwait

import (
	"testing"
	"time"
)

// Bound gives busy suite runs room to observe an event that holds once reached.
const Bound = 30 * time.Second

// Receive waits for a closed channel or a buffered result that stays readable.
// Do not use it for absence windows or bounds that are part of a timing contract.
func Receive[T any](t testing.TB, what string, ch <-chan T) T {
	t.Helper()
	return receive(t, what, ch, Bound)
}

func receive[T any](t testing.TB, what string, ch <-chan T, bound time.Duration) T {
	t.Helper()
	timer := time.NewTimer(bound)
	defer timer.Stop()
	select {
	case value := <-ch:
		return value
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

// Until waits for a state that holds once reached. A false result may include an
// error describing the last observation; errors are retried until the bound.
// The shared 30-second bound gives busy suite runs room without slowing success.
func Until(t testing.TB, what string, observe func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(Bound)
	for {
		ready, err := observe()
		if ready && err == nil {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("timed out waiting for %s: last observed ready=%t error=%v", what, ready, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
