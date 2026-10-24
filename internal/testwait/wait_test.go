package testwait

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestReceiveReturnsBufferedValue(t *testing.T) {
	ch := make(chan string, 1)
	ch <- "retained result"
	if got := Receive(t, "buffered result", ch); got != "retained result" {
		t.Fatalf("received %q, want retained result", got)
	}
}

func TestReceiveReturnsOnClosedChannel(t *testing.T) {
	ch := make(chan struct{})
	close(ch)
	Receive(t, "closed channel", ch)
}

func TestReceiveTimeoutNamesObservation(t *testing.T) {
	defer func() {
		failure := recover()
		message, ok := failure.(string)
		if !ok || !strings.Contains(message, "timed out waiting for runner start") {
			t.Fatalf("timeout failure = %v, want runner start diagnostic", failure)
		}
	}()
	receive[int](receiveFailure{}, "runner start", make(chan int), time.Millisecond)
}

// Embed testing.TB so the fake follows its private interface; only the methods
// used by receive are overridden. Panic keeps Fatalf from returning, like testing.T.
type receiveFailure struct{ testing.TB }

func (receiveFailure) Helper() {}
func (receiveFailure) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

func TestUntilReturnsOnFirstReadyObservation(t *testing.T) {
	calls := 0
	Until(t, "ready state", func() (bool, error) {
		calls++
		return true, nil
	})
	if calls != 1 {
		t.Fatalf("observations = %d, want 1", calls)
	}
}

func TestUntilRetriesFalseAndErrors(t *testing.T) {
	calls := 0
	Until(t, "retained result", func() (bool, error) {
		calls++
		switch calls {
		case 1:
			return false, errors.New("result unavailable")
		case 2:
			return false, nil
		case 3:
			return true, errors.New("observation failed")
		default:
			return true, nil
		}
	})
	if calls != 4 {
		t.Fatalf("observations = %d, want 4", calls)
	}
}
