package testwait

import (
	"errors"
	"testing"
)

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
