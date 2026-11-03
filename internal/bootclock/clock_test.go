package bootclock

import (
	"errors"
	"testing"
)

func TestElapsedClockAdvances(t *testing.T) {
	a, err := Now()
	if err != nil {
		t.Fatal(err)
	}
	b, err := Now()
	if err != nil {
		t.Fatal(err)
	}
	if b < a || a <= 0 {
		t.Fatalf("invalid elapsed clock: %v -> %v", a, b)
	}
}

func TestAwakeClockAdvances(t *testing.T) {
	a, err := AwakeNow()
	if errors.Is(err, ErrAwakeUnavailable) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	b, err := AwakeNow()
	if err != nil {
		t.Fatal(err)
	}
	if b < a || a <= 0 {
		t.Fatalf("invalid awake clock: %v -> %v", a, b)
	}
}
