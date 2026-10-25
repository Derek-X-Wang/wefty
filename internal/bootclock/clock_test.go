package bootclock

import "testing"

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
