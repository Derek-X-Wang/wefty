package process

import (
	"testing"
	"time"
)

func TestTerminationGraceTime(t *testing.T) {
	for _, grace := range []time.Duration{0, time.Second, 4 * time.Second, 10 * time.Second} {
		runner := New(Config{TerminationGraceTime: grace})
		want := grace
		if want == 0 {
			want = DefaultTerminationGraceTime
		}
		if got := NewAdapter(runner).TerminationGraceTime(); got != want {
			t.Errorf("configured grace %s: adapter grace %s, want %s", grace, got, want)
		}
	}
}
