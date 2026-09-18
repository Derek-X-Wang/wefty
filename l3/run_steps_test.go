package l3

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

var stepBase = time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)

// stepEnvelope builds one step bracket as the agent publishes it.
func stepEnvelope(id, name, status string, offset time.Duration) contract.Envelope {
	extensions, err := json.Marshal(map[string]any{
		mailboxExtensionNamespace: map[string]any{"kind": "step", "step_status": status, "name": name},
	})
	if err != nil {
		panic(err)
	}
	return contract.Envelope{
		EnvelopeID: id, StepID: name, Status: contract.EnvelopePartial,
		Extensions: extensions, CreatedAt: stepBase.Add(offset),
	}
}

// plainEnvelope is an ordinary envelope: same step ID, same shape, not a
// bracket. Telling these two apart is the whole reason the kind is recorded.
func plainEnvelope(id, name string, offset time.Duration) contract.Envelope {
	extensions, err := json.Marshal(map[string]any{
		mailboxExtensionNamespace: map[string]any{"kind": "envelope", "name": name},
	})
	if err != nil {
		panic(err)
	}
	return contract.Envelope{
		EnvelopeID: id, StepID: name, Status: contract.EnvelopeSucceeded,
		Extensions: extensions, CreatedAt: stepBase.Add(offset),
	}
}

func TestDeriveRunSteps(t *testing.T) {
	t.Parallel()

	second := float64(1)
	twoSeconds := float64(2)

	tests := []struct {
		name      string
		envelopes []contract.Envelope
		current   string
		steps     []RunStep
	}{
		{
			name:  "a run that reported nothing is in no step",
			steps: []RunStep{},
		},
		{
			name: "ordinary envelopes are not brackets",
			envelopes: []contract.Envelope{
				plainEnvelope("e1", "build", 0),
				{EnvelopeID: "e2", StepID: "build", CreatedAt: stepBase},
			},
			steps: []RunStep{},
		},
		{
			name: "a closed step has a duration and leaves no current step",
			envelopes: []contract.Envelope{
				stepEnvelope("e1", "build", "started", 0),
				stepEnvelope("e2", "build", "ended", time.Second),
			},
			steps: []RunStep{{
				Name: "build", StartedAt: stepBase, EndedAt: ptr(stepBase.Add(time.Second)),
				Seconds: &second,
			}},
		},
		{
			// The case the feature exists for: a running workflow.
			name: "an unterminated step is the current one and has no duration",
			envelopes: []contract.Envelope{
				stepEnvelope("e1", "build", "started", 0),
				stepEnvelope("e2", "build", "ended", time.Second),
				stepEnvelope("e3", "test", "started", 2*time.Second),
			},
			current: "test",
			steps: []RunStep{
				{Name: "build", StartedAt: stepBase, EndedAt: ptr(stepBase.Add(time.Second)), Seconds: &second},
				{Name: "test", StartedAt: stepBase.Add(2 * time.Second), Open: true},
			},
		},
		{
			// A workflow that brackets an outer step around inner ones has
			// several open at once; "where is it now" is the innermost.
			name: "the current step is the most recently started open one",
			envelopes: []contract.Envelope{
				stepEnvelope("e1", "gates", "started", 0),
				stepEnvelope("e2", "vet", "started", time.Second),
			},
			current: "vet",
			steps: []RunStep{
				{Name: "gates", StartedAt: stepBase, Open: true},
				{Name: "vet", StartedAt: stepBase.Add(time.Second), Open: true},
			},
		},
		{
			// Closing the inner step returns the reader to the outer one,
			// rather than to no step at all.
			name: "closing an inner step leaves the outer one current",
			envelopes: []contract.Envelope{
				stepEnvelope("e1", "gates", "started", 0),
				stepEnvelope("e2", "vet", "started", time.Second),
				stepEnvelope("e3", "vet", "ended", 2*time.Second),
			},
			current: "gates",
			steps: []RunStep{
				{Name: "gates", StartedAt: stepBase, Open: true},
				{Name: "vet", StartedAt: stepBase.Add(time.Second),
					EndedAt: ptr(stepBase.Add(2 * time.Second)), Seconds: &second},
			},
		},
		{
			// A workflow that runs the same step twice ran it twice.
			name: "a repeated name is two intervals, not one replaced",
			envelopes: []contract.Envelope{
				stepEnvelope("e1", "test", "started", 0),
				stepEnvelope("e2", "test", "ended", time.Second),
				stepEnvelope("e3", "test", "started", 2*time.Second),
				stepEnvelope("e4", "test", "ended", 4*time.Second),
			},
			steps: []RunStep{
				{Name: "test", StartedAt: stepBase, EndedAt: ptr(stepBase.Add(time.Second)), Seconds: &second},
				{Name: "test", StartedAt: stepBase.Add(2 * time.Second),
					EndedAt: ptr(stepBase.Add(4 * time.Second)), Seconds: &twoSeconds},
			},
		},
		{
			// An end closes the most recent open interval of that name, so two
			// concurrent runs of one step close in the order a reader expects.
			name: "an end closes the most recent open interval of its name",
			envelopes: []contract.Envelope{
				stepEnvelope("e1", "test", "started", 0),
				stepEnvelope("e2", "test", "started", time.Second),
				stepEnvelope("e3", "test", "ended", 2*time.Second),
			},
			current: "test",
			steps: []RunStep{
				{Name: "test", StartedAt: stepBase, Open: true},
				{Name: "test", StartedAt: stepBase.Add(time.Second),
					EndedAt: ptr(stepBase.Add(2 * time.Second)), Seconds: &second},
			},
		},
		{
			// Publication is a sweep, so arrival order is not run order.
			name: "out-of-order arrival is ordered by the run's own clock",
			envelopes: []contract.Envelope{
				stepEnvelope("e3", "test", "started", 2*time.Second),
				stepEnvelope("e1", "build", "started", 0),
				stepEnvelope("e2", "build", "ended", time.Second),
			},
			current: "test",
			steps: []RunStep{
				{Name: "build", StartedAt: stepBase, EndedAt: ptr(stepBase.Add(time.Second)), Seconds: &second},
				{Name: "test", StartedAt: stepBase.Add(2 * time.Second), Open: true},
			},
		},
		{
			// A step that ended without the ledger ever seeing it begin is not
			// a zero-length interval; it is nothing to measure.
			name: "an end with no start is ignored rather than invented",
			envelopes: []contract.Envelope{
				stepEnvelope("e1", "test", "ended", time.Second),
			},
			steps: []RunStep{},
		},
		{
			name: "a second end for one start closes nothing further",
			envelopes: []contract.Envelope{
				stepEnvelope("e1", "test", "started", 0),
				stepEnvelope("e2", "test", "ended", time.Second),
				stepEnvelope("e3", "test", "ended", 2*time.Second),
			},
			steps: []RunStep{
				{Name: "test", StartedAt: stepBase, EndedAt: ptr(stepBase.Add(time.Second)), Seconds: &second},
			},
		},
		{
			// Two events stamped in the same nanosecond still have one order.
			name: "identical timestamps are broken by envelope ID",
			envelopes: []contract.Envelope{
				stepEnvelope("e2", "build", "ended", 0),
				stepEnvelope("e1", "build", "started", 0),
			},
			steps: []RunStep{{
				Name: "build", StartedAt: stepBase, EndedAt: ptr(stepBase), Seconds: ptr(float64(0)),
			}},
		},
		{
			// A workload's clock is the workload's. An end stamped before its
			// own start sorts first, finds nothing open, and is ignored -- so
			// the step reads as still running rather than as an interval of
			// negative length. No duration here is ever negative, because the
			// clock that orders the events is the one that measures them.
			name: "a backwards clock leaves the step open rather than negative",
			envelopes: []contract.Envelope{
				stepEnvelope("e1", "build", "started", time.Second),
				stepEnvelope("e2", "build", "ended", 0),
			},
			current: "build",
			steps: []RunStep{{
				Name: "build", StartedAt: stepBase.Add(time.Second), Open: true,
			}},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			derived := DeriveRunSteps(test.envelopes)
			if derived.Current != test.current {
				t.Fatalf("current step = %q, want %q", derived.Current, test.current)
			}
			if len(derived.Steps) != len(test.steps) {
				t.Fatalf("derived %d steps, want %d: %#v", len(derived.Steps), len(test.steps), derived.Steps)
			}
			for index, want := range test.steps {
				got := derived.Steps[index]
				if got.Name != want.Name || !got.StartedAt.Equal(want.StartedAt) || got.Open != want.Open {
					t.Fatalf("step %d = %#v, want %#v", index, got, want)
				}
				switch {
				case want.EndedAt == nil && got.EndedAt != nil:
					t.Fatalf("step %d ended at %v, want still open", index, got.EndedAt)
				case want.EndedAt != nil && got.EndedAt == nil:
					t.Fatalf("step %d has no end, want %v", index, want.EndedAt)
				case want.EndedAt != nil && !got.EndedAt.Equal(*want.EndedAt):
					t.Fatalf("step %d ended at %v, want %v", index, got.EndedAt, want.EndedAt)
				}
				switch {
				case want.Seconds == nil && got.Seconds != nil:
					t.Fatalf("an open step %d reported a duration of %v", index, *got.Seconds)
				case want.Seconds != nil && got.Seconds == nil:
					t.Fatalf("step %d has no duration, want %v", index, *want.Seconds)
				case want.Seconds != nil && *got.Seconds != *want.Seconds:
					t.Fatalf("step %d lasted %v, want %v", index, *got.Seconds, *want.Seconds)
				}
			}
		})
	}
}

func ptr[T any](value T) *T { return &value }
