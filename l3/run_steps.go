package l3

import (
	"encoding/json"
	"slices"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// A run's steps are derived, never stored.
//
// The ledger keeps envelopes. Some of those envelopes are the brackets a
// workload writes with `wefty run step --name NAME [--end]`, and a pair of them
// -- a start and its end -- is an interval with a name and a duration. That
// interval is what an operator means by "where is this run right now", and it
// is the only such answer the system can give without asking the workload to
// report anything it does not already report.
//
// Derivation lives here, in one function, for a reason worth stating: it is a
// reading of an append-only log that arrives out of order, can repeat a name,
// can overlap, and can simply stop. Every one of those is a real shape a real
// workflow produces, and each needs a decided answer rather than whatever the
// first caller's loop happened to do.
//
// The vocabulary is "step", not "phase". CONTEXT.md ratified `Step` and lists
// `phase` among the words it is not to be called.

// RunStep is one named interval within a run.
type RunStep struct {
	Name      string     `json:"name"`
	StartedAt time.Time  `json:"started_at"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
	// Seconds is the interval's duration, present only once it has one. A step
	// that is still running has no duration, and reporting the time so far as
	// though it were one would be a different claim.
	Seconds *float64 `json:"seconds,omitempty"`
	// Open reports a step that started and has not ended.
	Open bool `json:"open"`
}

// RunSteps is what a reader is told about a run's progress.
type RunSteps struct {
	// Current is the run's current step: the most recently started step that
	// has not ended. It is empty for a run that has reported no step, and for
	// one whose steps have all ended -- which is a real state, not a gap, and
	// must not be filled in with the last step that finished.
	Current string    `json:"current,omitempty"`
	Steps   []RunStep `json:"steps,omitempty"`
}

// mailboxExtension is the one namespace a run mailbox event's envelope carries.
type mailboxExtension struct {
	Kind       string `json:"kind"`
	StepStatus string `json:"step_status"`
	Name       string `json:"name"`
}

const (
	mailboxExtensionNamespace = "dev.wefty.mailbox"
	mailboxKindStep           = "step"
	mailboxStepStarted        = "started"
	mailboxStepEnded          = "ended"
)

// DeriveRunSteps reads a run's envelopes and returns its step intervals in the
// order they started, plus the step it is currently in.
//
// The rules, each chosen because the alternative loses information:
//
//   - Order is the envelope's own creation time, with the envelope ID breaking
//     ties. Envelopes are published in sweeps and can arrive out of order, so
//     arrival order is not the run's order.
//   - A start always opens an interval. A repeated name opens a second one
//     rather than replacing the first, because a workflow that runs the same
//     step twice ran it twice.
//   - An end closes the most recent still-open interval of that name, so
//     nested or interleaved steps close in the order a reader expects.
//   - An end with no open start of that name is ignored. It says a step ended
//     and the ledger never saw it begin; inventing a zero-length interval would
//     put a duration on something nobody measured.
//   - Several steps may be open at once. The current one is the most recently
//     started of them, which is what "where is it now" means when a workflow
//     brackets an outer step around inner ones.
//
// Envelopes that are not step brackets are ignored entirely, including every
// envelope written before the agent recorded the event kind: a run whose
// evidence cannot say where it is reports no steps rather than a guess.
func DeriveRunSteps(envelopes []contract.Envelope) RunSteps {
	ordered := slices.Clone(envelopes)
	slices.SortStableFunc(ordered, func(a, b contract.Envelope) int {
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.Compare(b.CreatedAt)
		}
		switch {
		case a.EnvelopeID < b.EnvelopeID:
			return -1
		case a.EnvelopeID > b.EnvelopeID:
			return 1
		default:
			return 0
		}
	})

	steps := make([]RunStep, 0, len(ordered))
	// open maps a step name to the indexes of its still-open intervals, most
	// recent last.
	open := map[string][]int{}
	for _, envelope := range ordered {
		extension, ok := stepBracket(envelope)
		if !ok {
			continue
		}
		name := extension.Name
		if name == "" {
			name = envelope.StepID
		}
		if name == "" {
			continue
		}
		switch extension.StepStatus {
		case mailboxStepStarted:
			steps = append(steps, RunStep{Name: name, StartedAt: envelope.CreatedAt, Open: true})
			open[name] = append(open[name], len(steps)-1)
		case mailboxStepEnded:
			indexes := open[name]
			if len(indexes) == 0 {
				continue
			}
			index := indexes[len(indexes)-1]
			open[name] = indexes[:len(indexes)-1]
			ended := envelope.CreatedAt
			steps[index].EndedAt = &ended
			steps[index].Open = false
			// Never negative: ordering is by the same clock the interval is
			// measured with, so an end that precedes its start sorts before it
			// and is ignored above for having nothing open to close.
			seconds := ended.Sub(steps[index].StartedAt).Seconds()
			steps[index].Seconds = &seconds
		}
	}

	derived := RunSteps{Steps: steps}
	for _, step := range steps {
		if step.Open {
			derived.Current = step.Name
		}
	}
	return derived
}

// stepBracket reports whether one envelope is a step bracket, reading the
// mailbox's own extension namespace rather than guessing from a summary a
// workload is free to replace.
func stepBracket(envelope contract.Envelope) (mailboxExtension, bool) {
	if len(envelope.Extensions) == 0 {
		return mailboxExtension{}, false
	}
	var namespaces map[string]json.RawMessage
	if err := json.Unmarshal(envelope.Extensions, &namespaces); err != nil {
		return mailboxExtension{}, false
	}
	body, present := namespaces[mailboxExtensionNamespace]
	if !present {
		return mailboxExtension{}, false
	}
	var extension mailboxExtension
	if err := json.Unmarshal(body, &extension); err != nil {
		return mailboxExtension{}, false
	}
	if extension.Kind != mailboxKindStep {
		return mailboxExtension{}, false
	}
	if extension.StepStatus != mailboxStepStarted && extension.StepStatus != mailboxStepEnded {
		return mailboxExtension{}, false
	}
	return extension, true
}
