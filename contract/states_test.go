package contract

import "testing"

func TestStateTransitionTablesCoverAllStates(t *testing.T) {
	t.Parallel()

	assertStates(t, JobTransitions, []JobState{
		JobQueued, JobClaimed, JobRunning, JobAwaitingInput, JobSucceeded, JobFailed,
	})
	assertStates(t, AttemptTransitions, []AttemptState{
		AttemptClaimed, AttemptRunning, AttemptAwaitingInput, AttemptSucceeded, AttemptFailed, AttemptLost,
	})
	assertStates(t, NodeTransitions, []NodeState{
		NodeAlive, NodeStale, NodeDraining, NodeDead,
	})
	assertStates(t, RunTransitions, []RunState{
		RunPending, RunDispatching, RunQueued, RunRunning, RunAwaitingInput, RunSucceeded, RunFailed,
	})
}

func TestStateTransitionRules(t *testing.T) {
	t.Parallel()

	if !CanTransition(JobTransitions, JobRunning, JobAwaitingInput) {
		t.Fatal("running job must be able to enter reserved awaiting-input")
	}
	if !CanTransition(AttemptTransitions, AttemptRunning, AttemptLost) {
		t.Fatal("running attempt must become lost on lease expiry")
	}
	if CanTransition(AttemptTransitions, AttemptLost, AttemptClaimed) {
		t.Fatal("lost is terminal; v0.1 never requeues an attempt")
	}
	if CanTransition(JobTransitions, JobSucceeded, JobRunning) {
		t.Fatal("succeeded job must be terminal")
	}
}

func assertStates[S ~string](t *testing.T, table map[S][]S, states []S) {
	t.Helper()

	if len(table) != len(states) {
		t.Fatalf("transition table has %d states, want %d", len(table), len(states))
	}
	known := make(map[S]bool, len(states))
	for _, state := range states {
		known[state] = true
		if _, ok := table[state]; !ok {
			t.Errorf("missing state %q", state)
		}
	}
	for from, destinations := range table {
		if !known[from] {
			t.Errorf("unknown source state %q", from)
		}
		for _, to := range destinations {
			if !known[to] {
				t.Errorf("%q transitions to unknown state %q", from, to)
			}
		}
	}
}
