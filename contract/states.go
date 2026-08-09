package contract

type JobState string

const (
	JobQueued        JobState = "queued"
	JobClaimed       JobState = "claimed"
	JobRunning       JobState = "running"
	JobAwaitingInput JobState = "awaiting-input"
	JobSucceeded     JobState = "succeeded"
	JobFailed        JobState = "failed"
)

var JobTransitions = map[JobState][]JobState{
	JobQueued:        {JobClaimed, JobFailed},
	JobClaimed:       {JobRunning, JobFailed},
	JobRunning:       {JobAwaitingInput, JobSucceeded, JobFailed},
	JobAwaitingInput: {JobRunning, JobFailed},
	JobSucceeded:     {},
	JobFailed:        {},
}

type AttemptState string

const (
	AttemptClaimed       AttemptState = "claimed"
	AttemptRunning       AttemptState = "running"
	AttemptAwaitingInput AttemptState = "awaiting-input"
	AttemptSucceeded     AttemptState = "succeeded"
	AttemptFailed        AttemptState = "failed"
	AttemptLost          AttemptState = "lost"
)

var AttemptTransitions = map[AttemptState][]AttemptState{
	AttemptClaimed:       {AttemptRunning, AttemptFailed, AttemptLost},
	AttemptRunning:       {AttemptAwaitingInput, AttemptSucceeded, AttemptFailed, AttemptLost},
	AttemptAwaitingInput: {AttemptRunning, AttemptFailed, AttemptLost},
	AttemptSucceeded:     {},
	AttemptFailed:        {},
	AttemptLost:          {},
}

type NodeState string

const (
	NodeAlive    NodeState = "alive"
	NodeStale    NodeState = "stale"
	NodeDraining NodeState = "draining"
	NodeDead     NodeState = "dead"
)

var NodeTransitions = map[NodeState][]NodeState{
	NodeAlive:    {NodeStale, NodeDraining, NodeDead},
	NodeStale:    {NodeAlive, NodeDraining, NodeDead},
	NodeDraining: {NodeDead},
	NodeDead:     {NodeAlive},
}

type RunState string

const (
	RunPending       RunState = "pending"
	RunDispatching   RunState = "dispatching"
	RunQueued        RunState = "queued"
	RunRunning       RunState = "running"
	RunAwaitingInput RunState = "awaiting-input"
	RunSucceeded     RunState = "succeeded"
	RunFailed        RunState = "failed"
)

var RunTransitions = map[RunState][]RunState{
	RunPending:       {RunDispatching, RunFailed},
	RunDispatching:   {RunQueued, RunFailed},
	RunQueued:        {RunRunning, RunFailed},
	RunRunning:       {RunAwaitingInput, RunSucceeded, RunFailed},
	RunAwaitingInput: {RunRunning, RunFailed},
	RunSucceeded:     {},
	RunFailed:        {},
}

func CanTransition[S ~string](table map[S][]S, from, to S) bool {
	for _, candidate := range table[from] {
		if candidate == to {
			return true
		}
	}
	return false
}
