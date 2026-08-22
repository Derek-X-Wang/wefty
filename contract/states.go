package contract

type JobState string

const (
	JobQueued                     JobState = "queued"
	JobClaimed                    JobState = "claimed"
	JobRunning                    JobState = "running"
	JobStopping                   JobState = "stopping"
	JobStopped                    JobState = "stopped"
	JobAwaitingInput              JobState = "awaiting-input"
	JobSucceeded                  JobState = "succeeded"
	JobFailed                     JobState = "failed"
	JobRemovalPending             JobState = "removal_pending"
	JobAgentCleaned               JobState = "agent_cleaned"
	JobRemovedVerified            JobState = "removed_verified"
	JobForgottenCleanupUnverified JobState = "forgotten_cleanup_unverified"
)

var JobTransitions = map[JobState][]JobState{
	JobQueued:                     {JobClaimed, JobFailed},
	JobClaimed:                    {JobRunning, JobQueued, JobFailed},
	JobRunning:                    {JobAwaitingInput, JobSucceeded, JobFailed},
	JobStopping:                   {},
	JobStopped:                    {},
	JobAwaitingInput:              {JobRunning, JobFailed},
	JobSucceeded:                  {},
	JobFailed:                     {},
	JobRemovalPending:             {},
	JobAgentCleaned:               {},
	JobRemovedVerified:            {},
	JobForgottenCleanupUnverified: {},
}

// ServiceJobTransitions is the observed-state machine for service-class jobs.
// It is deliberately separate from JobTransitions: automatic requeue and
// operator restart are service lifecycle semantics and must never make a
// one-shot terminal state resumable.
var ServiceJobTransitions = map[JobState][]JobState{
	JobQueued:                     {JobClaimed, JobStopped, JobFailed, JobRemovalPending},
	JobClaimed:                    {JobRunning, JobStopping, JobQueued, JobFailed, JobRemovalPending},
	JobRunning:                    {JobStopping, JobQueued, JobFailed, JobRemovalPending},
	JobStopping:                   {JobStopped, JobFailed, JobRemovalPending},
	JobStopped:                    {JobQueued, JobRemovalPending},
	JobFailed:                     {JobQueued, JobRemovalPending},
	JobRemovalPending:             {JobAgentCleaned, JobForgottenCleanupUnverified},
	JobAgentCleaned:               {JobRemovedVerified, JobForgottenCleanupUnverified},
	JobRemovedVerified:            {},
	JobForgottenCleanupUnverified: {},
}

type ServiceDesiredState string

const (
	ServiceDesiredRunning ServiceDesiredState = "running"
	ServiceDesiredStopped ServiceDesiredState = "stopped"
	ServiceDesiredRemoved ServiceDesiredState = "removed"
)

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
