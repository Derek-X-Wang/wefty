package agent

import (
	"errors"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// LifecycleState is the agent-local session state. It deliberately does not
// extend contract.NodeState: node state is control-plane reachability, while
// this state explains what the local daemon is doing about it.
type LifecycleState string

const (
	LifecycleRegistering LifecycleState = "registering"
	LifecycleReady       LifecycleState = "ready"
	LifecycleRejoining   LifecycleState = "rejoining"
	LifecycleQuarantined LifecycleState = "quarantined"
	LifecycleDraining    LifecycleState = "draining"
)

// AttemptLifecycleState is the local state of one attempt owned by the
// daemon. Reaping is intentionally observable because a runner that does not
// return after cancellation must leave the daemon alive but unhealthy.
type AttemptLifecycleState string

const (
	AttemptStarting   AttemptLifecycleState = "starting"
	AttemptPulling    AttemptLifecycleState = "pulling"
	AttemptRunning    AttemptLifecycleState = "running"
	AttemptServing    AttemptLifecycleState = "serving"
	AttemptReaping    AttemptLifecycleState = "reaping"
	AttemptFinalizing AttemptLifecycleState = "finalizing"
)

// SemanticError records the most recent code-bearing protocol rejection.
// Transport and timeout errors have no semantic code and do not overwrite it.
type SemanticError struct {
	Code    contract.ErrorCode `json:"code"`
	Message string             `json:"message"`
	At      time.Time          `json:"at"`
}

// ClassOccupancy reports the local admission count for one workload class.
type ClassOccupancy struct {
	Occupied      int  `json:"occupied"`
	Limit         int  `json:"limit"`
	Overcommitted bool `json:"overcommitted"`
}

// AttemptStatus is the agent-local projection of one resident attempt.
type AttemptStatus struct {
	AttemptID string                `json:"attempt_id"`
	JobID     string                `json:"job_id"`
	Class     string                `json:"class"`
	State     AttemptLifecycleState `json:"state"`
	LastError string                `json:"last_error,omitempty"`
	// StartupSatisfied and Ready apply only to portful service attempts. The
	// former is monotonic; the latter follows current local forwarding.
	StartupSatisfied *bool `json:"startup_satisfied,omitempty"`
	Ready            *bool `json:"ready,omitempty"`
}

// RetainedResultsStatus is what the last accounting pass found in this node's
// process handoff root. It is a measurement and not a budget: nothing enforces
// any of these figures yet, and this slice of #494 deliberately stops at making
// them exist and be right.
//
// Both byte figures are reported because they answer different questions.
// LogicalBytes is what the files hold and what trimming one would recover, with
// a file two runs hard-link counted once. ChargedBytes is what the node is
// really giving up, with a floor under every entry, so that a tree of a million
// empty files -- no logical bytes and a node out of inodes -- is a number
// somebody can see. Neither is derived from the other.
type RetainedResultsStatus struct {
	MeasuredAt time.Time `json:"measured_at"`
	// Runs counts every run this node has a record for, and InFlight how many
	// of those have been admitted and have not finished. An in-flight run's
	// bytes are on the node and are counted below; its results are not
	// retained yet and it has no expiry deadline.
	Runs     int `json:"runs"`
	InFlight int `json:"in_flight"`
	// Entries is every directory entry the pass reached, which is the inode
	// cost the byte figures cannot show on their own.
	Entries      int64 `json:"entries"`
	LogicalBytes int64 `json:"logical_bytes"`
	ChargedBytes int64 `json:"charged_bytes"`
	// QuarantinedRecords counts the runs whose directory the sweep has paused
	// on. They are included in every figure above: "unsafe to delete" is not
	// "excluded from accounting", and excluding them would make quarantine a
	// way to hide storage.
	QuarantinedRecords int `json:"quarantined_records"`
	// Unrecorded counts entries under the handoff root that no record names.
	// They are neither measured nor removed, because they are not this
	// agent's, and a count that keeps growing is the shape of a node quietly
	// filling up with storage it cannot give back.
	Unrecorded int `json:"unrecorded"`
	// Replaced counts subtrees that stopped being the directory this pass was
	// measuring -- moved, or replaced by another -- and were therefore left
	// out of the figures above rather than measured somewhere else.
	Replaced int `json:"replaced"`
	// Truncated counts runs whose tree the pass stopped measuring partway,
	// because it spent its opens budget or the agent began shutting down.
	// Their figures above are short by whatever was under them.
	Truncated int `json:"truncated"`
	// PerRun is each run's own share of the figures above, which is what
	// choosing a run to give up needs. It lives here and nowhere durable: it
	// moves with the files, and a retention record is authority to delete and
	// has to stay what an attempt wrote.
	PerRun []RetainedRunFigures `json:"per_run,omitempty"`
}

// RetainedRunFigures is one run's share of a pass.
//
// The bytes are as the pass saw them, which is not the same as what deleting
// that run would recover: a file two runs hard-link is charged to whichever the
// pass reached first, so giving up the other recovers nothing for it. The node
// remeasures after every deletion rather than subtracting, which is what makes
// that safe to act on.
type RetainedRunFigures struct {
	RunID        string `json:"run_id"`
	Entries      int64  `json:"entries"`
	LogicalBytes int64  `json:"logical_bytes"`
	ChargedBytes int64  `json:"charged_bytes"`
	// Published is the run's own record, carried here so an eviction order can
	// read it without re-reading every record.
	Published bool `json:"published,omitempty"`
	// Truncated says this run's figures are short, so they are a floor rather
	// than a measurement.
	Truncated bool `json:"truncated,omitempty"`
}

// Status is a point-in-time, process-local health projection. SessionBackoff
// is separate from per-attempt failures and class occupancy so an idle daemon
// cannot be confused with one pinned in recovery.
type Status struct {
	State             LifecycleState           `json:"state"`
	SessionBackoff    time.Duration            `json:"session_backoff"`
	LastSemanticError *SemanticError           `json:"last_semantic_error,omitempty"`
	OneShot           ClassOccupancy           `json:"one_shot"`
	Services          ClassOccupancy           `json:"services"`
	Attempts          map[string]AttemptStatus `json:"attempts"`
	// RetainedResults is absent until the first accounting pass has run, which
	// is at startup, so an agent that reports none has not measured rather
	// than measured nothing.
	RetainedResults *RetainedResultsStatus `json:"retained_results,omitempty"`
}

type lifecycleObserver struct {
	mu                sync.RWMutex
	state             LifecycleState
	sessionBackoff    time.Duration
	lastSemanticError *SemanticError
	attempts          map[string]AttemptStatus
	retainedResults   *RetainedResultsStatus
	clock             Clock
}

func newLifecycleObserver(clock Clock) *lifecycleObserver {
	return &lifecycleObserver{
		state:    LifecycleRegistering,
		attempts: make(map[string]AttemptStatus),
		clock:    clock,
	}
}

func (observer *lifecycleObserver) setSession(state LifecycleState, backoff time.Duration, err error) {
	if observer == nil {
		return
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.state = state
	observer.sessionBackoff = backoff
	observer.recordSemanticLocked(err)
}

func (observer *lifecycleObserver) recordSemanticLocked(err error) {
	var protocolErr *ProtocolError
	if errors.As(err, &protocolErr) && protocolErr.APIError.Code != "" {
		recorded := &SemanticError{
			Code: protocolErr.APIError.Code, Message: protocolErr.APIError.Message,
			At: observer.clock.Now(),
		}
		observer.lastSemanticError = recorded
	}
}

func (observer *lifecycleObserver) beginAttempt(attemptID, jobID, class string) {
	if observer == nil {
		return
	}
	observer.mu.Lock()
	observer.attempts[attemptID] = AttemptStatus{
		AttemptID: attemptID, JobID: jobID, Class: class, State: AttemptStarting,
	}
	observer.mu.Unlock()
}

func (observer *lifecycleObserver) setAttempt(attemptID string, state AttemptLifecycleState, err error) {
	if observer == nil {
		return
	}
	observer.mu.Lock()
	status, ok := observer.attempts[attemptID]
	if ok {
		status.State = state
		if err != nil {
			status.LastError = err.Error()
			observer.recordSemanticLocked(err)
		}
		observer.attempts[attemptID] = status
	}
	observer.mu.Unlock()
}

func (observer *lifecycleObserver) configurePortfulAttempt(attemptID string) {
	if observer == nil {
		return
	}
	observer.mu.Lock()
	status, ok := observer.attempts[attemptID]
	if ok {
		startupSatisfied := false
		ready := false
		status.StartupSatisfied = &startupSatisfied
		status.Ready = &ready
		observer.attempts[attemptID] = status
	}
	observer.mu.Unlock()
}

func (observer *lifecycleObserver) setServiceReadiness(attemptID string, startupSatisfied, ready bool) {
	if observer == nil {
		return
	}
	observer.mu.Lock()
	status, ok := observer.attempts[attemptID]
	if ok {
		if status.StartupSatisfied == nil {
			initial := false
			status.StartupSatisfied = &initial
		}
		if startupSatisfied && !*status.StartupSatisfied {
			value := true
			status.StartupSatisfied = &value
		}
		readyValue := ready
		status.Ready = &readyValue
		if ready {
			status.State = AttemptServing
		} else if *status.StartupSatisfied {
			status.State = AttemptRunning
		}
		observer.attempts[attemptID] = status
	}
	observer.mu.Unlock()
}

// recordRetainedResults publishes one accounting pass. The collector runs on
// its own goroutine, so this is the seam where its figures become readable by
// whatever asks the daemon what it is holding.
func (observer *lifecycleObserver) recordRetainedResults(status RetainedResultsStatus) {
	if observer == nil {
		return
	}
	observer.mu.Lock()
	observer.retainedResults = &status
	observer.mu.Unlock()
}

// retainedResultsSnapshot reports the last pass, and whether one has run. It is
// separate from snapshot() because the node doctor asks for this alone and
// building a whole lifecycle projection to answer it would make the doctor pay
// for the attempt map it does not read.
func (observer *lifecycleObserver) retainedResultsSnapshot() (RetainedResultsStatus, bool) {
	if observer == nil {
		return RetainedResultsStatus{}, false
	}
	observer.mu.RLock()
	defer observer.mu.RUnlock()
	if observer.retainedResults == nil {
		return RetainedResultsStatus{}, false
	}
	return *observer.retainedResults, true
}

func (observer *lifecycleObserver) finishAttempt(attemptID string) {
	if observer == nil {
		return
	}
	observer.mu.Lock()
	delete(observer.attempts, attemptID)
	observer.mu.Unlock()
}

func (observer *lifecycleObserver) snapshot(oneShot, services ClassOccupancy) Status {
	observer.mu.RLock()
	defer observer.mu.RUnlock()
	attempts := make(map[string]AttemptStatus, len(observer.attempts))
	for attemptID, attempt := range observer.attempts {
		attempts[attemptID] = attempt
	}
	var semanticError *SemanticError
	if observer.lastSemanticError != nil {
		copied := *observer.lastSemanticError
		semanticError = &copied
	}
	var retained *RetainedResultsStatus
	if observer.retainedResults != nil {
		copied := *observer.retainedResults
		retained = &copied
	}
	return Status{
		State: observer.state, SessionBackoff: observer.sessionBackoff,
		LastSemanticError: semanticError, OneShot: oneShot, Services: services,
		Attempts: attempts, RetainedResults: retained,
	}
}
