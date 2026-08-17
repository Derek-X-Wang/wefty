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
}

type lifecycleObserver struct {
	mu                sync.RWMutex
	state             LifecycleState
	sessionBackoff    time.Duration
	lastSemanticError *SemanticError
	attempts          map[string]AttemptStatus
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
	return Status{
		State: observer.state, SessionBackoff: observer.sessionBackoff,
		LastSemanticError: semanticError, OneShot: oneShot, Services: services,
		Attempts: attempts,
	}
}
