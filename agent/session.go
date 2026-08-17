package agent

import (
	"context"
	"errors"
	"fmt"
	rand "math/rand/v2"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

const (
	DefaultSessionBackoffBase = 250 * time.Millisecond
	DefaultSessionBackoffMax  = 30 * time.Second
)

type workloadClass uint8

const (
	workloadClassOneShot workloadClass = iota
	workloadClassService
)

type classPool struct {
	class    workloadClass
	selector string
}

var classPools = []classPool{
	{class: workloadClassOneShot, selector: contract.JobClassOneShot},
	{class: workloadClassService, selector: contract.JobClassService},
}

type sessionAttemptExecution func(context.Context, l1.Claim, time.Time) (errorDestination, error)

// agentSession owns control-plane connectivity, heartbeat error routing, and
// the barrier that joins every attempt before session resources may close.
type agentSession struct {
	client            *Client
	registration      contract.NodeRegistration
	heartbeatInterval time.Duration
	claimInterval     time.Duration
	clock             Clock
	observer          *lifecycleObserver
	routeError        destinationErrorPolicy
	gates             map[workloadClass]classAdmissionGate
	localLimits       map[workloadClass]int
	logf              func(string, ...any)

	capacityMu      sync.Mutex
	poolTargets     map[workloadClass]int
	capacityChanged chan struct{}
	claimsEnabled   bool

	claimMu       sync.Mutex
	residentJobID map[string]struct{}

	drainOnce      sync.Once
	drainRequested chan struct{}
	attempts       sync.WaitGroup
}

type destinationError struct {
	destination errorDestination
	err         error
}

type routedDestinationError struct {
	destination errorDestination
	err         error
}

func (err *routedDestinationError) Error() string { return err.err.Error() }
func (err *routedDestinationError) Unwrap() error { return err.err }

func newAgentSession(
	client *Client,
	registration contract.NodeRegistration,
	heartbeatInterval, claimInterval time.Duration,
	clock Clock,
	observer *lifecycleObserver,
	logf func(string, ...any),
	maxOneshotSlots, maxServiceSlots int,
) *agentSession {
	return &agentSession{
		client: client, registration: registration,
		heartbeatInterval: heartbeatInterval, claimInterval: claimInterval,
		clock: clock, observer: observer, logf: logf,
		routeError: func(destination errorDestination, err error) error {
			if destination == errorDestinationAttemptAuthority {
				return nil
			}
			return &routedDestinationError{destination: destination, err: err}
		},
		gates: map[workloadClass]classAdmissionGate{
			workloadClassOneShot: newAdmissionGate(maxOneshotSlots),
			workloadClassService: newAdmissionGate(maxServiceSlots),
		},
		localLimits: map[workloadClass]int{
			workloadClassOneShot: maxOneshotSlots,
			workloadClassService: maxServiceSlots,
		},
		poolTargets: map[workloadClass]int{
			workloadClassOneShot: maxOneshotSlots,
			workloadClassService: maxServiceSlots,
		},
		capacityChanged: make(chan struct{}, 1),
		claimsEnabled:   true,
		residentJobID:   make(map[string]struct{}),
		drainRequested:  make(chan struct{}),
	}
}

func (session *agentSession) close() {
	if session != nil && session.client != nil {
		session.client.Close()
	}
}

func (session *agentSession) register(ctx context.Context) (l1.Node, error) {
	node, err := session.client.Register(ctx, session.registration)
	if err == nil {
		session.observeGrantedCapacity(node)
	}
	return node, err
}

func (session *agentSession) drain(ctx context.Context) (l1.Node, error) {
	session.observer.setSession(LifecycleDraining, 0, nil)
	session.drainOnce.Do(func() { close(session.drainRequested) })
	return session.client.Drain(ctx, session.registration.NodeID, session.registration.BootSessionID)
}

// run is process-lifetime supervision. Protocol failures are acted on at the
// narrowest destination; only local invariant failures escape after every
// resident attempt has returned.
func (session *agentSession) run(ctx context.Context, execute sessionAttemptExecution) error {
	runContext, stop := context.WithCancel(ctx)
	defer func() {
		stop()
		session.attempts.Wait()
	}()

	backoff := newSessionBackoff(DefaultSessionBackoffBase, DefaultSessionBackoffMax)
	state := LifecycleRegistering
	for {
		if runContext.Err() != nil {
			return nil
		}
		if session.isDraining() {
			session.observer.setSession(LifecycleDraining, 0, nil)
			return nil
		}
		session.observer.setSession(state, 0, nil)
		if _, err := session.register(runContext); err != nil {
			if runContext.Err() != nil {
				return nil
			}
			classification := classifyAgentProtocolError(err)
			switch classification.nodeSessionReaction {
			case nodeSessionDrain:
				session.observer.setSession(LifecycleDraining, 0, err)
				return nil
			case nodeSessionStopRecordAndEscalate:
				return session.quarantine(runContext, err)
			}
			if classification.destination != errorDestinationTransient && classification.nodeSessionReaction != nodeSessionReregister {
				return fmt.Errorf("agent: register node: %w", err)
			}
			delay := backoff.next()
			session.observer.setSession(LifecycleRejoining, delay, err)
			if err := session.waitWithoutHeartbeat(runContext, delay); err != nil {
				return nil
			}
			state = LifecycleRejoining
			continue
		}
		backoff.reset()
		session.markReady()

		failure := session.serveRegistered(runContext, execute)
		if failure.err == nil {
			return nil
		}
		classification := classifyAgentProtocolError(failure.err)
		if failure.destination == errorDestinationNodeSession {
			classification.destination = failure.destination
		}
		switch classification.nodeSessionReaction {
		case nodeSessionReregister:
			state = LifecycleRejoining
			continue
		case nodeSessionDrain:
			session.observer.setSession(LifecycleDraining, 0, failure.err)
			return nil
		case nodeSessionStopRecordAndEscalate:
			return session.quarantine(runContext, failure.err)
		}
		if failure.destination == errorDestinationTransient {
			state = LifecycleRejoining
			continue
		}
		return failure.err
	}
}

func (session *agentSession) serveRegistered(ctx context.Context, execute sessionAttemptExecution) destinationError {
	sessionContext, stopSession := context.WithCancel(ctx)
	heartbeatErrors := make(chan destinationError, 1)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		session.heartbeatLoop(sessionContext, heartbeatErrors)
	}()
	type claimWorker struct {
		id       int
		pool     classPool
		retire   chan struct{}
		retiring bool
	}
	type workerResult struct {
		id      int
		failure destinationError
	}
	workers := make(map[int]*claimWorker)
	workerDone := make(chan workerResult)
	nextWorkerID := 1
	startWorker := func(pool classPool) {
		worker := &claimWorker{id: nextWorkerID, pool: pool, retire: make(chan struct{})}
		nextWorkerID++
		workers[worker.id] = worker
		go func() {
			workerDone <- workerResult{
				id: worker.id,
				failure: session.claimClassLoop(
					sessionContext, worker.retire, worker.pool.class, worker.pool.selector, execute,
				),
			}
		}()
	}
	reconcileWorkers := func() {
		for _, pool := range classPools {
			target := session.poolTarget(pool.class)
			var available []*claimWorker
			for _, worker := range workers {
				if worker.pool.class == pool.class && !worker.retiring {
					available = append(available, worker)
				}
			}
			for len(available) < target {
				startWorker(pool)
				available = append(available, workers[nextWorkerID-1])
			}
			// A service worker that already established a binding remains the
			// pull path for that reserved service across restart backoff. The
			// lowered gate and L1 transaction refuse newcomers; retaining idle
			// workers here prevents a capacity reduction from stranding a bound
			// service that L1 explicitly permits to restart while overcommitted.
			if pool.class == workloadClassService {
				continue
			}
			for len(available) > target {
				last := available[len(available)-1]
				available = available[:len(available)-1]
				last.retiring = true
				close(last.retire)
			}
		}
	}
	reconcileWorkers()
	defer func() {
		stopSession()
		<-heartbeatDone
		for len(workers) > 0 {
			result := <-workerDone
			delete(workers, result.id)
		}
	}()

	drainRequested := session.drainRequested
	draining := false
	for {
		select {
		case <-sessionContext.Done():
			return destinationError{}
		case <-drainRequested:
			draining = true
			session.observer.setSession(LifecycleDraining, 0, nil)
			if session.logf != nil {
				session.logf("agent: draining; waiting for %d class claim loops and their resident attempts", len(workers))
			}
			drainRequested = nil
			if len(workers) == 0 {
				return destinationError{}
			}
		case result := <-workerDone:
			delete(workers, result.id)
			// A claim RPC already in flight can race the L1 drain mutation and
			// return node_draining before this select observes drainRequested.
			// Local admission is nevertheless already closed, so join the other
			// workers instead of letting that expected refusal cancel siblings.
			if session.isDraining() {
				draining = true
				session.observer.setSession(LifecycleDraining, 0, nil)
				if len(workers) == 0 {
					return destinationError{}
				}
				continue
			}
			if result.failure.err != nil {
				return result.failure
			}
			if draining {
				if len(workers) == 0 {
					return destinationError{}
				}
				continue
			}
			reconcileWorkers()
		case <-session.capacityChanged:
			if !draining {
				reconcileWorkers()
			}
		case failure := <-heartbeatErrors:
			return failure
		}
	}
}

// claimClassLoop owns one blocking claim-execute-wait path for one fixed
// workload class. The gate remains only a policy and occupancy counter; #85
// widens the number of loop instances after reading L1-granted capacity.
func (session *agentSession) claimClassLoop(
	ctx context.Context,
	retire <-chan struct{},
	gateKey workloadClass,
	selector string,
	execute sessionAttemptExecution,
) destinationError {
	backoff := newSessionBackoff(DefaultSessionBackoffBase, DefaultSessionBackoffMax)
	serviceReservation := false
	for {
		select {
		case <-ctx.Done():
			return destinationError{}
		case <-retire:
			return destinationError{}
		case <-session.drainRequested:
			session.observer.setSession(LifecycleDraining, 0, nil)
			return destinationError{}
		default:
		}

		claimStarted := session.clock.Now()
		if !session.claimsAllowed() {
			if err := session.waitForClaimWork(ctx, retire, session.claimInterval); err != nil {
				return destinationFromError(err)
			}
			continue
		}
		claim, admitted, err := session.claim(ctx, session.gates[gateKey], selector, serviceReservation)
		if !admitted {
			if err := session.waitForClaimWork(ctx, retire, session.claimInterval); err != nil {
				return destinationFromError(err)
			}
			continue
		}
		if err != nil {
			classification := classifyAgentProtocolError(err)
			failure := destinationError{
				destination: classification.destination,
				err:         fmt.Errorf("agent: claim %s job: %w", selector, err),
			}
			if failure.destination != errorDestinationTransient {
				return failure
			}
			delay := backoff.next()
			session.observer.setSession(LifecycleRejoining, delay, err)
			if waitErr := session.waitForClaimWork(ctx, retire, delay); waitErr != nil {
				return destinationFromError(waitErr)
			}
			continue
		}
		backoff.reset()
		session.markReady()
		if claim == nil {
			if err := session.waitForClaimWork(ctx, retire, session.claimInterval); err != nil {
				return destinationFromError(err)
			}
			continue
		}
		if gateKey == workloadClassService {
			serviceReservation = true
		}
		session.attempts.Add(1)
		destination, executeErr := session.executeResident(ctx, gateKey, *claim, claimStarted, execute)
		if executeErr != nil {
			if routed := session.routeError(destination, executeErr); routed != nil {
				return destinationFromError(routed)
			}
		}
	}
}

func (session *agentSession) executeResident(
	ctx context.Context,
	gateKey workloadClass,
	claim l1.Claim,
	claimStarted time.Time,
	execute sessionAttemptExecution,
) (errorDestination, error) {
	defer session.attempts.Done()
	defer session.gates[gateKey].release()
	defer session.releaseResidentJob(claim.Job.JobID)
	return execute(ctx, claim, claimStarted)
}

func (session *agentSession) heartbeatLoop(ctx context.Context, failures chan<- destinationError) {
	backoff := newSessionBackoff(DefaultSessionBackoffBase, DefaultSessionBackoffMax)
	nextDelay := session.heartbeatInterval
	for {
		timer := session.clock.NewTimer(nextDelay)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return
		case <-timer.C():
			response, err := session.client.Heartbeat(ctx, session.registration.NodeID, session.registration.BootSessionID)
			if err != nil {
				classification := classifyAgentProtocolError(err)
				if classification.destination == errorDestinationTransient {
					nextDelay = backoff.next()
					session.observer.setSession(LifecycleRejoining, nextDelay, err)
					continue
				}
				select {
				case failures <- destinationError{destination: classification.destination, err: fmt.Errorf("agent: heartbeat: %w", err)}:
				default:
				}
				return
			}
			session.observeGrantedCapacity(response.Node)
			backoff.reset()
			nextDelay = session.heartbeatInterval
			session.markReady()
		}
	}
}

func (session *agentSession) claim(
	ctx context.Context,
	gate classAdmissionGate,
	selector string,
	serviceReservation bool,
) (*l1.Claim, bool, error) {
	// Serializing the claim snapshot with the winning response closes the only
	// local race: a second worker cannot send a stale exclusion set after L1
	// has requeued a job but before the first worker finishes local finalization.
	session.claimMu.Lock()
	defer session.claimMu.Unlock()
	if !gate.canAcquire() && !serviceReservation {
		return nil, false, nil
	}
	excluded := make([]string, 0, len(session.residentJobID))
	for jobID := range session.residentJobID {
		excluded = append(excluded, jobID)
	}
	claim, err := session.client.Claim(
		ctx, session.registration.NodeID, session.registration.BootSessionID, selector, excluded...,
	)
	if err != nil || claim == nil {
		return claim, true, err
	}
	if _, exists := session.residentJobID[claim.Job.JobID]; exists {
		return nil, true, fmt.Errorf("agent: L1 returned locally resident job %q despite exclusion", claim.Job.JobID)
	}
	if !gate.tryAcquire() {
		if !serviceReservation || selector != contract.JobClassService {
			return nil, true, fmt.Errorf("agent: %s admission changed while claiming job %q", selector, claim.Job.JobID)
		}
		// L1 admits a service above the current limit only when its existing
		// binding already holds the slot. Reflect that execution locally
		// without treating the gate as the authority for service placement.
		gate.acquireReserved()
	}
	session.residentJobID[claim.Job.JobID] = struct{}{}
	return claim, true, nil
}

func (session *agentSession) releaseResidentJob(jobID string) {
	session.claimMu.Lock()
	delete(session.residentJobID, jobID)
	session.claimMu.Unlock()
}

func (session *agentSession) observeGrantedCapacity(node l1.Node) {
	targets := map[workloadClass]int{
		workloadClassOneShot: min(session.localLimits[workloadClassOneShot], node.MaxOneshotSlots),
		workloadClassService: min(session.localLimits[workloadClassService], node.MaxServiceSlots),
	}
	session.claimMu.Lock()
	defer session.claimMu.Unlock()
	session.capacityMu.Lock()
	session.claimsEnabled = node.ClaimsEnabled
	changed := false
	for class, target := range targets {
		if session.poolTargets[class] != target {
			session.poolTargets[class] = target
			session.gates[class].setLimit(target)
			changed = true
		}
	}
	session.capacityMu.Unlock()
	if changed {
		select {
		case session.capacityChanged <- struct{}{}:
		default:
		}
	}
}

func (session *agentSession) claimsAllowed() bool {
	session.capacityMu.Lock()
	defer session.capacityMu.Unlock()
	return session.claimsEnabled && !session.isDraining()
}

func (session *agentSession) poolTarget(class workloadClass) int {
	session.capacityMu.Lock()
	defer session.capacityMu.Unlock()
	return session.poolTargets[class]
}

func (session *agentSession) waitForClaimWork(ctx context.Context, retire <-chan struct{}, duration time.Duration) error {
	timer := session.clock.NewTimer(duration)
	defer stopTimer(timer)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-retire:
		return context.Canceled
	case <-session.drainRequested:
		return context.Canceled
	case <-timer.C():
		return nil
	}
}

func (session *agentSession) waitWithoutHeartbeat(ctx context.Context, duration time.Duration) error {
	timer := session.clock.NewTimer(duration)
	defer stopTimer(timer)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-session.drainRequested:
		return context.Canceled
	case <-timer.C():
		return nil
	}
}

func (session *agentSession) quarantine(ctx context.Context, err error) error {
	session.observer.setSession(LifecycleQuarantined, DefaultSessionBackoffMax, err)
	select {
	case <-ctx.Done():
		return nil
	case <-session.drainRequested:
		session.observer.setSession(LifecycleDraining, 0, nil)
		return nil
	}
}

func (session *agentSession) isDraining() bool {
	select {
	case <-session.drainRequested:
		return true
	default:
		return false
	}
}

func (session *agentSession) markReady() {
	if session.isDraining() {
		session.observer.setSession(LifecycleDraining, 0, nil)
		return
	}
	session.observer.setSession(LifecycleReady, 0, nil)
}

func destinationFromError(err error) destinationError {
	if err == nil || errors.Is(err, context.Canceled) {
		return destinationError{}
	}
	var routed *routedDestinationError
	if errors.As(err, &routed) {
		return destinationError{destination: routed.destination, err: routed.err}
	}
	return destinationError{destination: errorDestinationUnclassified, err: err}
}

func workloadClassFor(class string) workloadClass {
	if class == contract.JobClassService {
		return workloadClassService
	}
	return workloadClassOneShot
}

type sessionBackoff struct {
	base    time.Duration
	maximum time.Duration
	current time.Duration
}

func newSessionBackoff(base, maximum time.Duration) *sessionBackoff {
	return &sessionBackoff{base: base, maximum: maximum}
}

func (backoff *sessionBackoff) next() time.Duration {
	if backoff.current == 0 {
		backoff.current = backoff.base
	} else {
		backoff.current = min(backoff.current*2, backoff.maximum)
	}
	half := backoff.current / 2
	if half <= 0 {
		return backoff.current
	}
	return half + time.Duration(rand.Int64N(int64(backoff.current-half)+1))
}

func (backoff *sessionBackoff) reset() { backoff.current = 0 }
