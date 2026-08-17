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
	prefactorClassLimit = 1
)

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
	logf              func(string, ...any)

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

func newAgentSession(client *Client, registration contract.NodeRegistration, heartbeatInterval, claimInterval time.Duration, clock Clock, observer *lifecycleObserver, logf func(string, ...any)) *agentSession {
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
			workloadClassOneShot: newAdmissionGate(prefactorClassLimit),
			workloadClassService: newAdmissionGate(prefactorClassLimit),
		},
		drainRequested: make(chan struct{}),
	}
}

func (session *agentSession) close() {
	if session != nil && session.client != nil {
		session.client.Close()
	}
}

func (session *agentSession) register(ctx context.Context) (l1.Node, error) {
	return session.client.Register(ctx, session.registration)
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
	claimFailures := make(chan destinationError, 2)
	claimLoopsDone := make(chan struct{}, 2)
	for _, claimClass := range []struct {
		gateKey  workloadClass
		selector string
	}{
		{gateKey: workloadClassOneShot, selector: contract.JobClassOneShot},
		{gateKey: workloadClassService, selector: contract.JobClassService},
	} {
		go func() {
			defer func() { claimLoopsDone <- struct{}{} }()
			failure := session.claimClassLoop(sessionContext, claimClass.gateKey, claimClass.selector, execute)
			if failure.err != nil {
				claimFailures <- failure
			}
		}()
	}
	claimLoopsRemaining := 2
	defer func() {
		stopSession()
		<-heartbeatDone
		for claimLoopsRemaining > 0 {
			<-claimLoopsDone
			claimLoopsRemaining--
		}
	}()

	drainRequested := session.drainRequested
	for {
		select {
		case <-sessionContext.Done():
			return destinationError{}
		case <-drainRequested:
			session.observer.setSession(LifecycleDraining, 0, nil)
			if session.logf != nil {
				session.logf("agent: draining; waiting for %d class claim loops and their resident attempts", claimLoopsRemaining)
			}
			drainRequested = nil
		case <-claimLoopsDone:
			claimLoopsRemaining--
			if claimLoopsRemaining == 0 {
				return destinationError{}
			}
		case failure := <-heartbeatErrors:
			return failure
		case failure := <-claimFailures:
			return failure
		}
	}
}

// claimClassLoop owns one blocking claim-execute-wait path for one fixed
// workload class. Its admission gate remains the only execution-state owner;
// #85 widens the number of loop instances after reading L1-granted capacity.
func (session *agentSession) claimClassLoop(
	ctx context.Context,
	gateKey workloadClass,
	selector string,
	execute sessionAttemptExecution,
) destinationError {
	backoff := newSessionBackoff(DefaultSessionBackoffBase, DefaultSessionBackoffMax)
	for {
		select {
		case <-ctx.Done():
			return destinationError{}
		case <-session.drainRequested:
			session.observer.setSession(LifecycleDraining, 0, nil)
			return destinationError{}
		default:
		}

		claimStarted := session.clock.Now()
		claim, err := session.client.Claim(ctx, session.registration.NodeID, session.registration.BootSessionID, selector)
		if err != nil {
			if ctx.Err() != nil || session.isDraining() {
				return destinationError{}
			}
			classification := classifyAgentProtocolError(err)
			if classification.destination == errorDestinationTransient {
				delay := backoff.next()
				session.observer.setSession(LifecycleRejoining, delay, err)
				if waitErr := session.waitWithoutHeartbeat(ctx, delay); waitErr != nil {
					return destinationFromError(waitErr)
				}
				continue
			}
			return destinationError{destination: classification.destination, err: fmt.Errorf("agent: claim %s job: %w", selector, err)}
		}
		backoff.reset()
		session.markReady()
		if claim == nil {
			if err := session.waitWithoutHeartbeat(ctx, session.claimInterval); err != nil {
				return destinationFromError(err)
			}
			continue
		}

		attemptDone := make(chan error, 1)
		session.attempts.Add(1)
		go func(claim l1.Claim, started time.Time) {
			defer session.attempts.Done()
			admitted, err := session.gates[gateKey].execute(ctx, func(attemptContext context.Context) (errorDestination, error) {
				return execute(attemptContext, claim, started)
			}, session.routeError)
			if !admitted && err == nil {
				err = fmt.Errorf("agent: %s admission gate rejected a claimed attempt", selector)
			}
			attemptDone <- err
		}(*claim, claimStarted)

		draining := false
		for {
			select {
			case <-ctx.Done():
				<-attemptDone
				return destinationError{}
			case <-session.drainRequested:
				draining = true
				session.observer.setSession(LifecycleDraining, 0, nil)
			case err := <-attemptDone:
				if draining {
					return destinationError{}
				}
				if err != nil {
					return destinationFromError(err)
				}
				goto nextClaim
			}
		}
	nextClaim:
	}
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
			if _, err := session.client.Heartbeat(ctx, session.registration.NodeID, session.registration.BootSessionID); err != nil {
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
			backoff.reset()
			nextDelay = session.heartbeatInterval
			session.markReady()
		}
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
