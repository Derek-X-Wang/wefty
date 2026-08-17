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

func newAgentSession(client *Client, registration contract.NodeRegistration, heartbeatInterval, claimInterval time.Duration, clock Clock, observer *lifecycleObserver) *agentSession {
	return &agentSession{
		client: client, registration: registration,
		heartbeatInterval: heartbeatInterval, claimInterval: claimInterval,
		clock: clock, observer: observer,
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
	node, err := session.client.Drain(ctx, session.registration.NodeID, session.registration.BootSessionID)
	session.drainOnce.Do(func() { close(session.drainRequested) })
	return node, err
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

		failure := session.serveRegistered(runContext, execute, backoff)
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

func (session *agentSession) serveRegistered(ctx context.Context, execute sessionAttemptExecution, backoff *sessionBackoff) destinationError {
	sessionContext, stopSession := context.WithCancel(ctx)
	heartbeatErrors := make(chan destinationError, 1)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		session.heartbeatLoop(sessionContext, heartbeatErrors)
	}()
	defer func() {
		stopSession()
		<-heartbeatDone
	}()

	for {
		select {
		case <-sessionContext.Done():
			return destinationError{}
		case <-session.drainRequested:
			session.observer.setSession(LifecycleDraining, 0, nil)
			return destinationError{}
		case failure := <-heartbeatErrors:
			return failure
		default:
		}

		claimStarted := session.clock.Now()
		claim, err := session.client.Claim(sessionContext, session.registration.NodeID, session.registration.BootSessionID)
		if err != nil {
			if sessionContext.Err() != nil || session.isDraining() {
				return destinationError{}
			}
			classification := classifyAgentProtocolError(err)
			if classification.destination == errorDestinationTransient {
				delay := backoff.next()
				session.observer.setSession(LifecycleRejoining, delay, err)
				if waitErr := session.wait(sessionContext, delay, heartbeatErrors); waitErr != nil {
					return destinationFromError(waitErr)
				}
				continue
			}
			return destinationError{destination: classification.destination, err: fmt.Errorf("agent: claim job: %w", err)}
		}
		backoff.reset()
		session.markReady()
		if claim == nil {
			if err := session.wait(sessionContext, session.claimInterval, heartbeatErrors); err != nil {
				return destinationFromError(err)
			}
			continue
		}

		attemptDone := make(chan error, 1)
		session.attempts.Add(1)
		go func(claim l1.Claim, started time.Time) {
			defer session.attempts.Done()
			gate := session.gates[workloadClassFor(claim.Job.Spec.Class)]
			admitted, err := gate.execute(sessionContext, func(attemptContext context.Context) (errorDestination, error) {
				return execute(attemptContext, claim, started)
			}, session.routeError)
			if !admitted && err == nil {
				err = errors.New("agent: one-shot admission gate rejected a claimed attempt")
			}
			attemptDone <- err
		}(*claim, claimStarted)

		draining := false
		for {
			select {
			case <-ctx.Done():
				stopSession()
				<-attemptDone
				return destinationError{}
			case <-session.drainRequested:
				draining = true
				session.observer.setSession(LifecycleDraining, 0, nil)
			case failure := <-heartbeatErrors:
				stopSession()
				<-attemptDone
				return failure
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

func (session *agentSession) wait(ctx context.Context, duration time.Duration, heartbeatErrors <-chan destinationError) error {
	timer := session.clock.NewTimer(duration)
	defer stopTimer(timer)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-session.drainRequested:
		return context.Canceled
	case failure := <-heartbeatErrors:
		return &routedDestinationError{destination: failure.destination, err: failure.err}
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
