package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

type workloadClass uint8

const (
	workloadClassOneShot workloadClass = iota
	workloadClassService
	prefactorClassLimit = 1
)

// agentSession owns control-plane connectivity, heartbeat error routing, and
// the barrier that joins every attempt before session resources may close.
type agentSession struct {
	client            *Client
	registration      contract.NodeRegistration
	heartbeatInterval time.Duration
	claimInterval     time.Duration
	clock             Clock
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

func newAgentSession(client *Client, registration contract.NodeRegistration, heartbeatInterval, claimInterval time.Duration, clock Clock) *agentSession {
	return &agentSession{
		client: client, registration: registration,
		heartbeatInterval: heartbeatInterval, claimInterval: claimInterval,
		clock:      clock,
		routeError: func(_ errorDestination, err error) error { return err },
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
	node, err := session.client.Drain(ctx, session.registration.NodeID, session.registration.BootSessionID)
	session.drainOnce.Do(func() { close(session.drainRequested) })
	return node, err
}

func (session *agentSession) run(ctx context.Context, execute func(context.Context, l1.Claim) (errorDestination, error)) error {
	runContext, stop := context.WithCancel(ctx)
	defer func() {
		stop()
		session.attempts.Wait()
	}()

	if _, err := session.register(runContext); err != nil {
		return session.routeError(classifyAgentProtocolError(err).destination, fmt.Errorf("agent: register node: %w", err))
	}
	if session.isDraining() {
		if _, err := session.client.Drain(runContext, session.registration.NodeID, session.registration.BootSessionID); err != nil {
			return session.routeError(classifyAgentProtocolError(err).destination, fmt.Errorf("agent: drain node: %w", err))
		}
		return nil
	}

	heartbeatErrors := make(chan destinationError, 1)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		session.heartbeatLoop(runContext, heartbeatErrors)
	}()
	defer func() {
		stop()
		<-heartbeatDone
	}()

	for {
		select {
		case <-runContext.Done():
			return nil
		case <-session.drainRequested:
			return nil
		case failure := <-heartbeatErrors:
			return session.routeError(failure.destination, failure.err)
		default:
		}

		claim, err := session.client.Claim(runContext, session.registration.NodeID, session.registration.BootSessionID)
		if err != nil {
			if runContext.Err() != nil || session.isDraining() {
				return nil
			}
			classification := classifyAgentProtocolError(err)
			if classification.destination == errorDestinationTransient {
				if waitErr := session.wait(runContext, session.claimInterval, heartbeatErrors); waitErr != nil {
					if errors.Is(waitErr, context.Canceled) {
						return nil
					}
					return waitErr
				}
				continue
			}
			return session.routeError(classification.destination, fmt.Errorf("agent: claim job: %w", err))
		}
		if claim == nil {
			if err := session.wait(runContext, session.claimInterval, heartbeatErrors); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			}
			continue
		}

		session.attempts.Add(1)
		admitted, err := session.gates[workloadClassOneShot].execute(runContext, func(attemptContext context.Context) (errorDestination, error) {
			defer session.attempts.Done()
			return execute(attemptContext, *claim)
		}, session.routeError)
		if !admitted {
			session.attempts.Done()
			return errors.New("agent: one-shot admission gate rejected a claimed attempt")
		}
		if err != nil {
			if runContext.Err() != nil {
				return nil
			}
			return err
		}
	}
}

func (session *agentSession) heartbeatLoop(ctx context.Context, failures chan<- destinationError) {
	for {
		timer := session.clock.NewTimer(session.heartbeatInterval)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return
		case <-timer.C():
			if _, err := session.client.Heartbeat(ctx, session.registration.NodeID, session.registration.BootSessionID); err != nil {
				classification := classifyAgentProtocolError(err)
				if classification.destination == errorDestinationTransient {
					continue
				}
				select {
				case failures <- destinationError{destination: classification.destination, err: fmt.Errorf("agent: heartbeat: %w", err)}:
				default:
				}
				return
			}
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
		return session.routeError(failure.destination, failure.err)
	case <-timer.C():
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
