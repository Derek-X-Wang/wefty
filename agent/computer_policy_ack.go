package agent

import (
	"context"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

const computerPolicyPendingReportInterval = time.Second

type computerPolicyAckController struct {
	client  *Client
	clock   Clock
	logf    func(string, ...any)
	mu      sync.Mutex
	pending map[l1.ComputerPolicyInstallAcknowledgement]ComputerPolicyInstallReceipt
	changed chan struct{}
}

func newComputerPolicyAckController(client *Client, clock Clock, logf func(string, ...any)) *computerPolicyAckController {
	return &computerPolicyAckController{client: client, clock: clock, logf: logf,
		pending: make(map[l1.ComputerPolicyInstallAcknowledgement]ComputerPolicyInstallReceipt), changed: make(chan struct{}, 1)}
}

func (controller *computerPolicyAckController) submit(receipt ComputerPolicyInstallReceipt) {
	controller.mu.Lock()
	controller.pending[receipt.Acknowledgement] = receipt
	controller.mu.Unlock()
	select {
	case controller.changed <- struct{}{}:
	default:
	}
}

func (controller *computerPolicyAckController) run(ctx context.Context) {
	backoff := newSessionBackoff(DefaultSessionBackoffBase, DefaultSessionBackoffMax)
	for ctx.Err() == nil {
		acknowledgement, ready, pending := controller.takeReady()
		if ready {
			if err := controller.client.AcknowledgeComputerPolicy(ctx, acknowledgement); err == nil {
				backoff.reset()
				continue
			} else if ctx.Err() == nil {
				controller.submit(ComputerPolicyInstallReceipt{Acknowledgement: acknowledgement, SessionsClosed: closedSignal()})
				if controller.logf != nil {
					controller.logf("agent: Computer policy acknowledgement will retry: %v", err)
				}
				if !controller.wait(ctx, backoff.next()) {
					return
				}
				continue
			}
			return
		}
		if pending > 0 && controller.logf != nil {
			controller.logf("agent: Computer policy installation still pending on %d session barrier(s)", pending)
		}
		if !controller.wait(ctx, computerPolicyPendingReportInterval) {
			return
		}
	}
}

func (controller *computerPolicyAckController) takeReady() (l1.ComputerPolicyInstallAcknowledgement, bool, int) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	var selected l1.ComputerPolicyInstallAcknowledgement
	found := false
	for acknowledgement, receipt := range controller.pending {
		select {
		case <-receipt.SessionsClosed:
			if !found || acknowledgement.PolicyGeneration > selected.PolicyGeneration ||
				acknowledgement.PolicyGeneration == selected.PolicyGeneration && acknowledgement.PolicyRevision > selected.PolicyRevision {
				selected, found = acknowledgement, true
			}
		default:
		}
	}
	if !found {
		return l1.ComputerPolicyInstallAcknowledgement{}, false, len(controller.pending)
	}
	for acknowledgement := range controller.pending {
		if acknowledgement.PolicyGeneration < selected.PolicyGeneration ||
			acknowledgement.PolicyGeneration == selected.PolicyGeneration && acknowledgement.PolicyRevision <= selected.PolicyRevision {
			delete(controller.pending, acknowledgement)
		}
	}
	return selected, true, len(controller.pending)
}

func (controller *computerPolicyAckController) wait(ctx context.Context, duration time.Duration) bool {
	timer := controller.clock.NewTimer(duration)
	defer stopTimer(timer)
	select {
	case <-ctx.Done():
		return false
	case <-controller.changed:
		return true
	case <-timer.C():
		return true
	}
}

func closedSignal() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
