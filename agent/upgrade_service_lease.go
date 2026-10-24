package agent

import (
	"context"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// The service worker can join before its evidence settles. The session retains
// this exact Attempt's completion lease across bounded barrier calls and replay;
// payload cancellation must not put a deadline on that recovery.
type upgradeServiceLease struct {
	cancel context.CancelFunc
	done   chan struct{}
	// stopCause is protected by claimMu. A directive ends renewal without
	// discarding evidence or deciding whether completion replay can settle it.
	stopCause error
}

// Called with claimMu held, before selective payload cancellation. A lease is
// never acquired for another Attempt or an agent without generation identity.
func (s *agentSession) keepUpgradeServiceLease(attemptID string, interval time.Duration) {
	record := s.retirementAttempts[attemptID]
	if s.client == nil || record.fencingToken == "" || record.completionLease != nil || s.upgradeLeasesClosed {
		return
	}
	parent := s.upgradeLeaseContext
	if parent == nil {
		// Register-only callers still have Agent.Close as their lifetime owner.
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	lease := &upgradeServiceLease{cancel: cancel, done: make(chan struct{})}
	record.completionLease = lease
	jobID, fence := record.jobID, record.fencingToken
	interval = durationOrDefault(interval, DefaultRenewalInterval)
	go func() {
		defer close(lease.done)
		defer cancel()
		delay := time.Duration(0)
		var deadline time.Time
		for {
			if delay > 0 {
				timer := s.clock.NewTimer(delay)
				select {
				case <-ctx.Done():
					stopTimer(timer)
					return
				case <-timer.C():
				}
			}
			if ctx.Err() != nil {
				return
			}
			timeout := s.client.operationTimeout
			if remaining := deadline.Sub(s.clock.Now()); remaining > 0 {
				timeout = renewalRequestTimeout(remaining, timeout)
			}
			request, cancelRequest := context.WithTimeout(ctx, timeout)
			updated, err := s.client.Renew(request, jobID, attemptID, fence)
			cancelRequest()
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				// RenewLease uses conflict for a terminal Attempt. Other operations
				// may retry conflict, but this exact predecessor cannot renew again.
				if protocolErrorCode(err) != contract.ErrorConflict && classifyAgentProtocolError(err).destination == errorDestinationTransient {
					delay = min(interval, DefaultLogRetryInterval)
					continue
				}
				// A terminal renewal response can mean /complete committed but
				// its receipt was lost. Only completion replay decides settlement;
				// never let this response preempt that authoritative replay.
				if s.logf != nil {
					s.logf("agent: upgrade service %q attempt %q completion lease: %v", jobID, attemptID, err)
				}
				return
			}
			if updated.Directive == l1.AttemptDirectiveStop {
				s.claimMu.Lock()
				lease.stopCause = errAttemptDirectiveStop
				s.claimMu.Unlock()
				return
			}
			deadline = s.clock.Now().Add(updated.LeaseTTL)
			delay = renewalDelay(updated.LeaseTTL, interval)
		}
	}()
}

// Positive settlement stops this Attempt's keeper even when another service
// keeps the barrier pending. Cancel and join without holding claimMu.
func (s *agentSession) stopUpgradeServiceLease(attemptID string) {
	s.claimMu.Lock()
	var lease *upgradeServiceLease
	if record := s.retirementAttempts[attemptID]; record != nil {
		lease = record.completionLease
		if lease != nil {
			lease.cancel()
		}
	}
	s.claimMu.Unlock()
	if lease != nil {
		<-lease.done
	}
}

// Joins only service completion keepers; resident one-shots have independent
// lease owners. No mutex needed by a keeper is held while waiting for it.
func (s *agentSession) stopUpgradeServiceLeases(closing bool) {
	s.claimMu.Lock()
	if closing {
		s.upgradeLeasesClosed = true
	}
	var leases []*upgradeServiceLease
	for _, record := range s.retirementAttempts {
		if record.completionLease != nil {
			record.completionLease.cancel()
			leases = append(leases, record.completionLease)
		}
	}
	s.claimMu.Unlock()
	for _, lease := range leases {
		<-lease.done
	}
}
