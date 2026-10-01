package agent

import (
	"context"

	"github.com/Derek-X-Wang/wefty/l3"
)

type hostTokenRevocation struct {
	revoker       ComputerTokenRevoker
	stableNodeID  string
	bootSessionID string
	clock         Clock
	backoff       *sessionBackoff
	logf          func(string, ...any)
}

func (revocation hostTokenRevocation) run(ctx context.Context, registered <-chan struct{}) {
	select {
	case <-ctx.Done():
		return
	case <-registered:
	}
	clock := revocation.clock
	if clock == nil {
		clock = systemClock{}
	}
	backoff := revocation.backoff
	if backoff == nil {
		backoff = newSessionBackoff(DefaultSessionBackoffBase, DefaultSessionBackoffMax)
	}
	failed := false
	for {
		err := revocation.revoker.RevokeHostComputerTokens(ctx, l3.HostComputerTokenRevocationRequest{
			Reason: "agent_restart", StableNodeID: revocation.stableNodeID, BootSessionID: revocation.bootSessionID,
		})
		if err == nil {
			if failed && ctx.Err() == nil && revocation.logf != nil {
				revocation.logf("revoke prior-boot Computer tokens recovered after retry")
			}
			return
		}
		if ctx.Err() != nil {
			return
		}
		if !failed && revocation.logf != nil {
			revocation.logf("revoke prior-boot Computer tokens: %v", err)
		}
		failed = true
		timer := clock.NewTimer(backoff.next())
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return
		case <-timer.C():
		}
	}
}
