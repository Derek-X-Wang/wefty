package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestUpgradeRenewalIgnoresCLIClockSkew(t *testing.T) {
	for _, mode := range []string{"survivors", "control", "legacy", "fleet"} {
		t.Run(mode, func(t *testing.T) {
			l1Now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
			cliNow := l1Now.Add(time.Hour)
			start := cliNow
			baseline := l1.Node{NodeRegistration: contract.NodeRegistration{BootSessionID: "boot", AgentVersion: "next"}, AuthorityGeneration: 1, State: contract.NodeAlive, LastHeartbeatAt: l1Now,
				ActiveAttempts: []l1.NodeActiveAttempt{{JobID: "job", AttemptID: "attempt", Class: contract.JobClassOneShot, LeaseExpiresAt: l1Now.Add(l1.DefaultLeaseDuration)}}}
			op := upgradeHost{
				now: func() time.Time { return cliNow },
				wait: func(context.Context, time.Duration) error {
					cliNow = cliNow.Add(time.Second)
					l1Now = l1Now.Add(time.Second)
					return nil
				},
				observe: func(context.Context, connprofile.Profile) (l1.Node, error) {
					node := baseline
					node.LastHeartbeatAt = l1Now
					node.ActiveAttempts = append([]l1.NodeActiveAttempt(nil), baseline.ActiveAttempts...)
					node.ActiveAttempts[0].LeaseExpiresAt = l1Now.Add(l1.DefaultLeaseDuration)
					return node, nil
				},
				readJob: func(context.Context, connprofile.Profile, string) (l1.Job, error) {
					// Decode the actual wire shape so this regression can run before
					// the new read-only field exists in the Go type.
					payload, err := json.Marshal(map[string]any{"attempt_id": "attempt", "state": "running", "updated_at": l1Now, "lease_expires_at": l1Now.Add(l1.DefaultLeaseDuration), "lease_live": true})
					if err != nil {
						t.Fatal(err)
					}
					var a l1.Attempt
					if err := json.Unmarshal(payload, &a); err != nil {
						t.Fatal(err)
					}
					return l1.Job{JobID: "job", Attempts: []l1.Attempt{a}}, nil
				},
			}
			r := rollingUpgrade{op: op, receipt: upgradeRollingReceipt{Candidate: agentinstall.Generation{Version: "next", CandidateBootSessionID: "boot"}, Preserved: baseline.ActiveAttempts}}
			var err error
			switch mode {
			case "survivors":
				err = r.waitSurvivors(t.Context(), l1.NodeGenerationInventory{}, start)
			case "control":
				_, err = r.waitControlRenewal(t.Context(), baseline, start, start)
			case "legacy":
				_, err = waitUpgradeRenewal(t.Context(), op, connprofile.Profile{}, baseline, start, start)
			case "fleet":
				_, err = waitFleetUpgradeRenewal(t.Context(), op, connprofile.Profile{}, baseline, start, start)
			}
			if err != nil {
				t.Fatalf("renewal rejected with CLI one hour ahead: %v", err)
			}
		})
	}
}
