//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/l1"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRebootRetirementCustodyReplaysNonemptySharedOutboxOnce(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprint(unknown), func(t *testing.T) {
			store, old := newUpgradeControlStore(t)
			const oldBoot = "darwin:11111111-1111-1111-1111-111111111111"
			const nextBoot = "darwin:22222222-2222-2222-2222-222222222222"
			primary, err := store.PrepareCandidate(old.Executable, "next", oldBoot, []int{1}, "promotion")
			if err != nil {
				t.Fatal(err)
			}
			c, err := store.Read()
			if err != nil {
				t.Fatal(err)
			}
			old.Role = "retiring"
			old.Process = &agentinstall.Binding{PID: 1, ProcessStartID: "prior-process", BootSessionID: "old-session", SystemBootID: oldBoot, ReadyAt: time.Now()}
			primary.Role = "primary"
			c.Primary = primary.ID
			c.Generations[old.ID] = old
			c.Generations[primary.ID] = primary
			raw, _ := json.Marshal(c)
			if err = os.WriteFile(filepath.Join(store.Root(), "catalog.json"), raw, 0600); err != nil {
				t.Fatal(err)
			}
			store.Manager = launchagent.Manager{Run: func(context.Context, string, ...string) ([]byte, error) { return nil, launchagent.ErrServiceNotFound }}
			outbox, err := newEvidenceOutbox(t.TempDir(), "node", 1024, systemClock{}, 8, time.Hour, time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			defer outbox.Close()
			outbox.spool.generationID = old.ID
			claim := spoolTestClaim("reboot-output")
			claim.Job.Spec.Kind = contract.JobKindProcess
			if err = outbox.spool.ensureAttempt(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			if err = outbox.spool.append(t.Context(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 0, "retained output")); err != nil {
				t.Fatal(err)
			}
			if _, err = outbox.spool.db.Exec(`UPDATE spool_attempts SET publication_settled=1`); err != nil {
				t.Fatal(err)
			}
			resident := l1.ResidentAttempt{JobID: claim.Job.JobID, AttemptID: claim.Lease.AttemptID, FencingToken: claim.Lease.FencingToken, BootSessionID: "old-session", AuthorityGeneration: 1}
			oldRemote := l1.NodeGeneration{AgentGenerationID: old.ID, BootSessionID: "old-session", AuthorityGeneration: 1, Role: l1.NodeGenerationRetiring, PromotionOperationID: "promotion", Registration: contract.NodeRegistration{SystemBootID: oldBoot}, ResidentAttempts: []l1.ResidentAttempt{resident}}
			if unknown {
				extra := spoolTestClaim("unknown")
				if err = outbox.spool.ensureAttempt(t.Context(), extra); err != nil {
					t.Fatal(err)
				}
				if err = outbox.spool.append(t.Context(), spoolTestEvent("unknown", contract.LogStdout, 0, "unknown")); err != nil {
					t.Fatal(err)
				}
			}
			outbox.spool.generationID = primary.ID
			if _, err = outbox.spool.db.Exec(`INSERT INTO generation_evidence_custodians VALUES(?)`, primary.ID); err != nil {
				t.Fatal(err)
			}
			var closed atomic.Bool
			var closes, logs atomic.Int32
			inventory := func() l1.NodeGenerationInventory {
				remote := oldRemote
				if closed.Load() {
					remote.Role = l1.NodeGenerationClosed
					now := time.Now()
					remote.ClosedAt = &now
				}
				return l1.NodeGenerationInventory{NodeID: "node", Generations: []l1.NodeGeneration{{AgentGenerationID: primary.ID, BootSessionID: "new-session", Role: l1.NodeGenerationPrimary}, remote}}
			}
			client := newRoundTripClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/generations"):
					_ = json.NewEncoder(w).Encode(inventory())
				case strings.HasSuffix(r.URL.Path, "/close"):
					closes.Add(1)
					closed.Store(true)
					_ = json.NewEncoder(w).Encode(inventory())
				case strings.HasSuffix(r.URL.Path, "/logs"):
					logs.Add(1)
					_ = json.NewEncoder(w).Encode(l1.AppendLogsResponse{Acknowledged: map[contract.LogStream]uint64{contract.LogStdout: 0}})
				default:
					t.Errorf("unexpected request %s", r.URL.Path)
				}
			}))
			clock := systemClock{}
			s := newAgentSession(client, contract.NodeRegistration{NodeID: "node", AgentGenerationID: primary.ID, BootSessionID: "new-session", SystemBootID: nextBoot}, nil, time.Second, time.Second, clock, newLifecycleObserver(clock), nil, 1, 1)
			s.retirementEvidenceSettled = outbox.settleRetirementEvidence(client, "node")
			a := &Agent{session: s, outbox: outbox}
			if err = a.reconcileRebootRetirees(t.Context(), store, inventory()); err != nil {
				t.Fatal(err)
			}
			if unknown {
				if closes.Load() != 0 {
					t.Fatal("unknown retained evidence closed")
				}
				return
			}
			if closes.Load() != 1 {
				t.Fatal("known retained output deadlocked closure")
			}
			if err = a.reconcileRebootRetirees(t.Context(), store, inventory()); err != nil {
				t.Fatal(err)
			}
			if closes.Load() != 1 {
				t.Fatal("closure repeated")
			}
			if err = outbox.refreshClosedGenerations(t.Context(), client, "node"); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				pending, err := outbox.spool.pendingAttempts(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				for _, attempt := range pending {
					if err = outbox.recoverOwnedAttempt(t.Context(), client, attempt); err != nil {
						t.Fatal(err)
					}
				}
			}
			if logs.Load() != 1 {
				t.Fatalf("replayed output %d times", logs.Load())
			}
		})
	}
}
