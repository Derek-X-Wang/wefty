//go:build darwin || linux

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestRemovalAcknowledgementAuthorityRecoveryKeepsLiveGenerationResident(t *testing.T) {
	for _, source := range []string{"heartbeat", "background-ack"} {
		t.Run(source, func(t *testing.T) {
			clock := newManualClock(time.Now().UTC())
			ackClock := &removalACKStepClock{manualClock: newManualClock(clock.Now()), armed: make(chan time.Duration, 16)}
			var stage, ackCalls, registrations atomic.Int32
			refused := make(chan contract.ErrorCode, 2)
			started := make(chan struct{}, 1)
			pending := localRemoval{jobID: "ack-generation", kind: contract.JobKindProcess, generation: 1, rootInstanceID: "root", cleanupFence: "fence", processTreeReaped: true}
			a, store, done, _ := newResidentRejoinFixture(t, clock, func(a *Agent, store *l1.Store) {
				// One busy claim worker and explicit heartbeat wakes isolate the
				// refusal source while the real resident continues renewing.
				a.session.localLimits[workloadClassOneShot] = 1
				a.session.poolTargets[workloadClassOneShot] = 1
				a.session.heartbeatInterval = time.Hour
				a.session.removals.managed = &resumedManagedResource{completed: []localRemoval{pending}}
				a.session.removals.clock = ackClock
				base := a.session.client.httpClient.Transport
				a.session.client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
					var code contract.ErrorCode
					if source == "background-ack" && strings.HasSuffix(r.URL.Path, "/removal-acknowledgement") {
						switch ackCalls.Add(1) {
						case 1, 3:
							return nil, io.ErrUnexpectedEOF
						case 2:
							code = contract.ErrorNodeNotRegistered
						case 4:
							code = contract.ErrorNodeSessionReplaced
						default:
							t.Error("unexpected extra ACK after authority refusal")
						}
					} else if source == "heartbeat" && r.URL.Path == "/v1/agent/nodes/node/heartbeat" {
						if stage.CompareAndSwap(1, 0) {
							code = contract.ErrorNodeNotRegistered
						} else if stage.CompareAndSwap(2, 0) {
							code = contract.ErrorNodeSessionReplaced
						}
					}
					if code != "" {
						if code == contract.ErrorNodeSessionReplaced {
							promoteRetirementFixture(t, store, "node", "retirement-agent")
						}
						refused <- code
						payload, _ := json.Marshal(contract.ErrorResponse{Error: contract.APIError{Code: code}})
						return &http.Response{StatusCode: 409, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(payload))}, nil
					}
					response, err := base.RoundTrip(r)
					if err == nil && response.StatusCode == http.StatusOK && strings.HasSuffix(r.URL.Path, "/started") {
						select {
						case started <- struct{}{}:
						default:
						}
					}
					if err == nil && r.URL.Path == "/v1/agent/nodes/register" && response.StatusCode == 200 {
						registrations.Add(1)
					}
					return response, err
				})
			})
			job, ready, release := retirementReviewJob(t, store, "ack-refusal-"+source, false, nil)
			retirementWait(t, "live resident ready", func() bool { _, err := os.Stat(ready); return err == nil })
			// The payload can write ready before its asynchronous Started RPC
			// commits. Establish Running before testing that recovery preserves it.
			reviewWait(t, "resident Started acknowledgement", started)
			a.session.claimMu.Lock()
			resident := a.session.resident[job.JobID]
			a.session.claimMu.Unlock()
			if resident == nil {
				t.Fatal("payload has no live resident")
			}
			inventory, err := store.NodeGenerations(t.Context(), "retirement-agent", "node")
			if err != nil || len(inventory.Generations) != 1 || inventory.Generations[0].AgentGenerationID != "generation" || len(inventory.Generations[0].ResidentAttempts) != 1 {
				t.Fatalf("real generation/resident inventory=%+v err=%v", inventory, err)
			}
			assertAlive := func() {
				t.Helper()
				select {
				case <-resident.done:
					t.Fatal("authority recovery canceled the unrelated resident")
				default:
				}
				current, err := store.GetJob(t.Context(), job.JobID)
				if err != nil || current.State != contract.JobRunning {
					t.Fatalf("resident state=%s err=%v", current.State, err)
				}
				select {
				case err := <-done:
					t.Fatalf("node agent stopped during authority recovery: %v", err)
				default:
				}
			}
			nextACKDelay := func() time.Duration {
				t.Helper()
				select {
				case delay := <-ackClock.armed:
					return delay
				case <-time.After(testwait.Bound):
					t.Fatal("background ACK did not arm a retry")
					return 0
				}
			}
			expectRefusal := func(want contract.ErrorCode) {
				t.Helper()
				select {
				case got := <-refused:
					if got != want {
						t.Fatalf("refusal=%s want %s", got, want)
					}
				case <-time.After(testwait.Bound):
					t.Fatal("authority refusal did not occur")
				}
			}
			if source == "background-ack" {
				if err := a.outbox.spool.beginRemoval(t.Context(), pending, clock.Now()); err != nil {
					t.Fatal(err)
				}
				if err := a.session.removals.resume(t.Context()); err != nil {
					t.Fatal(err)
				}
				ackClock.Advance(nextACKDelay())
			} else {
				stage.Store(1)
				a.session.heartbeatWake <- struct{}{}
			}
			expectRefusal(contract.ErrorNodeNotRegistered)
			retirementWait(t, "same generation rejoined", func() bool { return registrations.Load() >= 2 })
			assertAlive()
			if err := a.session.removals.ackContext.Err(); err != nil {
				t.Fatalf("rejoin stopped controller-owned delivery: %v", err)
			}
			if source == "background-ack" {
				ackClock.Advance(nextACKDelay())
			} else {
				stage.Store(2)
				a.session.heartbeatWake <- struct{}{}
			}
			expectRefusal(contract.ErrorNodeSessionReplaced)
			retirementWait(t, "authenticated retirement after promotion", func() bool { return a.session.isRetiring() })
			if retired, err := a.Retire(t.Context()); err != nil || retired.AgentGenerationID != "generation" {
				t.Fatalf("authenticated retirement=%+v err=%v", retired, err)
			}
			// Join delivery and observe its terminal handling before checking survival.
			a.session.removals.wait()
			if err := a.session.removals.ackContext.Err(); err != context.Canceled {
				t.Fatalf("retirement left controller-owned delivery active: %v", err)
			}
			assertAlive()
			inventory, err = store.NodeGenerations(t.Context(), "retirement-agent", "node")
			if err != nil {
				t.Fatal(err)
			}
			retiring := false
			for _, generation := range inventory.Generations {
				if generation.AgentGenerationID == "generation" {
					retiring = generation.Role == l1.NodeGenerationRetiring
				}
			}
			if !retiring {
				t.Fatal("L1 did not authenticate this generation's retirement")
			}
			finishRejoinResident(t, store, job, release)
		})
	}
}
