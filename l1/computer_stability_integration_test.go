package l1

import (
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestComputerStartedRequiresDisplayPublicationForHealth(t *testing.T) {
	h, _, node, _ := computerCompletionHarness(t)
	h.stopServer()
	if _, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{
		Name: "started-readiness", Spec: computerCapabilityJobSpec("computer:started-readiness"), Actor: "operator",
	}); err != nil {
		t.Fatal(err)
	}
	claim := startComputerAttempt(t, h, node, nil)
	job := getRestartService(t, h, claim.Job.JobID)
	if job.State != contract.JobRunning || job.HealthySinceAt != nil || job.PublishedAttemptID != "" {
		t.Fatalf("Started without display: state=%s healthy_since=%v published_attempt=%q, want running/nil/empty",
			job.State, job.HealthySinceAt, job.PublishedAttemptID)
	}
}

func TestComputerRenewalRequiresCurrentDisplayPublicationForHealth(t *testing.T) {
	for _, scenario := range []string{"withdrawn-readiness", "submission-change", "republished-current-attempt"} {
		t.Run(scenario, func(t *testing.T) {
			h := newIntegrationHarness(t, map[string][]string{"computer-node": {contract.StableNodeTagPrefix + "computer-node"}})
			h.stopServer()
			node := registerCapabilityNodeWithTags(t, h, "computer-node", computerNodeCapabilities(),
				[]string{contract.StableNodeTagPrefix + "computer-node"})
			computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{
				Name: "renewal-readiness", Spec: computerCapabilityJobSpec("computer:renewal-readiness"), Actor: "operator",
			})
			if err != nil {
				t.Fatal(err)
			}
			var previousAttemptID string
			if scenario == "republished-current-attempt" {
				previous := startComputerAttempt(t, h, node, nil)
				previousAttemptID = previous.Lease.AttemptID
				exitCode := 1
				queued, err := h.store.CompleteAttempt(t.Context(), "fabric-computer-node", previous.Job.JobID,
					previousAttemptID, CompletionRequest{FencingToken: previous.Lease.FencingToken,
						IdempotencyKey: "previous-attempt", Result: ProcessResult{ExitCode: &exitCode}})
				if err != nil || queued.NextRestartAt == nil {
					t.Fatalf("previous attempt completion: next_restart=%v err=%v", queued.NextRestartAt, err)
				}
				h.clock.Advance(queued.NextRestartAt.Sub(h.clock.Now()))
			}
			claim := startComputerAttempt(t, h, node, nil)
			publish := func(ready bool) {
				t.Helper()
				request := PublicationRequest{FencingToken: claim.Lease.FencingToken, Ready: &ready}
				if ready {
					endpoint := "ws://computer-node.private:43123/websockify"
					request.DisplayEndpoint = &endpoint
				}
				if _, err := h.store.SetAttemptPublication(t.Context(), "fabric-computer-node", claim.Job.JobID,
					claim.Lease.AttemptID, request); err != nil {
					t.Fatal(err)
				}
			}
			renew := func() {
				t.Helper()
				if _, err := h.store.RenewLease(t.Context(), "fabric-computer-node", claim.Job.JobID,
					claim.Lease.AttemptID, claim.Lease.FencingToken); err != nil {
					t.Fatal(err)
				}
			}
			assertWithdrawn := func() {
				t.Helper()
				job := getRestartService(t, h, claim.Job.JobID)
				if job.HealthySinceAt != nil || job.PublishedAttemptID != "" {
					t.Fatalf("unpublished Computer regained health: healthy_since=%v published_attempt=%q, want nil/empty",
						job.HealthySinceAt, job.PublishedAttemptID)
				}
			}
			publish(true)
			if job := getRestartService(t, h, claim.Job.JobID); job.HealthySinceAt == nil || job.PublishedAttemptID != claim.Lease.AttemptID {
				t.Fatalf("initial publication did not establish health: %+v", job)
			}
			h.clock.Advance(time.Millisecond)
			if scenario == "submission-change" {
				admin := fabric.Identity{FabricID: "fabric-test", UserID: "admin", DeviceID: "device-1"}
				challenge, err := h.store.InitiateAdminBootstrap(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				policy, err := h.store.BootstrapAdmin(t.Context(), admin, challenge.Nonce)
				if err != nil {
					t.Fatal(err)
				}
				_, replayed, applied, err := h.store.MutateComputerSubmission(t.Context(), admin, computer.ComputerID,
					ComputerSubmissionRequest{PolicyRevision: policy.Revision, SubmitIntentRevision: computer.SubmitIntentRevision,
						SubmitEnabled: boolPointer(true), IdempotencyKey: "clear-ready"})
				if err != nil || replayed || !applied {
					t.Fatalf("submission change: replayed=%t applied=%t err=%v", replayed, applied, err)
				}
			} else {
				publish(false)
			}
			assertWithdrawn()
			// Neither a Started replay nor OCI log evidence may undo the clear.
			if _, err := h.store.StartAttempt(t.Context(), "fabric-computer-node", claim.Job.JobID,
				claim.Lease.AttemptID, StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
				t.Fatal(err)
			}
			if _, err := h.store.AppendLogs(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID,
				AppendLogsRequest{FencingToken: claim.Lease.FencingToken, Events: []contract.LogEvent{{
					AttemptID: claim.Lease.AttemptID, Stream: contract.LogStdout, Sequence: 0,
					Timestamp: h.clock.Now(), Bytes: []byte("display is down"),
				}}}); err != nil {
				t.Fatal(err)
			}
			assertWithdrawn()
			h.clock.Advance(time.Millisecond)
			renew()
			assertWithdrawn()
			if scenario != "republished-current-attempt" {
				return
			}

			// A retained marker for another attempt cannot substitute for the
			// current attempt's display publication, even on the same Computer.
			if _, err := h.store.db.Exec(`UPDATE service_jobs SET published_attempt_id=? WHERE job_id=?`,
				previousAttemptID, claim.Job.JobID); err != nil {
				t.Fatal(err)
			}
			renew()
			if job := getRestartService(t, h, claim.Job.JobID); job.HealthySinceAt != nil {
				t.Fatalf("another attempt's publication re-armed current health: %v", job.HealthySinceAt)
			}
			publish(true)
			// Keep the lease live while the node becomes stale. Reconcile clears
			// health, but retains this attempt's durable publication marker.
			for i := 0; i < 4; i++ {
				h.clock.Advance(15 * time.Second)
				renew()
			}
			result, err := h.store.Reconcile(t.Context())
			if err != nil || result.StaleNodes != 1 {
				t.Fatalf("stale reconciliation=%+v err=%v", result, err)
			}
			job := getRestartService(t, h, claim.Job.JobID)
			if job.HealthySinceAt != nil || job.PublishedAttemptID != claim.Lease.AttemptID {
				t.Fatalf("stale Computer health/publication = %+v", job)
			}
			if _, err := h.store.HeartbeatNode(t.Context(), "fabric-computer-node", node.NodeID, node.BootSessionID); err != nil {
				t.Fatal(err)
			}
			renew()
			job = getRestartService(t, h, claim.Job.JobID)
			if job.HealthySinceAt == nil || !job.HealthySinceAt.Equal(h.clock.Now()) || job.PublishedAttemptID != claim.Lease.AttemptID {
				t.Fatalf("current published attempt did not re-arm health at %s: %+v", h.clock.Now(), job)
			}
		})
	}
}
