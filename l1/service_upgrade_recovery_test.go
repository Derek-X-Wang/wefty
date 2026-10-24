package l1

import (
	"reflect"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// Use the production restart transaction. Upgrade recovery must not turn an
// operator change between the upgrader's read and write into a fresh restart.
func TestServiceUpgradeRecoveryGuardAndReplay(t *testing.T) {
	for _, mode := range []string{"always", "never", "stopped", "removed", "policy-stopped", "replaced", "wrong-boot", "live"} {
		t.Run(mode, func(t *testing.T) {
			f := newGenerationFixture(t)
			s, ctx := f.h.store, t.Context()
			registration := generationRegistration(f.h, "g1")
			registration.RootInstanceID = "upgrade-test-root"
			if _, err := s.RegisterNode(ctx, fabric.Identity{NodeID: "agent"}, registration, NodePolicy{MaxOneshotSlots: 3, MaxServiceSlots: 2}, true); err != nil {
				t.Fatal(err)
			}
			spec := validJobSpec("upgrade-"+mode, nil)
			spec.Class, spec.Execution.HandoffDirectory = contract.JobClassService, ""
			spec.Restart = contract.RestartAlways
			if mode == "never" || mode == "policy-stopped" {
				spec.Restart = contract.RestartNever
			}
			job, _, err := s.CreateJob(ctx, spec)
			if err != nil {
				t.Fatal(err)
			}
			old, err := s.ClaimJob(ctx, "agent", "node", "boot-g1", contract.JobClassService)
			if err != nil || old == nil {
				t.Fatalf("claim=%+v %v", old, err)
			}
			if _, err = s.StartAttempt(ctx, "agent", job.JobID, old.Lease.AttemptID, StartedRequest{FencingToken: old.Lease.FencingToken}); err != nil {
				t.Fatal(err)
			}
			f.intent(t, false)
			if mode != "live" {
				result := ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}
				if mode == "policy-stopped" {
					zero := 0
					result = ProcessResult{ExitCode: &zero}
				}
				_, err = s.CompleteAttempt(ctx, "agent", job.JobID, old.Lease.AttemptID, CompletionRequest{FencingToken: old.Lease.FencingToken, IdempotencyKey: "quiesced", Result: result, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt})
				if err != nil {
					t.Fatal(err)
				}
			}
			switch mode {
			case "stopped":
				_, err = s.SetServiceDesiredState(ctx, job.JobID, contract.ServiceDesiredStopped)
			case "removed":
				_, err = s.RemoveService(ctx, job.JobID)
			case "replaced":
				_, _, err = s.RestartService(ctx, job.JobID, ServiceRestartRequest{IdempotencyKey: "operator-replacement"})
				if err == nil {
					f.intent(t, true)
					var next *Claim
					next, err = s.ClaimJob(ctx, "agent", "node", "boot-g1", contract.JobClassService)
					if next == nil {
						t.Fatalf("replacement absent: %v", err)
					}
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			before, err := s.GetJob(ctx, job.JobID)
			if err != nil {
				t.Fatal(err)
			}
			request := ServiceRestartRequest{IdempotencyKey: "activation:boot-g1:" + old.Lease.AttemptID, Upgrade: &ServiceUpgradeRecovery{NodeID: "node", BootSessionID: "boot-g1", AttemptID: old.Lease.AttemptID}}
			if mode == "wrong-boot" {
				request.Upgrade.BootSessionID = "another-boot"
			}
			recovered, replay, err := s.RestartService(ctx, job.JobID, request)
			eligible := mode == "always" || mode == "never"
			if mode == "removed" && errorCode(err) == contract.ErrorNotFound {
				var count int
				if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM service_restart_requests WHERE job_id=? AND idempotency_key=?", job.JobID, request.IdempotencyKey).Scan(&count); err != nil || count != 0 {
					t.Fatalf("removed service recovered: %d %v", count, err)
				}
				return
			}
			if mode == "wrong-boot" {
				if err == nil {
					t.Fatal("wrong boot accepted")
				}
				return
			}
			if err != nil || replay {
				t.Fatalf("recovery=%+v replay=%v error=%v", recovered, replay, err)
			}
			var count int
			if err = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM service_restart_requests WHERE job_id=? AND idempotency_key=?", job.JobID, request.IdempotencyKey).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if eligible {
				if recovered.State != contract.JobQueued || count != 1 {
					t.Fatalf("missing recovery: %+v requests=%d", recovered, count)
				}
				_, replay, err = s.RestartService(ctx, job.JobID, request)
				if err != nil || !replay {
					t.Fatalf("lost-reply replay=%v error=%v", replay, err)
				}
				// Promotion sees no service residents. Only its new primary may
				// claim the fresh Attempt, with an independent fence.
				f.promote(t, "g2")
				f.intent(t, true)
				next, err := s.ClaimJob(ctx, "agent", "node", "boot-g2", contract.JobClassService)
				if err != nil || next == nil || next.Lease.AttemptID == old.Lease.AttemptID || next.Lease.FencingToken == old.Lease.FencingToken {
					t.Fatalf("fresh successor=%+v %v", next, err)
				}
				_, replay, err = s.RestartService(ctx, job.JobID, request)
				if err != nil || !replay {
					t.Fatalf("replay after successor=%v %v", replay, err)
				}
				if err = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM service_restart_requests WHERE job_id=? AND idempotency_key=?", job.JobID, request.IdempotencyKey).Scan(&count); err != nil || count != 1 {
					t.Fatalf("duplicate requests=%d %v", count, err)
				}
			} else if count != 0 || recovered.State != before.State || recovered.CurrentAttemptID != before.CurrentAttemptID || recovered.DesiredState != before.DesiredState || !reflect.DeepEqual(recovered.PolicyStop, before.PolicyStop) {
				t.Fatalf("recovery changed excluded service: before=%+v after=%+v requests=%d", before, recovered, count)
			}
		})
	}
}
