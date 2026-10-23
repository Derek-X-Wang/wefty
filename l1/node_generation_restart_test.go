package l1

import (
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestNodeGenerationRestartableRecoveryAcrossPromotion(t *testing.T) {
	for _, mode := range []string{"lease_loss", "accepted_interruption"} {
		t.Run(mode, func(t *testing.T) {
			f := newGenerationFixture(t)
			store, clock, ctx := f.h.store, f.h.clock, t.Context()
			advertiseRestartable := func(id string) {
				t.Helper()
				registration := generationRegistration(f.h, id)
				registration.Capabilities[CapabilityOneshotRestartable] = true
				registration.CapabilityRevision = 2
				if _, err := store.RegisterNode(ctx, fabric.Identity{NodeID: "agent"}, registration, NodePolicy{MaxOneshotSlots: 3, MaxServiceSlots: 2}, true); err != nil {
					t.Fatal(err)
				}
				f.refresh(t)
			}
			advertiseRestartable("g1")
			spec := validJobSpec("retiring-restartable-"+mode, nil)
			spec.Restartable = true
			job, _, err := store.CreateJobAs(ctx, spec, JobOrigin{OriginatingSubmitter: "operator"})
			if err != nil {
				t.Fatal(err)
			}
			predecessor, err := store.ClaimJob(ctx, "agent", "node", "boot-g1", contract.JobClassOneShot)
			if err != nil || predecessor == nil || predecessor.Job.JobID != job.JobID {
				t.Fatalf("predecessor=%+v err=%v", predecessor, err)
			}
			originalLog := contract.LogEvent{AttemptID: predecessor.Lease.AttemptID, Stream: contract.LogStdout, Sequence: 0, Timestamp: clock.Now(), Bytes: []byte("original retiring output\n")}
			if _, err = store.AppendLogs(ctx, "agent", job.JobID, predecessor.Lease.AttemptID, AppendLogsRequest{FencingToken: predecessor.Lease.FencingToken, Events: []contract.LogEvent{originalLog}}); err != nil {
				t.Fatal(err)
			}
			predecessorResult := AttemptResultRequest{FencingToken: predecessor.Lease.FencingToken, Document: []byte(`{"generation":"g1"}`)}
			if _, err = store.SetAttemptResult(ctx, "agent", job.JobID, predecessor.Lease.AttemptID, predecessorResult); err != nil {
				t.Fatal(err)
			}
			if result, err := store.GetJobResult(ctx, job.JobID); err != nil || result.AttemptID != predecessor.Lease.AttemptID {
				t.Fatalf("original result=%+v err=%v", result, err)
			}
			f.promote(t, "g2")
			f.intent(t, true)
			keeper := f.claim(t, "other-retiring-resident-"+mode)
			f.promote(t, "g3")
			advertiseRestartable("g3")
			staleScope, err := store.ResolveAttemptCredential(ctx, predecessor.AttemptToken, "agent")
			if err != nil {
				t.Fatalf("retiring predecessor credential before recovery: %v", err)
			}

			frozen := map[string]NodeGeneration{}
			for _, g := range f.inventory.Generations {
				if g.Role == NodeGenerationRetiring {
					frozen[g.AgentGenerationID] = g
				}
			}
			if len(frozen) != 2 || len(frozen["g1"].ResidentAttempts) != 1 || frozen["g1"].ResidentAttempts[0].AttemptID != predecessor.Lease.AttemptID || len(frozen["g2"].ResidentAttempts) != 1 || frozen["g2"].ResidentAttempts[0].AttemptID != keeper.Lease.AttemptID {
				t.Fatal("fixture must have two exact retiring resident sets")
			}
			inventory := func() NodeGenerationInventory {
				t.Helper()
				value, err := store.NodeGenerations(ctx, "agent", "node")
				if err != nil {
					t.Fatal(err)
				}
				for _, g := range value.Generations {
					if original, ok := frozen[g.AgentGenerationID]; ok && !reflect.DeepEqual(g.ResidentAttempts, original.ResidentAttempts) {
						t.Fatalf("%s frozen resident membership changed: %+v", g.AgentGenerationID, g.ResidentAttempts)
					}
				}
				return value
			}
			generation := func(id string) NodeGeneration {
				t.Helper()
				for _, g := range inventory().Generations {
					if g.AgentGenerationID == id {
						return g
					}
				}
				t.Fatalf("missing generation %s", id)
				return NodeGeneration{}
			}
			// Advance only the injected clock. Keep g2's resident and g3's primary
			// alive through their production renewal/heartbeat methods, independently
			// of the predecessor whose Lease must end.
			advance := func(until time.Time) {
				t.Helper()
				for clock.Now().Before(until) {
					step := min(10*time.Second, until.Sub(clock.Now()))
					clock.Advance(step)
					if _, err := store.RenewLease(ctx, "agent", keeper.Job.JobID, keeper.Lease.AttemptID, keeper.Lease.FencingToken); err != nil {
						t.Fatalf("other retiree lost its resident: %v", err)
					}
					if _, err := store.HeartbeatNode(ctx, "agent", "node", "boot-g3"); err != nil {
						t.Fatal(err)
					}
				}
			}
			var interruptionRequest InterruptRestartableAttemptRequest
			var completion CompletionRequest
			expectedPredecessorState := contract.AttemptLost
			if mode == "lease_loss" {
				advance(predecessor.Lease.LeaseExpires)
				result, err := store.Reconcile(ctx)
				if err != nil || result.ExpiredAttempts != 1 {
					t.Fatalf("Lease-loss recovery=%+v err=%v", result, err)
				}
			} else {
				expectedPredecessorState = contract.AttemptInterrupted
				interruptionRequest = InterruptRestartableAttemptRequest{JobID: job.JobID, BootSessionID: "boot-g1", IntentRevision: f.node.IntentRevision, IdempotencyKey: "retiring-interruption", Reason: InterruptionUpgrade}
				if _, replayed, err := store.InterruptRestartableAttempt(ctx, "node", predecessor.Lease.AttemptID, interruptionRequest); err != nil || replayed {
					t.Fatalf("retiring interruption replay=%t err=%v", replayed, err)
				}
				lease, err := store.RenewLease(ctx, "agent", job.JobID, predecessor.Lease.AttemptID, predecessor.Lease.FencingToken)
				if err != nil || lease.Directive != AttemptDirectiveRestart || lease.InterruptionRequestKey != interruptionRequest.IdempotencyKey {
					t.Fatalf("retiring interruption directive=%+v err=%v", lease, err)
				}
				completion = CompletionRequest{FencingToken: predecessor.Lease.FencingToken, IdempotencyKey: "accepted-retiring-interruption", Result: ProcessResult{Signal: "SIGTERM", TerminationCause: contract.TerminationCauseAgent}, InterruptionReason: InterruptionUpgrade, InterruptionRequestKey: interruptionRequest.IdempotencyKey, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt}
				outcome, err := store.CompleteAttemptOutcome(ctx, "agent", job.JobID, predecessor.Lease.AttemptID, completion)
				if err != nil || outcome.Replayed || outcome.Job.State != contract.JobQueued {
					t.Fatalf("retiring interruption completion=%+v err=%v", outcome, err)
				}
			}
			queued, err := store.GetJob(ctx, job.JobID)
			if err != nil || queued.State != contract.JobQueued || queued.CurrentAttemptID != "" || queued.RestartRecovery == nil || queued.RestartRecovery.Count != 0 || !queued.RestartRecovery.Pending || queued.RestartRecovery.NextRestartAt == nil {
				t.Fatalf("queued recovery=%+v err=%v", queued, err)
			}
			// Reconciliation cannot schedule an additional restart of an already ended
			// predecessor. The restart budget is consumed only by the successor claim.
			if result, err := store.Reconcile(ctx); err != nil || result.ExpiredAttempts != 0 {
				t.Fatalf("repeated recovery=%+v err=%v", result, err)
			}
			f.intent(t, true)
			if mode == "lease_loss" {
				if c, err := store.ClaimJob(ctx, "agent", "node", "boot-g3", contract.JobClassOneShot); err != nil || c != nil {
					t.Fatalf("claimed before recovery backoff: %+v %v", c, err)
				}
				advance(*queued.RestartRecovery.NextRestartAt)
			}
			successor, err := store.ClaimJob(ctx, "agent", "node", "boot-g3", contract.JobClassOneShot)
			if err != nil || successor == nil || successor.Job.JobID != job.JobID || successor.Job.RestartRecovery.Count != 1 || successor.Lease.AttemptID == predecessor.Lease.AttemptID || successor.Lease.FencingToken == predecessor.Lease.FencingToken || successor.AttemptToken == predecessor.AttemptToken {
				t.Fatalf("fresh primary successor=%+v err=%v", successor, err)
			}
			if c, err := store.ClaimJob(ctx, "agent", "node", "boot-g3", contract.JobClassOneShot); err != nil || c != nil {
				t.Fatalf("duplicate successor admitted: %+v %v", c, err)
			}
			attempts, err := store.ListJobAttempts(ctx, job.JobID)
			if err != nil || len(attempts) != 2 || attempts[0].AttemptID != predecessor.Lease.AttemptID || attempts[0].State != expectedPredecessorState || attempts[1].AttemptID != successor.Lease.AttemptID || attempts[1].NodeID != "node" {
				t.Fatalf("exact successor accounting=%+v err=%v", attempts, err)
			}
			primary := generation("g3")
			if primary.Role != NodeGenerationPrimary || len(primary.ResidentAttempts) != 1 || primary.ResidentAttempts[0].AttemptID != successor.Lease.AttemptID || primary.ResidentAttempts[0].BootSessionID != "boot-g3" {
				t.Fatalf("successor belongs to wrong generation: %+v", primary)
			}
			if _, err = store.RenewLease(ctx, "agent", job.JobID, predecessor.Lease.AttemptID, predecessor.Lease.FencingToken); errorCode(err) != contract.ErrorAttemptMismatch {
				t.Fatalf("predecessor renewal authority survived successor: %v", err)
			}
			if _, err = store.ResolveAttemptCredential(ctx, predecessor.AttemptToken, "agent"); errorCode(err) != contract.ErrorUnauthorized {
				t.Fatalf("predecessor child credential survived successor: %v", err)
			}
			if _, _, err = store.CreateJobAs(ctx, validJobSpec("stale-retiring-child-"+mode, nil), JobOrigin{Parent: &staleScope}); errorCode(err) != contract.ErrorUnauthorized {
				t.Fatalf("cached predecessor scope created child: %v", err)
			}
			if children, err := store.ListChildJobs(ctx, job.JobID, "", 10); err != nil || len(children.Jobs) != 0 {
				t.Fatalf("refused child was persisted: %+v %v", children, err)
			}
			if _, err = store.ResolveAttemptCredential(ctx, successor.AttemptToken, "agent"); err != nil {
				t.Fatalf("new primary credential invalid: %v", err)
			}

			closeRequest := func(id string) CloseNodeGenerationRequest {
				g := frozen[id]
				return CloseNodeGenerationRequest{BootSessionID: g.BootSessionID, PromotionOperationID: g.PromotionOperationID, ResidentAttempts: g.ResidentAttempts, RuntimesReaped: true, EvidenceSettled: true}
			}
			if ready, err := store.NodeGenerationClosureReady(ctx, "agent", "node", "g1"); err != nil || !ready {
				t.Fatalf("recovered predecessor did not permit closure: %t %v", ready, err)
			}
			if ready, err := store.NodeGenerationClosureReady(ctx, "agent", "node", "g2"); err != nil || ready {
				t.Fatalf("live other retiree became closable: %t %v", ready, err)
			}
			if _, err = store.CloseNodeGeneration(ctx, "agent", "node", "g2", closeRequest("g2")); errorCode(err) != contract.ErrorConflict {
				t.Fatalf("closed unrelated live retiree: %v", err)
			}
			if _, err = store.CloseNodeGeneration(ctx, "agent", "node", "g1", closeRequest("g1")); err != nil {
				t.Fatal(err)
			}
			closedFirst := generation("g1")
			if closedFirst.Role != NodeGenerationClosed || closedFirst.ClosedAt == nil || generation("g2").Role != NodeGenerationRetiring {
				t.Fatal("closing g1 changed g2's retirement")
			}
			if _, err = store.RenewLease(ctx, "agent", keeper.Job.JobID, keeper.Lease.AttemptID, keeper.Lease.FencingToken); err != nil {
				t.Fatalf("closing g1 revoked g2 authority: %v", err)
			}

			// The latest-result row cannot expose the old result after successor claim,
			// nor can original provenance overwrite the successor's own result.
			if _, err = store.GetJobResult(ctx, job.JobID); errorCode(err) != contract.ErrorNotFound {
				t.Fatalf("predecessor result remained latest: %v", err)
			}
			successorDocument := []byte(`{"generation":"g3"}`)
			if _, err = store.SetAttemptResult(ctx, "agent", job.JobID, successor.Lease.AttemptID, AttemptResultRequest{FencingToken: successor.Lease.FencingToken, Document: successorDocument}); err != nil {
				t.Fatal(err)
			}
			resultBefore, err := store.GetJobResult(ctx, job.JobID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = store.SetAttemptResult(ctx, "agent", job.JobID, predecessor.Lease.AttemptID, predecessorResult); errorCode(err) != contract.ErrorSupersededAttempt {
				t.Fatalf("predecessor overwrote latest result: %v", err)
			}
			before, err := store.GetJob(ctx, job.JobID)
			if err != nil {
				t.Fatal(err)
			}
			successorBefore := attempts[1]
			historicalLog := originalLog
			if mode == "lease_loss" {
				historicalLog.Sequence = 1
				historicalLog.Timestamp = clock.Now()
				historicalLog.Bytes = []byte("late lost-predecessor output\n")
			}
			// Lost predecessors retain new historical output. Interrupted terminal
			// predecessors retain identical accepted-log replay, not new log rows.
			if reply, err := store.AppendLogs(ctx, "agent", job.JobID, predecessor.Lease.AttemptID, AppendLogsRequest{FencingToken: predecessor.Lease.FencingToken, Events: []contract.LogEvent{historicalLog}}); err != nil || reply.AttemptState != expectedPredecessorState {
				t.Fatalf("historical log acceptance=%+v err=%v", reply, err)
			}
			if mode == "accepted_interruption" {
				nextLog := originalLog
				nextLog.Sequence = 1
				if _, err = store.AppendLogs(ctx, "agent", job.JobID, predecessor.Lease.AttemptID, AppendLogsRequest{FencingToken: predecessor.Lease.FencingToken, Events: []contract.LogEvent{nextLog}}); errorCode(err) != contract.ErrorConflict {
					t.Fatalf("terminal interruption gained new log authority: %v", err)
				}
				outcome, err := store.CompleteAttemptOutcome(ctx, "agent", job.JobID, predecessor.Lease.AttemptID, completion)
				if err != nil || !outcome.Replayed || outcome.Job.CurrentAttemptID != successor.Lease.AttemptID || outcome.Job.RestartRecovery.Count != 1 {
					t.Fatalf("accepted interruption replay after successor claim=%+v err=%v", outcome, err)
				}
				if _, replayed, err := store.InterruptRestartableAttempt(ctx, "node", predecessor.Lease.AttemptID, interruptionRequest); err != nil || !replayed {
					t.Fatalf("interruption request replay=%t err=%v", replayed, err)
				}
			} else {
				exit := 1
				if _, err = store.CompleteAttempt(ctx, "agent", job.JobID, predecessor.Lease.AttemptID, CompletionRequest{FencingToken: predecessor.Lease.FencingToken, IdempotencyKey: "late-lost-result", Result: ProcessResult{ExitCode: &exit}}); errorCode(err) != contract.ErrorLeaseExpired {
					t.Fatalf("lost predecessor completion gained live authority: %v", err)
				}
			}
			after, err := store.GetJob(ctx, job.JobID)
			if err != nil || !reflect.DeepEqual(before, after) || after.State != contract.JobClaimed {
				t.Fatalf("historical evidence changed live Job: before=%+v after=%+v err=%v", before, after, err)
			}
			attempts, err = store.ListJobAttempts(ctx, job.JobID)
			if err != nil || len(attempts) != 2 || !reflect.DeepEqual(successorBefore, attempts[1]) {
				t.Fatalf("historical evidence changed successor: %+v %v", attempts, err)
			}
			resultAfter, err := store.GetJobResult(ctx, job.JobID)
			if err != nil || !reflect.DeepEqual(resultBefore, resultAfter) || resultAfter.AttemptID != successor.Lease.AttemptID || !reflect.DeepEqual(resultAfter.Document, successorDocument) {
				t.Fatalf("latest-result fencing failed: %+v %v", resultAfter, err)
			}
			logs, err := store.GetJobLogs(ctx, job.JobID, "", 10)
			wantLogs := 1
			if mode == "lease_loss" {
				wantLogs = 2
			}
			if err != nil || len(logs.Events) != wantLogs || !reflect.DeepEqual(logs.Events[len(logs.Events)-1], historicalLog) {
				t.Fatalf("historical output not retained: %+v %v", logs, err)
			}
			if mode == "lease_loss" && (attempts[0].Result != nil || attempts[0].LateResult == nil || attempts[0].LateResult.Kind != LateResultObservation) {
				t.Fatalf("lost completion did not remain late evidence: %+v", attempts[0])
			}

			clock.Advance(time.Second)
			completeGenerationAttempt(t, f, keeper)
			if _, err = store.CloseNodeGeneration(ctx, "agent", "node", "g2", closeRequest("g2")); err != nil {
				t.Fatal(err)
			}
			closedSecond := generation("g2")
			if closedSecond.Role != NodeGenerationClosed || closedSecond.ClosedAt == nil || !closedSecond.ClosedAt.After(*closedFirst.ClosedAt) || !reflect.DeepEqual(closedFirst, generation("g1")) || generation("g3").Role != NodeGenerationPrimary {
				t.Fatal("retirees did not close independently")
			}
			if _, err = store.ResolveAttemptCredential(ctx, successor.AttemptToken, "agent"); err != nil {
				t.Fatalf("closing retirees revoked successor: %v", err)
			}
			completeGenerationAttempt(t, f, successor)
			done, err := store.GetJob(ctx, job.JobID)
			if err != nil || done.State != contract.JobSucceeded || done.RestartRecovery.Count != 1 {
				t.Fatalf("final restart accounting=%+v err=%v", done, err)
			}
			if attempts, err := store.ListJobAttempts(ctx, job.JobID); err != nil || len(attempts) != 2 || attempts[1].State != contract.AttemptSucceeded {
				t.Fatalf("one successor must settle once: %+v %v", attempts, err)
			}
			if c, err := store.ClaimJob(ctx, "agent", "node", "boot-g3", contract.JobClassOneShot); err != nil || c != nil {
				t.Fatalf("settled recovery queued another successor: %+v %v", c, err)
			}
			inventory()
		})
	}
}
