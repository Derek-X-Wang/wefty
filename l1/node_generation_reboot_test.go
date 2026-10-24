package l1

import (
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"testing"
	"time"
)

func TestGenerationRebootFencesRetireesAndRecoversOnlyRestartable(t *testing.T) {
	f := newGenerationFixture(t)
	store, ctx := f.h.store, t.Context()
	registration := generationRegistration(f.h, "g1")
	registration.CapabilityRevision++
	registration.Capabilities[CapabilityOneshotRestartable] = true
	_, err := store.RegisterNode(ctx, fabric.Identity{NodeID: "agent"}, registration, NodePolicy{MaxOneshotSlots: 3, MaxServiceSlots: 2}, true)
	if err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	ordinary := f.claim(t, "ordinary")
	spec := validJobSpec("restartable", nil)
	spec.Restartable = true
	job, _, err := store.CreateJobAs(ctx, spec, JobOrigin{OriginatingSubmitter: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	repeatable, err := store.ClaimJob(ctx, "agent", "node", "boot-g1", contract.JobClassOneShot)
	if err != nil || repeatable == nil || repeatable.Job.JobID != job.JobID {
		t.Fatal("restartable claim", err)
	}
	f.promote(t, "g2")
	f.promote(t, "g3")
	register := func(boot, system string) {
		t.Helper()
		r := generationRegistration(f.h, "g3")
		r.BootSessionID = boot
		r.SystemBootID = system
		r.CapabilityRevision = 10
		r.Capabilities[CapabilityOneshotRestartable] = true
		_, err := store.RegisterNode(ctx, fabric.Identity{NodeID: "agent"}, r, NodePolicy{MaxOneshotSlots: 3, MaxServiceSlots: 2}, true)
		if err != nil {
			t.Fatal(err)
		}
		f.refresh(t)
	}
	const first = "darwin:11111111-1111-1111-1111-111111111111"
	const second = "darwin:22222222-2222-2222-2222-222222222222"
	register("boot-g3", first)
	// A primary process restart on the same system boot preserves unrelated retirees.
	register("boot-g3-process-restart", first)
	if _, err = store.RenewLease(ctx, "agent", ordinary.Job.JobID, ordinary.Lease.AttemptID, ordinary.Lease.FencingToken); err != nil {
		t.Fatal("primary restart revoked live retiree", err)
	}
	register("boot-g3-reboot", second)
	f.intent(t, true)
	for _, old := range []*Claim{ordinary, repeatable} {
		if _, err = store.RenewLease(ctx, "agent", old.Job.JobID, old.Lease.AttemptID, old.Lease.FencingToken); err == nil {
			t.Fatal("prior-boot retiree renewed")
		}
	}
	if _, err = store.HeartbeatRetiringNodeGeneration(ctx, "agent", "node", "g1", "boot-g1"); err == nil {
		t.Fatal("prior-boot retiree heartbeat accepted")
	}
	if claim, err := store.ClaimJob(ctx, "agent", "node", "boot-g1", contract.JobClassOneShot); err == nil || claim != nil {
		t.Fatal("stale retiree claimed", err)
	}
	if claim, err := store.ClaimJob(ctx, "agent", "node", "boot-g3-reboot", contract.JobClassOneShot); err != nil || claim != nil {
		t.Fatal("unknown predecessor embargo bypassed", err)
	}
	f.h.clock.Advance(DefaultLeaseDuration + time.Second)
	if _, err = store.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	ended, err := store.GetJob(ctx, ordinary.Job.JobID)
	if err != nil || ended.State != contract.JobFailed {
		t.Fatalf("ordinary job replayed: %+v %v", ended, err)
	}
	recovered, err := store.GetJob(ctx, repeatable.Job.JobID)
	if err != nil || recovered.State != contract.JobQueued || recovered.RestartRecovery == nil {
		t.Fatalf("restartable not recovered: %+v %v", recovered, err)
	}
	if recovered.RestartRecovery.NextRestartAt != nil {
		f.h.clock.Advance(recovered.RestartRecovery.NextRestartAt.Sub(f.h.clock.Now()) + time.Millisecond)
	}
	successor, err := store.ClaimJob(ctx, "agent", "node", "boot-g3-reboot", contract.JobClassOneShot)
	if err != nil || successor == nil || successor.Job.JobID != repeatable.Job.JobID || successor.Lease.AttemptID == repeatable.Lease.AttemptID {
		t.Fatal("fresh restartable successor", err)
	}
	attempts, err := store.ListJobAttempts(ctx, ordinary.Job.JobID)
	if err != nil || len(attempts) != 1 || attempts[0].State != contract.AttemptLost || attempts[0].Result != nil {
		t.Fatalf("interrupted ordinary outcome invented: %+v %v", attempts, err)
	}
}
