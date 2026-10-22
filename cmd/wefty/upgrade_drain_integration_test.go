package main

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent/managedroot"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Exercise the upgrade entry point against real L1 intent/claim/completion/
// restart transactions. Only build, launchctl and elapsed time are simulated.
func TestUpgradeDrainRealL1KeepsResultsQueueServiceDataAndPort(t *testing.T) {
	f := newUpgradeFixture(t)
	h := newNodeCLIHarness(t)
	h.clock.Advance(f.now.Sub(h.clock.Now()) - time.Second)
	ctx := t.Context()
	policy := l1.NodePolicy{Tags: []string{"upgrade"}, MaxOneshotSlots: 4, MaxServiceSlots: 2}
	registered := h.registerNode(t, f.node.NodeID, "upgrade", policy)
	f.node = registered.node
	oldBoot := f.node.BootSessionID
	newJob := func(key, class string) l1.Job {
		spec := nodeCLIJobSpec(key, class, "upgrade")
		if class == contract.JobClassService {
			port := 18081
			spec.PublishedPort = &port
			spec.Restart = contract.RestartNever
		}
		job, _, err := h.store.CreateJob(ctx, spec)
		if err != nil {
			t.Fatal(err)
		}
		return job
	}
	claim := func(class string) *l1.Claim {
		c, err := h.store.ClaimJob(ctx, registered.identity.NodeID, f.node.NodeID, f.node.BootSessionID, class)
		if err != nil || c == nil {
			t.Fatalf("claim: %+v %v", c, err)
		}
		if _, err := h.store.StartAttempt(ctx, registered.identity.NodeID, c.Job.JobID, c.Lease.AttemptID, l1.StartedRequest{FencingToken: c.Lease.FencingToken}); err != nil {
			t.Fatal(err)
		}
		return c
	}
	service := newJob("never-hosted-app", contract.JobClassService)
	serviceClaim := claim(contract.JobClassService)
	ready := true
	if _, err := h.store.SetAttemptPublication(ctx, registered.identity.NodeID, service.JobID, serviceClaim.Lease.AttemptID, l1.PublicationRequest{FencingToken: serviceClaim.Lease.FencingToken, Ready: &ready}); err != nil {
		t.Fatal(err)
	}
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	managerConfig := managedroot.Config{Root: managedRoot, NodeID: f.node.NodeID, BootSessionID: oldBoot, HostBootIdentity: func() (string, error) { return "test-host-boot", nil }}
	manager, err := managedroot.Initialize(managerConfig)
	if err != nil {
		t.Fatal(err)
	}
	servicePaths, err := manager.CreateService(service.JobID, 1)
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(servicePaths.Data, "durable-app-data")
	if err := os.WriteFile(marker, []byte("keep me across replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	resident := newJob("resident", contract.JobClassOneShot)
	residentClaim := claim(contract.JobClassOneShot)
	var late l1.Job
	var lateClaim *l1.Claim
	var queued l1.Job
	syncClock := func() { h.clock.Advance(f.now.Sub(h.clock.Now())) }
	f.operations.setClaims = func(ctx context.Context, p connprofile.Profile, request l1.NodeIntentRequest) (l1.Node, error) {
		syncClock()
		if !request.ClaimsEnabled {
			late = newJob("late-claim", contract.JobClassOneShot)
			lateClaim = claim(contract.JobClassOneShot)
		}
		node, err := h.clients.setNodeClaims(ctx, f.node.NodeID, request)
		if err == nil {
			f.node = node
		}
		if !request.ClaimsEnabled && err == nil {
			queued = newJob("arrived-during-drain", contract.JobClassOneShot)
			got, err := h.store.ClaimJob(ctx, registered.identity.NodeID, f.node.NodeID, oldBoot, contract.JobClassOneShot)
			if err != nil || got != nil {
				t.Fatalf("drain admitted new work: %+v %v", got, err)
			}
		}
		return node, err
	}
	f.operations.observe = func(ctx context.Context, p connprofile.Profile) (l1.Node, error) {
		syncClock()
		if f.restarted {
			if _, err := h.store.HeartbeatNode(ctx, registered.identity.NodeID, f.node.NodeID, f.node.BootSessionID); err != nil {
				return l1.Node{}, err
			}
			if !f.replaced {
				if _, err := h.store.RenewLease(ctx, registered.identity.NodeID, service.JobID, serviceClaim.Lease.AttemptID, serviceClaim.Lease.FencingToken); err != nil {
					return l1.Node{}, err
				}
			}
		}
		node, err := h.clients.getNode(ctx, f.node.NodeID)
		if err == nil {
			f.node = node
		}
		return node, err
	}
	f.operations.readNode = func(ctx context.Context, p connprofile.Profile) (l1.Node, error) {
		return h.clients.getNode(ctx, f.node.NodeID)
	}
	f.operations.readJob = func(ctx context.Context, p connprofile.Profile, id string) (l1.Job, error) {
		return h.clients.getService(ctx, id)
	}
	restarted := 0
	f.operations.restartService = func(ctx context.Context, p connprofile.Profile, id, key string) (l1.Job, error) {
		before, err := h.clients.getService(ctx, id)
		if err != nil {
			return l1.Job{}, err
		}
		if before.State != contract.JobFailed || before.Spec.Restart != contract.RestartNever {
			t.Fatalf("not testing never interruption recovery: %+v", before)
		}
		restarted++
		return h.clients.restartService(ctx, id, key)
	}
	wait := f.operations.wait
	f.operations.wait = func(ctx context.Context, d time.Duration) error {
		if d == upgradeDrainPoll {
			active, err := h.clients.getService(ctx, service.JobID)
			if err != nil || active.State != contract.JobRunning || active.PublishedPort == nil || *active.PublishedPort != 18081 {
				t.Fatalf("service interrupted during drain: %+v %v", active, err)
			}
			for _, c := range []*l1.Claim{residentClaim, lateClaim} {
				if c == nil {
					continue
				}
				job, err := h.store.GetJob(ctx, c.Job.JobID)
				if err != nil {
					t.Fatal(err)
				}
				if job.State == contract.JobSucceeded {
					continue
				}
				zero := 0
				if _, err := h.store.CompleteAttempt(ctx, registered.identity.NodeID, c.Job.JobID, c.Lease.AttemptID, l1.CompletionRequest{FencingToken: c.Lease.FencingToken, IdempotencyKey: "normal-completion", Result: l1.ProcessResult{ExitCode: &zero}}); err != nil {
					t.Fatal(err)
				}
			}
		}
		return wait(ctx, d)
	}
	launch := f.host.launchctl
	f.host.launchctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if args[0] == "bootout" && strings.HasSuffix(args[1], "/"+setupNodeAgentLabel) {
			syncClock()
			if _, err := h.store.DrainNode(ctx, registered.identity.NodeID, f.node.NodeID, oldBoot); err != nil {
				return nil, err
			}
			_, err := h.store.CompleteAttempt(ctx, registered.identity.NodeID, service.JobID, serviceClaim.Lease.AttemptID, l1.CompletionRequest{FencingToken: serviceClaim.Lease.FencingToken, IdempotencyKey: "upgrade-shutdown", Result: l1.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}, RuntimeQuiescenceEvidence: l1.RuntimeQuiescenceAttempt})
			if err != nil {
				return nil, err
			}
		}
		output, err := launch(ctx, name, args...)
		if err == nil && args[0] == "bootstrap" && strings.HasSuffix(args[2], setupNodeAgentLabel+".plist") {
			syncClock()
			registration := registered.node.NodeRegistration
			registration.BootSessionID = "upgraded-boot"
			registration.AgentVersion = f.commit
			registration.CapabilityRevision = 0
			f.node, err = h.store.RegisterNode(ctx, registered.identity, registration, policy, true)
			managerConfig.BootSessionID = registration.BootSessionID
			manager, err = managedroot.Open(managerConfig)
			if err == nil {
				_, err = manager.CreateService(service.JobID, 1)
			}
		}
		return output, err
	}
	var output bytes.Buffer
	report, err := runUpgrade(ctx, f.host, false, &output)
	if err != nil {
		t.Fatal(err)
	}
	if restarted != 1 || len(report.RestartedServices) != 1 || report.RestartedServices[0] != service.JobID {
		t.Fatalf("restart not issued exactly once: %+v count=%d", report.RestartedServices, restarted)
	}
	for _, job := range []l1.Job{resident, late} {
		var done l1.Job
		err := h.clients.l1.do(ctx, http.MethodGet, "/v1/jobs/"+job.JobID, nil, nil, &done, http.StatusOK)
		if err != nil || done.State != contract.JobSucceeded || len(done.Attempts) != 1 || done.Attempts[0].Result == nil || done.Attempts[0].Result.ExitCode == nil || *done.Attempts[0].Result.ExitCode != 0 {
			t.Fatalf("normal one-shot result lost: %+v %v", done, err)
		}
	}
	next := claim(contract.JobClassOneShot)
	if next.Job.JobID != queued.JobID {
		t.Fatalf("queued submission not admitted after upgrade: %s", next.Job.JobID)
	}
	returned := claim(contract.JobClassService)
	if returned.Job.JobID != service.JobID {
		t.Fatal("service identity changed")
	}
	if _, err := h.store.SetAttemptPublication(ctx, registered.identity.NodeID, service.JobID, returned.Lease.AttemptID, l1.PublicationRequest{FencingToken: returned.Lease.FencingToken, Ready: &ready}); err != nil {
		t.Fatal(err)
	}
	after, err := h.clients.getService(ctx, service.JobID)
	if err != nil || after.PublishedPort == nil || *after.PublishedPort != 18081 || after.Spec.Restart != contract.RestartNever || after.BoundNodeID != registered.node.NodeID {
		t.Fatalf("binding, restart policy or published port changed: %+v %v", after, err)
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "keep me across replacement" {
		t.Fatalf("service data lost: %q %v", data, err)
	}
	assertNoUpgradeDrainReceipt(t, f)
}
