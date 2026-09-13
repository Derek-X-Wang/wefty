//go:build darwin || linux

package agent

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

// TestRefusedOCIRemovalDeclaresAStallAndSurvivesALostResponse is the wedge end
// to end, over the real acknowledgement path: a standing directive the node
// keeps being refused, a declaration that L1 commits, and a response the node
// never sees. The retry must replay the same declaration rather than build a
// newer one -- a newer one carries a higher attempt count under the same key
// and would conflict forever, trading the old wedge for a new one.
func TestRefusedOCIRemovalDeclaresAStallAndSurvivesALostResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	// L1 measures the bound on its own clock, so the test moves that clock
	// rather than asserting the agent's word for how long it waited.
	var l1Offset atomic.Int64
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{
		Clock: l1.ClockFunc(func() time.Time { return time.Now().Add(time.Duration(l1Offset.Load())) }),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{
		NodePolicies: map[string]l1.NodePolicy{"stall-node": l1.DefaultNodePolicy("linux")},
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}

	// Drop exactly one accepted acknowledgement response. L1 has committed;
	// the agent cannot tell that from a refusal.
	var accepted, dropped atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !bytes.HasSuffix([]byte(r.URL.Path), []byte("/removal-acknowledgement")) {
			server.Handler().ServeHTTP(w, r)
			return
		}
		buffered := httptest.NewRecorder()
		server.Handler().ServeHTTP(buffered, r)
		if buffered.Code == http.StatusOK {
			accepted.Add(1)
			if dropped.CompareAndSwap(0, 1) {
				// The committed response never reaches the agent.
				w.WriteHeader(http.StatusBadGateway)
				return
			}
		}
		for key, values := range buffered.Header() {
			w.Header()[key] = values
		}
		w.WriteHeader(buffered.Code)
		_, _ = w.Write(buffered.Body.Bytes())
	})
	httpServer := &http.Server{Handler: handler}
	serveDone := make(chan error, 1)
	go func() { serveDone <- httpServer.Serve(listener) }()
	defer func() {
		cancel()
		_ = httpServer.Close()
		if err := <-serveDone; err != nil && err != http.ErrServerClosed {
			t.Errorf("serve: %v", err)
		}
	}()

	agentClient, err := NewClient(network.NewFabric(fabric.Identity{
		NodeID: "fabric-agent", Tags: []string{l1.DefaultAgentPrincipalTag}}), "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	defer agentClient.Close()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	managed, err := initializeManagedResource(root, "stall-node", "stall-boot")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agentClient.Register(ctx, contract.NodeRegistration{
		NodeID: "stall-node", BootSessionID: "stall-boot", RootInstanceID: managed.rootInstanceID(),
		OS: "linux", Architecture: "arm64", AgentVersion: "test", CapabilityRevision: 1,
		CapabilityObservedAt: time.Now(), MissingCapabilities: []string{},
		Capabilities: map[string]bool{"kind:process": true, "kind:oci": true,
			"runtime_handler:io.containerd.runc.v2": true},
	}); err != nil {
		t.Fatal(err)
	}

	digest := "sha256:0000000000000000000000000000000000000000000000000000000000000001"
	spec := contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "stall-delivery", Kind: contract.JobKindOCI,
		Class: contract.JobClassService, Restart: contract.RestartAlways, RoutingTags: []string{"linux"},
		RuntimeHandler: "io.containerd.runc.v2",
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image: contract.OCIImageSpec{Reference: "ghcr.io/example/tool:latest", Digest: &digest}}},
	}
	if err := contract.ValidateJobSpec(&spec); err != nil {
		t.Fatalf("OCI service fixture does not satisfy the public contract: %v", err)
	}
	job, _, err := store.CreateJob(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := agentClient.Claim(ctx, "stall-node", "stall-boot", contract.JobClassService)
	if err != nil || claim == nil || claim.Job.JobID != job.JobID {
		observed, _ := store.GetJob(ctx, job.JobID)
		t.Fatalf("claim=%+v err=%v unschedulable=%q state=%q", claim, err, observed.UnschedulableReason, observed.State)
	}
	outbox, err := newEvidenceOutbox(t.TempDir(), "stall-node", 1<<20, systemClock{}, 1, time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	if err := outbox.spool.ensureAttempt(ctx, *claim); err != nil {
		t.Fatal(err)
	}
	if err := outbox.spool.storeRuntimeResourceManifest(ctx,
		testRuntimeResourceManifest(job.JobID, claim.Lease.AttemptID), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RemoveService(ctx, job.JobID); err != nil {
		t.Fatal(err)
	}
	directives, err := store.ListNodeRemovalDirectives(ctx, "fabric-agent", "stall-node", "stall-boot")
	if err != nil || len(directives) != 1 {
		t.Fatalf("directives=%+v err=%v", directives, err)
	}
	directive := directives[0]
	// The directive now stands past both sides of the bound.
	l1Offset.Store(int64(l1.DefaultRemovalStallBound + time.Minute))

	controller := newRemovalController(agentClient, outbox, managed, nil, "stall-node", "stall-boot", t.Logf)
	// The helper is the one seam: it answers the refusal that produced #450.
	// Everything past it -- accounting, the bound, freezing, the HTTP
	// acknowledgement and L1's transaction -- is real.
	controller.reapService = func(context.Context, string, string, []workloadrunner.RuntimeResourceManifest) (workloadrunner.ReapReceipt, error) {
		return workloadrunner.ReapReceipt{}, wedgeRefusal()
	}
	past := time.Now().Add(l1.DefaultRemovalStallBound + time.Minute)
	controller.now = func() time.Time { return past }

	// Five heartbeats: the first three build the streak and still surface the
	// refusal, the third declares and has its response dropped, the fourth
	// replays what was frozen, and the fifth finds the removal already
	// declared. Beats before the declaration are expected to report the
	// refusal; that is the wedge, not a test failure.
	for beat := 0; beat < 5; beat++ {
		if err := controller.reconcile(ctx, directive); err != nil {
			t.Logf("beat %d still refused: %v", beat, err)
		}
	}
	if accepted.Load() < 2 {
		t.Fatalf("L1 accepted %d declarations; the lost response was never retried", accepted.Load())
	}

	observed, err := store.GetJob(ctx, job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if observed.State != contract.JobStalledCleanupUnverified || observed.Removal == nil ||
		observed.Removal.RemovalOutcome != l1.ServiceRemovalOutcomeCleanupStalled {
		t.Fatalf("declared removal projection = %#v", observed)
	}
	if observed.Removal.Stall == nil || observed.Removal.Stall.LastRefusalCode != "unauthorized_attempt" ||
		observed.Removal.Stall.Attempts < l1.MinimumServiceRemovalStallAttempts {
		t.Fatalf("declared evidence = %#v", observed.Removal.Stall)
	}
	// A further refusal must stay a no-op rather than resend a newer body.
	if err := controller.reconcile(ctx, directive); err != nil {
		t.Fatalf("refusal after a declared stall was reported as a boot failure: %v", err)
	}
	nodes, err := store.ListNodes(ctx)
	if err != nil || len(nodes) != 1 {
		t.Fatalf("nodes = %#v, %v", nodes, err)
	}
	if nodes[0].ServiceOccupancy != 0 {
		t.Fatalf("service occupancy after a declared stall = %d, want 0", nodes[0].ServiceOccupancy)
	}
}
