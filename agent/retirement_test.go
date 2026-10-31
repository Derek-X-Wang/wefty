//go:build darwin || linux

package agent

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent/managedroot"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
)

func promoteRetirementFixture(t *testing.T, store *l1.Store, nodeID, identity string) l1.NodeGeneration {
	t.Helper()
	node, err := store.GetNode(t.Context(), nodeID)
	if err != nil {
		t.Fatal(err)
	}
	node, err = store.SetNodeClaimsByOperator(t.Context(), nodeID, "operator", l1.NodeIntentRequest{ClaimsEnabled: false, IntentRevision: node.IntentRevision, Reason: "upgrade"})
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := store.NodeGenerations(t.Context(), identity, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	var primary l1.NodeGeneration
	for _, g := range inventory.Generations {
		if g.Role == l1.NodeGenerationPrimary {
			primary = g
		}
	}
	candidate := primary.Registration
	candidate.AgentGenerationID, candidate.BootSessionID = "next-generation", "next-boot"
	inventory, err = store.StageNodeGeneration(t.Context(), identity, candidate)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.PromoteNodeGeneration(t.Context(), identity, nodeID, l1.PromoteNodeGenerationRequest{
		OperationID: "promotion", ExpectedPrimaryGenerationID: primary.AgentGenerationID,
		ExpectedPrimaryBootSessionID: primary.BootSessionID, ExpectedAuthorityGeneration: primary.AuthorityGeneration,
		CandidateGenerationID: candidate.AgentGenerationID, CandidateBootSessionID: candidate.BootSessionID,
		ExpectedCatalogRevision: inventory.CatalogRevision, CatalogRevision: inventory.CatalogRevision + 1,
		IntentRevision: node.IntentRevision, SelectedProtocol: 1, RetiringGenerationIDs: []string{}, ResidentAttempts: primary.ResidentAttempts,
	})
	if err != nil {
		t.Fatal(err)
	}
	return primary
}

func newRetirementFixture(t *testing.T) (*Agent, *l1.Store, <-chan error, context.CancelFunc) {
	t.Helper()
	return newRetirementFixtureWithHook(t, nil)
}

func newRetirementFixtureWithHook(t *testing.T, hook func(*Agent, *l1.Store)) (*Agent, *l1.Store, <-chan error, context.CancelFunc) {
	t.Helper()
	return newRetirementFixtureWithStoreClock(t, nil, hook)
}

func newRetirementFixtureWithStoreClock(t *testing.T, clock l1.Clock, hook func(*Agent, *l1.Store)) (*Agent, *l1.Store, <-chan error, context.CancelFunc) {
	t.Helper()
	network := plain.NewNetwork()
	lease := 2 * time.Second
	if clock != nil {
		lease = 60 * time.Second
	}
	store, stop := startFailureServerWithPoliciesAndLease(t, network, clock, map[string]l1.NodePolicy{"node": {Tags: []string{"linux"}, MaxOneshotSlots: 3}}, lease)
	t.Cleanup(stop)
	zero := 0.0
	managed := temporaryManagedRoot(t)
	a, err := New(Config{Fabric: network.NewFabric(fabric.Identity{NodeID: "retirement-agent", Tags: []string{l1.DefaultAgentPrincipalTag}}), ControlPlaneAddress: "wefty://control-plane",
		NodeID: "node", BootSessionID: "boot", AgentGenerationID: "generation", OS: "darwin", Version: "test", Capabilities: map[string]bool{"kind:process": true, "upgrade:retirement:v1": true, contract.CapabilityExecCaptureBundle: true},
		HandoffRoot: t.TempDir(), GuardianExecutable: agentBinaryPath, LogSpoolDirectory: t.TempDir(), ManagedRootDirectory: managed, MaxOneshotSlots: 3, OneshotLoadLimit: &zero,
		HeartbeatInterval: 50 * time.Millisecond, ClaimInterval: 10 * time.Millisecond, RenewalInterval: 100 * time.Millisecond, Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	// Match the upgrade-service fixture: keep real process inventory but use
	// the host-boot seam before agent goroutines start (sandbox-safe on Mac).
	resource := a.managedResource.(*processManagedResource)
	resource.root, err = managedroot.Open(managedroot.Config{Root: managed, NodeID: "node", BootSessionID: "boot", HostBootIdentity: rootHostIdentity})
	if err != nil {
		t.Fatal(err)
	}
	if hook != nil {
		hook(a, store)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(testwait.Bound):
			t.Error("agent did not join")
		}
	})
	select {
	case <-a.Registered():
	case <-time.After(testwait.Bound):
		t.Fatal("registration timed out")
	}
	return a, store, done, cancel
}

func retirementWait(t *testing.T, description string, condition func() bool) {
	t.Helper()
	if err := awaitFixtureCondition(t.Context(), description, condition); err != nil {
		t.Fatal(err)
	}
}

func TestRetirementKeepsRealOrdinaryAndRestartableProcesses(t *testing.T) {
	appender := newRecordingAppender("")
	a, store, done, _ := newRetirementFixtureWithHook(t, func(a *Agent, _ *l1.Store) { a.runLedger = appender })
	var jobs []l1.Job
	var ready, releases []string
	for i, restartable := range []bool{false, true} {
		dir := t.TempDir()
		r, release := filepath.Join(dir, "ready"), filepath.Join(dir, "release")
		ready, releases = append(ready, r), append(releases, release)
		script := `echo before; touch "$1"; while [ ! -e "$2" ]; do sleep 0.1; done; echo after; printf 'wefty-protocol: 1\nkind: step\nname: after-retirement\n--\n' > "$WEFTY_RUN_DIR/events/0001-step"; exit 17`
		job, _, err := store.CreateJobAs(t.Context(), contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: fmt.Sprint("retirement-", i), Kind: contract.JobKindProcess, Class: contract.JobClassOneShot, Restartable: restartable, Labels: map[string]string{"run_id": fmt.Sprint("run-retirement-", i)}, RoutingTags: []string{"linux"},
			Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", "-c", script, "sh", r, release}, WorkingDirectory: dir, HandoffDirectory: filepath.Join(a.handoffs.root, fmt.Sprint("run-retirement-", i)), Env: map[string]string{contract.EnvRunID: fmt.Sprint("run-retirement-", i), contract.EnvL3Endpoint: "http://127.0.0.1:9/"}, SensitiveEnv: map[string]string{contract.EnvRunToken: mailboxTestToken}}}, l1.JobOrigin{OriginatingSubmitter: "ledger", SubmittedByRunLedger: true})
		if err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, job)
		t.Cleanup(func() { _ = os.WriteFile(release, nil, 0600) })
	}
	for _, path := range ready {
		retirementWait(t, "payload ready", func() bool { _, err := os.Stat(path); return err == nil })
	}
	primary := promoteRetirementFixture(t, store, "node", "retirement-agent")
	retired, err := a.Retire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if retired.PromotionOperationID != "promotion" || len(retired.ResidentAttempts) != 2 {
		t.Fatalf("retirement=%+v", retired)
	}
	if _, err := a.Retire(t.Context()); err != nil {
		t.Fatalf("replay: %v", err)
	}
	queued := createAgentTestJob(t, store, "after-retirement")
	before, err := store.ListJobAttempts(t.Context(), jobs[0].JobID)
	if err != nil {
		t.Fatal(err)
	}
	// Longer than the original Lease: only ongoing renewal can keep both payloads.
	time.Sleep(2400 * time.Millisecond)
	for _, job := range jobs {
		current, err := store.GetJob(t.Context(), job.JobID)
		if err != nil || current.State != contract.JobRunning {
			t.Fatalf("resident ended: %+v %v", current, err)
		}
	}
	after, _ := store.ListJobAttempts(t.Context(), jobs[0].JobID)
	if len(before) != 1 || len(after) != 1 || !after[0].LeaseExpiresAt.After(before[0].LeaseExpiresAt) {
		t.Fatal("retiring Lease did not renew")
	}
	node, _ := store.GetNode(t.Context(), "node")
	if node.BootSessionID == primary.BootSessionID {
		t.Fatal("promotion did not replace primary")
	}
	if a.Status().State != LifecycleRetiring {
		t.Fatalf("state=%s", a.Status().State)
	}
	select {
	case err := <-done:
		t.Fatalf("retiree ended before payloads: %v", err)
	default:
	}
	for _, path := range releases {
		if err := os.WriteFile(path, nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		done = nil
	case <-time.After(15 * time.Second):
		a.session.claimMu.Lock()
		for id, record := range a.session.retirementAttempts {
			t.Logf("retained proof %s: %+v", id, *record)
		}
		a.session.claimMu.Unlock()
		t.Fatalf("retiree did not close: %+v", a.Status())
	}
	for _, job := range jobs {
		current, err := store.GetJob(t.Context(), job.JobID)
		if err != nil {
			t.Fatal(err)
		}
		attempts, e := store.ListJobAttempts(t.Context(), job.JobID)
		if e != nil {
			t.Fatal(e)
		}
		if len(attempts) != 1 || attempts[0].Result == nil || attempts[0].Result.ExitCode == nil || *attempts[0].Result.ExitCode != 17 {
			t.Fatalf("lost real exit: %+v", current)
		}
		page, err := store.GetJobLogs(t.Context(), job.JobID, "", l1.MaxLogPageLimit)
		if err != nil {
			t.Fatal(err)
		}
		var output strings.Builder
		for _, event := range page.Events {
			output.Write(event.Bytes)
		}
		if !strings.Contains(output.String(), "before") || !strings.Contains(output.String(), "after") || strings.Contains(output.String(), "interrupted") {
			t.Fatalf("output=%q", output.String())
		}
	}
	if documents := appender.snapshot(); len(documents) != 2 {
		t.Fatalf("mailbox publication=%+v", documents)
	}
	current, _ := store.GetJob(t.Context(), queued.JobID)
	if current.State != contract.JobQueued {
		t.Fatalf("new claim after retirement: %s", current.State)
	}
	inventory, err := store.NodeGenerations(t.Context(), "retirement-agent", "node")
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range inventory.Generations {
		if g.AgentGenerationID == "generation" && g.Role != l1.NodeGenerationClosed {
			t.Fatalf("generation not closed: %+v", g)
		}
	}
}

func TestRetirementIdleClosesAndExits(t *testing.T) {
	a, store, done, _ := newRetirementFixture(t)
	if _, err := a.Retire(t.Context()); err == nil {
		t.Fatal("local request retired primary without L1 promotion")
	}
	promoteRetirementFixture(t, store, "node", "retirement-agent")
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(testwait.Bound):
		t.Fatalf("idle retiree stuck: %+v", a.Status())
	}
}

func TestRetirementJoinsInFlightClaimWithoutCancelingIt(t *testing.T) {
	committed, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var claimsAfterRetirement atomic.Int32
	var job l1.Job
	a, store, done, _ := newRetirementFixtureWithHook(t, func(a *Agent, store *l1.Store) {
		var err error
		dir := t.TempDir()
		job, _, err = store.CreateJob(t.Context(), contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: "in-flight-retirement", Kind: contract.JobKindProcess, Class: contract.JobClassOneShot, RoutingTags: []string{"linux"}, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", "-c", "exit 0"}, WorkingDirectory: dir, HandoffDirectory: dir}})
		if err != nil {
			t.Fatal(err)
		}
		base := a.session.client.httpClient.Transport
		a.session.client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == "/v1/agent/jobs/claim" && a.session.isRetiring() {
				claimsAfterRetirement.Add(1)
			}
			response, err := base.RoundTrip(r)
			if err == nil && r.URL.Path == "/v1/agent/jobs/claim" && response.StatusCode == http.StatusOK {
				once.Do(func() { close(committed); <-release })
			}
			return response, err
		})
	})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	select {
	case <-committed:
	case <-time.After(testwait.Bound):
		t.Fatal("claim did not commit")
	}
	promoteRetirementFixture(t, store, "node", "retirement-agent")
	retired := make(chan error, 1)
	go func() { _, err := a.Retire(t.Context()); retired <- err }()
	select {
	case err := <-retired:
		t.Fatalf("did not join in-flight claim: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-retired:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(testwait.Bound):
		t.Fatal("retirement did not acknowledge claim")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(testwait.Bound):
		t.Fatalf("retirement stuck: %+v", a.Status())
	}
	current, err := store.GetJob(t.Context(), job.JobID)
	if err != nil || current.State != contract.JobSucceeded {
		attempts, _ := store.ListJobAttempts(t.Context(), job.JobID)
		for _, at := range attempts {
			if at.Result != nil {
				t.Logf("result=%+v", *at.Result)
				if at.Result.SpawnError != nil {
					t.Logf("spawn=%+v", *at.Result.SpawnError)
				}
			}
		}
		t.Fatalf("in-flight claim not completed: %+v %v attempts=%+v", current, err, attempts)
	}
	if claimsAfterRetirement.Load() != 0 {
		t.Fatal("claim RPC after retirement started")
	}
}

func TestRetirementClosureRequiresReapEvidenceAndL1Settlement(t *testing.T) {
	var calls atomic.Int32
	var accepted atomic.Bool
	client, stop := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if !accepted.Load() {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":{"code":"conflict","message":"live completion pending"}}`))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}), time.Second)
	defer stop()
	defer client.Close()
	clock := newManualClock(mailboxTestClockOrigin)
	session := newAgentSession(client, contract.NodeRegistration{NodeID: "node", BootSessionID: "boot", AgentGenerationID: "generation"}, nil, time.Second, time.Second, clock, newLifecycleObserver(clock), nil, 1, 0)
	session.retiringGeneration = &l1.NodeGeneration{AgentGenerationID: "generation", BootSessionID: "boot", PromotionOperationID: "promotion", ResidentAttempts: []l1.ResidentAttempt{{JobID: "job", AttemptID: "attempt", BootSessionID: "boot", FencingToken: "fence"}}}
	close(session.retiringRequested)
	record := &retirementAttempt{jobID: "job", fencingToken: "fence"}
	session.retirementAttempts["attempt"] = record
	session.retirementEvidenceSettled = func(context.Context, l1.NodeGeneration, []l1.ResidentAttempt) (bool, error) { return true, nil }
	if closed, _ := session.closeRetirement(t.Context()); closed || calls.Load() != 0 {
		t.Fatal("custody replaced runtime reap")
	}
	record.reaped = true
	session.retirementEvidenceSettled = nil
	if closed, _ := session.closeRetirement(t.Context()); closed || calls.Load() != 0 {
		t.Fatal("pending evidence allowed closure")
	}
	session.retirementEvidenceSettled = func(_ context.Context, _ l1.NodeGeneration, pending []l1.ResidentAttempt) (bool, error) {
		if len(pending) != 1 || pending[0].AttemptID != "attempt" || pending[0].FencingToken != "fence" {
			t.Fatalf("custody identities=%+v", pending)
		}
		return true, nil
	}
	if closed, err := session.closeRetirement(t.Context()); closed || err == nil || calls.Load() != 1 {
		t.Fatalf("custody bypassed L1 completion prerequisite: %v %v", closed, err)
	}
	// Local time, even days later, does not manufacture an authoritative Lease end.
	clock.Advance(48 * time.Hour)
	if closed, _ := session.closeRetirement(t.Context()); closed {
		t.Fatal("local clock closed a live L1 Lease")
	}
	record.evidenceSettled = true
	session.retirementEvidenceSettled = func(_ context.Context, _ l1.NodeGeneration, pending []l1.ResidentAttempt) (bool, error) {
		if len(pending) != 0 {
			t.Fatalf("unexpected pending identities: %+v", pending)
		}
		return false, nil
	}
	accepted.Store(true)
	if closed, _ := session.closeRetirement(t.Context()); closed {
		t.Fatal("shared evidence veto ignored when process-local rows were settled")
	}
	session.retirementEvidenceSettled = nil
	if closed, err := session.closeRetirement(t.Context()); !closed || err != nil {
		t.Fatalf("settled close=%v %v", closed, err)
	}
}

func TestRetirementRestartableTERMUsesAgentShutdownInterruption(t *testing.T) {
	a, store, done, _ := newRetirementFixture(t)
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	job, _, err := store.CreateJob(t.Context(), contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: "retiring-term-restartable", Kind: contract.JobKindProcess, Class: contract.JobClassOneShot, Restartable: true, RoutingTags: []string{"linux"}, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", "-c", `touch "$1"; while :; do sleep 0.1; done`, "sh", ready}, WorkingDirectory: dir, HandoffDirectory: dir}})
	if err != nil {
		t.Fatal(err)
	}
	retirementWait(t, "restartable payload ready", func() bool { _, err := os.Stat(ready); return err == nil })
	promoteRetirementFixture(t, store, "node", "retirement-agent")
	if _, err := a.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	// These are the production TERM callback's two operations, in order.
	a.InterruptRestartableForShutdown()
	_, _ = a.Drain(t.Context())
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("TERM did not interrupt Restartable resident")
	}
	attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
	if err != nil || len(attempts) != 1 || attempts[0].InterruptionReason != l1.InterruptionAgentShutdown {
		t.Fatalf("TERM interruption=%+v %v", attempts, err)
	}
}

func TestRetirementRenewsWhileFirstCompletionWaitsForPendingLogs(t *testing.T) {
	var blocked atomic.Bool
	blocked.Store(true)
	var completions atomic.Int32
	a, store, done, _ := newRetirementFixtureWithHook(t, func(a *Agent, _ *l1.Store) {
		a.finalizationTimeout = 100 * time.Millisecond
		base := a.session.client.httpClient.Transport
		a.session.client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
			if strings.HasSuffix(r.URL.Path, "/logs") && blocked.Load() {
				return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":{"code":"internal","message":"injected outage","retryable":true}}`)), Request: r}, nil
			}
			if strings.HasSuffix(r.URL.Path, "/complete") {
				completions.Add(1)
			}
			return base.RoundTrip(r)
		})
	})
	dir := t.TempDir()
	ready, release := filepath.Join(dir, "ready"), filepath.Join(dir, "release")
	t.Cleanup(func() { _ = os.WriteFile(release, nil, 0600) })
	job, _, err := store.CreateJob(t.Context(), contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: "retiring-pending-logs", Kind: contract.JobKindProcess, Class: contract.JobClassOneShot, RoutingTags: []string{"linux"}, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", "-c", `echo pending-evidence; touch "$1"; while [ ! -e "$2" ]; do sleep 0.1; done; exit 17`, "sh", ready, release}, WorkingDirectory: dir, HandoffDirectory: dir}})
	if err != nil {
		t.Fatal(err)
	}
	retirementWait(t, "payload ready", func() bool { _, err := os.Stat(ready); return err == nil })
	promoteRetirementFixture(t, store, "node", "retirement-agent")
	if _, err := a.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	retirementWait(t, "pending live finalization", func() bool {
		for _, attempt := range a.Status().Attempts {
			if attempt.State == AttemptFinalizing {
				return true
			}
		}
		return false
	})
	before, _ := store.ListJobAttempts(t.Context(), job.JobID)
	time.Sleep(2200 * time.Millisecond)
	after, _ := store.ListJobAttempts(t.Context(), job.JobID)
	if len(before) != 1 || len(after) != 1 || !after[0].LeaseExpiresAt.After(before[0].LeaseExpiresAt) {
		t.Fatal("first completion pending: agent stopped renewing")
	}
	if completions.Load() != 0 {
		t.Fatal("completion overtook ordered pending logs")
	}
	select {
	case err := <-done:
		t.Fatalf("retirement closed with first completion pending: %v", err)
	default:
	}
	blocked.Store(false)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("pending completion never settled: %+v", a.Status())
	}
	attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
	if err != nil || len(attempts) != 1 || attempts[0].Result == nil || attempts[0].Result.ExitCode == nil || *attempts[0].Result.ExitCode != 17 {
		t.Fatalf("result=%+v %v", attempts, err)
	}
	page, err := store.GetJobLogs(t.Context(), job.JobID, "", l1.MaxLogPageLimit)
	if err != nil || len(page.Events) != 1 || !strings.Contains(string(page.Events[0].Bytes), "pending-evidence") {
		t.Fatalf("pending logs=%+v %v", page, err)
	}
}

func TestRetirementLostClaimReplyWaitsForAuthoritativeLeaseEnd(t *testing.T) {
	committed, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	dir := t.TempDir()
	unexpected := filepath.Join(dir, "unexpected-start")
	a, store, done, _ := newRetirementFixtureWithHook(t, func(a *Agent, store *l1.Store) {
		a.session.claimInterval = 50 * time.Millisecond
		_, _, err := store.CreateJob(t.Context(), contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: "lost-claim-retirement", Kind: contract.JobKindProcess, Class: contract.JobClassOneShot, RoutingTags: []string{"linux"}, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", "-c", `touch "$1"`, "sh", unexpected}, WorkingDirectory: dir, HandoffDirectory: dir}})
		if err != nil {
			t.Fatal(err)
		}
		base := a.session.client.httpClient.Transport
		a.session.client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
			response, err := base.RoundTrip(r)
			lost := false
			if err == nil && r.URL.Path == "/v1/agent/jobs/claim" && response.StatusCode == http.StatusOK {
				once.Do(func() { close(committed); <-release; lost = true })
			}
			if lost {
				_ = response.Body.Close()
				return nil, context.DeadlineExceeded
			}
			return response, err
		})
	})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	select {
	case <-committed:
	case <-time.After(testwait.Bound):
		t.Fatal("claim did not commit")
	}
	promoteRetirementFixture(t, store, "node", "retirement-agent")
	close(release)
	if _, err := a.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		t.Fatalf("retired with unaccepted live claim: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("ended Lease did not permit closure: %+v", a.Status())
	}
	if _, err := os.Stat(unexpected); !os.IsNotExist(err) {
		t.Fatalf("lost claim credentials were invented or payload started: %v", err)
	}
}
