//go:build darwin || linux

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

// This controlled ordering proof does not attribute a hosted failure. The only
// intervention delays delivery of a successful, fully committed removal ACK.
func TestRemovalAcknowledgementResponsePrecedesLocalIntentRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	started := time.Now()
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{"removal-node": l1.DefaultNodePolicy("linux")}})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	committed := make(chan time.Time, 1)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	var handlers sync.WaitGroup
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlers.Add(1)
		defer handlers.Done()
		if r.Method != http.MethodPost || !bytes.HasSuffix([]byte(r.URL.Path), []byte("/removal-acknowledgement")) {
			server.Handler().ServeHTTP(w, r)
			return
		}
		buffered := httptest.NewRecorder()
		server.Handler().ServeHTTP(buffered, r)
		if buffered.Code == http.StatusOK {
			committed <- time.Now()
			select {
			case <-gate:
			case <-r.Context().Done():
				return
			case <-ctx.Done():
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
		release()
		cancel()
		_ = httpServer.Close()
		if err := <-serveDone; err != nil && err != http.ErrServerClosed {
			t.Errorf("serve: %v", err)
		}
		handlers.Wait()
	}()
	agentClient, err := NewClient(network.NewFabric(fabric.Identity{NodeID: "fabric-agent", Tags: []string{l1.DefaultAgentPrincipalTag}}), "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	defer agentClient.Close()
	observer, err := NewClient(network.NewFabric(fabric.Identity{NodeID: "operator", Tags: []string{l1.DefaultClientPrincipalTag}}), "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	managed, err := initializeManagedResource(root, "removal-node", "removal-boot")
	if err != nil {
		t.Fatal(err)
	}
	_, err = agentClient.Register(ctx, contract.NodeRegistration{NodeID: "removal-node", BootSessionID: "removal-boot", RootInstanceID: managed.rootInstanceID(), OS: "linux", Architecture: "arm64", AgentVersion: "test", CapabilityRevision: 1, CapabilityObservedAt: time.Now(), MissingCapabilities: []string{}, Capabilities: map[string]bool{"kind:process": true}})
	if err != nil {
		t.Fatal(err)
	}
	spec := contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: "ack-observation", Kind: contract.JobKindProcess, Class: contract.JobClassService, Restart: contract.RestartAlways, RoutingTags: []string{"linux"}, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/true"}, Argv: []string{"true"}, WorkingDirectory: root}}
	job, _, err := store.CreateJob(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := agentClient.Claim(ctx, "removal-node", "removal-boot", contract.JobClassService)
	if err != nil || claim == nil || claim.Job.JobID != job.JobID {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	paths, cleanupAttempt, err := managed.prepareAttempt(job.JobID, claim.Lease.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupAttempt()
	outbox, err := newEvidenceOutbox(t.TempDir(), "removal-node", 1<<20, systemClock{}, 1, time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	if err := outbox.spool.ensureAttempt(ctx, *claim); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RemoveService(ctx, job.JobID); err != nil {
		t.Fatal(err)
	}
	directives, err := store.ListNodeRemovalDirectives(ctx, "fabric-agent", "removal-node", "removal-boot")
	if err != nil || len(directives) != 1 {
		t.Fatalf("directives=%+v err=%v", directives, err)
	}
	directive := directives[0]
	t.Logf("identity job=%s generation=%d fence=%s root=%s", directive.JobID, directive.RemovalGeneration, directive.CleanupFence, directive.RootInstanceID)
	controller := newRemovalController(agentClient, outbox, managed, nil, "removal-node", "removal-boot", t.Logf)
	// No payload is launched in this removal/HTTP/spool fixture. Reaping is the
	// existing controller fixture seam; actual managed deletion and persistence
	// below remain real. This proof makes no runtime-reaping claim.
	controller.reapService = func(context.Context, string, string, []workloadrunner.RuntimeResourceManifest) (workloadrunner.ReapReceipt, error) {
		return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt, BootSessionID: "removal-boot"}, nil
	}
	done := make(chan struct{})
	var operationErr error
	go func() { defer close(done); operationErr = controller.process(ctx, directive) }()
	defer func() { release(); cancel(); <-done }()
	select {
	case at := <-committed:
		t.Logf("real ACK finalized; response withheld elapsed=%s", at.Sub(started))
	case <-done:
		t.Fatalf("removal returned before successful ACK gate: %v", operationErr)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Both requests use authenticated real HTTP while the ACK response is held.
	for _, request := range []struct {
		method, path string
		body         any
	}{
		{http.MethodGet, "/v1/jobs/" + job.JobID + "?class=service", nil},
		{http.MethodPost, "/v1/jobs", spec},
	} {
		observed := removalProbeHTTPJob(t, ctx, observer, request.method, request.path, request.body)
		if observed.JobID != job.JobID || observed.State != contract.JobRemovedVerified {
			t.Fatalf("remote observation=%+v", observed)
		}
	}
	if _, err := os.Stat(paths.dataDirectory); !os.IsNotExist(err) {
		t.Fatalf("managed data remains: %v", err)
	}
	intent, found, err := outbox.removalIntent(ctx, job.JobID)
	if err != nil || !found || intent.generation != directive.RemovalGeneration || intent.cleanupFence != directive.CleanupFence || intent.rootInstanceID != directive.RootInstanceID {
		t.Fatalf("intent=%+v found=%t err=%v", intent, found, err)
	}
	attempts, removals := removalProbeCounts(t, ctx, outbox.spool, job.JobID)
	if attempts != 0 || removals != 1 {
		t.Fatalf("held ACK counts=%d/%d, want 0/1", attempts, removals)
	}
	t.Logf("RED immediate zero-row invariant: attempts=%d removals=%d elapsed=%s", attempts, removals, time.Since(started))
	select {
	case <-done:
		t.Fatalf("controller escaped gate: %v", operationErr)
	default:
	}
	release()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if operationErr != nil {
		t.Fatalf("real removal operation: %v", operationErr)
	}
	attempts, removals = removalProbeCounts(t, ctx, outbox.spool, job.JobID)
	if attempts != 0 || removals != 0 {
		t.Fatalf("released ACK counts=%d/%d, want 0/0", attempts, removals)
	}
	t.Logf("GREEN same zero-row invariant after controller/local commit: attempts=%d removals=%d elapsed=%s", attempts, removals, time.Since(started))
}

func removalProbeCounts(t *testing.T, ctx context.Context, spool *logSpool, jobID string) (int, int) {
	t.Helper()
	var attempts, removals int
	if err := spool.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM spool_attempts WHERE job_id=?", jobID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := spool.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM spool_removals WHERE job_id=?", jobID).Scan(&removals); err != nil {
		t.Fatal(err)
	}
	return attempts, removals
}

func removalProbeHTTPJob(t *testing.T, ctx context.Context, client *Client, method, path string, body any) l1.Job {
	t.Helper()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, method, client.baseURL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(response.Body)
		t.Fatal(fmt.Sprintf("%s %s status=%d body=%s", method, path, response.StatusCode, data))
	}
	var job l1.Job
	if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}
	return job
}
