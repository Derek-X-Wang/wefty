//go:build darwin || linux

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

// removalACKRetryClock completes waits on a controlled timeline. Cancellation
// happens after the selected timer is armed, before its channel is delivered.
type removalACKRetryClock struct {
	*manualClock
	delays      []time.Duration
	cancel      context.CancelFunc
	cancelAfter int
}

func (clock *removalACKRetryClock) NewTimer(delay time.Duration) Timer {
	timer := clock.manualClock.NewTimer(delay)
	clock.delays = append(clock.delays, delay)
	if clock.cancel != nil && len(clock.delays) == clock.cancelAfter {
		clock.cancel()
	} else {
		clock.Advance(delay)
	}
	return timer
}

func assertRemovalACKBackoff(t *testing.T, clock *removalACKRetryClock, retries int) {
	t.Helper()
	ceilings := []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second, 5 * time.Second}
	if len(clock.delays) != retries {
		t.Fatalf("retry timers=%v, want %d", clock.delays, retries)
	}
	var elapsed, bound time.Duration
	for i, delay := range clock.delays {
		// sessionBackoff uses equal jitter in [ceiling/2, ceiling]. Every
		// random sample must obey these fixed bounds, including the cap.
		if delay < ceilings[i]/2 || delay > ceilings[i] {
			t.Fatalf("retry %d delay=%s, want [%s,%s]", i+1, delay, ceilings[i]/2, ceilings[i])
		}
		elapsed += delay
		bound += ceilings[i]
	}
	start := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	if actual := clock.Now().Sub(start); actual != elapsed || actual > bound {
		t.Fatalf("virtual elapsed=%s, want scheduled sum=%s <= %s", actual, elapsed, bound)
	}
	t.Logf("ACK retry delays=%v virtual elapsed=%s ceiling sum=%s", clock.delays, elapsed, bound)
}

// The transport seam loses a response only after real L1 acknowledgement and
// finalization commits. Runtime reaping and managed deletion are injected; the
// controller, HTTP encoding/decoding, L1 replay, and local spool are real.
func TestRemovalAcknowledgementLostResponseReleasesLocalIntentPromptly(t *testing.T) {
	for _, scenario := range []struct {
		name        string
		failure     string
		failures    int
		cancelAfter int
	}{
		{name: "transport", failure: "transport", failures: 3},
		{name: "invalid-success", failure: "invalid-success", failures: 3},
		{name: "invalid-gateway", failure: "invalid-gateway", failures: 3},
		{name: "internal", failure: "internal", failures: 3},
		{name: "unavailable", failure: "unavailable", failures: 2},
		{name: "unavailable-unmarked", failure: "unavailable-unmarked", failures: 2},
		{name: "retryable-refusal", failure: "retryable-refusal", failures: 2},
		{name: "capped", failure: "transport", failures: 8},
		{name: "canceled-backoff", failure: "transport", failures: 3, cancelAfter: 2},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			clock := &removalACKRetryClock{manualClock: newManualClock(time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)), cancel: cancel, cancelAfter: scenario.cancelAfter}
			store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{Clock: l1.ClockFunc(clock.Now)})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			store.SetReadSnapshotHardLimitForTest(2 * time.Second)
			const nodeID, bootID = "removal-node", "removal-boot"
			_, err = store.RegisterNode(ctx, fabric.Identity{NodeID: "removal-principal"}, contract.NodeRegistration{
				NodeID: nodeID, BootSessionID: bootID, RootInstanceID: "removal-root", OS: "linux", Architecture: "arm64",
				AgentVersion: "test", CapabilityRevision: 1, CapabilityObservedAt: clock.Now(),
				Capabilities: map[string]bool{"kind:process": true}, MissingCapabilities: []string{},
			}, l1.DefaultNodePolicy("linux"), true)
			if err != nil {
				t.Fatal(err)
			}
			job, _, err := store.CreateJob(ctx, contract.JobSpec{
				SchemaVersion: contract.SchemaVersionV1, DispatchKey: "removal-response-loss", Kind: contract.JobKindProcess,
				Class: contract.JobClassService, Restart: contract.RestartAlways, RoutingTags: []string{"linux"},
				Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/true"}, Argv: []string{"true"}, WorkingDirectory: t.TempDir()},
			})
			if err != nil {
				t.Fatal(err)
			}
			claim, err := store.ClaimJob(ctx, "removal-principal", nodeID, bootID, contract.JobClassService)
			if err != nil || claim == nil || claim.Job.JobID != job.JobID {
				t.Fatalf("claim=%+v err=%v", claim, err)
			}
			running, err := store.StartAttempt(ctx, "removal-principal", job.JobID, claim.Lease.AttemptID, l1.StartedRequest{FencingToken: claim.Lease.FencingToken})
			if err != nil || running.State != contract.JobRunning {
				t.Fatalf("running service=%+v err=%v", running, err)
			}
			spool := openTestLogSpool(t, t.TempDir(), nodeID, 1<<20)
			defer spool.Close()
			if err := spool.ensureAttempt(ctx, *claim); err != nil {
				t.Fatal(err)
			}
			if _, err := store.RemoveService(ctx, job.JobID); err != nil {
				t.Fatal(err)
			}
			directives, err := store.ListNodeRemovalDirectives(ctx, "removal-principal", nodeID, bootID)
			if err != nil || len(directives) != 1 {
				t.Fatalf("directives=%+v err=%v", directives, err)
			}
			var firstBody []byte
			calls := 0
			client := &Client{baseURL: "http://removal.invalid", operationTimeout: time.Second}
			client.httpClient = &http.Client{Transport: captureRoundTrip(func(request *http.Request) (*http.Response, error) {
				calls++
				body, err := io.ReadAll(request.Body)
				if err != nil {
					t.Fatal(err)
				}
				if calls == 1 {
					firstBody = body
				} else if !bytes.Equal(firstBody, body) {
					t.Fatalf("ACK replay changed request: first=%s replay=%s", firstBody, body)
				}
				var acknowledgement l1.RemovalAcknowledgementRequest
				if err := json.Unmarshal(body, &acknowledgement); err != nil {
					t.Fatal(err)
				}
				if _, err := store.AcknowledgeServiceRemoval(ctx, "removal-principal", job.JobID, acknowledgement); err != nil {
					t.Fatal(err)
				}
				finalized, _, err := store.FinalizeServiceRemoval(ctx, job.JobID)
				if err != nil || finalized.State != contract.JobRemovedVerified || finalized.Removal.CleanupAcknowledgedAt == nil {
					t.Fatalf("committed removal=%+v err=%v", finalized, err)
				}
				attempts, removals := removalProbeCounts(t, ctx, spool, job.JobID)
				if attempts != 0 || removals != 1 {
					t.Fatalf("before successful ACK response: attempts=%d removals=%d, want 0/1", attempts, removals)
				}
				status := http.StatusOK
				payload, err := json.Marshal(finalized)
				if err != nil {
					t.Fatal(err)
				}
				if calls <= scenario.failures {
					switch scenario.failure {
					case "transport":
						return nil, io.ErrUnexpectedEOF
					case "invalid-success":
						payload = []byte("{")
					case "invalid-gateway":
						status, payload = http.StatusBadGateway, []byte("gateway lost upstream response")
					case "unavailable", "unavailable-unmarked", "retryable-refusal":
						if calls == 1 {
							return nil, io.ErrUnexpectedEOF
						}
						code := contract.ErrorUnavailable
						if scenario.failure == "retryable-refusal" {
							code = contract.ErrorCode("temporary_ack_refusal")
						}
						status = http.StatusServiceUnavailable
						payload, _ = json.Marshal(contract.ErrorResponse{Error: contract.APIError{Code: code, Retryable: scenario.failure != "unavailable-unmarked"}})
					case "internal":
						status = http.StatusInternalServerError
						payload, _ = json.Marshal(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorInternal}})
					}
				}
				return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(payload))}, nil
			})}
			controller := newRemovalController(client, &evidenceOutbox{clock: clock, spool: spool}, nil, nil, nodeID, bootID, t.Logf)
			if scenario.cancelAfter != 0 {
				clock.cancel = controller.ackCancel
			}
			controller.beginRemoval = func(ctx context.Context, removal localRemoval) error {
				return spool.beginRemoval(ctx, removal, clock.Now())
			}
			controller.purgeJob = spool.purgeJob
			controller.finishRemoval = spool.completeRemoval
			deletions := 0
			controller.reapService = func(context.Context, string, string, []workloadrunner.RuntimeResourceManifest) (workloadrunner.ReapReceipt, error) {
				return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt, BootSessionID: bootID}, nil
			}
			controller.removeResource = func(context.Context, localRemoval) error { deletions++; return nil }
			err = controller.reconcile(ctx, directives[0])
			controller.wait()
			attempts, removals := removalProbeCounts(t, t.Context(), spool, job.JobID)
			if scenario.cancelAfter != 0 {
				if err != nil || controller.ackContext.Err() != context.Canceled || calls != scenario.cancelAfter || deletions != 1 || attempts != 0 || removals != 1 {
					t.Fatalf("canceled ACK: calls=%d cleanup=%d rows=%d/%d err=%v", calls, deletions, attempts, removals, err)
				}
				clock.Advance(time.Hour)
				for _, timer := range clock.timers {
					if timer.active || len(timer.channel) != 0 {
						t.Fatal("cancellation left an armed retry timer")
					}
				}
				return
			}
			if err != nil || calls != scenario.failures+1 || deletions != 1 || attempts != 0 || removals != 0 {
				t.Fatalf("ACK calls=%d cleanup=%d local rows=%d/%d err=%v, want %d, 1, 0/0, nil", calls, deletions, attempts, removals, err, scenario.failures+1)
			}
			assertRemovalACKBackoff(t, clock, scenario.failures)
		})
	}
}

func TestRemovalAcknowledgementReplayPreservesRefusals(t *testing.T) {
	for _, scenario := range []struct {
		name      string
		code      contract.ErrorCode
		cancel    bool
		success   bool
		calls     int
		failures  int
		retryable bool
	}{
		{name: "success", success: true, calls: 1},
		{name: "conflict", code: contract.ErrorConflict, calls: 1},
		{name: "stale-fence", code: contract.ErrorStaleFence, calls: 1},
		{name: "replaced-session", code: contract.ErrorNodeSessionReplaced, calls: 1},
		{name: "retryable-replaced-session", code: contract.ErrorNodeSessionReplaced, retryable: true, calls: 1},
		{name: "retryable-stale-fence", code: contract.ErrorStaleFence, retryable: true, calls: 1},
		{name: "refusal-after-ambiguity", code: contract.ErrorNodeSessionReplaced, failures: 3, calls: 4},
		{name: "canceled", cancel: true, calls: 1},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			clock := &removalACKRetryClock{manualClock: newManualClock(time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC))}
			var stopDelivery context.CancelFunc
			client := &Client{baseURL: "http://removal.invalid", operationTimeout: time.Second}
			client.httpClient = &http.Client{Transport: captureRoundTrip(func(*http.Request) (*http.Response, error) {
				calls++
				if calls > scenario.calls {
					stopDelivery() // Bound a semantic-refusal retry mutant.
				}
				if calls <= scenario.failures {
					return nil, io.ErrUnexpectedEOF
				}
				if scenario.success {
					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewBufferString("{}"))}, nil
				}
				if scenario.cancel {
					stopDelivery()
				}
				if scenario.code == "" {
					return nil, io.ErrUnexpectedEOF
				}
				payload, _ := json.Marshal(contract.ErrorResponse{Error: contract.APIError{Code: scenario.code, Retryable: scenario.retryable}})
				return &http.Response{StatusCode: http.StatusConflict, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(payload))}, nil
			})}
			controller := newRemovalController(client, &evidenceOutbox{clock: clock}, nil, nil, "node", "boot", t.Logf)
			stopDelivery = controller.ackCancel
			controller.finishRemoval = func(context.Context, localRemoval) error {
				t.Error("refused ACK released local intent")
				return nil
			}
			err := controller.acknowledge(ctx, testRuntimeRemoval("job"))
			if errors.Is(err, errRemovalAcknowledgementPending) {
				controller.wait()
				select {
				case failure := <-controller.ackFailures:
					err = failure.err
				default:
					t.Fatal("background authority refusal was not routed to the session")
				}
			}
			if (err == nil) != scenario.success || calls != scenario.calls {
				t.Fatalf("calls=%d err=%v, want %d calls and success=%t", calls, err, scenario.calls, scenario.success)
			}
			assertRemovalACKBackoff(t, clock, scenario.failures)
			if scenario.code != "" {
				var protocolErr *ProtocolError
				if !errors.As(err, &protocolErr) || protocolErr.APIError.Code != scenario.code {
					t.Fatalf("lost refusal identity: %v", err)
				}
			}
		})
	}
}
