package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Accept real connections and retain them without answering any protocol.
func reviewSilentListener(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var connections []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			connections = append(connections, conn)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range connections {
			_ = conn.Close()
		}
	})
	return listener.Addr().String()
}

func TestReviewTransportTimeouts(t *testing.T) {
	for _, operation := range []string{"takeover-open", "takeover-perform", "registry-head", "registry-get"} {
		t.Run(operation, func(t *testing.T) {
			address := reviewSilentListener(t)
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			var err error
			switch operation {
			case "takeover-open":
				_, err = openTakeoverViewWithPolicyRetry(ctx, directDialFabric{}, "ws://"+address+contract.ComputerDisplayWebSocketPath, 1)
			case "takeover-perform":
				clients := &apiClients{fabric: directDialFabric{}}
				_, err = clients.performComputerTakeoverAction(ctx, "ws://"+address+contract.ComputerDisplayWebSocketPath, "test-token", "take")
			default:
				// Use the same Client.Timeout mechanism as the default 30s registry client.
				resolver := newRegistryResolver(&http.Client{Timeout: 40 * time.Millisecond})
				if operation == "registry-head" {
					_, err = resolver.headManifest(t.Context(), "https://"+address+"/v2/test/manifests/latest", "")
				} else {
					_, err = resolver.publicBearerToken(t.Context(), `Bearer realm="https://`+address+`/token"`)
				}
			}
			var stderr bytes.Buffer
			writeCommandError(&stderr, err, true)
			var envelope contract.ErrorResponse
			if decode := json.Unmarshal(stderr.Bytes(), &envelope); decode != nil || commandExitCode(err) != 13 || envelope.Error.Code != contract.ErrorUnavailable || !envelope.Error.Retryable || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("exit=%d error=%v stderr=%s decode=%v", commandExitCode(err), err, stderr.String(), decode)
			}
		})
	}
}

func TestReviewComputerWaitFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, verb := range []string{"resize", "remove"} {
		for _, applied := range []bool{true, false} {
			for _, emptyRead := range []bool{false, true} {
				t.Run(verb+map[bool]string{true: "/applied", false: "/replay"}[applied]+map[bool]string{true: "/empty-read", false: "/observed"}[emptyRead], func(t *testing.T) {
					base := "/v1/computers/computer-1"
					var mutated atomic.Bool
					computer := l1.Computer{ComputerID: "computer-1", IntentRevision: 3, StorageID: "storage-1", StorageGeneration: 1,
						CurrentJob:        l1.Job{JobID: "owned-job", ComputerID: "computer-1", State: contract.JobRemovalPending},
						LastGrowOperation: &l1.ComputerStorageGrowOutcome{OperationRevision: 3, Status: "planned"}}
					address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
						w.Header().Set("Content-Type", "application/json")
						switch {
						case r.Method == http.MethodPost && (r.URL.Path == base+"/grow" || r.URL.Path == base+"/remove"):
							mutated.Store(true)
							w.Header().Set("Mutation-Applied", map[bool]string{true: "true", false: "false"}[applied])
							w.Header().Set("Idempotent-Replay", map[bool]string{true: "false", false: "true"}[applied])
							w.WriteHeader(http.StatusAccepted)
							_ = json.NewEncoder(w).Encode(computer)
						case r.Method == http.MethodGet && r.URL.Path == base:
							if mutated.Load() && emptyRead {
								_ = json.NewEncoder(w).Encode(l1.Computer{})
								return
							}
							_ = json.NewEncoder(w).Encode(computer)
						case r.Method == http.MethodGet && r.URL.Path == "/v1/jobs/owned-job":
							_ = json.NewEncoder(w).Encode(computer.CurrentJob)
						default:
							t.Errorf("unexpected %s %s", r.Method, r.URL)
							http.Error(w, "unexpected", 404)
						}
					})
					target := "computer-1"
					if verb == "remove" {
						target = "owned-job"
					}
					args := []string{"--json", "--l1=" + address, "services", verb, target, "--intent-revision=2", "--storage-id=storage-1", "--storage-generation=1", "--wait=30ms", "--poll-interval=1ms"}
					if verb == "resize" {
						args = append(args, "--disk-bytes=1000000", "--idempotency-key=test")
					}
					code, stdout, stderr := runPolishBinary(t, binary, args...)
					var envelope contract.ErrorResponse
					if err := json.Unmarshal(stderr, &envelope); err != nil || code != 14 || envelope.Error.Code != "wait_timeout" || envelope.Error.Details["mutation_applied"] != applied {
						t.Fatalf("exit=%d stdout=%s stderr=%s decode=%v", code, stdout, stderr, err)
					}
					if emptyRead {
						if len(stdout) != 0 {
							t.Fatalf("unobserved Computer document: %s", stdout)
						}
						return
					}
					var result computerOperatorProjection
					if err := json.Unmarshal(stdout, &result); err != nil || result.MutationApplied == nil || *result.MutationApplied != applied || result.IdempotentReplay == nil || *result.IdempotentReplay != !applied || result.Observation == nil || result.Observation.Status != "failed" || envelope.Error.Details["mutation_applied"] != *result.MutationApplied {
						t.Fatalf("mutation receipt lost: stdout=%s decode=%v", stdout, err)
					}
				})
			}
		}
	}
}

func TestReviewUnknownMutationApplied(t *testing.T) {
	var stderr bytes.Buffer
	writeCommandError(&stderr, &mutationWaitTimeoutError{message: "unknown evidence"}, true)
	var envelope contract.ErrorResponse
	if err := json.Unmarshal(stderr.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	applied, present := envelope.Error.Details["mutation_applied"]
	if !present || applied != nil {
		t.Fatalf("unknown must be present as null: %s", stderr.String())
	}
}

func TestReviewRevocationWaitFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, applied := range []bool{true, false} {
		t.Run(map[bool]string{true: "applied", false: "replay"}[applied], func(t *testing.T) {
			revocation := l1.ComputerPolicyRevocation{ComputerID: "computer-1", SubjectFabricID: "fabric-1", SubjectUserID: "person-1", PolicyRevision: 2, State: l1.ComputerPolicyRevocationPending}
			address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/v1/computer-handle-resolutions/computer-1":
					_ = json.NewEncoder(w).Encode(l1.ComputerHandleResolution{ComputerID: "computer-1"})
				case "/v1/computers/computer-1/grants/person-1":
					_ = json.NewEncoder(w).Encode(l1.ComputerGrantMutationResult{MutationApplied: applied, Replayed: !applied, Revocation: &revocation})
				case "/v1/computers/computer-1/revocations/2":
					_ = json.NewEncoder(w).Encode(revocation)
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL)
					http.Error(w, "unexpected", 404)
				}
			})
			code, stdout, stderr := runPolishBinary(t, binary, "--json", "--l1="+address, "--plain-user-id=admin-1", "--plain-device-id=device-1", "services", "revoke", "computer-1", "person-1", "--policy-revision=1", "--idempotency-key=test", "--wait", "--wait-timeout=20ms", "--poll-interval=1ms")
			var envelope contract.ErrorResponse
			if err := json.Unmarshal(stderr, &envelope); err != nil || code != 14 || envelope.Error.Code != contract.ErrorRevocationWaitTimeout || envelope.Error.Details["mutation_applied"] != applied || !envelope.Error.Retryable {
				t.Fatalf("exit=%d stdout=%s stderr=%s decode=%v", code, stdout, stderr, err)
			}
		})
	}
}

func TestReviewServiceNoOpWaitFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, test := range []struct {
		verb    string
		state   contract.JobState
		desired contract.ServiceDesiredState
		removal *l1.ServiceRemoval
	}{
		{"start", contract.JobQueued, contract.ServiceDesiredRunning, nil},
		{"start", contract.JobClaimed, contract.ServiceDesiredRunning, nil},
		{"stop", contract.JobStopping, contract.ServiceDesiredStopped, nil},
		{"remove", contract.JobRemovalPending, contract.ServiceDesiredRemoved, &l1.ServiceRemoval{}},
	} {
		t.Run(test.verb+"/"+string(test.state), func(t *testing.T) {
			address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method != http.MethodGet {
					w.WriteHeader(http.StatusAccepted)
				}
				_ = json.NewEncoder(w).Encode(l1.Job{JobID: "job-1", State: test.state, Removal: test.removal, ServiceJob: &l1.ServiceJob{DesiredState: test.desired}})
			})
			code, stdout, stderr := runPolishBinary(t, binary, "--json", "--l1="+address, "services", test.verb, "job-1", "--wait=20ms", "--poll-interval=1ms")
			var envelope contract.ErrorResponse
			if err := json.Unmarshal(stderr, &envelope); err != nil || code != 14 || envelope.Error.Code != "wait_timeout" {
				t.Fatalf("exit=%d stdout=%s stderr=%s decode=%v", code, stdout, stderr, err)
			}
			if applied, present := envelope.Error.Details["mutation_applied"]; !present || applied != nil {
				t.Fatalf("service timeout must carry mutation_applied=null: %s", stderr)
			}
		})
	}
}

func TestReviewServiceLateNotFound(t *testing.T) {
	reads := 0
	clients := &apiClients{l1: &apiClient{name: "L1", client: &http.Client{Transport: forwardingRoundTripper(func(r *http.Request) (*http.Response, error) {
		status, body := http.StatusOK, `{"job_id":"job-1","state":"running"}`
		if r.Method != http.MethodGet {
			status = http.StatusAccepted
		}
		if r.Method == http.MethodGet {
			reads++
			if reads > 1 {
				<-r.Context().Done()
				status, body = 404, `{"error":{"code":"not_found","message":"gone","retryable":false}}`
			}
		}
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})}}}
	var stdout, stderr bytes.Buffer
	err := execute(t.Context(), clients, true, []string{"services", "stop", "job-1", "--wait=20ms", "--poll-interval=1ms"}, &stdout, &stderr)
	if commandExitCode(err) != exitNotFound {
		t.Fatalf("late 404 mislabelled: exit=%d err=%v", commandExitCode(err), err)
	}
}
