//go:build darwin || linux

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Requests traverse the production attempt-local HTTP proxy, Fabric identity
// transport, L1 credential middleware and creation transaction. No store-side
// creation shortcuts or mock upstreams participate in the reservation race.
func TestChildInstanceKeysThroughAttemptBridge(t *testing.T) {
	for _, class := range []string{contract.JobClassOneShot, contract.JobClassService} {
		for _, sameDispatch := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/same-dispatch-%t", class, sameDispatch), func(t *testing.T) {
				network := plain.NewNetwork()
				clock := newManualClock(time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC))
				path := filepath.Join(t.TempDir(), "l1.sqlite")
				startServer := func() func() {
					store, err := l1.OpenStore(path, l1.StoreOptions{Clock: clock, LeaseDuration: time.Minute})
					if err != nil {
						t.Fatal(err)
					}
					serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
					server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{
						ReconcileInterval: time.Hour,
						NodePolicies: map[string]l1.NodePolicy{"child-node": {
							Tags: []string{"parent"}, MaxOneshotSlots: 4, MaxServiceSlots: 1,
						}},
					})
					if err != nil {
						store.Close()
						t.Fatal(err)
					}
					listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
					if err != nil {
						store.Close()
						t.Fatal(err)
					}
					ctx, cancel := context.WithCancel(t.Context())
					done := make(chan error, 1)
					go func() { done <- server.Serve(ctx, listener) }()
					return func() {
						cancel()
						if err := <-done; err != nil {
							t.Errorf("serve L1: %v", err)
						}
						if err := store.Close(); err != nil {
							t.Errorf("close L1: %v", err)
						}
					}
				}
				stopServer := startServer()
				defer func() { stopServer() }()
				app := newHTTPClient(network.NewFabric(fabric.Identity{NodeID: "root-app", Tags: []string{l1.DefaultClientPrincipalTag}}), "wefty://control-plane")
				defer app.CloseIdleConnections()
				participant := network.NewFabric(fabric.Identity{NodeID: "child-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
				agentClient, err := NewClient(participant, "wefty://control-plane")
				if err != nil {
					t.Fatal(err)
				}
				defer agentClient.Close()
				_, err = agentClient.Register(t.Context(), contract.NodeRegistration{
					NodeID: "child-node", BootSessionID: "child-boot", RootInstanceID: "child-root",
					OS: "linux", Architecture: "arm64", AgentVersion: "test", CapabilityRevision: 1,
					Capabilities: map[string]bool{"kind:process": true}, CapabilityObservedAt: clock.Now(),
				})
				if err != nil {
					t.Fatal(err)
				}
				bridgeAgent := &Agent{fabric: participant, controlPlaneAddr: "wefty://control-plane"}
				bridge, err := bridgeAgent.startWorkflowBridge(t.Context(), contract.JobKindProcess, contract.ExecutionSpec{}, false)
				if err != nil || bridge == nil {
					t.Fatalf("start attempt bridge: %v", err)
				}
				defer bridge.close()
				bridgeClient := &http.Client{Timeout: 10 * time.Second}
				defer bridgeClient.CloseIdleConnections()
				submit := func(client *http.Client, endpoint, token string, spec contract.JobSpec) l1.Job {
					t.Helper()
					status, body, err := childKeyBridgeRequest(t.Context(), client, endpoint, token, spec)
					if err != nil || status != http.StatusCreated {
						t.Fatalf("submit=%d %s %v", status, body, err)
					}
					var job l1.Job
					if err := json.Unmarshal(body, &job); err != nil {
						t.Fatal(err)
					}
					return job
				}
				claimParent := func(dispatch string) *l1.Claim {
					t.Helper()
					spec := childKeyBridgeSpec(dispatch, contract.JobClassOneShot, nil)
					spec.RoutingTags = []string{"parent"}
					parent := submit(app, "http://control-plane.invalid", "", spec)
					claim, err := agentClient.Claim(t.Context(), "child-node", "child-boot", contract.JobClassOneShot)
					if err != nil || claim == nil || claim.Job.JobID != parent.JobID || claim.AttemptToken == "" {
						t.Fatalf("parent claim=%+v %v", claim, err)
					}
					return claim
				}
				parent := claimParent("parent-one")
				key := "agent-instance"
				type response struct {
					status int
					body   []byte
					err    error
					spec   contract.JobSpec
				}
				start := make(chan struct{})
				results := make(chan response, 12)
				for i := range cap(results) {
					dispatch := fmt.Sprintf("child-%d", i)
					if sameDispatch {
						dispatch = "child-shared-dispatch"
					}
					spec := childKeyBridgeSpec(dispatch, class, &key)
					go func() {
						<-start
						status, body, err := childKeyBridgeRequest(t.Context(), bridgeClient, bridge.l1Endpoint, parent.AttemptToken, spec)
						results <- response{status, body, err, spec}
					}()
				}
				close(start)
				all := make([]response, 0, cap(results))
				var holder l1.Job
				var winningSpec contract.JobSpec
				created := 0
				for range cap(results) {
					r := <-results
					all = append(all, r)
					if r.err == nil && r.status == http.StatusCreated {
						created++
						if err := json.Unmarshal(r.body, &holder); err != nil {
							t.Fatal(err)
						}
						winningSpec = r.spec
					}
				}
				if created != 1 || holder.ParentJobID != parent.Job.JobID || holder.ParentAttemptID != parent.Lease.AttemptID {
					statuses := make([]int, len(all))
					for i, response := range all {
						statuses[i] = response.status
					}
					t.Fatalf("created=%d holder=%s parent=%s statuses=%v first-body=%s", created, holder.JobID, holder.ParentJobID, statuses, all[0].body)
				}
				for _, r := range all {
					if r.err != nil {
						t.Fatal(r.err)
					}
					if r.status == http.StatusCreated {
						continue
					}
					if sameDispatch {
						var replay l1.Job
						if err := json.Unmarshal(r.body, &replay); err != nil || r.status != http.StatusOK || replay.JobID != holder.JobID {
							t.Fatalf("dispatch replay=%d %s %v", r.status, r.body, err)
						}
					} else {
						assertChildKeyBridgeConflict(t, r.status, r.body, holder.JobID)
					}
				}
				// A database close/reopen preserves both the live bearer and key.
				app.CloseIdleConnections()
				agentClient.Close()
				stopServer()
				stopServer = startServer()
				probe := childKeyBridgeSpec("after-reopen", class, &key)
				status, body, err := childKeyBridgeRequest(t.Context(), bridgeClient, bridge.l1Endpoint, parent.AttemptToken, probe)
				if err != nil {
					t.Fatal(err)
				}
				assertChildKeyBridgeConflict(t, status, body, holder.JobID)
				status, body, err = childKeyBridgeRequest(t.Context(), bridgeClient, bridge.l1Endpoint, parent.AttemptToken, winningSpec)
				var replay l1.Job
				if err != nil || json.Unmarshal(body, &replay) != nil || status != http.StatusOK || replay.JobID != holder.JobID {
					t.Fatalf("reopened replay=%d %s %v", status, body, err)
				}
				// The same root submitter's other parent has an independent key
				// namespace, but still cannot replay the first parent's dispatch.
				other := claimParent("parent-two")
				status, body, err = childKeyBridgeRequest(t.Context(), bridgeClient, bridge.l1Endpoint, other.AttemptToken, winningSpec)
				assertChildKeyBridgeError(t, status, body, err, http.StatusConflict, contract.ErrorDispatchKeyConflict)
				probe.DispatchKey = "other-parent-child"
				submit(bridgeClient, bridge.l1Endpoint, other.AttemptToken, probe)
				// A Fabric identity resembling the tagged job namespace cannot
				// alias it; neither can the inherited root identity.
				for i, identity := range []string{"root-app", "job:" + parent.Job.JobID} {
					rootClient := newHTTPClient(network.NewFabric(fabric.Identity{NodeID: identity, Tags: []string{l1.DefaultClientPrincipalTag}}), "wefty://control-plane")
					probe.DispatchKey = fmt.Sprintf("root-child-key-%d", i)
					submit(rootClient, "http://control-plane.invalid", "", probe)
					rootClient.CloseIdleConnections()
				}
				// Expiry is checked without reconcile, before key conflict or replay.
				clock.Advance(2 * time.Minute)
				for _, spec := range []contract.JobSpec{winningSpec, probe} {
					status, body, err = childKeyBridgeRequest(t.Context(), bridgeClient, bridge.l1Endpoint, parent.AttemptToken, spec)
					assertChildKeyBridgeError(t, status, body, err, http.StatusUnauthorized, contract.ErrorUnauthorized)
				}
			})
		}
	}
}

func childKeyBridgeSpec(dispatch, class string, key *string) contract.JobSpec {
	spec := contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: dispatch, InstanceKey: key,
		Kind: contract.JobKindProcess, Class: class, RoutingTags: []string{"child-only"},
		Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/true"}, Argv: []string{"true"}, WorkingDirectory: "/tmp"},
	}
	if class == contract.JobClassOneShot {
		spec.Execution.HandoffDirectory = "/tmp/handoff"
	}
	return spec
}

func childKeyBridgeRequest(ctx context.Context, client *http.Client, endpoint, token string, spec contract.JobSpec) (int, []byte, error) {
	raw, err := json.Marshal(spec)
	if err != nil {
		return 0, nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/v1/jobs", bytes.NewReader(raw))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	return response.StatusCode, body, err
}

func assertChildKeyBridgeError(t *testing.T, status int, body []byte, err error, wantStatus int, code contract.ErrorCode) contract.APIError {
	t.Helper()
	var refusal contract.ErrorResponse
	if err != nil || json.Unmarshal(body, &refusal) != nil || status != wantStatus || refusal.Error.Code != code || refusal.Error.Retryable {
		t.Fatalf("response=%d %s %v, want %d %s", status, body, err, wantStatus, code)
	}
	return refusal.Error
}

func assertChildKeyBridgeConflict(t *testing.T, status int, body []byte, holder string) {
	t.Helper()
	refusal := assertChildKeyBridgeError(t, status, body, nil, http.StatusConflict, contract.ErrorInstanceKeyConflict)
	if refusal.Details["job_id"] != holder || refusal.Details["instance_key"] != "agent-instance" {
		t.Fatalf("conflict holder=%+v", refusal)
	}
}
