package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/l1"
)

type deEnrollmentReplayTransport struct {
	inner        http.RoundTripper
	status, step int
}

func (f *deEnrollmentReplayTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if !strings.HasSuffix(r.URL.Path, "/generations/de-enroll") {
		return f.inner.RoundTrip(r)
	}
	f.step++
	if f.step == 2 {
		code := contract.ErrorUnauthorized
		if f.status == 403 {
			code = contract.ErrorPrincipalForbidden
		}
		body, _ := json.Marshal(contract.ErrorResponse{Error: contract.APIError{Code: code, Message: "authentication unavailable"}})
		return &http.Response{StatusCode: f.status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(string(body))), Request: r}, nil
	}
	response, err := f.inner.RoundTrip(r)
	if err == nil && f.step == 1 && response.StatusCode == 200 {
		response.Body.Close()
		return nil, io.ErrUnexpectedEOF // commit succeeded; its result never reached the agent
	}
	return response, err
}

func TestDeEnrollmentAdapterCommittedReplayAuthenticationFailure(t *testing.T) {
	for _, status := range []int{401, 403} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			installed, g := newUpgradeControlStore(t)
			catalog, err := installed.Read()
			if err != nil {
				t.Fatal(err)
			}
			g.Process = &agentinstall.Binding{PID: 123, ProcessStartID: "fixture", BootSessionID: "boot", SystemBootID: g.StartBootID, ReadyAt: time.Now()}
			catalog.Generations[g.ID] = g
			data, _ := json.Marshal(catalog)
			if err = os.WriteFile(filepath.Join(installed.Root(), "catalog.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			network := plain.NewNetwork()
			authority, stop := startFailureServerWithPoliciesAndLease(t, network, nil, nil, 2*time.Second)
			defer stop()
			identity := fabric.Identity{NodeID: "installed-control-agent", Tags: []string{l1.DefaultAgentPrincipalTag}}
			client, err := newClient(network.NewFabric(identity), "wefty://control-plane", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			client.httpClient.Transport = &deEnrollmentReplayTransport{inner: client.httpClient.Transport, status: status}
			clock := systemClock{}
			capabilities := newCapabilityState(map[string]bool{"kind:process": true, "upgrade:retirement:v1": true}, nil, clock, time.Second, nil)
			observed := capabilities.snapshot()
			registration := contract.NodeRegistration{NodeID: "node", AgentGenerationID: g.ID, BootSessionID: "boot", OS: "darwin", Architecture: "arm64", AgentVersion: "test", Capabilities: observed.Capabilities, CapabilityRevision: observed.Revision, CapabilityObservedAt: observed.ObservedAt}
			node, err := authority.RegisterNode(t.Context(), identity, registration, l1.DefaultNodePolicy(), true)
			if err != nil {
				t.Fatal(err)
			}
			node, err = authority.SetNodeClaimsByOperator(t.Context(), "node", "operator", l1.NodeIntentRequest{IntentRevision: node.IntentRevision, ClaimsEnabled: false, Reason: "wefty maintenance operation"})
			if err != nil {
				t.Fatal(err)
			}
			inventory, err := authority.NodeGenerations(t.Context(), identity.NodeID, "node")
			if err != nil {
				t.Fatal(err)
			}
			request := l1.DeEnrollNodeGenerationRequest{OperationID: "operation", PrimaryGenerationID: g.ID, PrimaryBootSessionID: "boot", AuthorityGeneration: node.AuthorityGeneration, CatalogRevision: inventory.CatalogRevision, IntentRevision: node.IntentRevision, IntentReason: node.IntentReason}
			session := newAgentSession(client, registration, capabilities, time.Second, time.Millisecond, clock, newLifecycleObserver(clock), nil, 1, 1)
			runtime := &Agent{session: session}
			body, _ := json.Marshal(request)
			exchange := func() (l1.NodeGenerationInventory, error) {
				return runtime.deEnrollGeneration(context.Background(), installed, agentinstall.ControlRequest{ID: request.OperationID, NodeID: "node", GenerationID: g.ID, BootSessionID: "boot", Action: "de_enroll_generation", Payload: body})
			}
			for attempt := 0; attempt < 2; attempt++ {
				if _, err = exchange(); err == nil {
					t.Fatal("fault was not returned", attempt)
				}
				saved, e := installed.ReadDeEnrollment()
				if e != nil || saved == nil || saved.Result != nil {
					t.Fatal("uncertain committed attestation erased", attempt, e)
				}
				rejected, e := installed.DeEnrollmentRejected(saved.Request)
				if e != nil || rejected {
					t.Fatal("authentication failure created a rejection", e)
				}
				current, e := authority.GetNode(t.Context(), "node")
				if e != nil {
					t.Fatal(e)
				}
				closed, e := authority.NodeGenerations(t.Context(), identity.NodeID, "node")
				if e != nil {
					t.Fatal(e)
				}
				if len(closed.Generations) != 1 || closed.Generations[0].Role != l1.NodeGenerationClosed {
					t.Fatal("lost reply did not follow a real commit")
				}
				if current.ClaimsEnabled || current.IntentRevision != node.IntentRevision || current.IntentReason != node.IntentReason {
					t.Fatal("admission reopened or changed")
				}
			}
			committed, err := authority.NodeGenerations(t.Context(), identity.NodeID, "node")
			if err != nil {
				t.Fatal(err)
			}
			recovered, err := exchange()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(committed, recovered) {
				t.Fatal("replay did not recover the original result")
			}
			saved, err := installed.ReadDeEnrollment()
			if err != nil || saved == nil || saved.Result == nil || !reflect.DeepEqual(*saved.Result, committed) {
				t.Fatal("committed result not durable", err)
			}
			current, err := authority.GetNode(t.Context(), "node")
			if err != nil {
				t.Fatal(err)
			}
			if current.ClaimsEnabled || current.IntentRevision != node.IntentRevision || current.IntentReason != node.IntentReason {
				t.Fatal("recovery reopened admission")
			}
		})
	}
}
