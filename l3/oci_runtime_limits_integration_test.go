package l3

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

// This is a compatibility guard: the CLI's --image/--max-runtime inputs use
// this public Run request and must still dispatch through the real L1 client.
func TestOCIImageRunRuntimeLimitDispatchesThroughL1(t *testing.T) {
	h := newIntegrationHarness(t)
	created := h.submit(CreateRunRequest{
		Image:  &contract.ImageProgram{Reference: "example/tool:latest", Argv: []string{"tool"}},
		Params: json.RawMessage(`{}`), Tags: []string{"linux"},
		Limits: &contract.RunLimits{MaxRuntimeSeconds: 30},
	}, "oci-runtime-limit")
	reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{l1.DefaultAgentPrincipalTag}}, DefaultL1Address)
	registration := contract.NodeRegistration{
		NodeID: "node-1", BootSessionID: "boot-1", OS: "linux", Architecture: "arm64", AgentVersion: "test",
		Capabilities:       map[string]bool{"kind:oci": true, "runtime_handler:io.containerd.runc.v2": true},
		CapabilityRevision: 1, CapabilityObservedAt: time.Now().UTC(), MissingCapabilities: []string{},
	}
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/nodes/register", registration, nil)
	if status != http.StatusOK {
		t.Fatalf("register OCI node: status=%d body=%s", status, body)
	}
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", l1.ClaimRequest{
		NodeID: "node-1", BootSessionID: "boot-1", Class: contract.JobClassOneShot,
	}, nil)
	if status != http.StatusOK {
		t.Fatalf("claim OCI run: status=%d body=%s", status, body)
	}
	var claim l1.Claim
	if err := json.Unmarshal(body, &claim); err != nil {
		t.Fatal(err)
	}
	spec := claim.Job.Spec
	if spec.Kind != contract.JobKindOCI || spec.Execution.OCI == nil || spec.Limits == nil ||
		spec.Limits.MaxRuntimeSeconds != 30 || spec.Limits.IdleTimeoutSeconds != 0 || spec.Labels["run_id"] != created.RunID {
		t.Fatalf("claimed OCI run limits/identity = %+v", spec)
	}
}
