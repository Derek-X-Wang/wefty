package l3

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

// TestAnOCIOneShotIsAcceptedAsARunAndRefusedWithoutOne is the other side of
// wefty #578's L1 refusal. The same image one-shot is accepted when the ledger
// dispatches it, because every dispatch names its run and the run is what the
// node keys the handoff volume by; posted straight to L1 without that identity
// it is refused with the typed code instead of being queued to fail forever.
func TestAnOCIOneShotIsAcceptedAsARunAndRefusedWithoutOne(t *testing.T) {
	h := newIntegrationHarness(t)
	run := h.submit(CreateRunRequest{
		Image:  &contract.ImageProgram{Reference: "ghcr.io/example/echo:v1", Argv: []string{"echo", "once"}},
		Params: json.RawMessage(`{}`),
	}, "oci-run-identity")

	reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, _, body := h.do(h.caller, http.MethodGet, "/v1/runs/"+run.RunID+"/execution", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("execution status = %d body=%s", status, body)
	}
	var execution RunExecution
	if err := json.Unmarshal(body, &execution); err != nil {
		t.Fatal(err)
	}
	if execution.Job == nil {
		t.Fatal("the ledger's OCI one-shot dispatch was not accepted by L1")
	}
	dispatched, err := h.l1Store.GetJob(context.Background(), execution.Job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if dispatched.Spec.Kind != contract.JobKindOCI || dispatched.Spec.Class != contract.JobClassOneShot ||
		contract.HandoffOwnerKey(dispatched.Spec) != run.RunID {
		t.Fatalf("dispatched job = kind %q class %q owner %q, want an OCI one-shot owned by run %s",
			dispatched.Spec.Kind, dispatched.Spec.Class, contract.HandoffOwnerKey(dispatched.Spec), run.RunID)
	}

	direct := dispatched.Spec
	direct.DispatchKey = "direct-" + direct.DispatchKey
	direct.Labels = nil
	operator := h.client(fabric.Identity{NodeID: "operator", Tags: []string{l1.DefaultClientPrincipalTag}}, DefaultL1Address)
	status, _, body = h.do(operator, http.MethodPost, "/v1/jobs", direct, nil)
	var refusal contract.ErrorResponse
	if err := json.Unmarshal(body, &refusal); err != nil {
		t.Fatalf("decode direct submission response %s: %v", body, err)
	}
	if status != http.StatusConflict || refusal.Error.Code != contract.ErrorRunIdentityRequired {
		t.Fatalf("direct submission status = %d body=%s, want 409 %s", status, body, contract.ErrorRunIdentityRequired)
	}

	// Nor can the operator borrow the run's identity (wefty #583): the same
	// spec naming the ledger's run, posted straight to L1, is refused as a
	// claim the operator is not entitled to make.
	borrowed := dispatched.Spec
	borrowed.DispatchKey = "borrowed-" + borrowed.DispatchKey
	status, _, body = h.do(operator, http.MethodPost, "/v1/jobs", borrowed, nil)
	refusal = contract.ErrorResponse{}
	if err := json.Unmarshal(body, &refusal); err != nil {
		t.Fatalf("decode borrowed submission response %s: %v", body, err)
	}
	if status != http.StatusForbidden || refusal.Error.Code != contract.ErrorRunIdentityNotEntitled {
		t.Fatalf("borrowed run identity status = %d body=%s, want 403 %s", status, body, contract.ErrorRunIdentityNotEntitled)
	}
}
