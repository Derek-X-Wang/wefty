package l1

import (
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestComputerSubmissionWithoutInternetRefusesEnable(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{})
	ctx := t.Context()
	admin := fabric.Identity{FabricID: "fabric-test", UserID: "admin", DeviceID: "device-1"}
	challenge, err := h.store.InitiateAdminBootstrap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := h.store.BootstrapAdmin(ctx, admin, challenge.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	computer, _, err := h.store.CreateComputer(ctx, CreateComputerRequest{
		Name: "without-internet", Spec: computerCapabilityJobSpec("computer:no-submission"), Actor: "operator",
		SeatRequestSettings: &ComputerSettings{SeatLimit: 2, IdleAction: ComputerIdleDelete, Network: ComputerNetworkNone},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"enable", "enable-again"} {
		_, _, applied, err := h.store.MutateComputerSubmission(ctx, admin, computer.ComputerID, ComputerSubmissionRequest{
			PolicyRevision: policy.Revision, SubmitIntentRevision: computer.SubmitIntentRevision,
			SubmitEnabled: boolPointer(true), IdempotencyKey: key,
		})
		if errorCode(err) != contract.ErrorConflict || applied {
			t.Fatalf("none submission enabled: applied=%t error=%v", applied, err)
		}
	}
	state, err := h.store.GetComputerSubmissionState(ctx, admin, computer.ComputerID)
	if err != nil || state.SubmitEnabled || state.SubmitIntentRevision != computer.SubmitIntentRevision || state.PolicyRevision != policy.Revision {
		t.Fatalf("refusal changed submission authority: %+v %v", state, err)
	}
	// Disabling remains safe and available, including for legacy stored state.
	if _, _, _, err := h.store.MutateComputerSubmission(ctx, admin, computer.ComputerID, ComputerSubmissionRequest{
		PolicyRevision: policy.Revision, SubmitIntentRevision: computer.SubmitIntentRevision,
		SubmitEnabled: boolPointer(false), IdempotencyKey: "disable",
	}); err != nil {
		t.Fatal(err)
	}
}
