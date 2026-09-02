package agent

import (
	"context"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l3"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

type recordingComputerTokenFileRuntime struct {
	writes chan computerSubmissionWrite
}

type computerSubmissionWrite struct{ token, endpoint string }

func (runtime *recordingComputerTokenFileRuntime) SetComputerControlState(context.Context, workloadrunner.AttemptAuthority, bool) error {
	return nil
}

func (runtime *recordingComputerTokenFileRuntime) SetComputerSubmission(_ context.Context, _ workloadrunner.AttemptAuthority, token, endpoint string) error {
	runtime.writes <- computerSubmissionWrite{token: token, endpoint: endpoint}
	return nil
}

func TestComputerSubmissionPolicyChangeRotatesAttemptTokenFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := &recordingComputerTokenFileRuntime{writes: make(chan computerSubmissionWrite, 4)}
	minter := &recordingComputerTokenMinter{grant: l3.ComputerTokenGrant{Token: "fresh-pass", ComputerID: "computer-1",
		ComputerAttemptID: "attempt-1", SubmitIntentRevision: 2, SubmitMaxInflight: 7}}
	updates := make(chan ComputerSubmissionAuthority, 2)
	done := make(chan error, 1)
	disabled := ComputerSubmissionAuthority{ComputerID: "computer-1", SubmitIntentRevision: 1, SubmitMaxInflight: 7}
	participant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "agent"})
	controller := newComputerAttemptBridgeController(ctx, func(ctx context.Context, _ string, _ contract.ExecutionSpec) (*workflowBridge, error) {
		return newComputerAttemptBridge(ctx, participant, "wefty://run-ledger", true)
	}, contract.JobKindOCI, contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Computer: &contract.OCIComputerSpec{DiskBytes: 8 << 30}}})
	go func() {
		done <- syncComputerTokenFile(ctx, runtime, workloadrunner.AttemptAuthority{}, systemClock{}, minter,
			controller, "computer-1", "attempt-1", disabled, disabled, updates)
	}()
	updates <- ComputerSubmissionAuthority{ComputerID: "computer-1", Enabled: true, SubmitIntentRevision: 2, SubmitMaxInflight: 7}
	assertTokenFileWrite(t, runtime.writes, "", "")
	write := assertTokenFileWrite(t, runtime.writes, "fresh-pass", "")
	if write.endpoint == "" {
		t.Fatal("mid-attempt Computer enable omitted the paired L3 endpoint")
	}
	updates <- ComputerSubmissionAuthority{ComputerID: "computer-1", Enabled: false, SubmitIntentRevision: 3, SubmitMaxInflight: 7}
	assertTokenFileWrite(t, runtime.writes, "", "")
	cancel()
	_ = controller.disable(errComputerAttemptClosed)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Computer token file synchronizer did not stop")
	}
}

func assertTokenFileWrite(t *testing.T, writes <-chan computerSubmissionWrite, wantToken, wantEndpoint string) computerSubmissionWrite {
	t.Helper()
	select {
	case got := <-writes:
		if got.token != wantToken || (wantEndpoint != "" && got.endpoint != wantEndpoint) {
			t.Fatalf("Computer submission file write = %+v, want token=%q endpoint=%q", got, wantToken, wantEndpoint)
		}
		return got
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for Computer token file write %q", wantToken)
	}
	return computerSubmissionWrite{}
}
