package agent

import (
	"context"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/l3"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

type recordingComputerTokenFileRuntime struct {
	writes chan string
}

func (runtime *recordingComputerTokenFileRuntime) SetComputerControlState(context.Context, workloadrunner.AttemptAuthority, bool) error {
	return nil
}

func (runtime *recordingComputerTokenFileRuntime) SetComputerToken(_ context.Context, _ workloadrunner.AttemptAuthority, token string) error {
	runtime.writes <- token
	return nil
}

func TestComputerSubmissionPolicyChangeRotatesAttemptTokenFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := &recordingComputerTokenFileRuntime{writes: make(chan string, 4)}
	minter := &recordingComputerTokenMinter{grant: l3.ComputerTokenGrant{Token: "fresh-pass", ComputerID: "computer-1",
		ComputerAttemptID: "attempt-1", SubmitIntentRevision: 2, SubmitMaxInflight: 7}}
	updates := make(chan ComputerSubmissionAuthority, 2)
	done := make(chan error, 1)
	disabled := ComputerSubmissionAuthority{ComputerID: "computer-1", SubmitIntentRevision: 1, SubmitMaxInflight: 7}
	go func() {
		done <- syncComputerTokenFile(ctx, runtime, workloadrunner.AttemptAuthority{}, minter,
			"computer-1", "attempt-1", disabled, disabled, updates)
	}()
	updates <- ComputerSubmissionAuthority{ComputerID: "computer-1", Enabled: true, SubmitIntentRevision: 2, SubmitMaxInflight: 7}
	assertTokenFileWrite(t, runtime.writes, "")
	assertTokenFileWrite(t, runtime.writes, "fresh-pass")
	updates <- ComputerSubmissionAuthority{ComputerID: "computer-1", Enabled: false, SubmitIntentRevision: 3, SubmitMaxInflight: 7}
	assertTokenFileWrite(t, runtime.writes, "")
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Computer token file synchronizer did not stop")
	}
}

func assertTokenFileWrite(t *testing.T, writes <-chan string, want string) {
	t.Helper()
	select {
	case got := <-writes:
		if got != want {
			t.Fatalf("Computer token file write = %q, want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for Computer token file write %q", want)
	}
}
