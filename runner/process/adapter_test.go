package process

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"os"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

func TestAdapterMaterializesInlineExecutableInsideProcessBoundary(t *testing.T) {
	content := []byte("#!/bin/sh\necho adapter-owned\n")
	digest := sha256.Sum256(content)
	executor := &materializationExecutor{t: t, content: content}
	adapter := NewAdapter(executor)
	request := workloadrunner.Request{
		Authority: testAuthority("job-materialization", "attempt-materialization", "fence-materialization"),
		Execution: contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{
				InlineBase64: base64.StdEncoding.EncodeToString(content),
				SHA256:       hex.EncodeToString(digest[:]),
				Mode:         0o700,
			},
			Argv: []string{"workflow"}, WorkingDirectory: t.TempDir(),
		},
	}
	admission, preflight, err := adapter.Preflight(context.Background(), request)
	if err != nil || preflight.Outcome.SpawnError != nil {
		t.Fatalf("adapter preflight = (%#v, %v)", preflight, err)
	}
	result, err := adapter.Run(context.Background(), admission.Request, nil)
	if err != nil || result.Outcome.ExitCode == nil || *result.Outcome.ExitCode != 0 {
		t.Fatalf("adapter run = (%#v, %v)", result, err)
	}
	if executor.path == "" {
		t.Fatal("adapter did not provide a materialized executable")
	}
	if _, err := os.Stat(executor.path); err != nil {
		t.Fatalf("materialized executable disappeared before admission release: %v", err)
	}
	admission.Release()
	if _, err := os.Stat(executor.path); !os.IsNotExist(err) {
		t.Fatalf("adapter left materialized executable after admission release: %v", err)
	}
}

func TestAdapterDoesNotVerifyAProcessReapTimeout(t *testing.T) {
	adapter := NewAdapter(reapTimeoutExecutor{})
	request := workloadrunner.Request{
		Authority: testAuthority("job-reap-timeout", "attempt-reap-timeout", "fence-reap-timeout"),
		Execution: contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{Path: "/bin/true"},
			Argv:       []string{"true"}, WorkingDirectory: t.TempDir(),
		},
	}
	admission, _, err := adapter.Preflight(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer admission.Release()
	if _, err := adapter.Run(context.Background(), admission.Request, nil); !errors.Is(err, ErrProcessReapTimeout) {
		t.Fatalf("adapter Run error = %v, want reap timeout", err)
	}
	receipt, err := adapter.ReapAndVerify(context.Background(), workloadrunner.ReapRequest{Authority: request.Authority})
	if !errors.Is(err, ErrProcessReapTimeout) || receipt.RuntimeQuiesced {
		t.Fatalf("reap receipt = %#v err=%v, want unverified timeout", receipt, err)
	}
	if _, err := adapter.ReapAndVerify(context.Background(), workloadrunner.ReapRequest{Authority: request.Authority}); err == nil {
		t.Fatal("reap timeout evidence was not consumed after it was reported")
	}
}

func TestAdapterReapEvidenceIsAuthorityBoundAndConsumedOnce(t *testing.T) {
	adapter := NewAdapter(&materializationExecutor{t: t})
	authority := testAuthority("job-authority", "attempt-authority", "fence-authority")
	request := workloadrunner.Request{Authority: authority, Execution: contract.ExecutionSpec{
		Executable: contract.ExecutableSpec{Path: "/bin/true"}, WorkingDirectory: t.TempDir(),
	}}
	admission, _, err := adapter.Preflight(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer admission.Release()
	if _, err := adapter.Run(context.Background(), admission.Request, nil); err != nil {
		t.Fatal(err)
	}
	for name, mismatched := range map[string]workloadrunner.AttemptAuthority{
		"wrong job":   testAuthority("other-job", authority.AttemptID, authority.FencingToken),
		"stale fence": testAuthority(authority.JobID, authority.AttemptID, "stale-fence"),
	} {
		t.Run(name, func(t *testing.T) {
			receipt, err := adapter.ReapAndVerify(context.Background(), workloadrunner.ReapRequest{Authority: mismatched})
			if err == nil || receipt.RuntimeQuiesced {
				t.Fatalf("mismatched authority receipt = %#v err=%v", receipt, err)
			}
		})
	}
	receipt, err := adapter.ReapAndVerify(context.Background(), workloadrunner.ReapRequest{Authority: authority})
	if err != nil || !receipt.RuntimeQuiesced || receipt.Evidence != workloadrunner.ReapEvidenceAttempt {
		t.Fatalf("matching authority receipt = %#v err=%v", receipt, err)
	}
	if receipt, err = adapter.ReapAndVerify(context.Background(), workloadrunner.ReapRequest{Authority: authority}); err == nil || receipt.RuntimeQuiesced {
		t.Fatalf("duplicate receipt = %#v err=%v", receipt, err)
	}
}

func TestFreshAdapterHasNoReapEvidence(t *testing.T) {
	adapter := NewAdapter(&materializationExecutor{t: t})
	receipt, err := adapter.ReapAndVerify(context.Background(), workloadrunner.ReapRequest{
		Authority: testAuthority("job-fresh", "attempt-fresh", "fence-fresh"),
	})
	if err == nil || receipt.RuntimeQuiesced {
		t.Fatalf("fresh adapter receipt = %#v err=%v", receipt, err)
	}
}

func TestAdapterIssuesPriorBootGuardianReceipt(t *testing.T) {
	adapter := NewAdapterForBoot(&materializationExecutor{t: t}, "boot-current")
	receipt, err := adapter.ReapPriorBoot(context.Background(), workloadrunner.PriorBootReapRequest{
		NodeID: "node-test", JobID: "job-prior-boot",
		PriorBootSessionID: "boot-prior", CurrentBootSessionID: "boot-current",
	})
	if err != nil || !receipt.RuntimeQuiesced || receipt.Evidence != workloadrunner.ReapEvidencePriorBootGuardian || receipt.BootSessionID != "boot-prior" {
		t.Fatalf("prior-boot Guardian receipt = %#v err=%v", receipt, err)
	}
	if receipt, err = adapter.ReapPriorBoot(context.Background(), workloadrunner.PriorBootReapRequest{
		NodeID: "node-test", JobID: "job-same-boot",
		PriorBootSessionID: "boot-current", CurrentBootSessionID: "boot-current",
	}); err == nil || receipt.RuntimeQuiesced {
		t.Fatalf("same-boot Guardian receipt = %#v err=%v", receipt, err)
	}
}

type materializationExecutor struct {
	t       *testing.T
	content []byte
	path    string
}

type reapTimeoutExecutor struct{}

func (reapTimeoutExecutor) Run(context.Context, Request, OutputSink) (contract.ProcessResult, error) {
	return contract.ProcessResult{}, ErrProcessReapTimeout
}

func (executor *materializationExecutor) Run(_ context.Context, request Request, _ OutputSink) (contract.ProcessResult, error) {
	executor.path = request.Execution.Executable.Path
	if executor.content != nil {
		payload, err := os.ReadFile(executor.path)
		if err != nil || string(payload) != string(executor.content) {
			executor.t.Fatalf("materialized payload = %q err=%v", payload, err)
		}
	}
	exitCode := 0
	return contract.ProcessResult{ExitCode: &exitCode}, nil
}

func testAuthority(jobID, attemptID, fence string) workloadrunner.AttemptAuthority {
	return workloadrunner.AttemptAuthority{
		NodeID: "node-test", BootSessionID: "boot-test", JobID: jobID,
		AttemptID: attemptID, FencingToken: fence,
	}
}
