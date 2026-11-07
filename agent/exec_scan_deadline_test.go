//go:build darwin || linux

package agent

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

func TestExecScanDeadline(t *testing.T) {
	for _, tc := range []struct{ grace, want time.Duration }{
		{10 * time.Second, 3 * time.Second},
		{5 * time.Second, 3 * time.Second},
		{4 * time.Second, 2 * time.Second},
		{3 * time.Second, time.Second},
		{time.Second, time.Second},
		{4500 * time.Millisecond, 2500 * time.Millisecond},
	} {
		if got := execScanDeadline(tc.grace); got != tc.want {
			t.Errorf("grace %s: deadline %s, want %s", tc.grace, got, tc.want)
		}
	}
}

type execDeadlineExecutor struct {
	*processrunner.Runner
	run completionDirectiveRunFunc
}

func (e execDeadlineExecutor) Run(ctx context.Context, request processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
	return e.run(ctx, request, sink)
}

func TestExecScanDeadlineEnvironment(t *testing.T) {
	for _, tc := range []struct {
		name, params, want string
		grace              time.Duration
	}{
		{"version1", `{"exec_version":1}`, "3", 0},
		{"version2", captureSpec("good").Labels[contract.LabelRunParams], "3", 0},
		{"ordinary_process", `{}`, "", 0},
		{"version1_custom_grace", `{"exec_version":1}`, "2", 4 * time.Second},
		{"version2_short_grace", captureSpec("good").Labels[contract.LabelRunParams], "1", 3 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			harness := newRetentionHarness(t, time.Hour)
			path := filepath.Join(harness.root, "run_deadline")
			claim := retentionClaim(t, "run_deadline", path)
			claim.Job.Spec.Execution.Env = mailboxSpec("run_deadline", path, "").Execution.Env
			claim.Job.Spec.Execution.SensitiveEnv = map[string]string{contract.EnvRunToken: mailboxTestToken}
			claim.SubmittedByRunLedger = true
			claim.Job.Spec.Labels[contract.LabelRunParams] = tc.params
			called := false
			executor := execDeadlineExecutor{
				Runner: processrunner.New(processrunner.Config{TerminationGraceTime: tc.grace}),
				run: func(_ context.Context, request processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
					called = true
					if got, present := request.Execution.Env[execScanDeadlineEnv]; got != tc.want || present != (tc.want != "") {
						t.Errorf("wrapper deadline = %q, present=%v; want %q", got, present, tc.want)
					}
					code := 0
					return contract.ProcessResult{ExitCode: &code}, nil
				},
			}
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
				handoffs: harness.manager, runtimes: testRuntimeSet(executor), runLedger: captureSweepAppender(),
				clock: systemClock{}, nodeID: "node-1", bootSessionID: "deadline-test", logf: t.Logf,
			})
			if _, err := lifecycle.runWorkload(t.Context(), claim); err != nil || !called {
				t.Fatalf("workload called=%v: %v", called, err)
			}
		})
	}
}
