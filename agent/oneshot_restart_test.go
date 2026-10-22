//go:build darwin || linux

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	"github.com/Derek-X-Wang/wefty/l1"
	wr "github.com/Derek-X-Wang/wefty/runner"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

func TestRestartableCompletionEvidenceAndReplay(t *testing.T) {
	code := 0
	claim := l1.Claim{Job: l1.Job{Spec: contract.JobSpec{Restartable: true}}, Lease: l1.AttemptLease{AttemptID: "a", FencingToken: "f"}}
	for _, tt := range []struct {
		name     string
		result   contract.ProcessResult
		evidence wr.ReapEvidence
		cause    error
		want     bool
	}{
		{"guardian termination", contract.ProcessResult{ExitCode: &code, TerminationInitiator: contract.TerminationCauseGuardian}, wr.ReapEvidenceAttempt, nil, true},
		{"natural exit", contract.ProcessResult{ExitCode: &code}, wr.ReapEvidenceAttempt, nil, false},
		{"missing quiescence", contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}, "", nil, false},
		{"runtime limit", contract.ProcessResult{ExitCode: &code, TerminationInitiator: contract.TerminationCauseGuardian, LimitReached: contract.LimitReachedMaxRuntime}, wr.ReapEvidenceAttempt, nil, false},
		{"cancel precedence", contract.ProcessResult{ExitCode: &code, TerminationInitiator: contract.TerminationCauseGuardian}, wr.ReapEvidenceAttempt, errAttemptDirectiveCancel, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lifecycle := &attemptLifecycle{}
			lifecycle.fenceMailbox(errAttemptDirectiveRestart)
			lifecycle.interruption.Store(&restartInterruption{reason: l1.InterruptionUpgrade, key: "request"})
			if tt.cause != nil {
				lifecycle.fenceCause.Store(&tt.cause)
			}
			request := lifecycle.restartCompletionRequest(claim, tt.result, tt.evidence)
			if (request.InterruptionReason != "") != tt.want {
				t.Fatalf("completion: %+v", request)
			}
			durable := durableCompletionOf(request)
			raw, err := json.Marshal(durable)
			if err != nil {
				t.Fatal(err)
			}
			var replay durableCompletion
			if err = json.Unmarshal(raw, &replay); err != nil {
				t.Fatal(err)
			}
			if replay.InterruptionReason != request.InterruptionReason || replay.InterruptionRequestKey != request.InterruptionRequestKey {
				t.Fatalf("replay lost interruption: %s", raw)
			}
		})
	}
	lifecycle := &attemptLifecycle{}
	shutdown := ErrAgentShutdown
	lifecycle.fenceCause.Store(&shutdown)
	result := contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseGuardian}
	if request := lifecycle.restartCompletionRequest(claim, result, wr.ReapEvidenceAttempt); request.InterruptionReason != l1.InterruptionAgentShutdown || request.InterruptionRequestKey != "" {
		t.Fatalf("shutdown: %+v", request)
	}
	failure := errors.New("agent failure")
	lifecycle.fenceCause.Store(&failure)
	if request := lifecycle.restartCompletionRequest(claim, result, wr.ReapEvidenceAttempt); request.InterruptionReason != "" {
		t.Fatalf("failure relabelled: %+v", request)
	}
}

func TestRestartableHandoffExcludesPredecessor(t *testing.T) {
	dir := t.TempDir()
	spec := contract.JobSpec{Restartable: true, Execution: contract.ExecutionSpec{HandoffDirectory: dir, Env: map[string]string{contract.EnvRunID: "run"}}}
	mailbox := filepath.Join(dir, runMailboxDirectoryName, "run")
	if err := os.MkdirAll(filepath.Join(mailbox, runMailboxEventsDirectoryName), 0700); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Join(dir, "result.json"), filepath.Join(mailbox, runMailboxEventsDirectoryName, "stale"), filepath.Join(mailbox, "params.json")} {
		if err := os.WriteFile(path, []byte("stale"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "unrelated"), []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	prepared, err := prepareRunMailbox(spec, "successor", newRecordingAppender(""), time.Hour, nil, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.close()
	if prepared.directory != filepath.Join(mailbox, "successor") {
		t.Fatalf("mailbox is not Attempt-scoped: %s", prepared.directory)
	}
	if _, err := os.Stat(filepath.Join(prepared.directory, runMailboxEventsDirectoryName, "stale")); !os.IsNotExist(err) {
		t.Fatal("predecessor event reached successor")
	}
	if _, err := os.Stat(filepath.Join(dir, "unrelated")); err != nil {
		t.Fatal(err)
	}
}

func TestRestartableMarkersUseProductionSequenceOwner(t *testing.T) {
	clock := newManualClock(mailboxTestClockOrigin)
	sink := &recordingOutputSink{}
	_, _, resource, _ := processInventoryLifecycleFixture(t)
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{managedResource: resource, clock: clock, nodeID: "node", bootSessionID: "boot", runtimes: testRuntimeSet(restartMarkerExecutor{t: t}), outputSinkFactory: func(l1.Claim) processrunner.OutputSink { return sink }})
	reason := l1.InterruptionUpgrade
	previous := "old"
	claim := l1.Claim{Job: l1.Job{JobID: "job", Spec: contract.JobSpec{Kind: contract.JobKindProcess, Class: contract.JobClassOneShot, Restartable: true, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh"}, WorkingDirectory: t.TempDir()}}, RestartRecovery: &l1.OneshotRestartFacts{Count: 1, Limit: 10, LastReason: &reason, PreviousAttemptID: &previous}}, Lease: l1.AttemptLease{AttemptID: "new", FencingToken: "fence"}}
	lifecycle.interruption.Store(&restartInterruption{reason: l1.InterruptionUpgrade, key: "key"})
	lifecycle.fenceMailbox(errAttemptDirectiveRestart)
	if _, err := lifecycle.runWorkload(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	var text strings.Builder
	var sequence uint64
	for _, event := range sink.events {
		if event.Stream == contract.LogStderr {
			if event.Sequence != sequence {
				t.Fatalf("sequence=%d want=%d", event.Sequence, sequence)
			}
			sequence++
			text.Write(event.Bytes)
		}
	}
	want := "wefty: restart 1/10 attempt=new previous_attempt=old reason=upgrade\npayload stderr\nwefty: attempt new interrupted; restart pending (reason=upgrade)\n"
	if text.String() != want {
		t.Fatalf("markers/output=%q want=%q", text.String(), want)
	}
	request := lifecycle.deliveryCompletionRequest(claim, contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseGuardian}, wr.ReapEvidenceAttempt)
	cancel := errAttemptDirectiveCancel
	lifecycle.fenceCause.Store(&cancel)
	if replay := lifecycle.deliveryCompletionRequest(claim, contract.ProcessResult{}, ""); !reflect.DeepEqual(request, replay) {
		t.Fatalf("canonical completion changed: %+v %+v", request, replay)
	}
}

type restartMarkerExecutor struct{ t *testing.T }

func (restartMarkerExecutor) GuardianAvailable() bool { return true }
func (e restartMarkerExecutor) Run(ctx context.Context, request processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
	if !request.RequireGuardian || !request.Guarded {
		e.t.Fatalf("unguarded request: %+v", request)
	}
	if err := sink.WriteOutput(ctx, contract.LogEvent{AttemptID: request.AttemptID, Stream: contract.LogStderr, Sequence: 0, Bytes: []byte("payload stderr\n")}); err != nil {
		return contract.ProcessResult{}, err
	}
	return contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseGuardian}, nil
}

func TestRestartableCapabilityRequiresPrerequisites(t *testing.T) {
	for _, tt := range []struct {
		name, guardian string
		capture, want  bool
	}{
		{"complete", agentBinaryPath, true, true}, {"missing Guardian", "", true, false}, {"missing capture", agentBinaryPath, false, false}, {"unusable Guardian", filepath.Join(t.TempDir(), "missing"), true, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			node, err := New(Config{Fabric: plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "agent"}), ControlPlaneAddress: "wefty://control-plane", NodeID: "node", BootSessionID: "boot", Version: "test", Capabilities: map[string]bool{"kind:process": true, contract.CapabilityExecCaptureBundle: tt.capture, l1.CapabilityOneshotRestartable: true}, GuardianExecutable: tt.guardian, ManagedRootDirectory: temporaryManagedRoot(t), LogSpoolDirectory: t.TempDir(), HandoffRoot: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer node.Close()
			if got := node.registration.Capabilities[l1.CapabilityOneshotRestartable]; got != tt.want {
				t.Fatalf("capability=%t want=%t", got, tt.want)
			}
		})
	}
}

func TestRestartableRenewalConsumesExactInterruption(t *testing.T) {
	for _, tt := range []struct {
		name, attempt, fence, key string
		want                      bool
	}{
		{"exact", "attempt", "fence", "request", true},
		{"stale Attempt", "predecessor", "fence", "request", false},
		{"stale fence", "attempt", "old-fence", "request", false},
		{"missing request", "attempt", "fence", "", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			clock := newManualClock(mailboxTestClockOrigin)
			lease := l1.AttemptLease{AttemptID: tt.attempt, FencingToken: tt.fence, LeaseTTL: time.Second, Directive: l1.AttemptDirectiveRestart, InterruptionReason: l1.InterruptionUpgrade, InterruptionRequestKey: tt.key}
			client := &Client{baseURL: "http://wefty.invalid", operationTimeout: time.Second, httpClient: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != "/v1/agent/jobs/job/attempts/attempt/lease" {
					t.Errorf("wrong renewal path %s", r.URL.Path)
				}
				raw, _ := json.Marshal(lease)
				return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(raw)), Header: make(http.Header)}, nil
			})}}
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{clock: clock, client: client, renewalInterval: 100 * time.Millisecond})
			claim := l1.Claim{Job: l1.Job{JobID: "job", Spec: contract.JobSpec{Restartable: true}}, Lease: l1.AttemptLease{AttemptID: "attempt", FencingToken: "fence", LeaseTTL: time.Second}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			failures := make(chan destinationError, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				lifecycle.renewalLoop(ctx, claim, localAuthority{deadline: clock.Now().Add(time.Second)}, failures, disabledAttemptWatch{}, nil)
			}()
			clock.waitForDeadline(t, clock.Now().Add(100*time.Millisecond))
			clock.Advance(100 * time.Millisecond)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("renewal did not return")
			}
			failure := <-failures
			if errors.Is(failure.err, errAttemptDirectiveRestart) != tt.want {
				t.Fatalf("directive=%v", failure.err)
			}
			lifecycle.fenceMailbox(failure.err)
			completion := lifecycle.restartCompletionRequest(claim, contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseGuardian}, wr.ReapEvidenceAttempt)
			if tt.want {
				if completion.InterruptionReason != l1.InterruptionUpgrade || completion.InterruptionRequestKey != tt.key {
					t.Fatalf("completion did not echo exact request: %+v", completion)
				}
			} else if completion.InterruptionReason != "" {
				t.Fatalf("stale request accepted: %+v", completion)
			}
		})
	}
}

func TestRestartableControlledCauseIsLatchedBeforeCancellation(t *testing.T) {
	for _, upgradeFirst := range []bool{false, true} {
		lifecycle := &attemptLifecycle{}
		if upgradeFirst {
			lifecycle.interruption.Store(&restartInterruption{reason: l1.InterruptionUpgrade, key: "key"})
			lifecycle.fenceMailbox(errAttemptDirectiveRestart)
		}
		lifecycle.fenceMailbox(ErrAgentShutdown)
		lifecycle.interruption.CompareAndSwap(nil, &restartInterruption{reason: l1.InterruptionUpgrade, key: "key"})
		request := lifecycle.restartCompletionRequest(l1.Claim{Job: l1.Job{Spec: contract.JobSpec{Restartable: true}}}, contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseGuardian}, wr.ReapEvidenceAttempt)
		want := l1.InterruptionAgentShutdown
		if upgradeFirst {
			want = l1.InterruptionUpgrade
		}
		if request.InterruptionReason != want {
			t.Fatalf("upgrade first=%t reason=%s want=%s", upgradeFirst, request.InterruptionReason, want)
		}
	}
}
