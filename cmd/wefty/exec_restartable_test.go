package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

func TestExecRestartableRefusesOldAgentBeforeCapture(t *testing.T) {
	binary := buildWefty(t)
	stack := newExecStack(t, execRealBinaryIdleLimit)
	// No checkout is required for refusal, so capture (and upload) cannot run.
	t.Chdir(t.TempDir())
	var out, stderr bytes.Buffer
	err := executeExec(t.Context(), stack.clients, true, []string{"--restartable", "--detach", "--", "true"}, &out, &stderr)
	if commandExitCode(err) != exitConflict || !strings.Contains(err.Error(), l1.CapabilityOneshotRestartable) || len(stack.requests) != 0 {
		t.Fatalf("refusal=%v requests=%+v stdout=%s", err, stack.requests, out.String())
	}
	code, output := runWefty(t, binary, 30*time.Second,
		"--l1="+stack.l1Address, "--l3="+stack.l3Address, "--node-config="+stack.clients.nodeConfigPath,
		"exec", "--json", "--restartable", "--detach", "--", "true")
	var envelope contract.ErrorResponse
	if json.Unmarshal([]byte(output), &envelope) != nil || code != exitConflict || envelope.Error.Code != contract.ErrorConflict || !envelope.Error.Retryable || !strings.Contains(envelope.Error.Message, l1.CapabilityOneshotRestartable) {
		t.Fatalf("real binary refusal: exit=%d output=%s", code, output)
	}
}

func TestExecRestartableJSONPolicyAndLimitExit(t *testing.T) {
	raw, err := json.Marshal(execParams{Restartable: true, ExecVersion: 2, Argv: []string{"a b", "$arg"}})
	if err != nil || !strings.Contains(string(raw), `"restartable":true`) {
		t.Fatalf("params=%s %v", raw, err)
	}
	limit := l1.RestartLimitReached
	execution := struct {
		Job *l1.Job `json:"job"`
	}{&l1.Job{RestartRecovery: &l1.OneshotRestartFacts{Count: 10, Limit: 10, TerminalReason: &limit}}}
	body, _ := json.Marshal(execution)
	clients := &apiClients{l3: &apiClient{name: "L3", client: &http.Client{Transport: execRestartRoundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
	})}}}
	event, err := resolveExecFinished(context.Background(), clients, contract.RunRecord{RunID: "run", Status: contract.RunFailed, Restartable: true}, execCapture{})
	if commandExitCode(err) != exitUnavailable || event.ExitCode != nil || event.Reason != limit {
		t.Fatalf("limit outcome %+v %v", event, err)
	}
}

type execRestartRoundTrip func(*http.Request) (*http.Response, error)

func (f execRestartRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRestartableLogFollowKeepsCursorThroughOutage(t *testing.T) {
	reads := 0
	clients := &apiClients{l3: &apiClient{name: "L3", client: &http.Client{Transport: execRestartRoundTrip(func(r *http.Request) (*http.Response, error) {
		var value any
		if strings.HasSuffix(r.URL.Path, "/logs") {
			reads++
			cursor := r.URL.Query().Get("cursor")
			if reads > 1 && cursor != "after-a" {
				t.Fatalf("cursor lost on read %d: %q", reads, cursor)
			}
			switch reads {
			case 1:
				value = l1.LogPage{Events: []contract.LogEvent{{AttemptID: "a", Stream: contract.LogStderr, Bytes: []byte("old attempt\n")}}, NextCursor: "after-a"}
			case 2:
				return nil, io.ErrUnexpectedEOF
			case 3:
				raw, _ := json.Marshal(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorInternal, Retryable: true}})
				return &http.Response{StatusCode: 503, Body: io.NopCloser(bytes.NewReader(raw)), Header: make(http.Header)}, nil
			case 4:
				value = l1.LogPage{Events: []contract.LogEvent{{AttemptID: "b", Stream: contract.LogStderr, Bytes: []byte("new attempt\n")}}, NextCursor: "after-a"}
			default:
				value = l1.LogPage{NextCursor: "after-a"}
			}
		} else {
			state := contract.RunRunning
			if reads >= 4 {
				state = contract.RunSucceeded
			}
			value = contract.RunRecord{RunID: "run", Status: state, Restartable: true}
		}
		raw, _ := json.Marshal(value)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(raw)), Header: make(http.Header)}, nil
	})}}}
	var stdout, stderr bytes.Buffer
	if err := executeLogs(t.Context(), clients, false, []string{"run", "--follow", "--poll-interval=1ms"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stderr.String() != "old attempt\nnew attempt\n" || reads != 5 {
		t.Fatalf("follow output=%s reads=%d", stderr.String(), reads)
	}
}

func TestRestartableWaitAcrossTransientOutages(t *testing.T) {
	reads := 0
	clients := &apiClients{l3: &apiClient{name: "L3", client: &http.Client{Transport: execRestartRoundTrip(func(r *http.Request) (*http.Response, error) {
		reads++
		if reads == 2 {
			return nil, io.ErrUnexpectedEOF
		}
		if reads == 3 {
			raw, _ := json.Marshal(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorUnavailable, Retryable: true}})
			return &http.Response{StatusCode: 503, Body: io.NopCloser(bytes.NewReader(raw)), Header: make(http.Header)}, nil
		}
		state := contract.RunRunning
		if reads >= 4 {
			state = contract.RunSucceeded
		}
		raw, _ := json.Marshal(contract.RunRecord{RunID: "run", Restartable: true, Status: state})
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(raw)), Header: make(http.Header)}, nil
	})}}}
	var out, stderr bytes.Buffer
	if err := executeWait(t.Context(), clients, false, []string{"run", "--timeout=10s"}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	if out.String() != "succeeded\n" || reads != 4 {
		t.Fatalf("wait reads=%d output=%q", reads, out.String())
	}
}

func TestRestartableInspectReadsFreshFactsAndAttemptEvidence(t *testing.T) {
	reads := 0
	refuse := false
	reason := l1.InterruptionUpgrade
	clients := &apiClients{l3: &apiClient{name: "L3", client: &http.Client{Transport: execRestartRoundTrip(func(r *http.Request) (*http.Response, error) {
		var value any
		status := 200
		switch r.URL.Path {
		case "/v1/runs/run":
			value = contract.RunRecord{RunID: "run", Restartable: true, L1JobID: "job", Status: contract.RunRunning}
		case "/v1/runs/run/lineage":
			value = l3.RunLineage{RunID: "run"}
		case "/v1/runs/run/execution":
			reads++
			if refuse {
				status = 503
				value = contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorUnavailable, Message: "L1 cannot be observed"}}
				break
			}
			value = l3.RunExecution{RunID: "run", Job: &l1.Job{JobID: "job", State: contract.JobQueued, RestartRecovery: &l1.OneshotRestartFacts{Count: reads, Limit: 10, Pending: true, LastReason: &reason}, Attempts: []l1.Attempt{{AttemptID: "old", State: contract.AttemptInterrupted, InterruptionReason: reason}}}}
		default:
			t.Fatalf("unexpected inspection read: %s", r.URL.Path)
		}
		raw, _ := json.Marshal(value)
		return &http.Response{StatusCode: status, Body: io.NopCloser(bytes.NewReader(raw)), Header: make(http.Header)}, nil
	})}}}
	var out bytes.Buffer
	if err := executeInspect(t.Context(), clients, false, []string{"run", "--execution"}, &out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"restartable: yes; restarts: 1/10; restart pending: yes; last reason: upgrade", "interrupted", "upgrade"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("inspect missing %q: %s", want, out.String())
		}
	}
	out.Reset()
	if err := executeInspect(t.Context(), clients, true, []string{"run"}, &out); err != nil {
		t.Fatal(err)
	}
	var inspection struct {
		Run       contract.RunRecord     `json:"run"`
		Restart   l1.OneshotRestartFacts `json:"restart"`
		Execution *l3.RunExecution       `json:"execution"`
	}
	if err := json.Unmarshal(out.Bytes(), &inspection); err != nil {
		t.Fatal(err)
	}
	if !inspection.Run.Restartable || inspection.Restart.Count != 2 || inspection.Execution != nil {
		t.Fatalf("fresh inspection: %s", out.String())
	}
	refuse = true
	out.Reset()
	if err := executeInspect(t.Context(), clients, true, []string{"run"}, &out); commandExitCode(err) != exitUnavailable || out.Len() != 0 {
		t.Fatalf("unavailable inspection=%v output=%s", err, out.String())
	}
}

func TestRestartableAttachedExitMapping(t *testing.T) {
	for _, tt := range []struct {
		name                     string
		status                   contract.RunState
		reason, cancel, terminal string
		missing                  bool
		command                  *int
		want                     int
	}{
		{name: "success", status: contract.RunSucceeded, want: 0, command: func() *int { v := 0; return &v }()},
		{name: "command failure", status: contract.RunFailed, reason: "exit 7", command: func() *int { v := 7; return &v }(), want: exitRunFailed},
		{name: "runtime limit", status: contract.RunFailed, reason: "maximum runtime reached (1s)", want: exitRunFailed},
		{name: "restart limit", status: contract.RunFailed, terminal: l1.RestartLimitReached, want: exitUnavailable},
		{name: "unavailable evidence", status: contract.RunFailed, missing: true, want: exitUnavailable},
		{name: "cancel", status: contract.RunFailed, cancel: "settled", want: exitRunFailed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			clients := &apiClients{l3: &apiClient{name: "L3", client: &http.Client{Transport: execRestartRoundTrip(func(r *http.Request) (*http.Response, error) {
				var value any
				if strings.HasSuffix(r.URL.Path, "/execution") {
					execution := l3.RunExecution{Job: &l1.Job{RestartRecovery: &l1.OneshotRestartFacts{Limit: 10}}}
					if tt.missing {
						execution.Job = nil
					} else if tt.terminal != "" {
						reason := tt.terminal
						execution.Job.RestartRecovery.TerminalReason = &reason
					}
					value = execution
				} else if strings.HasSuffix(r.URL.Path, "/result") {
					raw, _ := json.Marshal(map[string]any{"exit_code": tt.command})
					value = l3.RunResult{Document: raw}
				} else {
					t.Fatalf("unexpected read %s", r.URL.Path)
				}
				raw, _ := json.Marshal(value)
				return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(raw)), Header: make(http.Header)}, nil
			})}}}
			event, err := resolveExecFinished(t.Context(), clients, contract.RunRecord{RunID: "run", Status: tt.status, FailureReason: tt.reason, CancelStatus: tt.cancel, Restartable: true}, execCapture{})
			if execModeCode(err) != tt.want {
				t.Fatalf("exit=%d want=%d event=%+v err=%v", execModeCode(err), tt.want, event, err)
			}
			if (event.ExitCode == nil) != (tt.command == nil) || (tt.command != nil && *event.ExitCode != *tt.command) {
				t.Fatalf("command exit=%v want=%v", event.ExitCode, tt.command)
			}
		})
	}
}
