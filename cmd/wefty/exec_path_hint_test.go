package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

func TestExecCommandNotFoundHint(t *testing.T) {
	const hint = "wefty: exit 127 usually means a command was not found on the run's PATH; runs get the node's login-shell PATH, not yours. Use `bun run`, `npx`, `pnpm exec`, `uv run` or a path such as ./node_modules/.bin/<tool>; `wefty exec -- sh -c 'echo $PATH'` shows it.\n"
	for _, tc := range []struct {
		name, document, reason, cancel string
		status                         contract.RunState
		code                           int
		hint                           bool
	}{
		{name: "command not found", status: contract.RunFailed, document: `{"exit_code":127}`, reason: "command failed", code: exitRunFailed, hint: true},
		{name: "exit 1", status: contract.RunFailed, document: `{"exit_code":1}`, reason: "command failed", code: exitRunFailed},
		{name: "success", status: contract.RunSucceeded},
		{name: "cancelled", status: contract.RunFailed, document: `{"exit_code":127}`, cancel: "settled", code: exitRunFailed},
		{name: "infrastructure failure", status: contract.RunFailed, document: `{"exit_code":127,"infrastructure_error":"checkout failed"}`, reason: "wrapper failed", code: exitUnavailable},
		{name: "unknown exit", status: contract.RunFailed, document: `{}`, reason: "wrapper failed", code: exitUnavailable},
		{name: "runtime limit", status: contract.RunFailed, document: `{"exit_code":127}`, reason: "maximum runtime reached", code: exitRunFailed},
		{name: "idle limit", status: contract.RunFailed, document: `{"exit_code":127}`, reason: "idle limit reached", code: exitRunFailed},
	} {
		for _, jsonOutput := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/json=%v", tc.name, jsonOutput), func(t *testing.T) {
				clients := newStatusHarness(t).clients()
				record := contract.RunRecord{RunID: "finished", Status: tc.status, FailureReason: tc.reason, CancelStatus: tc.cancel}
				logReads := 0
				clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					var value any
					switch r.URL.Path {
					case "/v1/runs/finished":
						value = record
					case "/v1/runs/finished/logs":
						page := l1.LogPage{}
						if logReads == 0 {
							page.Events = []contract.LogEvent{
								{Stream: contract.LogStdout, Bytes: []byte("command output\n")},
								{Stream: contract.LogStderr, Bytes: []byte("command diagnostic\n")},
							}
						}
						logReads++
						value = page
					case "/v1/runs/finished/result":
						value = l3.RunResult{Document: []byte(tc.document)}
					default:
						t.Fatalf("unexpected read: %s", r.URL.Path)
					}
					raw, err := json.Marshal(value)
					if err != nil {
						t.Fatal(err)
					}
					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(raw))}, nil
				})
				clients.wait = func(context.Context, time.Duration) error { return nil }
				var out, stderr bytes.Buffer
				observed, err := observeExecRunOutput(t.Context(), clients, record.RunID, "local", jsonOutput, &out, &stderr)
				if err != nil {
					t.Fatal(err)
				}
				err = reportExecFinished(t.Context(), clients, observed, execCapture{Base: "base", Dirty: true}, jsonOutput, &out, &stderr)
				if got := execModeCode(err); got != tc.code {
					t.Fatalf("exit=%d want=%d err=%v", got, tc.code, err)
				}
				wantHints := 0
				if tc.hint {
					wantHints = 1
				}
				if got := strings.Count(stderr.String(), hint); got != wantHints {
					t.Errorf("hint count=%d want=%d stderr=%q", got, wantHints, &stderr)
				}
				if !jsonOutput {
					if out.String() != "command output\n" || !strings.HasPrefix(stderr.String(), "command diagnostic\n") {
						t.Fatalf("command output changed: stdout=%q stderr=%q", &out, &stderr)
					}
				} else if tc.hint {
					// These are the exact stdout bytes before the hint was added.
					const want = "{\"type\":\"output\",\"stream\":\"stdout\",\"text\":\"command output\\n\"}\n" +
						"{\"type\":\"output\",\"stream\":\"stderr\",\"text\":\"command diagnostic\\n\"}\n" +
						"{\"type\":\"finished\",\"outcome\":\"fail\",\"exit_code\":127,\"run_id\":\"finished\",\"base_commit\":\"base\",\"dirty\":true,\"status\":\"failed\",\"reason\":\"command failed\"}\n"
					if out.String() != want {
						t.Errorf("JSON stdout changed: got=%q want=%q", &out, want)
					}
					if stderr.String() != hint {
						t.Errorf("stderr=%q want=%q", &stderr, hint)
					}
				}
			})
		}
	}
}
