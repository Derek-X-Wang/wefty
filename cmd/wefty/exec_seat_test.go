package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

const execSeatTestID = "seat_0123456789abcdef0123456789abcdef"

func TestExecSeatUsageRefusesBeforeNetwork(t *testing.T) {
	for _, tc := range []struct {
		name    string
		flags   []string
		message string
	}{
		{"zero", []string{"--max-runtime", "0"}, "Seat command always has a limit; use a positive --max-runtime for a longer limit"},
		{"negative", []string{"--max-runtime=-1s"}, "Seat command always has a limit; use a positive --max-runtime for a longer limit"},
		{"node", []string{"--node", "node-other"}, "--node cannot be used with --seat: the Seat decides the node"},
		{"empty-node", []string{"--node="}, "--node cannot be used with --seat: the Seat decides the node"},
		{"tag", []string{"--tag", "kind:process"}, "--tag cannot be used with --seat: the Seat decides placement"},
		{"restartable", []string{"--restartable"}, "--restartable cannot be used with --seat: Seat state cannot be replayed"},
		{"restartable-false", []string{"--restartable=false"}, "--restartable cannot be used with --seat: Seat state cannot be replayed"},
		{"invalid-id", []string{"--seat", "run_0123456789abcdef0123456789abcdef"}, "--seat requires a valid Seat ID"},
		{"empty-id", []string{"--seat="}, "--seat requires a valid Seat ID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(500) }))
			defer server.Close()
			clients := &apiClients{l1: statusClient(t, "L1", "l1", server), l3: statusClient(t, "L3", "l3", server)}
			args := append([]string{"exec", "--seat", execSeatTestID}, tc.flags...)
			args = append(args, "--", "true")
			var out, stderr bytes.Buffer
			err := execute(t.Context(), clients, false, args, &out, &stderr)
			if execModeCode(err) != 2 || !strings.Contains(err.Error(), tc.message) || calls != 0 || out.Len() != 0 {
				t.Fatalf("exit=%d err=%v network=%d stdout=%q stderr=%q", execModeCode(err), err, calls, &out, &stderr)
			}
		})
	}
}

func TestExecSeatSubmitsWithoutCaptureAndPlainStillRequiresCheckout(t *testing.T) {
	for _, inside := range []bool{false, true} {
		for _, deadline := range []string{"", "1001ms", "1ns"} {
			t.Run(fmt.Sprintf("checkout=%t/deadline=%s", inside, deadline), func(t *testing.T) {
				directory := t.TempDir()
				if inside {
					directory = execTestRepo(t)
				}
				t.Chdir(directory)
				root := t.TempDir()
				t.Setenv("WEFTY_EXEC_STORAGE_ROOT", root)
				t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
				const secret = "private-seat-command-secret"
				if _, err := storeSeatCredential(execSeatTestID, secret); err != nil {
					t.Fatal(err)
				}
				argv := []string{"printf", "%s", "--json", "--seat", "--detach"}
				posts := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/v1/runs":
						posts++
						raw, _ := io.ReadAll(r.Body)
						if r.Method != "POST" || r.Header.Get(contract.SeatCredentialHeader) != secret || r.Header.Get("Idempotency-Key") == "" || bytes.Contains(raw, []byte(secret)) {
							t.Errorf("invalid submission metadata or credential leakage")
						}
						var request l3.CreateRunRequest
						if err := json.Unmarshal(raw, &request); err != nil {
							t.Error(err)
						}
						wantSeconds := int64(0)
						if deadline == "1001ms" {
							wantSeconds = 2
						}
						if deadline == "1ns" {
							wantSeconds = 1
						}
						if request.SeatRun == nil || request.SeatRun.SeatID != execSeatTestID || !reflect.DeepEqual(request.SeatRun.Argv, argv) || request.SeatRun.MaxRuntimeSeconds != wantSeconds {
							t.Errorf("wrong Seat source: %s", raw)
						}
						if string(request.Params) != "null" || request.InlineScript != nil || request.Image != nil || request.WorkflowRef != "" || len(request.Tags) != 0 || request.Limits != nil || request.Restartable || request.DispatchAuthority || request.ParentRunID != "" {
							t.Errorf("Seat command carried another source or params: %s", raw)
						}
						w.WriteHeader(http.StatusCreated)
						io.WriteString(w, `{"run_id":"run-seat"}`)
					case "/v1/seat-requests/" + execSeatTestID:
						json.NewEncoder(w).Encode(l1.SeatCallResult{SeatRequest: l1.SeatRequest{SeatID: execSeatTestID, State: l1.SeatLive}})
					case "/v1/nodes/node-cli":
						json.NewEncoder(w).Encode(l1.Node{NodeID: "node-cli", AuthoritativeTags: []string{contract.StableNodeTagPrefix + "node-cli"}})
					default:
						t.Errorf("unexpected request (capture/upload/selection): %s %s", r.Method, r.URL.Path)
						w.WriteHeader(500)
					}
				}))
				defer server.Close()
				clients := &apiClients{l1: statusClient(t, "L1", "l1", server), l3: statusClient(t, "L3", "l3", server), wait: func(context.Context, time.Duration) error { return nil }}
				clients.nodeConfigPath = filepath.Join(t.TempDir(), "node.json")
				writeExecFile(t, clients.nodeConfigPath, `{"version":1,"node_id":"node-cli","control_socket":"/tmp/unused.sock"}`)
				args := []string{"exec", "--seat", execSeatTestID, "--detach", "--json"}
				if deadline != "" {
					args = append(args, "--max-runtime", deadline)
				}
				args = append(append(args, "--"), argv...)
				var out, stderr bytes.Buffer
				err := execute(t.Context(), clients, false, args, &out, &stderr)
				entries, e := os.ReadDir(root)
				if err != nil || e != nil || len(entries) != 0 || posts != 1 || out.String() != "{\"run_id\":\"run-seat\"}\n" || strings.Contains(out.String()+stderr.String(), secret) {
					t.Fatalf("exit=%d err=%v posts=%d capture=%v stdout=%q stderr=%q", execModeCode(err), err, posts, entries, &out, &stderr)
				}
				if !inside {
					out.Reset()
					stderr.Reset()
					err = execute(t.Context(), clients, false, []string{"exec", "--", "true"}, &out, &stderr)
					if execModeCode(err) != 2 || !strings.Contains(err.Error(), "wefty exec runs from inside a Git checkout") || posts != 1 {
						t.Fatalf("plain exec outside checkout: %v posts=%d", err, posts)
					}
				}
			})
		}
	}
}

func TestExecSeatAttachedOutcomesAndDeadlineFix(t *testing.T) {
	for _, jsonOutput := range []bool{false, true} {
		for _, outcome := range []string{"pass", "fail", "limit"} {
			t.Run(fmt.Sprintf("json=%t/%s", jsonOutput, outcome), func(t *testing.T) {
				t.Chdir(t.TempDir())
				t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
				if _, err := storeSeatCredential(execSeatTestID, "secret"); err != nil {
					t.Fatal(err)
				}
				logs := 0
				record := contract.RunRecord{RunID: "run-seat", SeatID: execSeatTestID, Status: contract.RunSucceeded, Limits: &contract.RunLimits{MaxRuntimeSeconds: 7200}}
				wantCode, wantExit := 0, 0
				if outcome != "pass" {
					record.Status = contract.RunFailed
					wantCode = 10
					wantExit = 7
					record.FailureReason = "exit 7"
				}
				if outcome == "limit" {
					record.FailureReason = "maximum runtime reached (2h0m0s)"
				}
				clients := execSeatLedger(t, func(w http.ResponseWriter, r *http.Request) {
					if r.Header.Get(contract.SeatCredentialHeader) != "" && r.URL.Path != "/v1/runs" {
						t.Error("Seat credential sent to an observation endpoint")
					}
					switch r.URL.Path {
					case "/v1/runs":
						w.WriteHeader(201)
						io.WriteString(w, `{"run_id":"run-seat"}`)
					case "/v1/runs/run-seat/logs":
						page := l1.LogPage{}
						if logs == 0 {
							page.Events = []contract.LogEvent{{Stream: contract.LogStdout, Bytes: []byte("command-output\n")}}
						}
						logs++
						json.NewEncoder(w).Encode(page)
					case "/v1/runs/run-seat":
						json.NewEncoder(w).Encode(record)
					case "/v1/runs/run-seat/result":
						json.NewEncoder(w).Encode(l3.RunResult{Document: json.RawMessage(`{"exit_code":7}`)})
					case "/v1/seat-requests/" + execSeatTestID:
						json.NewEncoder(w).Encode(l1.SeatCallResult{SeatRequest: l1.SeatRequest{SeatID: execSeatTestID, State: l1.SeatLive}, UnreadEvents: 2})
					default:
						t.Errorf("unexpected request %s", r.URL.Path)
						w.WriteHeader(500)
					}
				})
				var out, stderr bytes.Buffer
				err := execute(t.Context(), clients, jsonOutput, []string{"exec", "--seat", execSeatTestID, "--", "true"}, &out, &stderr)
				if execModeCode(err) != wantCode || logs < 2 {
					t.Fatalf("exit=%d want=%d err=%v stdout=%q stderr=%q", execModeCode(err), wantCode, err, &out, &stderr)
				}
				if !strings.Contains(stderr.String(), "2 new Seat events: wefty logs "+execSeatTestID) {
					t.Errorf("missing shared Seat reply hint: %q", &stderr)
				}
				if outcome == "fail" && strings.Contains(stderr.String(), "stopped:") {
					t.Errorf("ordinary command failure got a Seat limit line: %q", &stderr)
				}
				if outcome == "limit" && !jsonOutput && !strings.Contains(stderr.String(), "stopped: reached its 2h limit. Run it again with a longer --max-runtime.") {
					t.Errorf("missing actual deadline and fix: %q", &stderr)
				}
				if jsonOutput {
					events := execModeEvents(t, out.String())
					final := events[len(events)-1]
					if final["type"] != "finished" || final["outcome"] != outcomeForSeatTest(outcome) || final["run_id"] != "run-seat" || final["seat_id"] != execSeatTestID || final["max_runtime_seconds"] != float64(7200) {
						t.Errorf("bad finished event: %+v", final)
					}
					if outcome == "limit" {
						if final["exit_code"] != nil {
							t.Error("deadline invented exit code")
						}
					} else if final["exit_code"] != float64(wantExit) {
						t.Errorf("exit code=%v", final["exit_code"])
					}
					if events[0]["text"] != "command-output\n" {
						t.Errorf("missing output event: %q", &out)
					}
				} else if out.String() != "command-output\n" {
					t.Errorf("stdout includes CLI metadata: %q", &out)
				}
			})
		}
	}
}
func outcomeForSeatTest(outcome string) string {
	if outcome == "limit" {
		return "fail"
	}
	return outcome
}

func execSeatLedger(t *testing.T, handler http.HandlerFunc) *apiClients {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return &apiClients{l1: statusClient(t, "L1", "l1", server), l3: statusClient(t, "L3", "l3", server), wait: func(ctx context.Context, _ time.Duration) error { return ctx.Err() }}
}

func TestExecSeatStoppingWatchCancelsUnlessKeepRunning(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(fmt.Sprintf("keep-running=%t", keep), func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
			if _, err := storeSeatCredential(execSeatTestID, "secret"); err != nil {
				t.Fatal(err)
			}
			cancels := 0
			record := contract.RunRecord{RunID: "run-seat", SeatID: execSeatTestID, Status: contract.RunQueued}
			clients := execSeatLedger(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/runs":
					w.WriteHeader(201)
					io.WriteString(w, `{"run_id":"run-seat"}`)
				case "/v1/runs/run-seat":
					json.NewEncoder(w).Encode(record)
				case "/v1/runs/run-seat/cancel":
					cancels++
					record.Status = contract.RunFailed
					record.CancelStatus = "settled"
					json.NewEncoder(w).Encode(record)
				case "/v1/seat-requests/" + execSeatTestID:
					json.NewEncoder(w).Encode(l1.SeatCallResult{SeatRequest: l1.SeatRequest{SeatID: execSeatTestID, State: l1.SeatLive}})
				default:
					t.Errorf("unexpected request %s", r.URL.Path)
					w.WriteHeader(500)
				}
			})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			base := clients.l3.client.Transport
			clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				response, err := base.RoundTrip(r)
				if err == nil && r.URL.Path == "/v1/runs" {
					raw, e := io.ReadAll(response.Body)
					response.Body.Close()
					response.Body = io.NopCloser(bytes.NewReader(raw))
					if e != nil {
						return nil, e
					}
					cancel()
				}
				return response, err
			})
			args := []string{"exec", "--seat", execSeatTestID, "--json"}
			if keep {
				args = append(args, "--keep-running")
			}
			args = append(args, "--", "true")
			var out, stderr bytes.Buffer
			err := execute(ctx, clients, false, args, &out, &stderr)
			wantCancels := 1
			outcome := "cancelled"
			if keep {
				wantCancels = 0
				outcome = "queued"
			}
			if cancels != wantCancels || err == nil || keep && execModeCode(err) != 13 || strings.Contains(stderr.String(), "snapshot") {
				t.Fatalf("cancels=%d err=%v stdout=%q stderr=%q", cancels, err, &out, &stderr)
			}
			events := execModeEvents(t, out.String())
			final := events[len(events)-1]
			if final["type"] != "finished" || final["outcome"] != outcome || final["exit_code"] != nil {
				t.Errorf("watch result: %+v", final)
			}
			if keep && (!strings.Contains(stderr.String(), "wefty logs run-seat --follow") || !strings.Contains(stderr.String(), "wefty cancel run-seat")) {
				t.Errorf("missing recovery: %q", &stderr)
			}
		})
	}
}

func TestExecSeatHelpAndBinaryContract(t *testing.T) {
	binary := buildWefty(t)
	code, stdout, stderr := runPolishBinary(t, binary, "exec", "--help")
	if code != 0 || len(stderr) != 0 || !strings.Contains(string(stdout), "wefty exec --seat SEAT_ID") || !strings.Contains(string(stdout), "No checkout capture or file copy") {
		t.Errorf("Seat help exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	t.Chdir(t.TempDir())
	t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
	t.Setenv("WEFTY_EXEC_STORAGE_ROOT", t.TempDir())
	if _, err := storeSeatCredential(execSeatTestID, "binary-secret"); err != nil {
		t.Fatal(err)
	}
	posts := 0
	address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/runs":
			posts++
			raw, _ := io.ReadAll(r.Body)
			var source l3.CreateRunRequest
			if json.Unmarshal(raw, &source) != nil || source.SeatRun == nil || source.SeatRun.SeatID != execSeatTestID || r.Header.Get(contract.SeatCredentialHeader) != "binary-secret" || bytes.Contains(raw, []byte("binary-secret")) {
				t.Error("binary submission source or credential wrong")
			}
			w.WriteHeader(201)
			io.WriteString(w, `{"run_id":"run-seat"}`)
		case "/v1/runs/run-seat":
			json.NewEncoder(w).Encode(contract.RunRecord{RunID: "run-seat", SeatID: execSeatTestID, Status: contract.RunFailed, FailureReason: "maximum runtime reached (1h30m0s)", Limits: &contract.RunLimits{MaxRuntimeSeconds: 5400}})
		case "/v1/runs/run-seat/logs":
			json.NewEncoder(w).Encode(l1.LogPage{})
		case "/v1/seat-requests/" + execSeatTestID:
			json.NewEncoder(w).Encode(l1.SeatCallResult{SeatRequest: l1.SeatRequest{SeatID: execSeatTestID, State: l1.SeatLive}, UnreadEvents: 1})
		case "/v1/nodes/node-cli":
			json.NewEncoder(w).Encode(l1.Node{NodeID: "node-cli", AuthoritativeTags: []string{contract.StableNodeTagPrefix + "node-cli"}})
		default:
			t.Errorf("unexpected binary request %s", r.URL.Path)
			w.WriteHeader(500)
		}
	})
	config := filepath.Join(t.TempDir(), "node.json")
	writeExecFile(t, config, `{"version":1,"node_id":"node-cli","control_socket":"/tmp/unused.sock"}`)
	base := []string{"--l1", address, "--l3", address, "--node-config", config}
	for _, args := range [][]string{
		{"exec", "--seat", execSeatTestID, "--max-runtime", "0", "--json", "--", "true"},
		{"exec", "--seat", execSeatTestID, "--node", "node-other", "--json", "--", "true"},
		{"exec", "--seat", execSeatTestID, "--tag", "kind:process", "--json", "--", "true"},
		{"exec", "--seat", execSeatTestID, "--restartable", "--json", "--", "true"},
		{"exec", "--json", "--", "true"},
	} {
		code, stdout, stderr = runPolishBinary(t, binary, append(append([]string{}, base...), args...)...)
		var envelope contract.ErrorResponse
		if code != 2 || len(stdout) != 0 || json.Unmarshal(stderr, &envelope) != nil || envelope.Error.Code != contract.ErrorInvalidRequest || posts != 0 {
			t.Fatalf("usage exit=%d stdout=%s stderr=%s posts=%d", code, stdout, stderr, posts)
		}
	}
	code, stdout, stderr = runPolishBinary(t, binary, append(append([]string{}, base...), "exec", "--seat", execSeatTestID, "--detach", "--json", "--", "true")...)
	if code != 0 || string(stdout) != "{\"run_id\":\"run-seat\"}\n" || !strings.Contains(string(stderr), "1 new Seat event: wefty logs "+execSeatTestID) || posts != 1 {
		t.Fatalf("detach exit=%d stdout=%s stderr=%s posts=%d", code, stdout, stderr, posts)
	}
	for _, jsonOutput := range []bool{false, true} {
		args := append(append([]string{}, base...), "exec", "--seat", execSeatTestID)
		if jsonOutput {
			args = append(args, "--json")
		}
		args = append(args, "--", "true")
		code, stdout, stderr = runPolishBinary(t, binary, args...)
		if code != 10 {
			t.Fatalf("limit exit=%d stdout=%s stderr=%s", code, stdout, stderr)
		}
		if !jsonOutput && !strings.Contains(string(stderr), "stopped: reached its 90m limit. Run it again with a longer --max-runtime.") {
			t.Errorf("deadline fix=%s", stderr)
		}
		if jsonOutput {
			events := execModeEvents(t, string(stdout))
			final := events[len(events)-1]
			if final["type"] != "finished" || final["exit_code"] != nil || final["max_runtime_seconds"] != float64(5400) {
				t.Errorf("limit event=%v", final)
			}
		}
	}
}

func TestExecSeatCommandNotFoundUsesSeatEnvironment(t *testing.T) {
	clients := ledgerStub(t, map[string]any{"/v1/runs/run-seat/result": l3.RunResult{Document: json.RawMessage(`{"exit_code":127}`)}})
	record := contract.RunRecord{RunID: "run-seat", SeatID: execSeatTestID, Status: contract.RunFailed, FailureReason: "exit 127"}
	var out, stderr bytes.Buffer
	err := reportExecFinished(t.Context(), clients, record, execCapture{}, false, &out, &stderr)
	if execModeCode(err) != 10 || !strings.Contains(stderr.String(), "Seat's environment") || strings.Contains(stderr.String(), "node's login-shell PATH") {
		t.Fatalf("err=%v stderr=%q", err, &stderr)
	}
}

func TestExecSeatInterruptedSubmissionRecoversOnce(t *testing.T) {
	for _, mode := range []string{"cancel", "keep-running", "detach", "recovery-fails"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
			if _, err := storeSeatCredential(execSeatTestID, "secret"); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			posts, cancels := 0, 0
			var key string
			var body []byte
			record := contract.RunRecord{RunID: "run-seat", SeatID: execSeatTestID, Status: contract.RunQueued}
			clients := execSeatLedger(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/runs":
					posts++
					raw, _ := io.ReadAll(r.Body)
					if posts == 1 {
						key = r.Header.Get("Idempotency-Key")
						body = raw
					} else if key == "" || key != r.Header.Get("Idempotency-Key") || !bytes.Equal(body, raw) || r.Header.Get(contract.SeatCredentialHeader) != "secret" {
						t.Error("recovery changed submission or lost credential")
					}
					if posts > 2 {
						t.Error("recovery submitted more than once")
					}
					if posts == 2 && mode == "recovery-fails" {
						w.WriteHeader(503)
						io.WriteString(w, `{"error":{"code":"unavailable","retryable":true}}`)
						return
					}
					w.WriteHeader(201)
					io.WriteString(w, `{"run_id":"run-seat"}`)
				case "/v1/runs/run-seat":
					json.NewEncoder(w).Encode(record)
				case "/v1/runs/run-seat/cancel":
					cancels++
					record.Status = contract.RunFailed
					record.CancelStatus = "settled"
					json.NewEncoder(w).Encode(record)
				case "/v1/seat-requests/" + execSeatTestID:
					json.NewEncoder(w).Encode(l1.SeatCallResult{SeatRequest: l1.SeatRequest{SeatID: execSeatTestID, State: l1.SeatLive}})
				default:
					t.Errorf("unexpected request %s", r.URL.Path)
					w.WriteHeader(500)
				}
			})
			base := clients.l3.client.Transport
			clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == "/v1/runs" && posts == 1 {
					if r.Context().Err() != nil {
						t.Error("recovery inherited caller cancellation")
					}
					deadline, ok := r.Context().Deadline()
					if !ok || time.Until(deadline) > 5*time.Second {
						t.Error("recovery needs a short deadline")
					}
				}
				response, err := base.RoundTrip(r)
				if err == nil && r.URL.Path == "/v1/runs" && posts == 1 {
					io.Copy(io.Discard, response.Body)
					response.Body.Close()
					cancel()
					return nil, context.Canceled // L3 committed, but the reply was lost on Ctrl-C.
				}
				return response, err
			})
			args := []string{"exec", "--seat", execSeatTestID, "--json"}
			if mode == "keep-running" || mode == "detach" {
				args = append(args, "--"+mode)
			}
			args = append(args, "--", "true")
			var out, stderr bytes.Buffer
			err := execute(ctx, clients, false, args, &out, &stderr)
			if posts != 2 {
				t.Fatalf("submission POSTs=%d want original plus one recovery; err=%v", posts, err)
			}
			if mode == "recovery-fails" {
				if execModeCode(err) != 13 || cancels != 0 || out.Len() != 0 {
					t.Fatalf("failed recovery err=%v cancels=%d stdout=%q", err, cancels, &out)
				}
				return
			}
			if mode == "detach" {
				if err != nil || cancels != 0 || out.String() != "{\"run_id\":\"run-seat\"}\n" {
					t.Fatalf("detach err=%v cancels=%d stdout=%q", err, cancels, &out)
				}
				return
			}
			wantCancels, wantOutcome := 1, "cancelled"
			if mode == "keep-running" {
				wantCancels, wantOutcome = 0, "queued"
				if execModeCode(err) != 13 {
					t.Errorf("keep-running exit=%d", execModeCode(err))
				}
			}
			events := execModeEvents(t, out.String())
			if err == nil || cancels != wantCancels || len(events) == 0 || events[len(events)-1]["outcome"] != wantOutcome {
				t.Fatalf("recovery err=%v cancels=%d stdout=%q stderr=%q", err, cancels, &out, &stderr)
			}
		})
	}
}

func TestExecSeatMissingCredentialTypedFromRealBinary(t *testing.T) {
	t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
	calls := 0
	address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(500) })
	code, out, stderr := runPolishBinary(t, buildWefty(t), "--l1", address, "--l3", address, "exec", "--seat", execSeatTestID, "--json", "--", "true")
	var envelope contract.ErrorResponse
	if code != 3 || len(out) != 0 || calls != 0 || json.Unmarshal(stderr, &envelope) != nil || envelope.Error.Code != contract.ErrorForbidden || envelope.Error.Retryable || !strings.Contains(envelope.Error.Message, "wefty seat wait "+execSeatTestID) {
		t.Fatalf("exit=%d calls=%d stdout=%s stderr=%s", code, calls, out, stderr)
	}
}

func TestExecSeatUsageFormsFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	code, out, stderr := runPolishBinary(t, binary, "--help")
	if code != 0 || len(stderr) != 0 || !strings.Contains(string(out), "exec --seat SEAT_ID") {
		t.Errorf("root help exit=%d stdout=%s stderr=%s", code, out, stderr)
	}
	code, out, stderr = runPolishBinary(t, binary, "exec", "--seat", execSeatTestID, "true")
	if code != 2 || len(out) != 0 || !strings.Contains(string(stderr), "wefty exec --seat SEAT_ID -- <command") {
		t.Errorf("missing separator exit=%d stdout=%s stderr=%s", code, out, stderr)
	}
}

func TestExecSeatLimitMessageDoesNotDescribePlainExec(t *testing.T) {
	record := contract.RunRecord{RunID: "run-plain", Status: contract.RunFailed, FailureReason: "maximum runtime reached", Limits: &contract.RunLimits{MaxRuntimeSeconds: 3600}}
	var out, stderr bytes.Buffer
	err := reportExecFinished(t.Context(), ledgerStub(t, nil), record, execCapture{}, false, &out, &stderr)
	if execModeCode(err) != 10 || strings.Contains(stderr.String(), "stopped:") {
		t.Fatalf("plain exec err=%v stderr=%q", err, &stderr)
	}
}
