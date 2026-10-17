package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func execModeCode(err error) int {
	if err == nil {
		return 0
	}
	return commandExitCode(err)
}

func execModeEvents(t *testing.T, out string) []map[string]any {
	t.Helper()
	var events []map[string]any
	scanner := bufio.NewScanner(strings.NewReader(out))
	for scanner.Scan() {
		var event map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatalf("invalid NDJSON line %q: %v", scanner.Text(), err)
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 {
		t.Fatal("no JSON events")
	}
	return events
}

func execModeFinished(t *testing.T, event map[string]any) {
	t.Helper()
	for _, key := range []string{"type", "outcome", "exit_code", "run_id", "base_commit", "dirty", "status"} {
		if _, ok := event[key]; !ok {
			t.Fatalf("finished lacks %s: %+v", key, event)
		}
	}
	if event["type"] != "finished" {
		t.Fatalf("last event is not finished: %+v", event)
	}
	if id, ok := event["run_id"].(string); !ok || id == "" {
		t.Fatalf("missing run ID: %+v", event)
	}
	if base, ok := event["base_commit"].(string); !ok || base == "" {
		t.Fatalf("missing base commit: %+v", event)
	}
	if _, ok := event["dirty"].(bool); !ok {
		t.Fatalf("dirty is not boolean: %+v", event)
	}
}

func execModeAwait(t *testing.T, stack *execStack, id string) contract.RunRecord {
	t.Helper()
	ctx, stop := context.WithTimeout(t.Context(), 20*time.Second)
	defer stop()
	for {
		record, err := stack.clients.getRun(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if runIsTerminal(record.Status) {
			return record
		}
		if err := stack.clients.wait(ctx, 10*time.Millisecond); err != nil {
			t.Fatalf("run %s did not finish: %v", id, err)
		}
	}
}

func TestExecSucceededWithoutCommandResult(t *testing.T) {
	for _, result := range []string{"upload_failed", "absent", "pending"} {
		for _, jsonMode := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/json=%v", result, jsonMode), func(t *testing.T) {
				// The run is healthy, so its reads keep the production budget.
				shortenExecTimers(t)
				stack := newExecStack(t, execTestIdleLimit)
				t.Chdir(execTestRepo(t))
				base := stack.clients.l3.client.Transport
				stack.clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					if strings.HasSuffix(r.URL.Path, "/result") {
						status, body := http.StatusOK, fmt.Sprintf(`{"skip_reason":%q}`, result)
						if result == "pending" {
							status, body = http.StatusNotFound, `{"error":{"code":"not_found","message":"upload pending","retryable":false}}`
						}
						return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
					}
					return base.RoundTrip(r)
				})
				var out, stderr bytes.Buffer
				err := execute(t.Context(), stack.clients, jsonMode, []string{"exec", "--", "true"}, &out, &stderr)
				if execModeCode(err) != 0 {
					t.Fatalf("success requires result upload: exit=%d err=%v stderr=%s output=%s", execModeCode(err), err, &stderr, &out)
				}
				if jsonMode {
					events := execModeEvents(t, out.String())
					final := events[len(events)-1]
					execModeFinished(t, final)
					if final["outcome"] != "pass" || final["status"] != string(contract.RunSucceeded) || final["exit_code"] != float64(0) {
						t.Fatalf("success verdict: %+v", final)
					}
				} else if !strings.Contains(stderr.String(), "succeeded") {
					t.Fatalf("missing plain success verdict: %s", &stderr)
				}
			})
		}
	}
}

func TestExecDetachReturnsAfterAcceptance(t *testing.T) {
	for _, jsonMode := range []bool{false, true} {
		t.Run(fmt.Sprint(jsonMode), func(t *testing.T) {
			shortenExecTimers(t)
			stack := newExecStack(t, execTestIdleLimit)
			t.Chdir(execTestRepo(t))
			release := filepath.Join(t.TempDir(), "release")
			defer writeExecFile(t, release, "")
			var out, stderr bytes.Buffer
			err := execute(t.Context(), stack.clients, jsonMode, []string{"exec", "--detach", "--", "/bin/sh", "-c", `while [ ! -e "$1" ]; do sleep 0.05; done; printf 'detached-tail\n'`, "sh", release}, &out, &stderr)
			if err != nil {
				t.Fatalf("detach: %v %s", err, stderr.String())
			}
			id := strings.TrimSpace(out.String())
			if jsonMode {
				events := execModeEvents(t, out.String())
				if len(events) != 1 || len(events[0]) != 1 {
					t.Fatalf("detach emitted more than run ID: %q", out.String())
				}
				id, _ = events[0]["run_id"].(string)
			}
			record, err := stack.clients.getRun(t.Context(), id)
			if err != nil || runIsTerminal(record.Status) {
				t.Fatalf("detach waited for result or returned invalid ID: %+v %v", record, err)
			}
			var params execParams
			_ = json.Unmarshal(record.Params, &params)
			if _, err := os.Stat(params.Storage); err != nil {
				t.Fatalf("live storage removed: %v", err)
			}
			writeExecFile(t, release, "")
			record = execModeAwait(t, stack, id)
			if record.Status != contract.RunSucceeded || record.CancelStatus != "" {
				t.Fatalf("detached run: %+v", record)
			}
			var logs bytes.Buffer
			if err := execute(t.Context(), stack.clients, false, []string{"logs", id, "--follow"}, &logs, io.Discard); err != nil || !strings.Contains(logs.String(), "detached-tail") {
				t.Fatalf("detached logs: %v %q", err, logs.String())
			}
		})
	}
}

func TestExecKeepRunningSurvivesStopAtAcceptance(t *testing.T) {
	for _, lostReply := range []bool{false, true} {
		t.Run(fmt.Sprint(lostReply), func(t *testing.T) {
			shortenExecTimers(t)
			stack := newExecStack(t, execTestIdleLimit)
			t.Chdir(execTestRepo(t))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if lostReply {
				stack.clients.l3.client.Transport = execStopTransport{base: stack.clients.l3.client.Transport, accepted: true, cancel: cancel}
			} else {
				stack.afterAccept = cancel
			}
			var stderr bytes.Buffer
			holdExecNode(t, stack)
			err := execute(ctx, stack.clients, false, []string{"exec", "--keep-running", "--", "true"}, io.Discard, &stderr)
			if execModeCode(err) != 13 {
				t.Fatalf("stopped watch: %v %q", err, stderr.String())
			}
			runs, err := stack.clients.listRuns(t.Context(), "", 20)
			if err != nil || len(runs.Runs) != 1 {
				t.Fatalf("accepted run missing: %+v %v", runs, err)
			}
			id := runs.Runs[0].RunID
			record, err := stack.clients.getRun(t.Context(), id)
			if err != nil || runIsTerminal(record.Status) || record.CancelStatus != "" {
				t.Fatalf("keep-running cancelled: %+v %v", record, err)
			}
			var params execParams
			_ = json.Unmarshal(record.Params, &params)
			t.Cleanup(func() { _ = cleanupExecStorage(params.Storage) })
			if _, err := os.Stat(params.Storage); err != nil {
				t.Fatalf("queued capture removed: %v", err)
			}
			for _, text := range []string{id, "the run continues", "wefty logs " + id + " --follow", "wefty cancel " + id} {
				if !strings.Contains(stderr.String(), text) {
					t.Errorf("missing %q: %s", text, stderr.String())
				}
			}
			if err := execute(t.Context(), stack.clients, false, []string{"cancel", id}, io.Discard, io.Discard); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestExecJSONReportsCommandResultsAndCapture(t *testing.T) {
	for _, dirty := range []bool{false, true} {
		for _, exit := range []int{0, 7} {
			t.Run(fmt.Sprintf("dirty=%v/exit=%d", dirty, exit), func(t *testing.T) {
				shortenExecTimers(t)
				stack := newExecStack(t, execTestIdleLimit)
				repo := execTestRepo(t)
				base := strings.TrimSpace(execTestGit(t, repo, "rev-parse", "HEAD"))
				if dirty {
					writeExecFile(t, filepath.Join(repo, "local"), "changed")
				}
				t.Chdir(repo)
				var out, stderr bytes.Buffer
				err := execute(t.Context(), stack.clients, true, []string{"exec", "--", "/bin/sh", "-c", fmt.Sprintf("printf 'first\\n'; printf 'error-line\\n' >&2; sleep 0.2; printf 'last\\n'; exit %d", exit)}, &out, &stderr)
				want := 0
				outcome := "pass"
				if exit != 0 {
					want = 10
					outcome = "fail"
				}
				if execModeCode(err) != want {
					t.Fatalf("exit: %v %s", err, stderr.String())
				}
				events := execModeEvents(t, out.String())
				var stdoutText, stderrText string
				heartbeats := 0
				for i, event := range events {
					switch event["type"] {
					case "output":
						if event["stream"] == "stdout" {
							stdoutText += event["text"].(string)
						} else if event["stream"] == "stderr" {
							stderrText += event["text"].(string)
						} else {
							t.Fatalf("invalid stream: %+v", event)
						}
					case "heartbeat":
						heartbeats++
					case "finished":
						if i != len(events)-1 {
							t.Fatal("logs after finished")
						}
					default:
						t.Fatalf("unknown event: %+v", event)
					}
				}
				final := events[len(events)-1]
				execModeFinished(t, final)
				if stdoutText != "first\nlast\n" || stderrText != "error-line\n" || heartbeats == 0 || final["type"] != "finished" || final["outcome"] != outcome || final["exit_code"] != float64(exit) || final["base_commit"] != base || final["dirty"] != dirty || final["run_id"] == "" {
					t.Fatalf("JSON contract: output=%q stderr=%q heartbeats=%d finished=%+v", stdoutText, stderrText, heartbeats, final)
				}
			})
		}
	}
}

func TestExecJSONReportsQueuedAndInfrastructureOutcomes(t *testing.T) {
	for _, outcome := range []string{"queued", "infrastructure_error"} {
		t.Run(outcome, func(t *testing.T) {
			// Every read here is healthy, including the command result, so the
			// reads keep the production budget.
			shortenExecTimers(t)
			if outcome == "infrastructure_error" {
				git, err := exec.LookPath("git")
				if err != nil {
					t.Fatal(err)
				}
				shim := t.TempDir()
				path := filepath.Join(shim, "git")
				writeExecFile(t, path, fmt.Sprintf("#!/bin/sh\nfor arg do [ \"$arg\" = --no-checkout ] && exit 1; done\nexec '%s' \"$@\"\n", git))
				if err := os.Chmod(path, 0755); err != nil {
					t.Fatal(err)
				}
				t.Setenv("PATH", shim+string(os.PathListSeparator)+os.Getenv("PATH"))
			}
			stack := newExecStack(t, execTestIdleLimit)
			t.Chdir(execTestRepo(t))
			args := []string{"exec"}
			ctx := t.Context()
			base := stack.clients.l3.client.Transport
			if outcome == "queued" {
				holdExecNode(t, stack)
				args = append(args, "--keep-running")
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
				// Interrupt after a public record read proves the queued status.
				stack.clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					response, err := base.RoundTrip(r)
					if r.Method == "GET" && strings.Count(r.URL.Path, "/") == 3 {
						cancel()
					}
					return response, err
				})
			}

			var out, stderr bytes.Buffer
			err := execute(ctx, stack.clients, true, append(args, "--", "true"), &out, &stderr)
			if execModeCode(err) != 13 {
				t.Fatalf("availability result: %v %s", err, stderr.String())
			}
			events := execModeEvents(t, out.String())
			final := events[len(events)-1]
			execModeFinished(t, final)
			if final["type"] != "finished" || final["outcome"] != outcome || final["exit_code"] != nil || final["run_id"] == "" || final["base_commit"] == "" {
				t.Fatalf("misreported command failure: %+v", final)
			}
			if outcome == "queued" {
				id := final["run_id"].(string)
				if err := execute(t.Context(), stack.clients, false, []string{"cancel", id}, io.Discard, io.Discard); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestExecKeepRunningSignalsAndJSONFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	stack := newExecStack(t, execRealBinaryIdleLimit)
	t.Chdir(execTestRepo(t))
	base := []string{"--l1=" + stack.l1Address, "--l3=" + stack.l3Address, "--node-config=" + stack.clients.nodeConfigPath, "exec"}
	for _, signal := range []os.Signal{os.Interrupt, syscall.SIGTERM, syscall.SIGHUP} {
		t.Run(signal.String(), func(t *testing.T) {
			started, release := filepath.Join(t.TempDir(), "started"), filepath.Join(t.TempDir(), "release")
			defer writeExecFile(t, release, "")
			cmd := exec.Command(binary, append(append([]string{}, base...), "--keep-running", "--", "/bin/sh", "-c", `touch "$1"; while [ ! -e "$2" ]; do sleep 0.05; done; printf 'continued-tail\n'`, "sh", started, release)...)
			var out bytes.Buffer
			cmd.Stdout, cmd.Stderr = &out, &out
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cmd.Process.Kill() })
			deadline := time.Now().Add(15 * time.Second)
			for {
				if _, err := os.Stat(started); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("payload did not start: %s", out.String())
				}
				time.Sleep(10 * time.Millisecond)
			}
			if err := cmd.Process.Signal(signal); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			select {
			case err := <-done:
				if command, ok := err.(*exec.ExitError); !ok || command.ExitCode() != 13 {
					t.Fatalf("stopped watch exit: %v %s", err, out.String())
				}
			case <-time.After(10 * time.Second):
				t.Fatal("watch did not stop")
			}
			runs, err := stack.clients.listRuns(t.Context(), "", 20)
			if err != nil {
				t.Fatal(err)
			}
			id := runs.Runs[0].RunID
			for _, text := range []string{"wefty logs " + id + " --follow", "wefty cancel " + id} {
				if !strings.Contains(out.String(), text) {
					t.Fatalf("missing recovery command: %s", out.String())
				}
			}
			record, err := stack.clients.getRun(t.Context(), id)
			var params execParams
			_ = json.Unmarshal(record.Params, &params)
			if err != nil || runIsTerminal(record.Status) || record.CancelStatus != "" {
				t.Fatalf("stopped run: %+v %v", record, err)
			}
			if _, err := os.Stat(params.Storage); err != nil {
				t.Fatal(err)
			}
			writeExecFile(t, release, "")
			record = execModeAwait(t, stack, id)
			if record.Status != contract.RunSucceeded || record.CancelStatus != "" {
				t.Fatalf("continued result: %+v", record)
			}
			code, logs := runWefty(t, binary, 20*time.Second, base[0], base[1], "logs", id, "--follow")
			if code != 0 || !strings.Contains(logs, "continued-tail") {
				t.Fatalf("printed follow command: %d %s", code, logs)
			}
			code, logs = runWefty(t, binary, 20*time.Second, base[0], base[1], "cancel", id)
			if code != 0 {
				t.Fatalf("printed cancel command: %d %s", code, logs)
			}
		})
	}
	// Real global JSON extraction and command flags after --.
	for _, exit := range []int{0, 7, 127} {
		cmd := exec.Command(binary, append(append([]string{}, base...), "--json", "--", "/bin/sh", "-c", fmt.Sprintf("printf '%%s\\n' \"$@\"; exit %d", exit), "sh", "--detach", "--keep-running", "--json")...)
		var out, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &stderr
		err := cmd.Run()
		code := 0
		if err != nil {
			if e, ok := err.(*exec.ExitError); ok {
				code = e.ExitCode()
			} else {
				t.Fatal(err)
			}
		}
		want := 0
		if exit != 0 {
			want = 10
		}
		if code != want {
			t.Fatalf("JSON binary: %d %s", code, stderr.String())
		}
		wantHints := 0
		if exit == 127 {
			wantHints = 1
		}
		if got := strings.Count(stderr.String(), "wefty: exit 127 usually means"); got != wantHints {
			t.Fatalf("JSON exit %d: hint count=%d stderr=%s", exit, got, &stderr)
		}
		events := execModeEvents(t, out.String())
		final := events[len(events)-1]
		execModeFinished(t, final)
		var text string
		for _, event := range events {
			if event["type"] == "output" && event["stream"] == "stdout" {
				text += event["text"].(string)
			}
		}
		if text != "--detach\n--keep-running\n--json\n" || final["exit_code"] != float64(exit) {
			t.Fatalf("literal flags/result: %s", out.String())
		}
	}
}

func TestExecDetachedJSONAndInfrastructureExitFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, infrastructure := range []bool{false, true} {
		t.Run(fmt.Sprint(infrastructure), func(t *testing.T) {
			if infrastructure {
				git, err := exec.LookPath("git")
				if err != nil {
					t.Fatal(err)
				}
				shim := t.TempDir()
				path := filepath.Join(shim, "git")
				writeExecFile(t, path, fmt.Sprintf("#!/bin/sh\nfor arg do [ \"$arg\" = --no-checkout ] && exit 1; done\nexec '%s' \"$@\"\n", git))
				if err := os.Chmod(path, 0755); err != nil {
					t.Fatal(err)
				}
				t.Setenv("PATH", shim+string(os.PathListSeparator)+os.Getenv("PATH"))
			}
			stack := newExecStack(t, execRealBinaryIdleLimit)
			t.Chdir(execTestRepo(t))
			base := []string{"--l1=" + stack.l1Address, "--l3=" + stack.l3Address, "--node-config=" + stack.clients.nodeConfigPath, "exec", "--json"}
			release := filepath.Join(t.TempDir(), "release")
			defer writeExecFile(t, release, "")
			args := append([]string{}, base...)
			if !infrastructure {
				args = append(args, "--detach")
			}
			args = append(args, "--", "/bin/sh", "-c", `while [ ! -e "$1" ]; do sleep 0.05; done; exit 0`, "sh", release)
			ctx, stop := context.WithTimeout(t.Context(), 20*time.Second)
			defer stop()
			cmd := exec.CommandContext(ctx, binary, args...)
			var out, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &out, &stderr
			err := cmd.Run()
			if ctx.Err() != nil {
				t.Fatalf("CLI waited too long: %s", stderr.String())
			}
			events := execModeEvents(t, out.String())
			if infrastructure {
				exit, ok := err.(*exec.ExitError)
				if !ok || exit.ExitCode() != 13 {
					t.Fatalf("infrastructure exit: %v %s", err, stderr.String())
				}
				final := events[len(events)-1]
				execModeFinished(t, final)
				if final["outcome"] != "infrastructure_error" || final["exit_code"] != nil {
					t.Fatalf("infrastructure verdict: %+v", final)
				}
				return
			}
			if err != nil {
				t.Fatalf("detach: %v %s", err, stderr.String())
			}
			if len(events) != 1 || len(events[0]) != 1 {
				t.Fatalf("detach emitted streaming events: %q", out.String())
			}
			id, _ := events[0]["run_id"].(string)
			record, err := stack.clients.getRun(t.Context(), id)
			if err != nil || runIsTerminal(record.Status) {
				t.Fatalf("detach acceptance: %+v %v", record, err)
			}
			writeExecFile(t, release, "")
			record = execModeAwait(t, stack, id)
			if record.Status != contract.RunSucceeded || record.CancelStatus != "" {
				t.Fatalf("detached run: %+v", record)
			}
		})
	}
}

func TestExecJSONReportsCancellationAndLostObservation(t *testing.T) {
	for _, cancelRun := range []bool{true, false} {
		t.Run(fmt.Sprint(cancelRun), func(t *testing.T) {
			shortenExecTimers(t)
			stack := newExecStack(t, execTestIdleLimit)
			t.Chdir(execTestRepo(t))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if cancelRun {
				// The cancel and its confirmation reads are healthy, so they
				// keep the production budgets.
				stack.afterAccept = cancel
			} else {
				// Every log read fails without reaching L3, so the short
				// budget ends the same way however slowly the stack answers.
				shortenExecReadRecovery(t)
				base := stack.clients.l3.client.Transport
				stack.clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/logs") {
						return execReadFailure("transport")
					}
					return base.RoundTrip(r)
				})
			}
			var out, stderr bytes.Buffer
			holdExecNode(t, stack)
			err := execute(ctx, stack.clients, true, []string{"exec", "--", "true"}, &out, &stderr)
			if err == nil {
				t.Fatal("interrupted watch returned a command pass")
			}
			events := execModeEvents(t, out.String())
			final := events[len(events)-1]
			execModeFinished(t, final)
			want := "queued"
			if cancelRun {
				want = "cancelled"
			} else if commandExitCode(err) != 13 {
				t.Fatalf("lost observation exit: %v", err)
			}
			if final["type"] != "finished" || final["outcome"] != want || final["exit_code"] != nil {
				t.Fatalf("interrupted verdict: %+v", final)
			}
			id := final["run_id"].(string)
			if !cancelRun {
				if err := execute(t.Context(), stack.clients, false, []string{"cancel", id}, io.Discard, io.Discard); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestExecJSONLateSignalDrainsBeforeVerdict(t *testing.T) {
	shortenExecTimers(t)
	stack := newExecStack(t, execTestIdleLimit)
	t.Chdir(execTestRepo(t))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	base := stack.clients.l3.client.Transport
	terminal, pending := false, false
	stack.clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		response, err := base.RoundTrip(r)
		if err != nil || r.Method != http.MethodGet {
			return response, err
		}
		if strings.Count(r.URL.Path, "/") == 3 {
			raw, e := io.ReadAll(response.Body)
			_ = response.Body.Close()
			response.Body = io.NopCloser(bytes.NewReader(raw))
			var record contract.RunRecord
			if e == nil && json.Unmarshal(raw, &record) == nil && runIsTerminal(record.Status) && !terminal {
				terminal = true
				pending = true
				cancel()
			}
		} else if strings.HasSuffix(r.URL.Path, "/logs") && pending {
			// The final cursor drain must read with a live context after the signal.
			if r.Context().Err() != nil {
				return nil, r.Context().Err()
			}
			raw, e := io.ReadAll(response.Body)
			_ = response.Body.Close()
			var page struct {
				Events     []contract.LogEvent `json:"events"`
				NextCursor string              `json:"next_cursor"`
			}
			if e != nil || json.Unmarshal(raw, &page) != nil {
				t.Fatalf("log page: %v %s", e, raw)
			}
			page.Events = append(page.Events, contract.LogEvent{Stream: contract.LogStdout, Bytes: []byte("pending-tail\n")})
			raw, _ = json.Marshal(page)
			response.Body = io.NopCloser(bytes.NewReader(raw))
			pending = false
		}
		return response, err
	})
	var out, stderr bytes.Buffer
	err := execute(ctx, stack.clients, true, []string{"exec", "--", "true"}, &out, &stderr)
	if err != nil {
		t.Fatalf("late signal replaced pass: %v %s", err, stderr.String())
	}
	events := execModeEvents(t, out.String())
	final := events[len(events)-1]
	execModeFinished(t, final)
	found := false
	for _, event := range events[:len(events)-1] {
		if event["type"] == "output" && event["text"] == "pending-tail\n" {
			found = true
		}
	}
	if !terminal || pending || !found || final["type"] != "finished" || final["outcome"] != "pass" {
		t.Fatalf("late signal skipped log drain: %s", out.String())
	}
}

func TestExecJSONPreservesUTF8AcrossLogPages(t *testing.T) {
	for _, split := range []int{1, 2} {
		t.Run(fmt.Sprintf("split=%d", split), func(t *testing.T) {
			shortenExecTimers(t)
			stack := newExecStack(t, execTestIdleLimit)
			t.Chdir(execTestRepo(t))
			check := []byte("✓")
			pages := [][]contract.LogEvent{
				{{Stream: contract.LogStdout, Bytes: check[:split]}},
				{{Stream: contract.LogStderr, Bytes: []byte("error ✓\n")}},
				{{Stream: contract.LogStdout, Bytes: append(append([]byte{}, check[split:]...), '\n')}, {Stream: contract.LogStderr, Bytes: []byte{0xe2}}},
			}
			base := stack.clients.l3.client.Transport
			stack.clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				response, err := base.RoundTrip(r)
				if err == nil && strings.HasSuffix(r.URL.Path, "/logs") {
					_ = response.Body.Close()
					page := l1.LogPage{}
					if len(pages) > 0 {
						page.Events, pages = pages[0], pages[1:]
					}
					raw, e := json.Marshal(page)
					if e != nil {
						return nil, e
					}
					response.Body = io.NopCloser(bytes.NewReader(raw))
				}
				return response, err
			})
			var out, stderr bytes.Buffer
			if err := execute(t.Context(), stack.clients, true, []string{"exec", "--", "true"}, &out, &stderr); err != nil {
				t.Fatalf("exec: %v %s", err, &stderr)
			}
			events := execModeEvents(t, out.String())
			var stdoutText, stderrText string
			for _, event := range events[:len(events)-1] {
				if event["type"] != "output" {
					t.Fatalf("unexpected event: %+v", event)
				}
				if event["stream"] == "stdout" {
					stdoutText += event["text"].(string)
				} else {
					stderrText += event["text"].(string)
				}
			}
			execModeFinished(t, events[len(events)-1])
			if len(pages) != 0 || stdoutText != "✓\n" || stderrText != "error ✓\n�" {
				t.Fatalf("UTF-8 damaged or trailing bytes lost: stdout=%q stderr=%q pending=%d output=%s", stdoutText, stderrText, len(pages), &out)
			}
		})
	}
}

func TestExecJSONTerminalDrainFailurePreservesVerdict(t *testing.T) {
	for _, exit := range []int{0, 7} {
		t.Run(fmt.Sprint(exit), func(t *testing.T) {
			shortenExecTimers(t)
			production := execReadRecoveryBudget
			t.Cleanup(func() { execReadRecoveryBudget = production })
			stack := newExecStack(t, execTestIdleLimit)
			t.Chdir(execTestRepo(t))
			base := stack.clients.l3.client.Transport
			terminal, drainFailed := false, false
			stack.clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if terminal && strings.HasSuffix(r.URL.Path, "/logs") {
					// The drain read already holds the short budget. Later
					// reads, such as the command result, are healthy again.
					execReadRecoveryBudget = production
					drainFailed = true
					return execReadFailure("transport")
				}
				response, err := base.RoundTrip(r)
				if err == nil && r.Method == http.MethodGet && strings.Count(r.URL.Path, "/") == 3 {
					raw, e := io.ReadAll(response.Body)
					_ = response.Body.Close()
					if e != nil {
						return nil, e
					}
					response.Body = io.NopCloser(bytes.NewReader(raw))
					var record contract.RunRecord
					if !terminal && json.Unmarshal(raw, &record) == nil && runIsTerminal(record.Status) {
						terminal = true
						// Only the drain read that follows fails, and it never
						// reaches L3, so only it gets the short budget.
						execReadRecoveryBudget = 300 * time.Millisecond
					}
				}
				return response, err
			})
			var out, stderr bytes.Buffer
			err := execute(t.Context(), stack.clients, true, []string{"exec", "--", "/bin/sh", "-c", fmt.Sprintf("exit %d", exit)}, &out, &stderr)
			want, outcome := 0, "pass"
			if exit != 0 {
				want, outcome = 10, "fail"
			}
			if !drainFailed || execModeCode(err) != want {
				t.Fatalf("drain replaced verdict: failed=%v exit=%d err=%v output=%s stderr=%s", drainFailed, execModeCode(err), err, &out, &stderr)
			}
			events := execModeEvents(t, out.String())
			final := events[len(events)-1]
			execModeFinished(t, final)
			if final["outcome"] != outcome || final["exit_code"] != float64(exit) || strings.Contains(stderr.String(), "the run continues") || !strings.Contains(stderr.String(), "could not drain logs") {
				t.Fatalf("terminal observation: %+v stderr=%s", final, &stderr)
			}
		})
	}
}
