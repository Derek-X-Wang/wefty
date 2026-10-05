//go:build darwin || linux

package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestAgentCancelProcessEndToEnd(t *testing.T) {
	for _, test := range []struct {
		name, trap, heartbeat, renew string
		forced                       bool
	}{
		{name: "TERM handled via renewal", trap: `trap 'printf "handled\\n"; exit 0' TERM`, heartbeat: "1h", renew: "100ms"},
		{name: "TERM ignored via heartbeat", trap: `trap '' TERM`, heartbeat: "100ms", renew: "1h", forced: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			readyFile := filepath.Join(directory, "l1-ready.json")
			server := startManagedProcess(t, controlPlanePath, "--fabric=plain", "--listen=127.0.0.1:0", "--run-ledger=", "--db="+filepath.Join(directory, "l1.sqlite"), "--node-tags=stable-node=linux", "--ready-file="+readyFile)
			server.command.Stdout = &lockedBuffer{}
			server.command.Stderr = server.command.Stdout
			server.start(t)
			address := waitForReadyAddress(t, readyFile, server, 10*time.Second)
			// Observe the real binary's completion request; L1 intentionally
			// projects an initiator only for signal results on its read routes.
			proxyNetwork := plain.NewNetwork()
			listener, err := proxyNetwork.NewFabric(fabric.Identity{NodeID: "completion-proxy"}).Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			upstream := newHTTPClient(proxyNetwork.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}}), address)
			defer upstream.CloseIdleConnections()
			completions := make(chan l1.CompletionRequest, 4)
			proxy := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/complete") {
					raw, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						return
					}
					var completion l1.CompletionRequest
					if err := json.Unmarshal(raw, &completion); err != nil {
						t.Error(err)
						return
					}
					completions <- completion
					r.Body = io.NopCloser(bytes.NewReader(raw))
				}
				request := r.Clone(r.Context())
				request.RequestURI = ""
				request.URL.Scheme, request.URL.Host = "http", "control-plane.invalid"
				response, err := upstream.Do(request)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadGateway)
					return
				}
				defer response.Body.Close()
				for name, values := range response.Header {
					w.Header()[name] = values
				}
				w.WriteHeader(response.StatusCode)
				_, _ = io.Copy(w, response.Body)
			})}
			go func() { _ = proxy.Serve(listener) }()
			defer proxy.Close()
			nodeLogs := &lockedBuffer{}
			node := startManagedProcess(t, agentBinaryPath, "--fabric=plain", "--control-plane="+listener.Addr().String(), "--node-id=stable-node", "--log-spool-dir="+filepath.Join(directory, "spool"), "--managed-root="+filepath.Join(directory, "managed"), "--handoff-root="+filepath.Join(directory, "handoffs"), "--plain-identity=fabric-node", "--heartbeat-interval="+test.heartbeat, "--renewal-interval="+test.renew, "--claim-interval=10ms")
			node.command.Stdout = nodeLogs
			node.command.Stderr = nodeLogs
			node.start(t)
			client := newHTTPClient(plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "cancel-client", Tags: []string{l1.DefaultClientPrincipalTag}}), address)
			defer client.CloseIdleConnections()
			ready, release := filepath.Join(directory, "payload-ready"), filepath.Join(directory, "release")
			registerE2EWorkloadRelease(t, release)
			script := filepath.Join(directory, "payload.sh")
			source := fmt.Sprintf("#!/bin/sh\n%s\nprintf '{\"cancellation_evidence\":true}' > \"$WEFTY_HANDOFF_DIR/result.json\"\nprintf 'started\\n'\n: > \"$1\"\nwhile [ ! -e \"$2\" ]; do sleep 0.05; done\n", test.trap)
			if err := os.WriteFile(script, []byte(source), 0700); err != nil {
				t.Fatal(err)
			}
			job := submitE2EJob(t, client, contract.JobSpec{SchemaVersion: 1, DispatchKey: "cancel-e2e", Kind: "process", Class: "one-shot", RoutingTags: []string{"linux"}, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", script, ready, release}, WorkingDirectory: directory}})
			deadline := time.Now().Add(10 * time.Second)
			for {
				if _, err := os.Stat(ready); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("payload not ready; node=%s", nodeLogs.Bytes())
				}
				time.Sleep(20 * time.Millisecond)
			}
			canceledAt := time.Now()
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://control-plane.invalid/v1/jobs/"+job.JobID+"/cancel", nil)
			if err != nil {
				t.Fatal(err)
			}
			response, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("cancel=%d %s", response.StatusCode, raw)
			}
			waitForE2EJobState(t, client, job.JobID, contract.JobFailed, 15*time.Second)
			done := getE2EJob(t, client, job.JobID)
			if done.Outcome != "canceled" || len(done.Attempts) != 1 || done.Attempts[0].Result == nil {
				t.Fatalf("canceled=%+v node=%s", done, nodeLogs.Bytes())
			}
			result := done.Attempts[0].Result
			if test.forced {
				if result.Signal != "killed" || result.TerminationCause != contract.TerminationCauseAgent || time.Since(canceledAt) < 5*time.Second {
					t.Fatalf("forced stop=%+v elapsed=%s", result, time.Since(canceledAt))
				}
			} else if result.ExitCode == nil || *result.ExitCode != 0 {
				t.Fatalf("TERM handler outcome=%+v", result)
			}
			select {
			case completion := <-completions:
				if !test.forced && completion.TerminationInitiator != contract.TerminationCauseAgent {
					t.Fatalf("TERM delivery initiator=%+v", completion)
				}
			case <-time.After(time.Second):
				t.Fatal("real agent sent no completion")
			}
			var uploaded l1.JobResult
			uploadDeadline := time.Now().Add(5 * time.Second)
			for {
				resultRequest, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://control-plane.invalid/v1/jobs/"+job.JobID+"/result", nil)
				if err != nil {
					t.Fatal(err)
				}
				resultResponse, err := client.Do(resultRequest)
				if err != nil {
					t.Fatal(err)
				}
				decodeErr := json.NewDecoder(resultResponse.Body).Decode(&uploaded)
				resultResponse.Body.Close()
				if resultResponse.StatusCode == http.StatusOK {
					if decodeErr != nil || string(uploaded.Document) != `{"cancellation_evidence":true}` || uploaded.AttemptID != done.Attempts[0].AttemptID {
						t.Fatalf("canceled result upload=%+v err=%v", uploaded, decodeErr)
					}
					break
				}
				if resultResponse.StatusCode != http.StatusNotFound || time.Now().After(uploadDeadline) {
					t.Fatalf("canceled result upload=%+v status=%d err=%v", uploaded, resultResponse.StatusCode, decodeErr)
				}
				time.Sleep(10 * time.Millisecond)
			}
			events, _ := pollAllE2ELogs(t, client, job.JobID, "")
			output := joinAgentLogStream(events, contract.LogStdout)
			if !bytes.Contains(output, []byte("started")) {
				t.Fatalf("final output lost=%s", output)
			}
			// The node remains available after cancel, and ordinary work still runs.
			follow := submitE2EJob(t, client, contract.JobSpec{SchemaVersion: 1, DispatchKey: "after-cancel", Kind: "process", Class: "one-shot", RoutingTags: []string{"linux"}, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: agentHelperPath}, Argv: []string{"processhelper", "stdout", "after cancel"}, WorkingDirectory: directory, HandoffDirectory: directory}})
			waitForE2EJobState(t, client, follow.JobID, contract.JobSucceeded, 10*time.Second)
		})
	}
}
