//go:build darwin || linux

package agent

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

// TestShutdownBudgetEndsAResidentAttemptInOrder is the installed node agent
// being stopped by launchd while a job runs: one SIGTERM, then a fixed wait
// before SIGKILL. With --shutdown-budget the agent cancels the resident
// attempt itself once the budget has passed, so the attempt is terminated by
// the agent and its result reaches L1 -- the orderly path -- and the process
// exits well before a kill would land. Without the flag one signal keeps
// waiting for the attempt, as before.
func TestShutdownBudgetEndsAResidentAttemptInOrder(t *testing.T) {
	for _, budgeted := range []bool{true, false} {
		t.Run(fmt.Sprintf("budgeted=%t", budgeted), func(t *testing.T) {
			directory := t.TempDir()
			readyFile := filepath.Join(directory, "ready.json")
			server := startManagedProcess(t, controlPlanePath,
				"--fabric=plain", "--listen=127.0.0.1:0", "--run-ledger=",
				"--db="+filepath.Join(directory, "l1.sqlite"),
				// A long lease, so lease expiry can never be what ends the attempt.
				"--lease-duration=60s",
				"--node-tags=stable-node=linux", "--ready-file="+readyFile,
			)
			server.command.Stdout = &lockedBuffer{}
			server.command.Stderr = server.command.Stdout
			server.start(t)
			address := waitForReadyAddress(t, readyFile, server, 10*time.Second)

			arguments := []string{"--fabric=plain", "--control-plane=" + address, "--node-id=stable-node",
				"--log-spool-dir=" + directory, "--managed-root=" + temporaryManagedRoot(t),
				"--plain-identity=fabric-node",
				"--heartbeat-interval=1s", "--claim-interval=10ms", "--renewal-interval=200ms"}
			if budgeted {
				arguments = append(arguments, "--shutdown-budget=1s")
			}
			agentLogs := &lockedBuffer{}
			node := startManagedProcess(t, agentBinaryPath, arguments...)
			node.command.Stdout = agentLogs
			node.command.Stderr = agentLogs
			node.start(t)

			client := newHTTPClient(plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "e2e-client", Tags: []string{l1.DefaultClientPrincipalTag}}), address)
			defer client.CloseIdleConnections()
			workloadReady := filepath.Join(directory, "workload-ready")
			workloadRelease := filepath.Join(directory, "workload-release")
			registerE2EWorkloadRelease(t, workloadRelease)
			workingDirectory := t.TempDir()
			job := submitE2EJob(t, client, contract.JobSpec{
				SchemaVersion: contract.SchemaVersionV1,
				DispatchKey:   "e2e-shutdown-" + fmt.Sprint(time.Now().UnixNano()),
				Kind:          "process", Class: contract.JobClassOneShot, RoutingTags: []string{"linux"},
				Execution: contract.ExecutionSpec{
					Executable:       contract.ExecutableSpec{Path: agentHelperPath},
					Argv:             []string{"processhelper", "wait-release", workloadReady, workloadRelease, "2m"},
					WorkingDirectory: workingDirectory, HandoffDirectory: workingDirectory,
				},
			})
			for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(10 * time.Millisecond) {
				if _, err := os.Stat(workloadReady); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("the attempt never started:\n%s", agentLogs.Bytes())
				}
			}

			signaled := time.Now()
			if err := node.command.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			if !budgeted {
				select {
				case <-node.done:
					t.Fatalf("one signal without a shutdown budget ended the agent and its resident attempt:\n%s", agentLogs.Bytes())
				case <-time.After(3 * time.Second):
				}
				if state := getE2EJob(t, client, job.JobID).State; state != contract.JobRunning {
					t.Fatalf("job state while the agent drains without a budget = %s", state)
				}
				return
			}
			select {
			case <-node.done:
			case <-time.After(30 * time.Second):
				t.Fatalf("the agent did not exit within its budget and cancellation margin:\n%s", agentLogs.Bytes())
			}
			if elapsed := time.Since(signaled); elapsed < time.Second {
				t.Fatalf("the agent exited after %s, before its 1s budget", elapsed)
			}
			if err := node.waitError(); err != nil {
				t.Fatalf("the agent's orderly shutdown exited with %v:\n%s", err, agentLogs.Bytes())
			}
			// The drain request returns at once; the agent then waits for the
			// resident attempt until the budget runs out.
			if !bytes.Contains(agentLogs.Bytes(), []byte("forced_shutdown transition=drained_to_forced reason=shutdown_budget")) {
				t.Fatalf("no shutdown-budget evidence:\n%s", agentLogs.Bytes())
			}
			ended := getE2EJob(t, client, job.JobID)
			if len(ended.Attempts) != 1 || ended.Attempts[0].Result == nil ||
				ended.Attempts[0].Result.TerminationCause != contract.TerminationCauseAgent {
				t.Fatalf("the resident attempt was not ended in order by the agent: %+v", ended)
			}
		})
	}
}
