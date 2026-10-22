//go:build (service_acceptance || service_acceptance_realtiming) && (darwin || linux)

package agent

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

// Real processes exercise the public CLI, capture store, Guardian, durable
// completion and Lease-expiry path. The only mutable external files are test
// synchronization and observations; all command inputs are captured once.
func assertRestartableExecRecovery(t *testing.T, crash bool) {
	t.Helper()
	dir, canonicalErr := filepath.EvalSymlinks(t.TempDir())
	if canonicalErr != nil {
		t.Fatal(canonicalErr)
	}
	build := func(name, pkg string) string {
		path := filepath.Join(dir, name)
		cmd := exec.Command("go", "build", "-o", path, pkg)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", name, err, out)
		}
		return path
	}
	cli := build("wefty", "github.com/Derek-X-Wang/wefty/cmd/wefty")
	ledgerBin := build("wefty-l3", "github.com/Derek-X-Wang/wefty/cmd/wefty-l3")
	profile := filepath.Join(dir, "profile.json")
	if err := os.WriteFile(profile, []byte(`{"version":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WEFTY_PROFILE", profile)
	t.Setenv("WEFTY_EXEC_STORAGE_ROOT", filepath.Join(dir, "captures"))
	l1Ready := filepath.Join(dir, "l1-ready.json")
	control := startManagedProcess(t, controlPlanePath, "--fabric=plain", "--listen=127.0.0.1:0", "--run-ledger=", "--db="+filepath.Join(dir, "l1.sqlite"), "--lease-duration=2s", "--node-tags=stable-node=linux,wefty:node:stable-node", "--ready-file="+l1Ready)
	controlLogs := &lockedBuffer{}
	control.command.Stdout = controlLogs
	control.command.Stderr = controlLogs
	control.start(t)
	l1Address := waitForReadyAddress(t, l1Ready, control, 15*time.Second)
	ledgerReady := filepath.Join(dir, "l3-ready.json")
	ledger := startManagedProcess(t, ledgerBin, "--fabric=plain", "--listen=127.0.0.1:0", "--control-plane="+l1Address, "--db="+filepath.Join(dir, "l3.sqlite"), "--reconcile-interval=20ms", "--ready-file="+ledgerReady)
	ledgerLogs := &lockedBuffer{}
	ledger.command.Stdout = ledgerLogs
	ledger.command.Stderr = ledgerLogs
	ledger.start(t)
	l3Address := waitForReadyAddress(t, ledgerReady, ledger, 15*time.Second)
	network := plain.NewNetwork()
	caller := network.NewFabric(fabric.Identity{NodeID: "restart-client", Tags: []string{l1.DefaultClientPrincipalTag, l3.DefaultCallerPrincipalTag}})
	one := newHTTPClient(caller, l1Address)
	defer one.CloseIdleConnections()
	three := newHTTPClient(caller, l3Address)
	defer three.CloseIdleConnections()
	handoff := filepath.Join(dir, "handoffs")
	startAgent := func(boot int) *managedProcess {
		ready := filepath.Join(dir, fmt.Sprintf("agent-%d.json", boot))
		node := startManagedProcess(t, agentBinaryPath, "--fabric=plain", "--control-plane="+l1Address, "--run-ledger="+l3Address, "--node-id=stable-node", "--plain-identity=fabric-node", "--log-spool-dir="+filepath.Join(dir, "spool"), "--managed-root="+filepath.Join(dir, "managed"), "--handoff-root="+handoff, "--heartbeat-interval=200ms", "--claim-interval=20ms", "--renewal-interval=200ms", "--oneshot-load-limit=0", "--shutdown-budget=15s", "--ready-file="+ready)
		logs := &lockedBuffer{}
		node.command.Stdout = logs
		node.command.Stderr = logs
		node.start(t)
		eventuallyRestart(t, "agent ready", 15*time.Second, func() bool {
			if node.exited() {
				t.Fatalf("agent exited: %v\n%s", node.waitError(), string(logs.Bytes()))
			}
			_, err := os.Stat(ready)
			return err == nil
		})
		return node
	}
	node := startAgent(1)
	config := filepath.Join(dir, "node.json")
	if err := os.WriteFile(config, []byte(`{"version":1,"node_id":"stable-node","control_socket":"/tmp/unused.sock"}`), 0600); err != nil {
		t.Fatal(err)
	}
	repo := filepath.Join(dir, "repo")
	if err := os.Mkdir(repo, 0700); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) {
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
	}
	git("init", "--quiet")
	git("config", "user.name", "Test")
	git("config", "user.email", "test@example.invalid")
	tracked := filepath.Join(repo, "tracked")
	if err := os.WriteFile(tracked, []byte("base"), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", "tracked")
	git("commit", "--quiet", "-m", "base")
	if err := os.WriteFile(tracked, []byte("frozen dirty"), 0600); err != nil {
		t.Fatal(err)
	}
	trace, once, workReady := filepath.Join(dir, "trace"), filepath.Join(dir, "once"), filepath.Join(dir, "work-ready")
	script := `printf '%s|%s|%s|%s\n' "$(cat tracked)" "$1" "$TMPDIR" "$$" >> "$2"; if [ ! -f "$3" ]; then touch "$3"; touch "$TMPDIR/stale"; touch "$4"; while :; do sleep 0.1; done; fi; [ ! -e "$TMPDIR/stale" ] || exit 31; echo SUCCESS`
	args := []string{"--fabric=plain", "--l1=" + l1Address, "--l3=" + l3Address, "--node-config=" + config, "exec", "--restartable", "--json", "--", "/bin/sh", "-c", script, "frozen-argv", "argument with spaces $literal", trace, once, workReady}
	attached := startManagedProcess(t, cli, args...)
	attached.command.Dir = repo
	output, diagnostics := &lockedBuffer{}, &lockedBuffer{}
	attached.command.Stdout = output
	attached.command.Stderr = diagnostics
	attached.start(t)
	var runID string
	eventuallyRestart(t, "first captured workload", 20*time.Second, func() bool {
		if attached.exited() {
			t.Fatalf("attached exec exited early: %v\n%s\n%s\nL3: %s", attached.waitError(), string(output.Bytes()), string(diagnostics.Bytes()), string(ledgerLogs.Bytes()))
		}
		fields := strings.Fields(string(diagnostics.Bytes()))
		for i := range fields {
			if fields[i] == "run" && i+1 < len(fields) && strings.HasPrefix(fields[i+1], "run_") {
				runID = fields[i+1]
			}
		}
		_, err := os.Stat(workReady)
		return err == nil && runID != ""
	})
	readRun := func() contract.RunRecord {
		var run contract.RunRecord
		restartGet(t, three, "/v1/runs/"+runID, &run)
		return run
	}
	eventuallyRestart(t, "Run running before interruption", 10*time.Second, func() bool { return readRun().Status == contract.RunRunning })
	run := readRun()
	if !run.Restartable {
		t.Fatalf("policy absent: %+v", run)
	}
	var params struct {
		ExecVersion int      `json:"exec_version"`
		Restartable bool     `json:"restartable"`
		Argv        []string `json:"argv"`
	}
	if err := json.Unmarshal(run.Params, &params); err != nil || params.ExecVersion != 2 || !params.Restartable {
		t.Fatalf("params=%s %v", run.Params, err)
	}
	var before l1.Job
	restartGet(t, one, "/v1/jobs/"+run.L1JobID, &before)
	if before.CurrentAttemptID == "" || len(before.Attempts) != 1 {
		t.Fatalf("first binding %+v", before)
	}
	first := before.Attempts[0]
	traceData, err := os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	firstFields := strings.Split(strings.TrimSpace(string(traceData)), "|")
	if len(firstFields) != 4 {
		t.Fatalf("trace %s", traceData)
	}
	pid, err := strconv.Atoi(firstFields[3])
	if err != nil {
		t.Fatal(err)
	}
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		t.Fatal(err)
	}
	// Keep independent observers attached to the Run while its Attempt changes.
	observerArgs := []string{"--fabric=plain", "--l1=" + l1Address, "--l3=" + l3Address}
	waiter := startManagedProcess(t, cli, append(append([]string{}, observerArgs...), "wait", runID, "--timeout=45s")...)
	waiter.command.Stdout = &lockedBuffer{}
	waiter.command.Stderr = &lockedBuffer{}
	waiter.start(t)
	follower := startManagedProcess(t, cli, append(append([]string{}, observerArgs...), "logs", runID, "--follow", "--poll-interval=20ms")...)
	followed := &lockedBuffer{}
	follower.command.Stdout = followed
	follower.command.Stderr = followed
	follower.start(t)
	if err = os.WriteFile(tracked, []byte("changed after acceptance"), 0600); err != nil {
		t.Fatal(err)
	}
	if crash {
		if err = node.command.Process.Kill(); err != nil {
			t.Fatal(err)
		}
	} else {
		if err = node.command.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
	}
	eventuallyRestart(t, "old boot exit", 20*time.Second, node.exited)
	eventuallyRestart(t, "Guardian process group quiescence", 10*time.Second, func() bool { return errors.Is(syscall.Kill(-pgid, 0), syscall.ESRCH) })
	if !crash && node.waitError() != nil {
		t.Fatalf("controlled shutdown failed: %v", node.waitError())
	}
	// Leave predecessor result and an unpublished invalid event after the boot
	// has joined, so only successor preparation can exclude them.
	oldHandoff := filepath.Join(handoff, runID)
	staleEvent := filepath.Join(oldHandoff, runMailboxDirectoryName, runID, first.AttemptID, runMailboxEventsDirectoryName, "predecessor")
	if err = os.MkdirAll(filepath.Dir(staleEvent), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(staleEvent, []byte("unpublished predecessor evidence"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(oldHandoff, "result.json"), []byte(`{"exit_code":99}`), 0600); err != nil {
		t.Fatal(err)
	}
	eventuallyRestart(t, "nonterminal recovery gap", 10*time.Second, func() bool {
		var pending l1.Job
		restartGet(t, one, "/v1/jobs/"+run.L1JobID, &pending)
		if pending.State != contract.JobQueued {
			return false
		}
		if observed := readRun(); observed.Status != contract.RunRunning {
			t.Fatalf("Run became terminal or regressed during recovery: %s", observed.Status)
		}
		return true
	})
	node = startAgent(2)
	eventuallyRestart(t, "attached terminal success", 35*time.Second, attached.exited)
	if attached.waitError() != nil {
		t.Fatalf("attached failed: %v\n%s\n%s\nL3: %s", attached.waitError(), string(output.Bytes()), string(diagnostics.Bytes()), string(ledgerLogs.Bytes()))
	}
	eventuallyRestart(t, "wait terminal success", 10*time.Second, waiter.exited)
	if waiter.waitError() != nil {
		t.Fatalf("wait: %v", waiter.waitError())
	}
	eventuallyRestart(t, "log follow terminal success", 10*time.Second, follower.exited)
	if follower.waitError() != nil {
		t.Fatalf("follow: %v", follower.waitError())
	}
	run = readRun()
	if run.Status != contract.RunSucceeded {
		t.Fatalf("run %+v", run)
	}
	var after l1.Job
	restartGet(t, one, "/v1/jobs/"+run.L1JobID, &after)
	if len(after.Attempts) != 2 || after.RestartRecovery == nil || after.RestartRecovery.Count != 1 {
		t.Fatalf("recovery %+v", after)
	}
	var predecessor, successor l1.Attempt
	for _, a := range after.Attempts {
		if a.AttemptID == first.AttemptID {
			predecessor = a
		} else {
			successor = a
		}
	}
	wantState, wantReason := contract.AttemptInterrupted, l1.InterruptionAgentShutdown
	if crash {
		wantState, wantReason = contract.AttemptLost, l1.InterruptionLeaseExpired
	}
	if predecessor.State != wantState || predecessor.InterruptionReason != wantReason || successor.State != contract.AttemptSucceeded {
		t.Fatalf("attempts %+v", after.Attempts)
	}
	traceData, err = os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(traceData)), "\n")
	if len(lines) != 2 {
		t.Fatalf("trace=%s", traceData)
	}
	secondFields := strings.Split(lines[1], "|")
	if len(secondFields) != 4 || firstFields[0] != "frozen dirty" || secondFields[0] != firstFields[0] || secondFields[1] != firstFields[1] || firstFields[1] != "argument with spaces $literal" || secondFields[2] == firstFields[2] {
		t.Fatalf("inputs/TMPDIR drifted: %s", traceData)
	}
	var page l1.LogPage
	restartGet(t, three, "/v1/runs/"+runID+"/logs?limit=1000", &page)
	db, err := sql.Open("sqlite", filepath.Join(dir, "l1.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var firstFence, secondFence, firstBoot, secondBoot string
	if err = db.QueryRow(`SELECT fencing_token,boot_session_id FROM attempts WHERE attempt_id=?`, predecessor.AttemptID).Scan(&firstFence, &firstBoot); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT fencing_token,boot_session_id FROM attempts WHERE attempt_id=?`, successor.AttemptID).Scan(&secondFence, &secondBoot); err != nil {
		t.Fatal(err)
	}
	if firstFence == secondFence || firstBoot == secondBoot {
		t.Fatal("successor reused fence or boot")
	}
	var stderr bytes.Buffer
	next := map[string]uint64{}
	for _, event := range page.Events {
		key := event.AttemptID + ":" + string(event.Stream)
		if event.Sequence != next[key] {
			t.Fatalf("log sequence %s got %d want %d", key, event.Sequence, next[key])
		}
		next[key]++
		if event.Stream == contract.LogStderr {
			stderr.Write(event.Bytes)
		}
	}
	successorMarker := fmt.Sprintf("wefty: restart 1/10 attempt=%s previous_attempt=%s reason=%s\n", successor.AttemptID, predecessor.AttemptID, wantReason)
	interruptionMarker := fmt.Sprintf("wefty: attempt %s interrupted; restart pending (reason=agent_shutdown)\n", predecessor.AttemptID)
	if !strings.Contains(stderr.String(), successorMarker) || (!crash && !strings.Contains(stderr.String(), interruptionMarker)) || (crash && strings.Contains(stderr.String(), "interrupted; restart pending")) {
		t.Fatalf("markers: %s", stderr.String())
	}
	if !strings.Contains(string(followed.Bytes()), successorMarker) {
		t.Fatalf("follower missed marker: %s", string(followed.Bytes()))
	}
	if strings.Count(string(output.Bytes()), `"type":"finished"`) != 1 || !strings.Contains(string(output.Bytes()), `"outcome":"pass"`) {
		t.Fatalf("attached NDJSON: %s", string(output.Bytes()))
	}
	var result l3.RunResult
	restartGet(t, three, "/v1/runs/"+runID+"/result", &result)
	if !bytes.Contains(result.Document, []byte(`"exit_code":0`)) {
		t.Fatalf("stale result inherited: %+v", result)
	}
	currentStaleEvent := filepath.Join(oldHandoff, runMailboxDirectoryName, runID, successor.AttemptID, runMailboxEventsDirectoryName, "predecessor")
	if _, err = os.Stat(currentStaleEvent); !os.IsNotExist(err) {
		t.Fatalf("predecessor event entered successor mailbox: %v", err)
	}
	// The ordinary control is still drained, then budget-canceled with a terminal
	// failure. It does not silently opt into the recovery policy.
	controlReady := filepath.Join(dir, "control-ready")
	command := startManagedProcess(t, cli, append(append([]string{}, observerArgs...), "--node-config="+config, "exec", "--detach", "--", "/bin/sh", "-c", `touch "$1"; while :; do sleep 0.1; done`, "control", controlReady)...)
	command.command.Dir = repo
	controlOut := &lockedBuffer{}
	command.command.Stdout = controlOut
	command.command.Stderr = &lockedBuffer{}
	command.start(t)
	eventuallyRestart(t, "ordinary accepted", 15*time.Second, command.exited)
	if command.waitError() != nil {
		t.Fatal(command.waitError())
	}
	controlID := strings.TrimSpace(string(controlOut.Bytes()))
	eventuallyRestart(t, "ordinary running", 15*time.Second, func() bool { _, err := os.Stat(controlReady); return err == nil })
	// A second signal cancels without waiting out the production budget.
	if err = node.command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if err = node.command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	eventuallyRestart(t, "control boot exit", 15*time.Second, node.exited)
	eventuallyRestart(t, "ordinary terminal failure", 10*time.Second, func() bool {
		var r contract.RunRecord
		restartGet(t, three, "/v1/runs/"+controlID, &r)
		return r.Status == contract.RunFailed && !r.Restartable
	})
}

func eventuallyRestart(t *testing.T, phase string, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("phase=%s exceeded %s", phase, timeout)
}
func restartGet(t *testing.T, client *http.Client, path string, target any) {
	t.Helper()
	response, err := client.Get("http://wefty.invalid" + path)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("GET %s: %d %s", path, response.StatusCode, data)
	}
	if err = json.NewDecoder(response.Body).Decode(target); err != nil {
		t.Fatal(err)
	}
}
