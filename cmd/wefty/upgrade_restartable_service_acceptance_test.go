//go:build service_acceptance && (darwin || linux)

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/serviceready"
	"github.com/Derek-X-Wang/wefty/l1"
)

type upgradeProcessOutput struct {
	sync.Mutex
	bytes.Buffer
}

func (b *upgradeProcessOutput) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	return b.Buffer.Write(p)
}
func (b *upgradeProcessOutput) text() string { b.Lock(); defer b.Unlock(); return b.Buffer.String() }

type upgradeTestProcess struct {
	cmd    *exec.Cmd
	done   chan struct{}
	err    error
	output *upgradeProcessOutput
}

func startUpgradeTestProcess(t *testing.T, dir, binary string, args ...string) *upgradeTestProcess {
	t.Helper()
	p := &upgradeTestProcess{cmd: exec.Command(binary, args...), done: make(chan struct{}), output: &upgradeProcessOutput{}}
	p.cmd.Dir = dir
	p.cmd.Stdout = p.output
	p.cmd.Stderr = p.output
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.err = p.cmd.Wait(); close(p.done) }()
	t.Cleanup(func() { p.stop(t) })
	return p
}
func (p *upgradeTestProcess) stop(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
		return
	default:
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(20 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
		t.Errorf("process did not stop: %s", p.output.text())
	}
}
func upgradeEventually(t *testing.T, label string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out: %s", label)
}
func upgradeProcessReady(t *testing.T, path string, p *upgradeTestProcess) serviceready.Metadata {
	t.Helper()
	var ready serviceready.Metadata
	upgradeEventually(t, "process ready", func() bool {
		select {
		case <-p.done:
			t.Fatalf("early process exit: %v\n%s", p.err, p.output.text())
		default:
		}
		b, err := os.ReadFile(path)
		return err == nil && json.Unmarshal(b, &ready) == nil
	})
	return ready
}

// Real CLI/L1/L3/agent processes and captured inputs exercise the production
// upgrade entry point. The service-manager adapter replaces the real agent;
// control-service restarts, builds and installation preflight use the existing
// confined fixture. This does not claim launchd or OS-reboot acceptance.
func TestUpgradeRestartableRealProcessesServiceAcceptance(t *testing.T) {
	f := newUpgradeFixture(t)
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	binaries := filepath.Join(dir, "bin")
	if err = os.Mkdir(binaries, 0700); err != nil {
		t.Fatal(err)
	}
	build := exec.Command("go", "build", "-p", "1", "-ldflags=-X main.version="+f.commit, "-o", binaries+string(filepath.Separator), ".", "../wefty-l1", "../wefty-l3", "../wefty-agent")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	nodeID := f.node.NodeID
	oneReady := filepath.Join(dir, "l1-ready")
	one := startUpgradeTestProcess(t, dir, filepath.Join(binaries, "wefty-l1"), "--fabric=plain", "--listen=127.0.0.1:0", "--run-ledger=", "--db="+filepath.Join(dir, "l1.sqlite"), "--node-tags="+nodeID+"=upgrade,wefty:node:"+nodeID, "--node-max-oneshot-slots="+nodeID+"=4", "--ready-file="+oneReady)
	oneAddress := upgradeProcessReady(t, oneReady, one).Address
	threeReady := filepath.Join(dir, "l3-ready")
	three := startUpgradeTestProcess(t, dir, filepath.Join(binaries, "wefty-l3"), "--fabric=plain", "--listen=127.0.0.1:0", "--control-plane="+oneAddress, "--db="+filepath.Join(dir, "l3.sqlite"), "--reconcile-interval=25ms", "--ready-file="+threeReady)
	threeAddress := upgradeProcessReady(t, threeReady, three).Address
	startAgent := func(binary, boot string) *upgradeTestProcess {
		ready := filepath.Join(dir, "agent-"+boot+"-ready")
		p := startUpgradeTestProcess(t, dir, binary, "--fabric=plain", "--control-plane="+oneAddress, "--run-ledger="+threeAddress, "--node-id="+nodeID, "--plain-identity=upgrade-agent", "--managed-root="+filepath.Join(dir, "managed"), "--log-spool-dir="+filepath.Join(dir, "spool"), "--handoff-root="+filepath.Join(dir, "handoffs"), "--heartbeat-interval=200ms", "--claim-interval=25ms", "--renewal-interval=200ms", "--oneshot-load-limit=0", "--ready-file="+ready)
		upgradeProcessReady(t, ready, p)
		return p
	}
	node := startAgent(filepath.Join(binaries, "wefty-agent"), "old")
	profile := f.profile(t)
	profile.L1Address = oneAddress
	profile.L3Address = threeAddress
	paths, _ := resolveSetupPaths(f.home)
	if err := connprofile.Write(paths.profile, profile); err != nil {
		t.Fatal(err)
	}
	cli := filepath.Join(binaries, "wefty")
	config := filepath.Join(dir, "node.json")
	if err := os.WriteFile(config, []byte(fmt.Sprintf(`{"version":1,"node_id":%q,"control_socket":""}`, nodeID)), 0600); err != nil {
		t.Fatal(err)
	}
	repo := filepath.Join(dir, "repo")
	if err := os.Mkdir(repo, 0700); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) {
		c := exec.Command("git", args...)
		c.Dir = repo
		if out, err := c.CombinedOutput(); err != nil {
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
	t.Setenv("WEFTY_EXEC_STORAGE_ROOT", filepath.Join(dir, "captures"))
	common := []string{"--fabric=plain", "--l1=" + oneAddress, "--l3=" + threeAddress, "--node-config=" + config}
	submit := func(restartable bool, script string, args ...string) string {
		flags := append(append([]string{}, common...), "exec", "--detach", "--json")
		if restartable {
			flags = append(flags, "--restartable")
		}
		flags = append(flags, "--", "/bin/sh", "-c", script, "workload")
		flags = append(flags, args...)
		cmd := exec.Command(cli, flags...)
		cmd.Dir = repo
		var out, diag bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &diag
		if err := cmd.Run(); err != nil {
			t.Fatalf("submit: %v %s %s", err, &out, &diag)
		}
		var receipt struct {
			RunID string `json:"run_id"`
		}
		if err := json.Unmarshal(out.Bytes(), &receipt); err != nil || receipt.RunID == "" {
			t.Fatalf("receipt: %s %v", &out, err)
		}
		return receipt.RunID
	}
	trace, once, control := filepath.Join(dir, "trace"), filepath.Join(dir, "once"), filepath.Join(dir, "control")
	runID := submit(true, `printf '%s\n' "$(cat tracked)" >> "$1"; if [ ! -f "$2" ]; then touch "$2"; sleep 120; fi; echo RESTARTED_OK`, trace, once)
	upgradeEventually(t, "first restartable payload", func() bool { _, err := os.Stat(once); return err == nil })
	releaseControl := filepath.Join(dir, "release-control")
	controlID := submit(false, `touch "$1"; while [ ! -f "$2" ]; do sleep 0.05; done; echo CONTROL_OK`, control, releaseControl)
	upgradeEventually(t, "non-restartable control", func() bool { _, err := os.Stat(control); return err == nil })
	var before l1.Job
	readRun := func(id string) contract.RunRecord {
		t.Helper()
		var run contract.RunRecord
		err := withUpgradeClients(t.Context(), profile, func(ctx context.Context, c *apiClients) error {
			var err error
			run, err = c.getRun(ctx, id)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		return run
	}
	run := readRun(runID)
	before, err = observeUpgradeJob(t.Context(), profile, run.L1JobID)
	if err != nil {
		t.Fatal(err)
	}
	oldNode, err := observeUpgradeNode(t.Context(), profile)
	if err != nil {
		t.Fatal(err)
	}
	if len(oldNode.ActiveAttempts) != 2 {
		t.Fatalf("not mixed work: %+v", oldNode.ActiveAttempts)
	}
	if err := os.WriteFile(tracked, []byte("changed after capture"), 0600); err != nil {
		t.Fatal(err)
	}
	// Independent real observers follow the stable Run through the upgrade.
	waiter := startUpgradeTestProcess(t, repo, cli, append(append([]string{}, common...), "wait", runID, "--timeout=60s")...)
	follower := startUpgradeTestProcess(t, repo, cli, append(append([]string{}, common...), "logs", runID, "--follow", "--poll-interval=50ms")...)
	originalBuild := f.operations.build
	f.operations.build = func(ctx context.Context, a, b, c, d string) error {
		if err := originalBuild(ctx, a, b, c, d); err != nil {
			return err
		}
		for _, name := range setupBinaries {
			payload, err := os.ReadFile(filepath.Join(binaries, name))
			if err != nil {
				return err
			}
			if err = os.WriteFile(filepath.Join(b, name), payload, 0755); err != nil {
				return err
			}
		}
		return nil
	}
	var progress bytes.Buffer
	controlJobID := readRun(controlID).L1JobID
	observedControlOpen, waitedForControl := false, false
	f.operations.observe = func(ctx context.Context, p connprofile.Profile) (l1.Node, error) {
		current, err := observeUpgradeNode(ctx, p)
		if err == nil && current.ClaimsEnabled {
			for _, attempt := range current.ActiveAttempts {
				if attempt.JobID == controlJobID {
					observedControlOpen = true
				}
			}
		}
		return current, err
	}
	f.operations.readNode = readUpgradeIntent
	f.operations.readJob = observeUpgradeJob
	f.operations.setClaims = setUpgradeClaims
	f.operations.interruptAttempt = interruptUpgradeAttempt
	f.operations.now = time.Now
	f.operations.wait = func(ctx context.Context, duration time.Duration) error {
		if !waitedForControl && duration == upgradeDrainPoll {
			current, err := readUpgradeIntent(ctx, profile)
			if err != nil {
				return err
			}
			for _, attempt := range current.ActiveAttempts {
				if attempt.JobID == controlJobID {
					lines := strings.Split(strings.TrimSpace(progress.String()), "\n")
					if len(lines) == 0 || !strings.Contains(lines[len(lines)-1], "waiting_nonrestartable=1") {
						t.Fatalf("wait did not classify the ordinary control: %s", progress.String())
					}
					if strings.Contains(lines[len(lines)-1], "claims remain open") && !current.ClaimsEnabled {
						t.Fatal("ordinary gap wait closed admission")
					}

					if !observedControlOpen {
						t.Fatal("ordinary wait without a production observation with claims open")
					}
					waitedForControl = true
					if err := os.WriteFile(releaseControl, []byte("release"), 0600); err != nil {
						return err
					}
				}
			}
		}
		return waitForContext(ctx, duration)
	}
	manager := f.host.launchctl
	replaced := false
	f.host.launchctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if args[0] == "bootout" && strings.HasSuffix(args[1], "/"+setupNodeAgentLabel) {
			pending, err := observeUpgradeJob(ctx, profile, run.L1JobID)
			if err != nil {
				t.Fatal(err)
			}
			current, err := readUpgradeIntent(ctx, profile)
			if err != nil {
				t.Fatal(err)
			}
			if current.ClaimsEnabled || pending.State != contract.JobQueued || pending.CurrentAttemptID != "" || len(pending.Attempts) != 1 || pending.Attempts[0].State != contract.AttemptInterrupted {
				t.Fatalf("replacement before settlement or successor escaped gate: node=%+v job=%+v", current, pending)
			}
			node.stop(t)
		}
		out, err := manager(ctx, name, args...)
		if err == nil && args[0] == "bootstrap" && strings.HasSuffix(args[2], setupNodeAgentLabel+".plist") {
			node = startAgent(filepath.Join(paths.bin, "wefty-agent"), "new")
			replaced = true
			pending, err := observeUpgradeJob(ctx, profile, run.L1JobID)
			if err != nil {
				t.Fatal(err)
			}
			if pending.State != contract.JobQueued || len(pending.Attempts) != 1 {
				t.Fatalf("new boot claimed before admission restored: %+v", pending)
			}
		}
		return out, err
	}
	started := time.Now()
	report, err := runUpgrade(t.Context(), f.host, false, &progress)
	if err != nil {
		t.Fatalf("upgrade: %v\n%s\nnode: %s", err, &progress, node.output.text())
	}
	if !observedControlOpen || !waitedForControl || !replaced || time.Since(started) >= time.Minute || report.RestartPending != 1 {
		t.Fatalf("no bounded replacement: %+v elapsed=%s", report, time.Since(started))
	}
	upgradeEventually(t, "success after replacement", func() bool { return readRun(runID).Status == contract.RunSucceeded })
	upgradeEventually(t, "wait observer", func() bool {
		select {
		case <-waiter.done:
			return true
		default:
			return false
		}
	})
	upgradeEventually(t, "log observer", func() bool {
		select {
		case <-follower.done:
			return true
		default:
			return false
		}
	})
	if waiter.err != nil || follower.err != nil {
		t.Fatalf("observers: wait=%v logs=%v\n%s", waiter.err, follower.err, follower.output.text())
	}
	if observed := readRun(runID); observed.RunID != runID || observed.L1JobID != run.L1JobID {
		t.Fatalf("Run or Job identity changed: %+v", observed)
	}
	after, err := observeUpgradeJob(t.Context(), profile, run.L1JobID)
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Attempts) != 2 || after.RestartRecovery == nil || after.RestartRecovery.Count != 1 || before.CurrentAttemptID == after.CurrentAttemptID {
		t.Fatalf("identity/restart facts: before=%+v after=%+v", before, after)
	}
	var predecessor, successor l1.Attempt
	for _, attempt := range after.Attempts {
		if attempt.AttemptID == before.CurrentAttemptID {
			predecessor = attempt
		} else {
			successor = attempt
		}
	}
	if predecessor.State != contract.AttemptInterrupted || predecessor.InterruptionReason != l1.InterruptionUpgrade || successor.State != contract.AttemptSucceeded {
		t.Fatalf("unexpected settlement: %+v", after.Attempts)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "l1.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var firstFence, secondFence, firstBoot, secondBoot string
	if err := db.QueryRow("SELECT fencing_token,boot_session_id FROM attempts WHERE attempt_id=?", predecessor.AttemptID).Scan(&firstFence, &firstBoot); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT fencing_token,boot_session_id FROM attempts WHERE attempt_id=?", successor.AttemptID).Scan(&secondFence, &secondBoot); err != nil {
		t.Fatal(err)
	}
	if firstFence == secondFence || firstBoot == secondBoot {
		t.Fatal("successor reused fence or boot")
	}
	newNode, err := readUpgradeIntent(t.Context(), profile)
	if err != nil || newNode.BootSessionID == oldNode.BootSessionID {
		t.Fatalf("no new boot: %+v %v", newNode, err)
	}
	data, err := os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != strings.Repeat("frozen dirty\n", 2) {
		t.Fatalf("Run or capture changed: %q", data)
	}
	if readRun(controlID).Status != contract.RunSucceeded {
		t.Fatal("ordinary control interrupted")
	}
	var logs bytes.Buffer
	err = withUpgradeClients(t.Context(), profile, func(ctx context.Context, c *apiClients) error {
		return execute(ctx, c, false, []string{"logs", runID, "--all"}, &logs, &logs)
	})
	if err != nil || !strings.Contains(logs.String(), "interrupted; restart pending (reason=upgrade)") || !strings.Contains(logs.String(), "restart 1/10 attempt=") {
		t.Fatalf("missing markers: %v\n%s", err, &logs)
	}
	assertNoUpgradeDrainReceipt(t, f)
}
