package lima

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func TestOnDemandIdleWaitAndGentleStop(t *testing.T) {
	now := time.Unix(1000, 0)
	items := []DemandItem{{Kind: "backup", ID: "backup-1"}}
	intent := OCIIntent{Version: OCIIntentVersion, Revision: 1, Enabled: true, Position: OCIPositionOnDemand, UpdatedAt: now}
	runner := &supervisorRunner{states: []InstanceState{InstanceRunning}}
	supervisor, err := NewSupervisor(SupervisorConfig{Instance: DefaultInstanceName, Intent: IntentSourceFunc(func(context.Context) (OCIIntent, error) { return intent, nil }), run: runner.run, now: func() time.Time { return now }, Demand: DemandSourceFunc(func(context.Context) ([]DemandItem, error) { return items, nil })})
	if err != nil {
		t.Fatal(err)
	}
	supervisor.record(InstanceRunning, true, false, "", false)
	if supervisor.IdleDue(t.Context()) {
		t.Fatal("Backup must keep host VM on")
	}
	now = now.Add(time.Hour)
	items = nil
	if supervisor.IdleDue(t.Context()) {
		t.Fatal("idle wait starts after Backup ends")
	}
	now = now.Add(10*time.Minute - time.Nanosecond)
	if supervisor.IdleDue(t.Context()) {
		t.Fatal("stopped before idle deadline")
	}
	now = now.Add(time.Nanosecond)
	if !supervisor.IdleDue(t.Context()) {
		t.Fatal("idle stop not due")
	}
	if err := supervisor.StopIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := runner.commandsSnapshot(); !reflect.DeepEqual(got, [][]string{{"limactl", "stop", DefaultInstanceName}}) {
		t.Fatalf("commands = %v", got)
	}
	if supervisor.Facts().ReasonCode != "host_vm_idle" {
		t.Fatalf("facts=%+v", supervisor.Facts())
	}
	runner.states = []InstanceState{InstanceStopped}
	if supervisor.recoveryNeeded(t.Context(), false) {
		t.Fatal("watchdog restarted idle host VM")
	}
	if err := supervisor.Ensure(t.Context()); err == nil {
		t.Fatal("idle host VM must not start")
	}
}

func TestIdleStopCancelsOnDemandAndForcesOnlyOnTimeout(t *testing.T) {
	for _, failure := range []error{errors.New("stop refused"), context.DeadlineExceeded} {
		t.Run(failure.Error(), func(t *testing.T) {
			now := time.Unix(1000, 0)
			items := []DemandItem{{Kind: "removal", ID: "computer-1"}}
			intent := OCIIntent{Version: OCIIntentVersion, Revision: 1, Enabled: true, Position: OCIPositionOnDemand, UpdatedAt: now}
			calls := [][]string{}
			var commandBudget time.Duration
			supervisor, err := NewSupervisor(SupervisorConfig{Instance: DefaultInstanceName, Intent: IntentSourceFunc(func(context.Context) (OCIIntent, error) { return intent, nil }), Demand: DemandSourceFunc(func(context.Context) ([]DemandItem, error) { return items, nil }), now: func() time.Time { return now }, withTimeout: func(ctx context.Context, budget time.Duration) (context.Context, context.CancelFunc) {
				commandBudget = budget
				return context.WithCancel(ctx)
			}, run: func(ctx context.Context, _ string, args ...string) ([]byte, error) {
				calls = append(calls, args)
				if len(args) == 2 {
					if commandBudget != 90*time.Second {
						t.Fatal("idle stop must have its independent 90-second command budget")
					}
					return nil, failure
				}
				return nil, nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			if err := supervisor.StopIdle(t.Context()); !errors.Is(err, errIdleStopCanceled) || len(calls) != 0 {
				t.Fatalf("demand did not cancel stop: %v %v", calls, err)
			}
			items = nil
			err = supervisor.StopIdle(t.Context())
			want := 1
			if errors.Is(failure, context.DeadlineExceeded) {
				want = 2
			}
			if len(calls) != want {
				t.Fatalf("force fallback = %v", calls)
			}
			if want == 1 && err == nil {
				t.Fatal("non-timeout failure hidden")
			}
		})
	}
}

func TestHostVMStartRefusesLowSpaceBeforeLimaCommand(t *testing.T) {
	intent := newMutableIntent(true)
	runner := &supervisorRunner{states: []InstanceState{InstanceStopped, InstanceRunning}}
	supervisor := newTestSupervisor(t, intent, runner)
	supervisor.config.CheckFreeSpace = func() error { return &HostVMFreeSpaceError{Minimum: 5 << 30, Available: 1} }
	err := supervisor.Ensure(t.Context())
	if err == nil || !strings.Contains(err.Error(), "free space") || !strings.Contains(err.Error(), "retry") || !strings.Contains(err.Error(), "5.00 GiB") {
		t.Fatalf("low-space refusal=%v", err)
	}
	for _, command := range runner.commandsSnapshot() {
		if len(command) > 1 && command[1] == "start" {
			t.Fatalf("low space started host VM: %v", command)
		}
	}
}

func TestOnDemandWatchdogRepairsOnlyWithDemand(t *testing.T) {
	now := time.Unix(1, 0)
	items := []DemandItem(nil)
	intent := OCIIntent{Version: OCIIntentVersion, Revision: 1, Enabled: true, Position: OCIPositionOnDemand, UpdatedAt: now}
	runner := &supervisorRunner{states: []InstanceState{InstanceBroken, InstanceBroken, InstanceBroken, InstanceRunning}}
	supervisor, err := NewSupervisor(SupervisorConfig{Instance: DefaultInstanceName, Intent: IntentSourceFunc(func(context.Context) (OCIIntent, error) { return intent, nil }), Demand: DemandSourceFunc(func(context.Context) ([]DemandItem, error) { return items, nil }), run: runner.run, wait: func(context.Context, time.Duration) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if supervisor.recoveryNeeded(t.Context(), false) {
		t.Fatal("idle broken host VM was restarted without demand")
	}
	if facts := supervisor.Facts(); facts.State != InstanceStopped || facts.ReasonCode != "host_vm_idle" {
		t.Fatalf("broken idle host was not force-stopped: %+v", facts)
	}
	if got := runner.commandsSnapshot(); !reflect.DeepEqual(got, [][]string{{"limactl", "list", "--json", DefaultInstanceName}, {"limactl", "stop", "--force", DefaultInstanceName}}) {
		t.Fatalf("idle broken host commands=%v", got)
	}
	items = []DemandItem{{Kind: "removal", ID: "job-1"}}
	if !supervisor.recoveryNeeded(t.Context(), false) {
		t.Fatal("demand did not request broken-host repair")
	}
	if err := supervisor.Ensure(t.Context()); err != nil {
		t.Fatal(err)
	}
	commands := runner.commandsSnapshot()
	found := false
	for _, command := range commands {
		if len(command) > 1 && command[1] == "start" {
			found = true
		}
	}
	if !found {
		t.Fatalf("demand did not start host VM: %v", commands)
	}
}

func TestIdleStopDetachesHelperBeforeHostShutdown(t *testing.T) {
	now := time.Unix(1, 0)
	items := []DemandItem{{Kind: "oci", ID: "job-1"}}
	intent := OCIIntent{Version: OCIIntentVersion, Revision: 1, Enabled: true, Position: OCIPositionOnDemand, UpdatedAt: now}
	runner := &supervisorRunner{states: []InstanceState{InstanceRunning}}
	supervisor, err := NewSupervisor(SupervisorConfig{Instance: DefaultInstanceName, Intent: IntentSourceFunc(func(context.Context) (OCIIntent, error) { return intent, nil }), Demand: DemandSourceFunc(func(context.Context) ([]DemandItem, error) { return items, nil }), run: runner.run})
	if err != nil {
		t.Fatal(err)
	}
	checksum := "sha256:" + strings.Repeat("a", 64)
	socket, _ := startReadyLimaHelper(t, checksum)
	client := ocihelper.NewUnixClient(socket, checksum)
	helper, err := ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{NodeID: "node", BootSessionID: "boot"})
	if err != nil {
		t.Fatal(err)
	}
	barrier := &SupervisedBootBarrier{Supervisor: supervisor, Barrier: helper}
	if err := barrier.Ensure(t.Context()); err != nil {
		t.Fatal(err)
	}
	items = nil
	generation, _ := helper.Generation()
	stopped, err := barrier.IdleStop(t.Context(), func() bool {
		// Demand arrives after the claim recheck but before the final Lima check.
		items = []DemandItem{{Kind: "load-image"}}
		return true
	})
	if current, ready := helper.Generation(); err != nil || stopped || !ready || current != generation {
		t.Fatalf("final cancellation retired the live session: ready=%t stopped=%t err=%v", ready, stopped, err)
	}
	items = nil
	observed := false
	runner.afterCommand = func(args []string) {
		if len(args) > 0 && args[0] == "stop" {
			observed = true
			if !supervisor.ensureMu.TryLock() {
				t.Error("idle command must be serialized by the cycle lock alone")
			} else {
				supervisor.ensureMu.Unlock()
			}
			if helper.Ready() {
				t.Error("host shutdown can publish helper_unreachable before deliberate session detachment")
			}
		}
	}
	stopped, err = barrier.IdleStop(t.Context(), func() bool { return true })
	if err != nil || !stopped || !observed {
		t.Fatalf("idle stop=%t observed=%t err=%v", stopped, observed, err)
	}
}

func TestIdleStopRequiresRunningAndRearmsAfterFailure(t *testing.T) {
	now := time.Unix(1000, 0)
	intent := OCIIntent{Version: OCIIntentVersion, Revision: 1, Enabled: true, Position: OCIPositionOnDemand, UpdatedAt: now}
	supervisor, err := NewSupervisor(SupervisorConfig{Instance: DefaultInstanceName, Intent: IntentSourceFunc(func(context.Context) (OCIIntent, error) { return intent, nil }), now: func() time.Time { return now }, run: func(context.Context, string, ...string) ([]byte, error) { return nil, errors.New("stop failed") }})
	if err != nil {
		t.Fatal(err)
	}
	supervisor.record(InstanceStopped, true, false, "host_vm_idle", false)
	supervisor.IdleDue(t.Context())
	now = now.Add(time.Hour)
	if supervisor.IdleDue(t.Context()) {
		t.Fatal("stopped host scheduled another idle stop")
	}
	supervisor.record(InstanceRunning, true, false, "", false)
	supervisor.IdleDue(t.Context())
	now = now.Add(10 * time.Minute)
	if !supervisor.IdleDue(t.Context()) {
		t.Fatal("running idle host did not become due")
	}
	if err := supervisor.StopIdle(t.Context()); err == nil {
		t.Fatal("failed stop hidden")
	}
	now = now.Add(5 * time.Second)
	if supervisor.IdleDue(t.Context()) {
		t.Fatal("failed idle stop retried on the next watchdog tick")
	}
}

func TestBriefDemandBetweenPollsResetsIdleWait(t *testing.T) {
	now := time.Unix(1000, 0)
	state := &DemandState{now: func() time.Time { return now }}
	intent := OCIIntent{Version: OCIIntentVersion, Revision: 1, Enabled: true, Position: OCIPositionOnDemand, UpdatedAt: now}
	supervisor, err := NewSupervisor(SupervisorConfig{Instance: DefaultInstanceName, Intent: IntentSourceFunc(func(context.Context) (OCIIntent, error) { return intent, nil }), Demand: state, now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	supervisor.record(InstanceRunning, true, false, "", false)
	supervisor.IdleDue(t.Context())
	now = now.Add(9 * time.Minute)
	state.Replace([]DemandItem{{Kind: "load-image"}}, nil)
	now = now.Add(time.Second)
	state.Replace(nil, nil)
	now = now.Add(time.Minute)
	if supervisor.IdleDue(t.Context()) {
		t.Fatal("short image import did not reset idle wait")
	}
	now = now.Add(9 * time.Minute)
	if !supervisor.IdleDue(t.Context()) {
		t.Fatal("host did not become idle after last demand")
	}
}

func TestIdleStoppingMarkSurvivesNodeRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "intent.json")
	if _, err := InitializeOCIIntent(path, time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	runner := &supervisorRunner{states: []InstanceState{InstanceStopped}}
	config := SupervisorConfig{Instance: DefaultInstanceName, Intent: FileIntentSource{Path: path}, run: runner.run}
	supervisor, err := NewSupervisor(config)
	if err != nil {
		t.Fatal(err)
	}
	barrier := &SupervisedBootBarrier{Supervisor: supervisor}
	if err := barrier.MarkStopping(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".stopping"); err != nil {
		t.Fatal("stopping mark is not durable", err)
	}
	restarted, err := NewSupervisor(config)
	if err != nil {
		t.Fatal(err)
	}
	if !restarted.Facts().Stopping {
		t.Fatal("restart lost the pending stop")
	}
	if restarted.recoveryNeeded(t.Context(), false) {
		t.Fatal("restart started an idle host VM")
	}
	if restarted.Facts().Stopping || restarted.Facts().ReasonCode != "host_vm_idle" {
		t.Fatalf("pending stop not reconciled: %+v", restarted.Facts())
	}
	if _, err := os.Stat(path + ".stopping"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stop receipt not cleared: %v", err)
	}
}
