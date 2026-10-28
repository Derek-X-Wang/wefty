package lima

import (
	"context"
	"errors"
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
					if commandBudget != 4*time.Minute {
						t.Fatal("idle stop retained 30-second command budget")
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
	if err == nil || !strings.Contains(err.Error(), "free space") || !strings.Contains(err.Error(), "retry") {
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
	observed := false
	runner.afterCommand = func(args []string) {
		if len(args) > 0 && args[0] == "stop" {
			observed = true
			if helper.Ready() {
				t.Error("host shutdown can publish helper_unreachable before deliberate session detachment")
			}
		}
	}
	stopped, err := barrier.IdleStop(t.Context(), func() bool { return true })
	if err != nil || !stopped || !observed {
		t.Fatalf("idle stop=%t observed=%t err=%v", stopped, observed, err)
	}
}
