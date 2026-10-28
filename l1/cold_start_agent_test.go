package l1_test

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type coldConfigClock struct{ at time.Time }

func (c coldConfigClock) Now() time.Time { return c.at }
func (c coldConfigClock) NewTimer(time.Duration) agent.Timer {
	panic("unexpected scheduling in registration-only test")
}

type coldIdleBarrier struct{}

func (coldIdleBarrier) Ready() bool                  { return false }
func (coldIdleBarrier) Ensure(context.Context) error { return errors.New("host VM idle") }
func (coldIdleBarrier) CapabilityReasonCode() contract.CapabilityReasonCode {
	return contract.CapabilityReasonHostVMIdle
}
func (coldIdleBarrier) Generation() (ocihelper.HelperSession, bool) {
	return ocihelper.HelperSession{}, false
}
func (coldIdleBarrier) SweepReceipt() (ocihelper.VerifiedSweepReceipt, bool) {
	return ocihelper.VerifiedSweepReceipt{}, false
}
func (coldIdleBarrier) SetLossHandler(func(ocihelper.HelperSession, error)) {}
func (coldIdleBarrier) Close() error                                        { return nil }

type coldUnrunProbe struct{ t *testing.T }

func (p coldUnrunProbe) Probe(context.Context) (agent.CapabilityProbeResult, error) {
	p.t.Error("idle path ran the functional probe")
	return agent.CapabilityProbeResult{}, errors.New("unexpected probe")
}

// Feed L1 real node-agent output, including a new process with no probe history.
func TestColdStartIdleComputerIsStartingNotUnschedulable(t *testing.T) {
	for _, mode := range []string{"fresh", "restart", "enable-on-demand", "no-room"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)
			store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{Clock: l1.ClockFunc(func() time.Time { return now }), ComputerDefaultImage: "ghcr.io/example/desktop@sha256:" + strings.Repeat("a", 64)})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			network := plain.NewNetwork()
			sf := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
			policy := l1.DefaultNodePolicy(contract.StableNodeTagPrefix + "cold")
			server, err := l1.NewServer(sf, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{"cold": policy}})
			if err != nil {
				t.Fatal(err)
			}
			listener, err := sf.Listen("tcp", "wefty://control-plane")
			if err != nil {
				t.Fatal(err)
			}
			httpServer := &http.Server{Handler: server.Handler()}
			done := make(chan error, 1)
			go func() { done <- httpServer.Serve(listener) }()
			defer func() { httpServer.Close(); <-done }()
			enabled := mode != "enable-on-demand"
			room := l1.DefaultComputerRoomConfig(3 << 30)
			managed, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			config := agent.Config{Fabric: network.NewFabric(fabric.Identity{NodeID: "fabric-cold", Tags: []string{l1.DefaultAgentPrincipalTag}}), ControlPlaneAddress: "wefty://control-plane", NodeID: "cold", BootSessionID: "boot", Version: "test", Clock: coldConfigClock{now}, OS: "darwin", Architecture: "arm64", Capabilities: map[string]bool{"kind:process": true, "kind:oci": true, "runtime_handler:" + ocihelper.DefaultRuntimeHandler: true}, CapabilityProbe: coldUnrunProbe{t}, OCIBootBarrier: coldIdleBarrier{}, ManagedRootDirectory: managed, LogSpoolDirectory: t.TempDir(), ComputerRoom: &room, OCIIntent: func(context.Context) (agent.OCIIntentObservation, error) {
				return agent.OCIIntentObservation{Enabled: enabled, Revision: 2}, nil
			}}
			if mode == "no-room" {
				config.ComputerRoom = nil
			}
			node, err := agent.New(config)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { node.Close() }()
			if _, err := node.Register(t.Context()); err != nil {
				t.Fatal(err)
			}
			if mode == "restart" {
				node.Close()
				node, err = agent.New(config)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := node.Register(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "enable-on-demand" {
				enabled = true
				if err := node.EnableOnDemand(t.Context(), 2); err != nil {
					t.Fatal(err)
				}
			} else {
				// Publish the actual first idle suppression produced by registration.
				if _, err := store.HeartbeatNodeWithCapabilityObservation(t.Context(), "fabric-cold", "cold", "boot", node.CapabilitySnapshot().CapabilityObservation, policy); err != nil {
					t.Fatal(err)
				}
			}
			observation := node.CapabilitySnapshot()
			if observation.LastProbe != nil {
				t.Fatal("fixture unexpectedly has probe history")
			}
			for _, capability := range []string{"kind:oci", "cgroup_v2", "runtime_handler:" + ocihelper.DefaultRuntimeHandler, "runtime_platform:linux/arm64"} {
				if !slices.Contains(observation.MissingCapabilities, capability) || observation.Capabilities[capability] {
					t.Errorf("configured capability %s not restoration-only: %+v", capability, observation)
				}
			}
			if mode == "no-room" {
				if slices.Contains(observation.MissingCapabilities, "computer") {
					t.Fatalf("unconfigured Computer capability: %+v", observation)
				}
				return
			}
			id := "seat_00000000000000000000000000001344"
			if _, err := store.CreateSeatCall(t.Context(), "cli", l1.SeatCallInput{SeatID: id, NodeID: "cold", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
				t.Fatal(err)
			}
			poll := l1.SeatCallPoll{SeatNodeRequest: l1.SeatNodeRequest{NodeID: "cold", BootSessionID: "boot"}, Room: room, HostEnabled: true, AnswerWait: time.Minute}
			if _, err := store.PollSeatCalls(t.Context(), "fabric-cold", poll); err != nil {
				t.Fatal(err)
			}
			if _, err := store.AnswerSeatApproval(t.Context(), "fabric-cold", id, l1.SeatApprovalAnswer{NodeID: "cold", BootSessionID: "boot", Approved: true}); err != nil {
				t.Fatal(err)
			}
			seat, err := store.AdvanceSeatCall(t.Context(), "fabric-cold", id, poll)
			if err != nil {
				t.Fatal(err)
			}
			hint, err := store.ClaimJobPoll(t.Context(), "fabric-cold", "cold", "boot", contract.JobClassService)
			if err != nil || !hint.QueuedOCIWork || hint.Claim != nil {
				t.Errorf("idle hint=%+v err=%v real observation=%+v", hint, err, observation)
			}
			computer, err := store.GetComputer(t.Context(), seat.ComputerID)
			if err != nil || computer.CurrentJob.Status != "host VM starting" || computer.CurrentJob.UnschedulableReason != "" {
				t.Fatalf("idle Computer=%+v err=%v", computer.CurrentJob, err)
			}
		})
	}
}
