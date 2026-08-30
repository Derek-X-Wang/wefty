package main

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type capabilityProbeAdapterStub struct {
	err   error
	calls *int
}

type readyMainTestOCIBootBarrier struct{}

func (readyMainTestOCIBootBarrier) Ready() bool                  { return true }
func (readyMainTestOCIBootBarrier) Ensure(context.Context) error { return nil }
func (readyMainTestOCIBootBarrier) Generation() (ocihelper.HelperSession, bool) {
	return ocihelper.HelperSession{HelperInstanceID: "main-test", SessionGeneration: 1}, true
}
func (readyMainTestOCIBootBarrier) SweepReceipt() (ocihelper.VerifiedSweepReceipt, bool) {
	generation, _ := (readyMainTestOCIBootBarrier{}).Generation()
	return ocihelper.VerifiedSweepReceipt{SweepEpoch: "main-test", HelperSession: generation}, true
}
func (readyMainTestOCIBootBarrier) SetLossHandler(func(ocihelper.HelperSession, error)) {}
func (readyMainTestOCIBootBarrier) Close() error                                        { return nil }

type atomicCapabilityProbeAdapterStub struct{ calls atomic.Int32 }

func (stub *atomicCapabilityProbeAdapterStub) Probe(context.Context, string, string, string, string, time.Duration) error {
	stub.calls.Add(1)
	return nil
}

func (stub capabilityProbeAdapterStub) Probe(context.Context, string, string, string, string, time.Duration) error {
	if stub.calls != nil {
		*stub.calls++
	}
	return stub.err
}

func TestOCIProbePublishesComputerOnlyAfterExactHelperProbe(t *testing.T) {
	for _, test := range []struct {
		name         string
		probeErr     error
		wantComputer bool
		wantErr      bool
	}{
		{name: "supporting helper", wantComputer: true},
		{name: "lost helper", probeErr: errors.New("helper session lost"), wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			probe := ociCapabilityProbe{adapter: capabilityProbeAdapterStub{err: test.probeErr}}
			result, err := probe.Probe(t.Context())
			if (err != nil) != test.wantErr {
				t.Fatalf("probe error = %v, want error %t", err, test.wantErr)
			}
			if result.Capabilities["computer"] != test.wantComputer {
				t.Fatalf("computer capability = %t, want %t in %+v", result.Capabilities["computer"], test.wantComputer, result)
			}
			if test.wantErr && len(result.MissingCapabilities) != 1 {
				t.Fatalf("lost helper result = %+v", result)
			}
		})
	}
}

func TestOCIProbeFailsClosedBeforeAdapterWhenIntentIsUnavailable(t *testing.T) {
	for _, test := range []struct {
		name   string
		intent limarunner.IntentSource
	}{
		{name: "missing", intent: limarunner.FileIntentSource{Path: filepath.Join(t.TempDir(), "missing.json")}},
		{name: "disabled", intent: limarunner.IntentSourceFunc(func(context.Context) (limarunner.OCIIntent, error) {
			return limarunner.OCIIntent{Version: limarunner.OCIIntentVersion, Revision: 2, Enabled: false, UpdatedAt: time.Now()}, nil
		})},
		{name: "malformed", intent: limarunner.IntentSourceFunc(func(context.Context) (limarunner.OCIIntent, error) {
			return limarunner.OCIIntent{}, errors.New("invalid marker")
		})},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			probe := ociCapabilityProbe{adapter: capabilityProbeAdapterStub{calls: &calls}, intent: test.intent}
			result, err := probe.Probe(t.Context())
			if err == nil || calls != 0 || result.ReasonCode != contract.CapabilityReasonOCIIntentDisabled {
				t.Fatalf("result=%+v calls=%d err=%v", result, calls, err)
			}
		})
	}
}

func TestDurableOCIIntentGatesBackgroundRecoveryUntilControllerStart(t *testing.T) {
	intentPath := filepath.Join(t.TempDir(), "oci-intent.json")
	if _, err := limarunner.InitializeOCIIntent(intentPath, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := limarunner.SetOCIIntent(t.Context(), intentPath, 1, false, time.Now()); err != nil {
		t.Fatal(err)
	}

	network := plain.NewNetwork()
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{
		"intent-node": {MaxOneshotSlots: 1, MaxServiceSlots: 1},
	}})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	serverContext, cancelServer := context.WithCancel(t.Context())
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(serverContext, listener) }()
	defer func() {
		cancelServer()
		if err := <-serverDone; err != nil {
			t.Errorf("serve L1: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("close L1: %v", err)
		}
	}()

	probeAdapter := &atomicCapabilityProbeAdapterStub{}
	probe := ociCapabilityProbe{
		adapter: probeAdapter, nodeID: "intent-node", bootSessionID: "intent-boot",
		reference: "example.invalid/probe:v1", digest: "sha256:" + strings.Repeat("a", 64),
		intent: limarunner.FileIntentSource{Path: intentPath},
	}
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "intent-agent", Tags: []string{l1.DefaultAgentPrincipalTag}})
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := agent.New(agent.Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "intent-node", BootSessionID: "intent-boot", Version: "test", OS: "linux", Architecture: "amd64",
		Capabilities: map[string]bool{"kind:process": true, "kind:oci": true}, CapabilityProbe: probe,
		OCIBootBarrier: readyMainTestOCIBootBarrier{}, HeartbeatInterval: 10 * time.Millisecond, ClaimInterval: time.Second,
		ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	agentContext, cancelAgent := context.WithCancel(t.Context())
	agentDone := make(chan error, 1)
	go func() { agentDone <- nodeAgent.Run(agentContext) }()
	waitForMainTestNode(t, store, "intent-node", func(node l1.Node) bool {
		return !node.Capabilities["kind:oci"] && node.CapabilityReasonCode == contract.CapabilityReasonOCIIntentDisabled
	})

	convergence := make(chan error, 4)
	for range 4 {
		go func() { convergence <- nodeAgent.RecoverOCIRuntimeCapabilities(t.Context()) }()
	}
	for range 4 {
		if err := <-convergence; err == nil {
			cancelAgent()
			t.Fatal("background convergence recovered across durable disabled OCI intent")
		}
	}
	time.Sleep(50 * time.Millisecond)
	disabled := waitForMainTestNode(t, store, "intent-node", func(node l1.Node) bool {
		return !node.Capabilities["kind:oci"] && node.CapabilityReasonCode == contract.CapabilityReasonOCIIntentDisabled
	})
	if probeAdapter.calls.Load() != 0 {
		cancelAgent()
		t.Fatalf("disabled OCI intent reached adapter probe: node=%+v calls=%d", disabled, probeAdapter.calls.Load())
	}

	controller, err := ocicontrol.NewController(ocicontrol.ControllerConfig{IntentPath: intentPath, Runtime: nodeAgent})
	if err != nil {
		cancelAgent()
		t.Fatal(err)
	}
	started, err := controller.Start(t.Context(), ocicontrol.IntentMutationRequest{ExpectedRevision: 2})
	if err != nil || !started.Intent.Enabled || !started.CapabilityPublished {
		cancelAgent()
		t.Fatalf("explicit OCI start=%+v err=%v", started, err)
	}
	waitForMainTestNode(t, store, "intent-node", func(node l1.Node) bool { return node.Capabilities["kind:oci"] })
	if probeAdapter.calls.Load() == 0 {
		cancelAgent()
		t.Fatal("explicit OCI start did not reach adapter probe")
	}
	cancelAgent()
	if err := <-agentDone; err != nil {
		t.Fatalf("agent shutdown: %v", err)
	}
}

func waitForMainTestNode(t *testing.T, store *l1.Store, nodeID string, predicate func(l1.Node) bool) l1.Node {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		nodes, err := store.ListNodes(t.Context())
		if err == nil {
			for _, node := range nodes {
				if node.NodeID == nodeID && predicate(node) {
					return node
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
	nodes, _ := store.ListNodes(t.Context())
	t.Fatalf("node %q did not reach capability predicate: %+v", nodeID, nodes)
	return l1.Node{}
}

func TestConsoleOutputIsAttributedAndSerialized(t *testing.T) {
	assertConsoleOutputIsAttributedAndSerialized(t)
}

func assertConsoleOutputIsAttributedAndSerialized(t *testing.T) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	claimOne := l1.Claim{Job: l1.Job{JobID: "job-one"}, Lease: l1.AttemptLease{AttemptID: "attempt-one"}}
	claimTwo := l1.Claim{Job: l1.Job{JobID: "job-two"}, Lease: l1.AttemptLease{AttemptID: "attempt-two"}}
	sinkOne := newConsoleOutputSink(&stdout, &stderr, claimOne)
	sinkTwo := newConsoleOutputSink(&stdout, &stderr, claimTwo)
	done := make(chan error, 2)
	go func() {
		done <- sinkOne.WriteOutput(context.Background(), contract.LogEvent{Stream: contract.LogStdout, Bytes: []byte("alpha\n")})
	}()
	go func() {
		done <- sinkTwo.WriteOutput(context.Background(), contract.LogEvent{Stream: contract.LogStdout, Bytes: []byte("beta\n")})
	}()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	one := "[job=job-one attempt=attempt-one stream=stdout] alpha\n"
	two := "[job=job-two attempt=attempt-two stream=stdout] beta\n"
	if got := stdout.String(); got != one+two && got != two+one {
		t.Fatalf("concurrent console output = %q, want two intact attributed records", got)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}
