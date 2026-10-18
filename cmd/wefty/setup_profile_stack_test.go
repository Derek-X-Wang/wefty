package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

// TestSetupProfileConnectsTheStackWithNoFlags is the in-process proof of the
// connection profile. Setup writes it into a temporary home; L1, L3 and the
// node agent take every endpoint, identity and admission rule from it, as the
// installed services do; and the CLI, with no flag and no WEFTY_* variable,
// finds them all, sees this machine's node ready, and runs a process job that
// the node claims.
func TestSetupProfileConnectsTheStackWithNoFlags(t *testing.T) {
	// A fresh shell: HOME is the owner's, and nothing else is set.
	for _, name := range []string{
		connprofile.EnvironmentVariable, "WEFTY_NODE_CONFIG", l1AddressEnv, l3AddressEnv,
		"WEFTY_DEV_PLAIN_FABRIC_ID", "WEFTY_DEV_PLAIN_USER_ID", "WEFTY_DEV_PLAIN_DEVICE_ID",
	} {
		t.Setenv(name, "")
	}
	// Every server stops, through the cleanup registered last, before the
	// stores and fabrics it uses close.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stack := bindProfileStack(t, nil)
	report, profile := stack.report, stack.profile
	l1Fabric, l3Fabric, l1Listener, l3Listener := stack.l1Fabric, stack.l3Fabric, stack.l1Listener, stack.l3Listener

	// L1, configured exactly as cmd/wefty-l1 configures it from the profile.
	l1Settings := profile.L1()
	l1Store, err := l1.OpenStore(filepath.Join(report.DataDir, "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l1Store.Close() })
	l1Server, err := l1.NewServer(l1Fabric, l1Store, l1.ServerConfig{
		NodePolicies: l1Settings.NodePolicies(), AllowSelfAssertedPersonIdentities: l1Settings.AllowPlainPersonIdentities,
	})
	if err != nil {
		t.Fatal(err)
	}

	// L3, as cmd/wefty-l3 configures it.
	l3Settings := profile.L3()
	l3Store, err := l3.OpenStore(filepath.Join(report.DataDir, "l3.sqlite"), l3.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l3Store.Close() })
	ledgerL1Client, err := l3.NewL1Client(l3Fabric, l3Settings.ControlPlane)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledgerL1Client.CloseIdleConnections)
	reconciler, err := l3.NewReconciler(l3Store, ledgerL1Client, l3.ReconcilerConfig{Interval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	l3Server, err := l3.NewServer(l3Fabric, l3Store, l3.ServerConfig{Reconciler: reconciler, Logs: ledgerL1Client})
	if err != nil {
		t.Fatal(err)
	}
	l1Done := serveTestServer(ctx, func() error { return l1Server.Serve(ctx, l1Listener) })
	l3Done := serveTestServer(ctx, func() error { return l3Server.Serve(ctx, l3Listener) })

	// The node agent, as cmd/wefty-agent configures it: its node ID, too,
	// comes from the profile.
	nodeSettings := profile.NodeAgent()
	if nodeSettings.NodeID != report.NodeID {
		t.Fatalf("profile node %q, setup chose %q", nodeSettings.NodeID, report.NodeID)
	}
	nodeFabric, closeNodeFabric, err := fabricconfig.Open(fabricconfig.Config{
		Mode: nodeSettings.Fabric, PlainFabricID: nodeSettings.PlainFabricID,
		Identity: fabric.Identity{NodeID: nodeSettings.NodeID, Tags: []string{l1.DefaultAgentPrincipalTag}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = closeNodeFabric() })
	nodeAgent, err := agent.New(agent.Config{
		Fabric: nodeFabric, ControlPlaneAddress: nodeSettings.ControlPlane, RunLedgerAddress: nodeSettings.RunLedger,
		NodeID: nodeSettings.NodeID, BootSessionID: "boot-setup-proof", Version: "setup-proof",
		Capabilities:      map[string]bool{"kind:process": true},
		HeartbeatInterval: 50 * time.Millisecond, ClaimInterval: 10 * time.Millisecond,
		RenewalInterval: 50 * time.Millisecond, LogFlushInterval: 5 * time.Millisecond,
		LogRetryInterval: 5 * time.Millisecond, LogSpoolDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nodeAgent.Close)
	agentDone := serveTestServer(ctx, func() error { return nodeAgent.Run(ctx) })
	t.Cleanup(func() {
		cancel()
		<-agentDone
		for name, done := range map[string]<-chan error{"L3": l3Done, "L1": l1Done} {
			if err := <-done; err != nil {
				t.Errorf("%s server: %v", name, err)
			}
		}
	})

	cli := func(args ...string) (string, error) {
		var stdout, stderr bytes.Buffer
		err := run(ctx, args, &stdout, &stderr)
		return stdout.String(), err
	}

	// `wefty status` with no flags: both services at the profile's addresses,
	// this machine's node admitted, alive and ready for work.
	var status clusterStatus
	deadline := time.Now().Add(30 * time.Second)
	for {
		output, err := cli("--json", "status")
		status = clusterStatus{}
		if decodeErr := json.Unmarshal([]byte(output), &status); decodeErr != nil {
			t.Fatalf("status --json: %v\n%s", decodeErr, output)
		}
		if err == nil && status.LocalNode != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the installed stack never became ready with no flags: %s (local node error %+v)", status.Verdict, status.LocalNodeError)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !status.Ready || len(status.Services) != 2 ||
		status.Services[0].Endpoint != profile.L1Address || status.Services[1].Endpoint != profile.L3Address {
		t.Fatalf("status = %+v", status)
	}
	if status.LocalNode.NodeID != report.NodeID || status.LocalNode.Tag != contract.StableNodeTagPrefix+report.NodeID {
		t.Fatalf("this machine = %+v, want node %q", status.LocalNode, report.NodeID)
	}
	if len(status.Nodes) != 1 || !status.Nodes[0].ThisMachine || !status.Nodes[0].ClaimsEnabled || !status.Nodes[0].AcceptsOneshot {
		t.Fatalf("status nodes = %+v; the installed node must be admitted for claims", status.Nodes)
	}

	// The person identity comes from the profile too, and L1 accepts it.
	output, err := cli("--json", "whoami")
	var person l1.AuthenticatedPerson
	if err != nil || json.Unmarshal([]byte(output), &person) != nil || person.UserID != "owner" || person.DeviceID != report.NodeID {
		t.Fatalf("whoami with no flags = %s err=%v", output, err)
	}

	// A process job Pinned to this machine's node: submitted, claimed and run.
	script := filepath.Join(t.TempDir(), "proof.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho claimed-by-the-installed-node\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	output, err = cli("--json", "submit", "--script", script, "--params", "{}",
		"--tag", status.LocalNode.Tag, "--idempotency-key", "setup-proof")
	if err != nil {
		t.Fatalf("submit with no flags: %v\n%s", err, output)
	}
	var accepted l3.RunAccepted
	if err := json.Unmarshal([]byte(output), &accepted); err != nil || accepted.RunID == "" {
		t.Fatalf("submit = %s err=%v", output, err)
	}
	if output, err := cli("wait", accepted.RunID, "--timeout", "60s"); err != nil {
		t.Fatalf("the installed node did not run the job: %v\n%s", err, output)
	}
	output, err = cli("--json", "inspect", accepted.RunID, "--execution")
	if err != nil {
		t.Fatal(err)
	}
	var inspection runInspection
	if err := json.Unmarshal([]byte(output), &inspection); err != nil {
		t.Fatal(err)
	}
	if inspection.Run.Status != contract.RunSucceeded || inspection.Execution == nil || inspection.Execution.Job == nil ||
		len(inspection.Execution.Job.Attempts) != 1 || inspection.Execution.Job.Attempts[0].NodeID != report.NodeID {
		t.Fatalf("inspection = %s", output)
	}
}

// distinctLoopbackAddresses holds count loopback ports at once, so no two are
// the same, and releases them together for the stack to bind. A port in avoid
// is held too but not returned, so the system cannot hand it out again. A
// port another process takes in the moment between is a bind conflict the
// caller retries.
func distinctLoopbackAddresses(t *testing.T, count int, avoid ...string) []string {
	t.Helper()
	listeners := make([]net.Listener, 0, count)
	addresses := make([]string, 0, count)
	for len(addresses) < count {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, listener)
		if address := listener.Addr().String(); !slices.Contains(avoid, address) {
			addresses = append(addresses, address)
		}
	}
	for _, listener := range listeners {
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return addresses
}

// profileStack is an installation whose L1 and L3 Fabrics have bound the
// profile's ports, with the record of every attempt it took.
type profileStack struct {
	fixture                *setupFixture
	report                 setupReport
	profile                connprofile.Profile
	l1Fabric, l3Fabric     fabric.Fabric
	l1Listener, l3Listener net.Listener
	attempts               []profileStackAttempt
}

type profileStackAttempt struct {
	addresses []string
	err       error
}

// bindProfileStack installs into a fresh home and binds L1 and L3 on the
// profile's ports, as the installed services do. The ports are reserved
// together so they differ; one taken by another process in the moment
// before the bind is a bind conflict, which releases what that attempt bound
// and installs again with fresh ports. occupy, when set, runs with each
// attempt's ports just before the bind.
func bindProfileStack(t *testing.T, occupy func(attempt int, addresses []string)) profileStack {
	t.Helper()
	var stack profileStack
	var used []string
	for attempt := 1; ; attempt++ {
		stack.fixture = newSetupFixture(t)
		// A retry's ports differ from every earlier attempt's, including one
		// the failed attempt bound and released.
		addresses := distinctLoopbackAddresses(t, 2, used...)
		used = append(used, addresses...)
		stack.fixture.host.l1Address, stack.fixture.host.l3Address = addresses[0], addresses[1]
		stack.report = stack.fixture.setup(t, setupRequest{})
		t.Setenv("HOME", stack.fixture.home)
		profilePath, err := connprofile.DefaultPath(stack.fixture.home)
		if err != nil {
			t.Fatal(err)
		}
		if stack.profile, _, err = connprofile.Resolve(profilePath, ""); err != nil {
			t.Fatal(err)
		}
		// L1 and L3 Fabrics, configured exactly as cmd/wefty-l1 and
		// cmd/wefty-l3 configure them from the profile.
		var closeL1Fabric, closeL3Fabric fabricconfig.CloseFunc
		stack.l1Fabric, closeL1Fabric, err = fabricconfig.Open(fabricconfig.Config{
			Mode: stack.profile.L1().Fabric, PlainFabricID: stack.profile.L1().PlainFabricID, Identity: fabric.Identity{NodeID: "control-plane"},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = closeL1Fabric() })
		stack.l3Fabric, closeL3Fabric, err = fabricconfig.Open(fabricconfig.Config{
			Mode: stack.profile.L3().Fabric, PlainFabricID: stack.profile.L3().PlainFabricID,
			Identity: fabric.Identity{NodeID: "run-ledger", Tags: []string{l1.DefaultClientPrincipalTag}},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = closeL3Fabric() })
		if occupy != nil {
			occupy(attempt, addresses)
		}
		stack.l1Listener, err = stack.l1Fabric.Listen("tcp", stack.profile.L1().Listen)
		if err == nil {
			stack.l3Listener, err = stack.l3Fabric.Listen("tcp", stack.profile.L3().Listen)
			if err != nil {
				_ = stack.l1Listener.Close()
				stack.l1Listener = nil
			}
		}
		stack.attempts = append(stack.attempts, profileStackAttempt{addresses: addresses, err: err})
		if err == nil {
			return stack
		}
		if !errors.Is(err, syscall.EADDRINUSE) || attempt == 3 {
			t.Fatalf("bind the profile's ports: %v", err)
		}
		t.Logf("attempt %d: a profile port was taken before the bind (%v); installing again with fresh ports", attempt, err)
	}
}

// A port taken between the reservation and the bind is retried with a fresh
// installation and fresh ports, and what the failed attempt had bound is
// released. The run ledger's port is the one taken here, after L1 has bound
// its own, so the release is observable.
func TestProfileStackRetriesABindConflictWithFreshPorts(t *testing.T) {
	for _, name := range []string{connprofile.EnvironmentVariable, "WEFTY_NODE_CONFIG", l1AddressEnv, l3AddressEnv} {
		t.Setenv(name, "")
	}
	var occupied net.Listener
	stack := bindProfileStack(t, func(attempt int, addresses []string) {
		if attempt != 1 {
			return
		}
		var err error
		if occupied, err = net.Listen("tcp", addresses[1]); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = occupied.Close() })
	})
	defer stack.l1Listener.Close()
	defer stack.l3Listener.Close()
	if len(stack.attempts) != 2 || !errors.Is(stack.attempts[0].err, syscall.EADDRINUSE) || stack.attempts[1].err != nil {
		t.Fatalf("attempts = %+v, want one bind conflict then a bind", stack.attempts)
	}
	first, second := stack.attempts[0].addresses, stack.attempts[1].addresses
	if slices.Contains(second, first[0]) || slices.Contains(second, first[1]) {
		t.Fatalf("the retry reused a port: first %v, second %v", first, second)
	}
	if stack.profile.L1Address != second[0] || stack.profile.L3Address != second[1] ||
		stack.l1Listener.Addr().String() != second[0] || stack.l3Listener.Addr().String() != second[1] {
		t.Fatalf("the stack is not on the retry's ports %v: profile %s/%s, bound %s/%s", second,
			stack.profile.L1Address, stack.profile.L3Address, stack.l1Listener.Addr(), stack.l3Listener.Addr())
	}
	// The failed attempt's L1 listener was released: its port binds again.
	released, err := net.Listen("tcp", first[0])
	if err != nil {
		t.Fatalf("the failed attempt still holds L1's port %s: %v", first[0], err)
	}
	_ = released.Close()
}
