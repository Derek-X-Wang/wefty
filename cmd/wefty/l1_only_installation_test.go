package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

// TestOnlyL1ConfiguredCLIDegradesCleanly is the ADR-0006 CLI contract: "The
// wefty CLI degrades cleanly when only --l1 is configured. An L1-only
// installation is a complete product, not half of one." --l3 defaults to
// l3.DefaultL3Address, so an L1-only installation opts in explicitly with
// an empty --l3 (--l3=). This test runs the real `run` entry point end to
// end with a live L1 server and an explicitly empty --l3, and checks both
// halves of that contract:
//   - an L1 command reaches L1 and succeeds;
//   - an L3 command (runs/workflows/lineage) fails with a short, clear
//     error naming the missing --l3 configuration, never a nil-pointer
//     panic or a dial to an empty address.
func TestOnlyL1ConfiguredCLIDegradesCleanly(t *testing.T) {
	network := plain.NewNetwork()
	controlFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})

	l1Store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l1Store.Close(); err != nil {
			t.Errorf("close L1 store: %v", err)
		}
	})
	l1Server, err := l1.NewServer(controlFabric, l1Store, l1.ServerConfig{
		AllowSelfAssertedPersonIdentities: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	// A literal loopback address (rather than a logical wefty:// name) so
	// this test's L1 listener is reachable from the fresh, unrelated plain
	// Fabric network that run() constructs internally via fabricconfig.Open.
	l1Listener, err := controlFabric.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l1Address := l1Listener.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	serverDone := serveTestServer(ctx, func() error { return l1Server.Serve(ctx, l1Listener) })
	t.Cleanup(func() {
		cancel()
		<-serverDone
	})

	// "--l3", "" is the explicit opt-in to an L1-only installation: --l3
	// defaults to l3.DefaultL3Address, so leaving it off entirely would
	// still point at that default logical address, not disable L3.
	baseArgs := []string{"--fabric", "plain", "--l1", l1Address, "--l3", ""}

	t.Run("L1 command reaches L1 and succeeds", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		err := run(ctx, append(append([]string{}, baseArgs...), "nodes", "list"), &stdout, &stderr)
		if err != nil {
			t.Fatalf("nodes list with only --l1 configured: %v (stderr=%s)", err, stderr.String())
		}
		if !strings.Contains(stdout.String(), "NODE ID") {
			t.Fatalf("nodes list stdout missing table header: %q", stdout.String())
		}
	})

	t.Run("L3 command fails with a clear, short error naming --l3", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		err := run(ctx, append(append([]string{}, baseArgs...), "runs", "list", "--origin", "computer:c1"), &stdout, &stderr)
		if err == nil {
			t.Fatal("runs list with no --l3 configured: want error, got nil")
		}
		message := err.Error()
		if !strings.Contains(message, "--l3") {
			t.Fatalf("error does not name the missing --l3 configuration: %q", message)
		}
		lowered := strings.ToLower(message)
		for _, symptom := range []string{"nil pointer", "invalid memory address", "panic"} {
			if strings.Contains(lowered, symptom) {
				t.Fatalf("error looks like a crash (%q) rather than a clean failure: %q", symptom, message)
			}
		}
		if stdout.Len() != 0 {
			t.Fatalf("runs list should not have produced output on failure: %q", stdout.String())
		}
	})

	t.Run("L3 command fails immediately without dialing", func(t *testing.T) {
		// A second, independent regression check on the same failure mode:
		// logs (also L3-only) must fail the same clear way, not hang or dial
		// an empty address.
		var stdout, stderr bytes.Buffer
		err := run(ctx, append(append([]string{}, baseArgs...), "logs", "some-run-id"), &stdout, &stderr)
		if err == nil {
			t.Fatal("logs with no --l3 configured: want error, got nil")
		}
		if !strings.Contains(err.Error(), "--l3") {
			t.Fatalf("logs error does not name the missing --l3 configuration: %q", err.Error())
		}
	})
}
