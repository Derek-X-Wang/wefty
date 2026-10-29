package main

import (
	"bytes"
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
)

// #1331: exercise command startup, the real setup-state writer and L1
// registration together. Only VM preparation and the native dialog are fakes.
func TestFreshPerUserBootstrapRegistersSeatRequestsWithoutRestart(t *testing.T) {
	path := perUserProofTestConfig(t)
	var config limarunner.UserInstallConfig
	if err := limarunner.ReadUserInstallJSON(path, &config); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(config.SetupState); !os.IsNotExist(err) {
		t.Fatalf("fresh install already has setup state: %v", err)
	}
	network, err := plain.NewNetworkWithID("plain-1331-startup")
	if err != nil {
		t.Fatal(err)
	}
	control := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{Clock: roomStartupClock{}})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server, err := l1.NewServer(control, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{"fresh-node": l1.DefaultNodePolicy("test")}})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := control.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	serverCtx, stopServer := context.WithCancel(t.Context())
	served := make(chan error, 1)
	go func() { served <- server.Serve(serverCtx, listener) }()
	defer func() { stopServer(); <-served }()

	var logs bytes.Buffer
	oldArgs, oldFlags, oldLog := os.Args, flag.CommandLine, log.Writer()
	defer func() { os.Args, flag.CommandLine = oldArgs, oldFlags; log.SetOutput(oldLog) }()
	log.SetOutput(&logs)
	flag.CommandLine = flag.NewFlagSet("fresh-node", flag.ContinueOnError)
	os.Args = []string{"wefty-agent", "--profile=", "--fabric=plain", "--plain-fabric-id=plain-1331-startup", "--node-id=fresh-node", "--control-plane=" + listener.Addr().String(),
		"--oci-user-install=" + path, "--oci-setup-state=" + config.SetupState,
		"--computer-seat-memory-bytes=536870912", "--computer-dedicated-cap=0",
		"--managed-root=" + realTempDir(t), "--log-spool-dir=" + t.TempDir(), "--handoff-root=" + realTempDir(t)}
	// Generous: the window covers Fabric, bootstrap, agent start and the first registration.
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	prepares := 0
	done := make(chan error, 1)
	go func() {
		done <- runWithUserOCIPrepare(ctx, func(ctx context.Context, path, nodeID, bootID string) (bool, error) {
			return prepareInstalledUserOCI(ctx, path, nodeID, bootID, "test",
				func(context.Context, limarunner.UserInstallConfig, string, string) error { prepares++; return nil },
				agent.SeatApprovalDialogFunc(func(context.Context, agent.SeatApprovalPrompt) (agent.SeatApprovalChoice, error) {
					return agent.SeatApprovalApprove, nil
				}), nil)
		})
	}()
	var registered l1.Node
	// Poll for the first registration, including the broken capability set.
	// Do not wait for the expected capability: that would hide the regression.
	for ctx.Err() == nil {
		registered, err = store.GetNode(ctx, "fresh-node")
		if err == nil {
			break
		}
		select {
		case runErr := <-done:
			t.Fatalf("startup before registration: %v; logs: %s", runErr, &logs)
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	if runErr := <-done; runErr != nil {
		t.Fatalf("node shutdown: %v; logs: %s", runErr, &logs)
	}
	if err != nil || prepares != 1 {
		t.Fatalf("registration=%v bootstrap calls=%d; logs: %s", err, prepares, &logs)
	}
	if !registered.Capabilities[l1.CapabilitySeatRequests] {
		t.Fatalf("first registration lacks %s: %+v; logs: %s", l1.CapabilitySeatRequests, registered.Capabilities, &logs)
	}
	// 4 GiB host VM minus 1 GiB infrastructure = 3 GiB, or six 512 MiB Seats.
	if !strings.Contains(logs.String(), "event=computer_room_configured memory_bytes=3221225472 seat_memory_bytes=536870912 dedicated_cap=0 service_slots=6") {
		t.Fatalf("bootstrap room or node settings not applied: %s", &logs)
	}
}

type roomStartupClock struct{}

func (roomStartupClock) Now() time.Time { return time.Now() }

// The agent refuses symlinked managed roots; the macOS temp folder is one.
func realTempDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return dir
}
