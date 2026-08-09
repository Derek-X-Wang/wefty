package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		log.Printf("wefty-agent: %v", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		fabricMode       = flag.String("fabric", "plain", "fabric implementation: plain or tsnet")
		controlPlane     = flag.String("control-plane", "wefty://control-plane", "control-plane Fabric address")
		nodeID           = flag.String("node-id", "", "stable operator-facing node ID")
		fabricIdentityID = flag.String("plain-identity", "", "plain Fabric identity node ID (defaults to node-id)")
		fabricName       = flag.String("fabric-name", "", "tsnet logical node name")
		stateDirectory   = flag.String("state-dir", "", "tsnet state directory")
		authKey          = flag.String("auth-key", os.Getenv("TS_AUTHKEY"), "tsnet auth key")
		controlURL       = flag.String("control-url", os.Getenv("TS_CONTROL_URL"), "optional tsnet coordination URL")
		ephemeral        = flag.Bool("ephemeral", false, "register an ephemeral tsnet node")
		heartbeat        = flag.Duration("heartbeat-interval", agent.DefaultHeartbeatInterval, "node heartbeat interval")
		claim            = flag.Duration("claim-interval", agent.DefaultClaimInterval, "idle claim polling interval")
		renewal          = flag.Duration("renewal-interval", agent.DefaultRenewalInterval, "maximum attempt lease-renewal interval")
	)
	flag.Parse()
	if *nodeID == "" {
		return fmt.Errorf("--node-id is required")
	}
	identityID := *fabricIdentityID
	if identityID == "" {
		identityID = *nodeID
	}
	participant, closeFabric, err := fabricconfig.Open(fabricconfig.Config{
		Mode: *fabricMode,
		Identity: fabric.Identity{
			NodeID: identityID,
			Tags:   []string{l1.DefaultAgentPrincipalTag},
		},
		Name:           *fabricName,
		StateDirectory: *stateDirectory,
		AuthKey:        *authKey,
		ControlURL:     *controlURL,
		Ephemeral:      *ephemeral,
		Logf:           log.Printf,
	})
	if err != nil {
		return err
	}
	defer closeFabric()

	bootSessionID, err := agent.NewBootSessionID()
	if err != nil {
		return err
	}
	nodeAgent, err := agent.New(agent.Config{
		Fabric:              participant,
		ControlPlaneAddress: *controlPlane,
		NodeID:              *nodeID,
		BootSessionID:       bootSessionID,
		Version:             version,
		Capabilities:        map[string]bool{"process": true},
		HeartbeatInterval:   *heartbeat,
		ClaimInterval:       *claim,
		RenewalInterval:     *renewal,
		OutputSinkFactory: func(l1.Claim) processrunner.OutputSink {
			return processrunner.OutputSinkFunc(writeOutput)
		},
		Logf: log.Printf,
	})
	if err != nil {
		return err
	}
	defer nodeAgent.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return nodeAgent.Run(ctx)
}

func writeOutput(_ context.Context, event contract.LogEvent) error {
	writer := os.Stdout
	if event.Stream == contract.LogStderr {
		writer = os.Stderr
	}
	_, err := writer.Write(event.Bytes)
	return err
}
