package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

var version = "dev"

func main() {
	if processrunner.IsGuardianInvocation(os.Args) {
		if err := processrunner.RunGuardianInvocation(os.Args); err != nil {
			log.Printf("wefty-agent guardian: %v", err)
			os.Exit(1)
		}
		return
	}
	// Agent.Run's session supervisor absorbs protocol failures at their attempt
	// or node-session destination. Only a pre-payload startup failure or a local
	// invariant failure escapes to this process-level non-zero exit.
	if err := run(); err != nil {
		log.Printf("wefty-agent: %v", err)
		os.Exit(1)
	}
}

func run() error {
	agentExecutable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locate wefty-agent executable: %w", err)
	}
	var (
		fabricMode        = flag.String("fabric", "plain", "fabric implementation: plain or tsnet")
		controlPlane      = flag.String("control-plane", "wefty://control-plane", "control-plane Fabric address")
		runLedger         = flag.String("run-ledger", "wefty://run-ledger", "run-ledger Fabric address used by workflow jobs")
		nodeID            = flag.String("node-id", "", "stable operator-facing node ID")
		fabricIdentityID  = flag.String("plain-identity", "", "plain Fabric identity node ID (defaults to node-id)")
		fabricName        = flag.String("fabric-name", "", "tsnet logical node name")
		stateDirectory    = flag.String("state-dir", "", "tsnet state directory")
		authKey           = flag.String("auth-key", os.Getenv("TS_AUTHKEY"), "tsnet auth key")
		controlURL        = flag.String("control-url", os.Getenv("TS_CONTROL_URL"), "optional tsnet coordination URL")
		ephemeral         = flag.Bool("ephemeral", false, "register an ephemeral tsnet node")
		heartbeat         = flag.Duration("heartbeat-interval", agent.DefaultHeartbeatInterval, "node heartbeat interval")
		claim             = flag.Duration("claim-interval", agent.DefaultClaimInterval, "idle claim polling interval")
		renewal           = flag.Duration("renewal-interval", agent.DefaultRenewalInterval, "maximum attempt lease-renewal interval")
		logSpoolDirectory = flag.String("log-spool-dir", "", "durable log spool directory (defaults to the user cache directory)")
		logSpoolMaxBytes  = flag.Int64("log-spool-max-bytes", agent.DefaultLogSpoolMaxBytes, "maximum unacknowledged one-shot log payload bytes retained on disk (service logs use a 32 MiB ring)")
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
		RunLedgerAddress:    *runLedger,
		NodeID:              *nodeID,
		BootSessionID:       bootSessionID,
		Version:             version,
		Capabilities:        map[string]bool{"process": true},
		HeartbeatInterval:   *heartbeat,
		ClaimInterval:       *claim,
		RenewalInterval:     *renewal,
		LogSpoolDirectory:   *logSpoolDirectory,
		LogSpoolMaxBytes:    *logSpoolMaxBytes,
		GuardianExecutable:  agentExecutable,
		OutputSinkFactory: func(l1.Claim) processrunner.OutputSink {
			return processrunner.OutputSinkFunc(writeOutput)
		},
		Logf: log.Printf,
	})
	if err != nil {
		return err
	}
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-signals:
		}
		drainContext, stopDrain := context.WithTimeout(context.Background(), 30*time.Second)
		_, err := nodeAgent.Drain(drainContext)
		stopDrain()
		if err != nil {
			log.Printf("wefty-agent: graceful drain failed: %v", err)
			cancel()
			return
		}
		select {
		case <-ctx.Done():
		case <-signals:
			cancel()
		}
	}()
	err = nodeAgent.Run(ctx)
	cancel()
	return err
}

func writeOutput(_ context.Context, event contract.LogEvent) error {
	writer := os.Stdout
	if event.Stream == contract.LogStderr {
		writer = os.Stderr
	}
	_, err := writer.Write(event.Bytes)
	return err
}
