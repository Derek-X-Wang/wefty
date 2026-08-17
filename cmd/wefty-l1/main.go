package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/l1"
)

type nodeTagsFlag struct {
	policies map[string]l1.NodePolicy
}

func (values nodeTagsFlag) Set(value string) error {
	nodeID, tags, found := strings.Cut(value, "=")
	nodeID = strings.TrimSpace(nodeID)
	if !found || nodeID == "" {
		return errors.New("node tags must use node-id=tag,tag syntax")
	}
	var parsed []string
	if tags != "" {
		parsed = strings.Split(tags, ",")
	}
	policy := policyForNode(values.policies, nodeID)
	policy.Tags = parsed
	values.policies[nodeID] = policy
	return nil
}

func (nodeTagsFlag) String() string { return "node-id=tag,tag" }

type nodeSlotsFlag struct {
	policies map[string]l1.NodePolicy
	service  bool
}

func (values nodeSlotsFlag) Set(value string) error {
	nodeID, rawSlots, found := strings.Cut(value, "=")
	nodeID = strings.TrimSpace(nodeID)
	slots, err := strconv.Atoi(strings.TrimSpace(rawSlots))
	if !found || nodeID == "" || err != nil || slots < 0 {
		return errors.New("node slot limits must use node-id=non-negative-integer syntax")
	}
	policy := policyForNode(values.policies, nodeID)
	if values.service {
		policy.MaxServiceSlots = slots
	} else {
		policy.MaxOneshotSlots = slots
	}
	values.policies[nodeID] = policy
	return nil
}

func (values nodeSlotsFlag) String() string { return "node-id=slots" }

func policyForNode(policies map[string]l1.NodePolicy, nodeID string) l1.NodePolicy {
	policy, ok := policies[nodeID]
	if !ok {
		policy = l1.DefaultNodePolicy()
	}
	return policy
}

func main() {
	if err := run(); err != nil {
		log.Printf("wefty-l1: %v", err)
		os.Exit(1)
	}
}

func run() error {
	nodePolicies := make(map[string]l1.NodePolicy)
	var (
		fabricMode         = flag.String("fabric", "plain", "fabric implementation: plain or tsnet")
		listenAddress      = flag.String("listen", "wefty://control-plane", "control-plane Fabric listen address")
		databasePath       = flag.String("db", "wefty-l1.sqlite", "SQLite database path")
		leaseDuration      = flag.Duration("lease-duration", l1.DefaultLeaseDuration, "attempt lease duration")
		lateEvidenceWindow = flag.Duration("late-evidence-window", l1.DefaultLateEvidenceWindow, "window for post-authority evidence observations")
		fabricName         = flag.String("fabric-name", "wefty://control-plane", "tsnet logical service name")
		stateDirectory     = flag.String("state-dir", "", "tsnet state directory")
		authKey            = flag.String("auth-key", os.Getenv("TS_AUTHKEY"), "tsnet auth key")
		controlURL         = flag.String("control-url", os.Getenv("TS_CONTROL_URL"), "optional tsnet coordination URL")
		ephemeral          = flag.Bool("ephemeral", false, "register an ephemeral tsnet node")
		readyFile          = flag.String("ready-file", "", "write listener metadata after the server is ready")
	)
	flag.Var(nodeTagsFlag{policies: nodePolicies}, "node-tags", "authoritative routing tags as node-id=tag,tag (repeatable)")
	flag.Var(nodeSlotsFlag{policies: nodePolicies}, "node-max-oneshot-slots", "authoritative one-shot capacity as node-id=slots (repeatable)")
	flag.Var(nodeSlotsFlag{policies: nodePolicies, service: true}, "node-max-service-slots", "authoritative service capacity as node-id=slots (repeatable)")
	flag.Parse()

	participant, closeFabric, err := fabricconfig.Open(fabricconfig.Config{
		Mode:           *fabricMode,
		Identity:       fabric.Identity{NodeID: "control-plane"},
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
	store, err := l1.OpenStore(*databasePath, l1.StoreOptions{
		LeaseDuration:      *leaseDuration,
		LateEvidenceWindow: *lateEvidenceWindow,
	})
	if err != nil {
		return err
	}
	defer store.Close()
	server, err := l1.NewServer(participant, store, l1.ServerConfig{NodePolicies: nodePolicies})
	if err != nil {
		return err
	}
	listener, err := participant.Listen("tcp", *listenAddress)
	if err != nil {
		return err
	}
	if *readyFile != "" {
		metadata := struct {
			Address string `json:"address"`
		}{Address: listener.Addr().String()}
		payload, err := json.Marshal(metadata)
		if err != nil {
			return err
		}
		temporaryReadyFile := *readyFile + ".tmp"
		if err := os.WriteFile(temporaryReadyFile, payload, 0o600); err != nil {
			return fmt.Errorf("write ready file: %w", err)
		}
		if err := os.Rename(temporaryReadyFile, *readyFile); err != nil {
			return fmt.Errorf("publish ready file: %w", err)
		}
	}
	log.Printf("listening on %s", listener.Addr())
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return server.Serve(ctx, listener)
}
