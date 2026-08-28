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
		fabricMode               = flag.String("fabric", "plain", "fabric implementation: plain or tsnet")
		listenAddress            = flag.String("listen", "wefty://control-plane", "control-plane Fabric listen address")
		databasePath             = flag.String("db", "wefty-l1.sqlite", "SQLite database path")
		leaseDuration            = flag.Duration("lease-duration", l1.DefaultLeaseDuration, "attempt lease duration")
		lateEvidenceWindow       = flag.Duration("late-evidence-window", l1.DefaultLateEvidenceWindow, "window for post-authority evidence observations")
		prestartBudget           = flag.Duration("prestart-infrastructure-budget", l1.DefaultPrestartInfrastructureBudget, "one job-level OCI pre-start retry budget")
		serviceStabilityWindow   = flag.Duration("service-stability-window", l1.DefaultServiceStabilityWindow, "continuous service stability required to reset the restart streak")
		serviceLogRetentionBytes = flag.Int64("service-log-retention-bytes", l1.DefaultServiceLogRetentionBytes, "maximum retained raw log payload per active service")
		serviceLogRetentionAge   = flag.Duration("service-log-retention-age", l1.DefaultServiceLogRetentionAge, "maximum retained service log age")
		fabricName               = flag.String("fabric-name", "wefty://control-plane", "tsnet logical service name")
		stateDirectory           = flag.String("state-dir", "", "tsnet state directory")
		authKey                  = flag.String("auth-key", os.Getenv("TS_AUTHKEY"), "tsnet auth key")
		controlURL               = flag.String("control-url", os.Getenv("TS_CONTROL_URL"), "optional tsnet coordination URL")
		ephemeral                = flag.Bool("ephemeral", false, "register an ephemeral tsnet node")
		readyFile                = flag.String("ready-file", "", "write listener metadata after the server is ready")
		initiateAdminBootstrap   = flag.Bool("initiate-admin-bootstrap", false, "create a short-lived local admin bootstrap challenge and exit")
		resetAdminPolicy         = flag.Bool("reset-admin-policy", false, "locally clear the admin roster, reopen bootstrap, audit the reset, and exit")
		allowPlainPersonIDs      = flag.Bool("allow-plain-person-identities", false, "DEVELOPMENT ONLY: allow self-asserted plain Fabric identities on person routes")
		runLedgerAddress         = flag.String("run-ledger", l1.DefaultRunLedgerAddress, "L3 run-ledger Fabric address")
		runLedgerNodeID          = flag.String("run-ledger-node-id", "run-ledger", "authenticated Fabric Node ID allowed to request Computer scope proofs")
	)
	flag.Var(nodeTagsFlag{policies: nodePolicies}, "node-tags", "authoritative routing tags as node-id=tag,tag (repeatable)")
	flag.Var(nodeSlotsFlag{policies: nodePolicies}, "node-max-oneshot-slots", "authoritative one-shot capacity as node-id=slots (repeatable)")
	flag.Var(nodeSlotsFlag{policies: nodePolicies, service: true}, "node-max-service-slots", "authoritative service capacity as node-id=slots (repeatable)")
	flag.Parse()
	storeOptions := l1.StoreOptions{
		LeaseDuration:                *leaseDuration,
		LateEvidenceWindow:           *lateEvidenceWindow,
		PrestartInfrastructureBudget: *prestartBudget,
		ServiceStabilityWindow:       *serviceStabilityWindow,
		ServiceLogRetentionBytes:     *serviceLogRetentionBytes,
		ServiceLogRetentionAge:       *serviceLogRetentionAge,
	}
	if *initiateAdminBootstrap && *resetAdminPolicy {
		return errors.New("-initiate-admin-bootstrap and -reset-admin-policy are mutually exclusive")
	}
	if *initiateAdminBootstrap || *resetAdminPolicy {
		store, err := l1.OpenStore(*databasePath, storeOptions)
		if err != nil {
			return err
		}
		defer store.Close()
		if *resetAdminPolicy {
			policy, err := store.ResetAdminPolicy(context.Background())
			if err != nil {
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(policy)
		}
		challenge, err := store.InitiateAdminBootstrap(context.Background())
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(challenge)
	}

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
	store, err := l1.OpenStore(*databasePath, storeOptions)
	if err != nil {
		return err
	}
	defer store.Close()
	computerTokenRevoker, err := l1.NewComputerTokenRevocationClient(participant, *runLedgerAddress)
	if err != nil {
		return err
	}
	defer computerTokenRevoker.CloseIdleConnections()
	server, err := l1.NewServer(participant, store, l1.ServerConfig{
		NodePolicies: nodePolicies, AllowSelfAssertedPersonIdentities: *allowPlainPersonIDs,
		ComputerTokenRevoker: computerTokenRevoker, RunLedgerNodeID: *runLedgerNodeID,
	})
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
