package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	ocirunner "github.com/Derek-X-Wang/wefty/runner/oci"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

var version = "dev"

type repeatedStringFlag []string

func (values *repeatedStringFlag) String() string { return fmt.Sprint([]string(*values)) }
func (values *repeatedStringFlag) Set(value string) error {
	if value == "" {
		return errors.New("value must not be empty")
	}
	*values = append(*values, value)
	return nil
}

func main() {
	if ocihelper.IsLoggerInvocation(os.Args) {
		if err := ocihelper.RunLoggerInvocation(os.Args); err != nil {
			log.Printf("wefty-agent OCI logger: %v", err)
			os.Exit(1)
		}
		return
	}
	if ocihelper.IsInvocation(os.Args) {
		helperFlags := flag.NewFlagSet("__wefty_oci_helper", flag.ContinueOnError)
		containerdAddress := helperFlags.String("oci-containerd-address", ocihelper.DefaultContainerdAddress, "root helper containerd socket")
		containerdStateRoot := helperFlags.String("oci-containerd-state-root", "/run/containerd", "root helper containerd state root used for shim verification")
		runtimeRoot := helperFlags.String("oci-runtime-root", "/var/lib/wefty/oci", "root helper OCI runtime state root")
		var allowedMountRoots repeatedStringFlag
		helperFlags.Var(&allowedMountRoots, "oci-allowed-mount-root", "operator bind-mount root allowed by the helper (repeatable)")
		if err := helperFlags.Parse(os.Args[2:]); err != nil {
			log.Printf("wefty-agent OCI helper flags: %v", err)
			os.Exit(2)
		}
		helperContext, stopHelper := signal.NotifyContext(context.TODO(), os.Interrupt, syscall.SIGTERM)
		defer stopHelper()
		engine, closeEngine, err := ocihelper.OpenNativeEngine(ocihelper.NativeEngineConfig{Address: *containerdAddress, ContainerdStateRoot: *containerdStateRoot, RuntimeRoot: *runtimeRoot, AllowedMountRoots: allowedMountRoots})
		if err != nil {
			log.Printf("wefty-agent OCI helper engine: %v", err)
			os.Exit(1)
		}
		defer closeEngine.Close()
		allowedUIDs, err := ocihelper.AllowedPeerUIDs(os.Getenv(ocihelper.AllowedUIDsEnvironment), uint32(os.Getuid()))
		if err != nil {
			log.Printf("wefty-agent OCI helper peer allowlist: %v", err)
			os.Exit(1)
		}
		if err := ocihelper.RunInvocation(helperContext, os.Args[:2], engine, ocihelper.ServerConfig{
			HelperVersion: version, AllowedUIDs: allowedUIDs,
		}); err != nil {
			log.Printf("wefty-agent OCI helper: %v", err)
			os.Exit(1)
		}
		return
	}
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
	managedRootDefault, err := agent.DefaultManagedRootDirectory()
	if err != nil {
		return err
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
		finalization      = flag.Duration("finalization-timeout", agent.DefaultFinalizationTimeout, "maximum final log flush and upload duration after a payload exits")
		maxOneshotSlots   = flag.Int("max-oneshot-slots", l1.DefaultMaxOneshotSlots, "local ceiling for concurrent one-shot attempts")
		maxServiceSlots   = flag.Int("max-service-slots", l1.DefaultMaxServiceSlots, "local ceiling for concurrent service attempts")
		logSpoolDirectory = flag.String("log-spool-dir", "", "durable log spool directory (defaults to the user cache directory)")
		logSpoolMaxBytes  = flag.Int64("log-spool-max-bytes", agent.DefaultLogSpoolMaxBytes, "maximum unacknowledged one-shot log payload bytes retained on disk (service logs use a 32 MiB ring)")
		managedRoot       = flag.String("managed-root", managedRootDefault, "persistent state root for agent-managed service resources")
		handoffRoot       = flag.String("handoff-root", contract.DefaultHandoffRoot, "agent-managed one-shot handoff root")
		ociHelperSocket   = flag.String("oci-helper-socket", "", "private OCI helper Unix socket; empty disables OCI")
		ociHelperChecksum = flag.String("oci-helper-checksum", "", "expected OCI helper binary checksum")
		ociProbeImage     = flag.String("oci-probe-image", "", "preloaded local OCI probe image reference")
		ociProbeDigest    = flag.String("oci-probe-digest", "", "immutable digest of the preloaded local OCI probe image")
	)
	flag.Parse()
	if *nodeID == "" {
		return fmt.Errorf("--node-id is required")
	}
	if *ociHelperSocket != "" && *ociHelperChecksum == "" {
		return fmt.Errorf("--oci-helper-checksum is required with --oci-helper-socket")
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
	capabilities := map[string]bool{"kind:process": true}
	runtimes := make(map[string]workloadrunner.WorkloadRuntime)
	var bootBarrier *ocihelper.BootBarrier
	var capabilityProbe agent.CapabilityProbe
	var deadman agent.AttemptDeadmanRenewer
	if *ociHelperSocket != "" {
		if *ociProbeImage == "" || *ociProbeDigest == "" {
			return fmt.Errorf("--oci-probe-image and --oci-probe-digest are required with --oci-helper-socket")
		}
		client := ocihelper.NewUnixClient(*ociHelperSocket, *ociHelperChecksum)
		bootBarrier, err = ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{
			NodeID: *nodeID, BootSessionID: bootSessionID, ExpectedHelperChecksum: *ociHelperChecksum,
		})
		if err != nil {
			return err
		}
		adapter := ocirunner.NewAdapter(bootBarrier)
		runtimes[contract.JobKindOCI] = adapter
		capabilities["kind:oci"] = true
		capabilities["runtime_handler:"+ocihelper.DefaultRuntimeHandler] = true
		capabilities["cgroup_v2"] = true
		capabilityProbe = ociCapabilityProbe{
			adapter: adapter, nodeID: *nodeID, bootSessionID: bootSessionID,
			reference: *ociProbeImage, digest: *ociProbeDigest,
		}
		deadman = ociAttemptDeadman{barrier: bootBarrier, nodeID: *nodeID, bootSessionID: bootSessionID}
	}
	nodeAgent, err := agent.New(agent.Config{
		Fabric:               participant,
		ControlPlaneAddress:  *controlPlane,
		RunLedgerAddress:     *runLedger,
		NodeID:               *nodeID,
		BootSessionID:        bootSessionID,
		Version:              version,
		Capabilities:         capabilities,
		CapabilityProbe:      capabilityProbe,
		OCIBootBarrier:       bootBarrier,
		WorkloadRuntimes:     runtimes,
		AttemptDeadman:       deadman,
		HeartbeatInterval:    *heartbeat,
		ClaimInterval:        *claim,
		RenewalInterval:      *renewal,
		FinalizationTimeout:  *finalization,
		MaxOneshotSlots:      *maxOneshotSlots,
		MaxServiceSlots:      *maxServiceSlots,
		LogSpoolDirectory:    *logSpoolDirectory,
		LogSpoolMaxBytes:     *logSpoolMaxBytes,
		ManagedRootDirectory: *managedRoot,
		GuardianExecutable:   agentExecutable,
		HandoffRoot:          *handoffRoot,
		OutputSinkFactory: func(claim l1.Claim) processrunner.OutputSink {
			return newConsoleOutputSink(os.Stdout, os.Stderr, claim)
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

var consoleOutputMu sync.Mutex

func newConsoleOutputSink(stdout, stderr io.Writer, claim l1.Claim) processrunner.OutputSink {
	return processrunner.OutputSinkFunc(func(_ context.Context, event contract.LogEvent) error {
		writer := stdout
		if event.Stream == contract.LogStderr {
			writer = stderr
		}
		consoleOutputMu.Lock()
		defer consoleOutputMu.Unlock()
		if _, err := fmt.Fprintf(writer, "[job=%s attempt=%s stream=%s] ", claim.Job.JobID, claim.Lease.AttemptID, event.Stream); err != nil {
			return err
		}
		_, err := writer.Write(event.Bytes)
		return err
	})
}

type ociCapabilityProbe struct {
	adapter               *ocirunner.Adapter
	nodeID, bootSessionID string
	reference, digest     string
}

func (probe ociCapabilityProbe) Probe(ctx context.Context) (agent.CapabilityProbeResult, error) {
	if err := probe.adapter.Probe(ctx, probe.nodeID, probe.bootSessionID, probe.reference, probe.digest, l1.DefaultLeaseDuration); err != nil {
		return agent.CapabilityProbeResult{
			MissingCapabilities: []string{"kind:oci"}, ReasonCode: contract.CapabilityReasonProbeFailed,
		}, err
	}
	return agent.CapabilityProbeResult{Capabilities: map[string]bool{
		"kind:oci": true, "runtime_handler:" + ocihelper.DefaultRuntimeHandler: true, "cgroup_v2": true,
	}}, nil
}

type ociAttemptDeadman struct {
	barrier               *ocihelper.BootBarrier
	nodeID, bootSessionID string
}

func (renewer ociAttemptDeadman) QueueSuccessfulRenewal(claim l1.Claim, ttl time.Duration) error {
	session, err := renewer.barrier.Session()
	if err != nil {
		return err
	}
	return session.QueueAttemptRenewal(ocihelper.AttemptAuthority{
		NodeID: renewer.nodeID, BootSessionID: renewer.bootSessionID,
		JobID: claim.Job.JobID, AttemptID: claim.Lease.AttemptID, FencingToken: claim.Lease.FencingToken,
		Class: claim.Job.Spec.Class, RemovalGeneration: "attempt",
	}, ttl)
}
