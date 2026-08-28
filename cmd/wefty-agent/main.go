package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
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
	if len(os.Args) > 1 && os.Args[1] == limarunner.BootstrapInvocationArg {
		if err := runMacBootstrap(os.Args[2:]); err != nil {
			log.Printf("wefty-agent Mac bootstrap: %v", err)
			os.Exit(1)
		}
		return
	}
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
		hostMountRoot := helperFlags.String("oci-lima-host-mount-root", "", "macOS operator mount root translated into the Lima guest")
		guestMountRoot := helperFlags.String("oci-lima-guest-mount-root", "", "Lima guest mount root corresponding to --oci-lima-host-mount-root")
		if err := helperFlags.Parse(os.Args[2:]); err != nil {
			log.Printf("wefty-agent OCI helper flags: %v", err)
			os.Exit(2)
		}
		helperContext, stopHelper := signal.NotifyContext(context.TODO(), os.Interrupt, syscall.SIGTERM)
		defer stopHelper()
		engine, closeEngine, err := ocihelper.OpenNativeEngine(ocihelper.NativeEngineConfig{
			Address: *containerdAddress, ContainerdStateRoot: *containerdStateRoot,
			RuntimeRoot: *runtimeRoot, AllowedMountRoots: allowedMountRoots,
			HostMountRoot: *hostMountRoot, GuestMountRoot: *guestMountRoot,
		})
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

func runMacBootstrap(arguments []string) error {
	if runtime.GOOS != "darwin" {
		return errors.New("Mac bootstrap is available only on macOS")
	}
	flags := flag.NewFlagSet(limarunner.BootstrapInvocationArg, flag.ContinueOnError)
	var agentArguments repeatedStringFlag
	flags.Var(&agentArguments, "agent-arg", "secret-free wefty-agent argument installed in the LaunchDaemon (repeatable)")
	operatorUser := flags.String("operator-user", "", "operator user that owns the agent and Lima instance")
	operatorHome := flags.String("operator-home", "", "absolute operator HOME")
	limaHome := flags.String("lima-home", "", "absolute operator LIMA_HOME")
	workingDirectory := flags.String("working-directory", "", "absolute agent working directory")
	agentPath := flags.String("agent-path", "", "absolute installed macOS wefty-agent path")
	linuxHelper := flags.String("linux-helper", "", "absolute matching Linux wefty-agent build")
	helperChecksum := flags.String("helper-checksum", "", "sha256 checksum of the Linux helper build")
	guestUser := flags.String("guest-user", "", "Lima guest user authorized for the helper socket")
	guestUID := flags.Uint("guest-uid", 0, "Lima guest UID authorized by the helper")
	probeArchive := flags.String("probe-archive", "", "absolute pinned probe OCI archive")
	probeReference := flags.String("probe-reference", "", "probe image reference")
	probeDigest := flags.String("probe-digest", "", "probe image top-level digest")
	nodeID := flags.String("node-id", "", "stable node ID used by the bootstrap probe")
	hostMountRoot := flags.String("host-mount-root", "", "absolute configured Lima operator mount root")
	instance := flags.String("lima-instance", limarunner.DefaultInstanceName, "Lima instance")
	limactl := flags.String("limactl", "limactl", "absolute or PATH-resolved limactl executable")
	factsPath := flags.String("minimal-doctor-facts", "", "absolute #128 facts JSON path")
	intentPath := flags.String("intent-file", "", "absolute read-only durable OCI intent file")
	stdoutPath := flags.String("stdout-path", "", "absolute LaunchDaemon stdout log path")
	stderrPath := flags.String("stderr-path", "", "absolute LaunchDaemon stderr log path")
	remove := flags.Bool("remove", false, "remove the interim Mac bootstrap idempotently and emit JSON evidence")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 || !*remove && (*guestUID == 0 || uint64(*guestUID) > uint64(^uint32(0))) {
		return errors.New("Mac bootstrap requires named flags and a positive --guest-uid")
	}
	if *intentPath == "" && filepath.IsAbs(*limaHome) {
		*intentPath = filepath.Join(*limaHome, "wefty-oci-intent.json")
	}
	if *remove {
		return removeMacBootstrap(*instance, *limactl, *factsPath, *intentPath)
	}
	for _, argument := range agentArguments {
		for _, reserved := range []string{
			"--node-id", "--oci-helper-socket", "--oci-helper-checksum", "--oci-probe-image", "--oci-probe-digest",
			"--oci-lima-instance", "--oci-lima-host-mount-root", "--oci-minimal-doctor-facts", "--oci-intent-file",
		} {
			if argument == reserved || strings.HasPrefix(argument, reserved+"=") {
				return fmt.Errorf("--agent-arg must not override bootstrap-owned %s", reserved)
			}
		}
	}
	helperSocket, err := limarunner.HelperSocketPath(*limaHome, *instance)
	if err != nil {
		return err
	}
	if !filepath.IsAbs(*factsPath) {
		return errors.New("--minimal-doctor-facts must be absolute")
	}
	if !filepath.IsAbs(*intentPath) {
		return errors.New("--intent-file must be absolute")
	}
	bootstrapID, err := agent.NewBootSessionID()
	if err != nil {
		return err
	}
	guestConfig := limarunner.GuestHelperInstallConfig{
		Instance: *instance, Limactl: *limactl, GuestUser: *guestUser, GuestUID: uint32(*guestUID),
		HelperBinary: *linuxHelper, ExpectedVersion: version, ExpectedChecksum: *helperChecksum,
		HostMountRoot: *hostMountRoot, HelperSocket: helperSocket,
		ProbeArchive: *probeArchive, ProbeReference: *probeReference, ProbeDigest: *probeDigest,
		NodeID: *nodeID, BootSessionID: bootstrapID,
	}
	if err := limarunner.ValidateGuestHelperInstall(guestConfig); err != nil {
		return err
	}
	installedArguments := append([]string(nil), agentArguments...)
	installedArguments = append(installedArguments,
		"--node-id="+*nodeID,
		"--oci-helper-socket="+helperSocket,
		"--oci-helper-checksum="+*helperChecksum,
		"--oci-probe-image="+*probeReference,
		"--oci-probe-digest="+*probeDigest,
		"--oci-lima-instance="+*instance,
		"--oci-lima-host-mount-root="+*hostMountRoot,
		"--oci-minimal-doctor-facts="+*factsPath,
		"--oci-intent-file="+*intentPath,
	)
	launchConfig := limarunner.LaunchDaemonConfig{
		AgentPath: *agentPath, Arguments: installedArguments, OperatorUser: *operatorUser,
		Home: *operatorHome, LimaHome: *limaHome, PATH: limarunner.DefaultLaunchPATH,
		WorkingDirectory: *workingDirectory, StandardOutPath: *stdoutPath, StandardErrorPath: *stderrPath,
	}
	if err := limarunner.ValidateLaunchDaemonInstall(launchConfig); err != nil {
		return err
	}
	if _, err := limarunner.InitializeOCIIntent(*intentPath, time.Now()); err != nil {
		return err
	}
	supervisor, err := limarunner.NewSupervisor(limarunner.SupervisorConfig{
		Instance: *instance, Limactl: *limactl, Intent: limarunner.FileIntentSource{Path: *intentPath},
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	if err := supervisor.Ensure(ctx); err != nil {
		return fmt.Errorf("prepare Lima for bootstrap: %w", err)
	}
	if err := limarunner.InstallGuestHelper(ctx, guestConfig); err != nil {
		return err
	}
	return limarunner.InstallLaunchDaemon(ctx, launchConfig)
}

type macBootstrapRemovalEvidence struct {
	Unit         limarunner.LaunchDaemonRemovalEvidence `json:"unit"`
	GuestHelper  limarunner.GuestHelperRemovalEvidence  `json:"guest_helper"`
	FactsAbsent  bool                                   `json:"facts_absent"`
	IntentAbsent bool                                   `json:"intent_absent"`
}

func removeMacBootstrap(instance, limactl, factsPath, intentPath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	evidence := macBootstrapRemovalEvidence{}
	unit, unitErr := limarunner.RemoveLaunchDaemon(ctx)
	evidence.Unit = unit
	helper, helperErr := limarunner.RemoveGuestHelper(ctx, limarunner.GuestHelperRemovalConfig{Instance: instance, Limactl: limactl})
	evidence.GuestHelper = helper
	removeLocal := func(path string) (bool, error) {
		if path == "" {
			return true, nil
		}
		if !filepath.IsAbs(path) {
			return false, errors.New("bootstrap removal paths must be absolute")
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		_, err := os.Stat(path)
		return errors.Is(err, os.ErrNotExist), nil
	}
	var factsErr, intentErr error
	evidence.FactsAbsent, factsErr = removeLocal(factsPath)
	evidence.IntentAbsent, intentErr = removeLocal(intentPath)
	encodeErr := json.NewEncoder(os.Stdout).Encode(evidence)
	return errors.Join(unitErr, helperErr, factsErr, intentErr, encodeErr)
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
		fabricMode            = flag.String("fabric", "plain", "fabric implementation: plain or tsnet")
		controlPlane          = flag.String("control-plane", "wefty://control-plane", "control-plane Fabric address")
		runLedger             = flag.String("run-ledger", "wefty://run-ledger", "run-ledger Fabric address used by workflow jobs")
		nodeID                = flag.String("node-id", "", "stable operator-facing node ID")
		fabricIdentityID      = flag.String("plain-identity", "", "plain Fabric identity node ID (defaults to node-id)")
		fabricName            = flag.String("fabric-name", "", "tsnet logical node name")
		stateDirectory        = flag.String("state-dir", "", "tsnet state directory")
		authKey               = flag.String("auth-key", os.Getenv("TS_AUTHKEY"), "tsnet auth key")
		controlURL            = flag.String("control-url", os.Getenv("TS_CONTROL_URL"), "optional tsnet coordination URL")
		ephemeral             = flag.Bool("ephemeral", false, "register an ephemeral tsnet node")
		heartbeat             = flag.Duration("heartbeat-interval", agent.DefaultHeartbeatInterval, "node heartbeat interval")
		claim                 = flag.Duration("claim-interval", agent.DefaultClaimInterval, "idle claim polling interval")
		renewal               = flag.Duration("renewal-interval", agent.DefaultRenewalInterval, "maximum attempt lease-renewal interval")
		finalization          = flag.Duration("finalization-timeout", agent.DefaultFinalizationTimeout, "maximum final log flush and upload duration after a payload exits")
		maxOneshotSlots       = flag.Int("max-oneshot-slots", l1.DefaultMaxOneshotSlots, "local ceiling for concurrent one-shot attempts")
		maxServiceSlots       = flag.Int("max-service-slots", l1.DefaultMaxServiceSlots, "local ceiling for concurrent service attempts")
		logSpoolDirectory     = flag.String("log-spool-dir", "", "durable log spool directory (defaults to the user cache directory)")
		logSpoolMaxBytes      = flag.Int64("log-spool-max-bytes", agent.DefaultLogSpoolMaxBytes, "maximum unacknowledged one-shot log payload bytes retained on disk (service logs use a 32 MiB ring)")
		managedRoot           = flag.String("managed-root", managedRootDefault, "persistent state root for agent-managed service resources")
		handoffRoot           = flag.String("handoff-root", contract.DefaultHandoffRoot, "agent-managed one-shot handoff root")
		ociHelperSocket       = flag.String("oci-helper-socket", "", "private OCI helper Unix socket; empty disables OCI")
		ociHelperChecksum     = flag.String("oci-helper-checksum", "", "expected OCI helper binary checksum")
		ociProbeImage         = flag.String("oci-probe-image", "", "preloaded local OCI probe image reference")
		ociProbeDigest        = flag.String("oci-probe-digest", "", "immutable digest of the preloaded local OCI probe image")
		ociImageBudget        = flag.Duration("oci-image-budget", ocirunner.DefaultImageBudget, "total resolve, pull/import, unpack, and shared-operation wait budget")
		ociImageCacheMaxBytes = flag.Int64("oci-image-cache-max-bytes", ocihelper.DefaultImageCacheMaxBytes, "maximum containerd content-store bytes retained in the wefty namespace")
		ociLimaInstance       = flag.String("oci-lima-instance", limarunner.DefaultInstanceName, "Lima instance carrying the private OCI helper on macOS")
		ociLimaMountRoot      = flag.String("oci-lima-host-mount-root", "", "macOS host mount root validated before Lima helper RPCs")
		ociDoctorFacts        = flag.String("oci-minimal-doctor-facts", "", "absolute path for the bounded Mac bootstrap facts JSON")
		ociIntentFile         = flag.String("oci-intent-file", "", "absolute read-only durable OCI intent file; missing disables OCI")
	)
	flag.Parse()
	if *nodeID == "" {
		return fmt.Errorf("--node-id is required")
	}
	if *ociHelperSocket != "" && *ociHelperChecksum == "" {
		return fmt.Errorf("--oci-helper-checksum is required with --oci-helper-socket")
	}
	if *ociImageBudget <= 0 {
		return fmt.Errorf("--oci-image-budget must be positive")
	}
	if *ociIntentFile != "" && !filepath.IsAbs(*ociIntentFile) {
		return errors.New("--oci-intent-file must be absolute when set")
	}
	if *ociImageCacheMaxBytes <= 0 {
		return fmt.Errorf("--oci-image-cache-max-bytes must be positive")
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
	var agentBootBarrier agent.OCIBootBarrier
	var capabilityProbe agent.CapabilityProbe
	var deadman agent.AttemptDeadmanRenewer
	var ociBridgeBinder workloadrunner.WorkflowBridgeBinder
	var limaSupervisor *limarunner.Supervisor
	var supervisedBootBarrier *limarunner.SupervisedBootBarrier
	if *ociHelperSocket != "" {
		if *ociProbeImage == "" || *ociProbeDigest == "" {
			return fmt.Errorf("--oci-probe-image and --oci-probe-digest are required with --oci-helper-socket")
		}
		if runtime.GOOS == "darwin" && *ociLimaMountRoot == "" {
			return errors.New("--oci-lima-host-mount-root is required with the Lima OCI helper")
		}
		client := ocihelper.NewUnixClient(*ociHelperSocket, *ociHelperChecksum)
		var limaDialer *limarunner.EpochSocketDialer
		if runtime.GOOS == "darwin" {
			client, limaDialer = limarunner.NewHelperClient(*ociHelperSocket, *ociHelperChecksum)
		}
		bootBarrier, err = ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{
			NodeID: *nodeID, BootSessionID: bootSessionID, ExpectedHelperChecksum: *ociHelperChecksum,
		})
		if err != nil {
			return err
		}
		agentBootBarrier = bootBarrier
		if runtime.GOOS == "darwin" {
			limaSupervisor, err = limarunner.NewSupervisor(limarunner.SupervisorConfig{
				Instance: *ociLimaInstance,
				Intent:   limarunner.FileIntentSource{Path: *ociIntentFile},
			})
			if err != nil {
				return err
			}
			supervisedBootBarrier = &limarunner.SupervisedBootBarrier{Supervisor: limaSupervisor, Barrier: bootBarrier}
			agentBootBarrier = supervisedBootBarrier
		}
		var adapterOptions []ocirunner.Option
		adapterOptions = append(adapterOptions, ocirunner.WithImageCache(*ociImageCacheMaxBytes, *ociProbeDigest))
		if runtime.GOOS == "darwin" {
			adapterOptions = append(adapterOptions, ocirunner.WithHostMountRoot(*ociLimaMountRoot))
		}
		adapter := ocirunner.NewAdapterWithPolicy(bootBarrier, ocirunner.ImagePolicy{Budget: *ociImageBudget}, adapterOptions...)
		if runtime.GOOS == "darwin" {
			binder := limarunner.NewBridgeBinder(*ociLimaInstance)
			binder.Transport = limaDialer
			binder.Epoch = bootBarrier
			ociBridgeBinder = binder
		}
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
		Fabric:                  participant,
		ControlPlaneAddress:     *controlPlane,
		RunLedgerAddress:        *runLedger,
		NodeID:                  *nodeID,
		BootSessionID:           bootSessionID,
		Version:                 version,
		Capabilities:            capabilities,
		CapabilityProbe:         capabilityProbe,
		OCIBootBarrier:          agentBootBarrier,
		WorkloadRuntimes:        runtimes,
		AttemptDeadman:          deadman,
		OCIWorkflowBridgeBinder: ociBridgeBinder,
		HeartbeatInterval:       *heartbeat,
		ClaimInterval:           *claim,
		RenewalInterval:         *renewal,
		FinalizationTimeout:     *finalization,
		MaxOneshotSlots:         *maxOneshotSlots,
		MaxServiceSlots:         *maxServiceSlots,
		LogSpoolDirectory:       *logSpoolDirectory,
		LogSpoolMaxBytes:        *logSpoolMaxBytes,
		ManagedRootDirectory:    *managedRoot,
		GuardianExecutable:      agentExecutable,
		HandoffRoot:             *handoffRoot,
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
	if supervisedBootBarrier != nil {
		go supervisedBootBarrier.Run(ctx, nodeAgent.RecoverOCIRuntimeCapabilities)
	}
	if *ociDoctorFacts != "" {
		if runtime.GOOS != "darwin" || limaSupervisor == nil || bootBarrier == nil {
			return errors.New("--oci-minimal-doctor-facts requires the macOS Lima OCI helper")
		}
		if !filepath.IsAbs(*ociDoctorFacts) {
			return errors.New("--oci-minimal-doctor-facts must be absolute")
		}
		go writeMinimalDoctorFactsLoop(ctx, *ociDoctorFacts, nodeAgent, limaSupervisor, bootBarrier, log.Printf)
	}
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

func writeMinimalDoctorFactsLoop(
	ctx context.Context,
	path string,
	nodeAgent *agent.Agent,
	supervisor *limarunner.Supervisor,
	barrier *ocihelper.BootBarrier,
	logf func(string, ...any),
) {
	write := func() {
		var handshake *ocihelper.AcquireSessionResponse
		if session, err := barrier.Session(); err == nil {
			value := session.Handshake()
			handshake = &value
		}
		snapshot := nodeAgent.CapabilitySnapshot()
		unitState := limarunner.UnitStateUnmanaged
		if os.Getenv("WEFTY_LAUNCH_UNIT") == limarunner.LaunchDaemonLabel {
			unitState = limarunner.UnitStateLaunchedByUnit
		}
		facts := limarunner.BuildMinimalDoctorFacts(
			unitState, supervisor.Facts(), handshake, snapshot.CapabilityObservation,
			snapshot.LastProbeAt, time.Now(),
		)
		if err := limarunner.WriteMinimalDoctorFacts(path, facts); err != nil && logf != nil {
			logf("wefty-agent: write minimal doctor facts: %v", err)
		}
	}
	write()
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			write()
			return
		case <-ticker.C:
			write()
		}
	}
}

func (renewer ociAttemptDeadman) QueueSuccessfulRenewal(claim l1.Claim, ttl time.Duration) error {
	session, err := renewer.barrier.Session()
	if err != nil {
		return err
	}
	removalGeneration := "attempt"
	if claim.Job.Spec.Class == contract.JobClassService {
		removalGeneration = fmt.Sprint(l1.InitialServiceRemovalGeneration)
	}
	return session.QueueAttemptRenewal(ocihelper.AttemptAuthority{
		NodeID: renewer.nodeID, BootSessionID: renewer.bootSessionID,
		JobID: claim.Job.JobID, AttemptID: claim.Lease.AttemptID, FencingToken: claim.Lease.FencingToken,
		Class: claim.Job.Spec.Class, RemovalGeneration: removalGeneration,
	}, ttl)
}
