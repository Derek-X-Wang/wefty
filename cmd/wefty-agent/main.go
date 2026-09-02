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
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
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
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

const gracefulDrainTimeout = 30 * time.Second

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
		runcExecutable := helperFlags.String("oci-runc-executable", "", "absolute setup-resolved runc executable; empty uses containerd runtime info")
		memoryCapacityBytes := helperFlags.Int64("oci-memory-capacity-bytes", 0, "setup-configured Computer memory ceiling; zero records an unknown ceiling")
		memoryReserveBytes := helperFlags.Int64("oci-memory-reserve-bytes", 0, "setup-configured infrastructure memory reserve")
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
			RuntimeRoot: *runtimeRoot, RuncExecutable: *runcExecutable, AllowedMountRoots: allowedMountRoots,
			HostMountRoot: *hostMountRoot, GuestMountRoot: *guestMountRoot,
			MemoryCapacityBytes: *memoryCapacityBytes, MemoryReserveBytes: *memoryReserveBytes,
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
	controlSocket := flags.String("control-socket", "", "absolute operator-only OCI control socket")
	nodeConfig := flags.String("node-config", "", "absolute installed node configuration for singular CLI commands")
	setupState := flags.String("setup-state", "", "absolute durable OCI setup convergence state")
	memoryCapacityBytes := flags.Int64("memory-capacity-bytes", 0, "setup-converged Computer memory ceiling in bytes; zero uses 4 GiB")
	memoryReserveBytes := flags.Int64("memory-reserve-bytes", 0, "setup-converged Lima infrastructure reserve in bytes; zero derives 25 percent of VM memory")
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
	if *controlSocket == "" && filepath.IsAbs(*limaHome) {
		*controlSocket = filepath.Join(*limaHome, "wefty-control.sock")
	}
	if *nodeConfig == "" && filepath.IsAbs(*operatorHome) {
		*nodeConfig, _ = ocicontrol.DefaultInstalledConfigPath(*operatorHome)
	}
	if *setupState == "" && filepath.IsAbs(*limaHome) {
		*setupState = filepath.Join(*limaHome, "wefty-oci-setup.json")
	}
	if *remove {
		return removeMacBootstrap(*instance, *limactl, *factsPath, *intentPath, *controlSocket, *nodeConfig, *setupState)
	}
	for _, argument := range agentArguments {
		for _, reserved := range []string{
			"--node-id", "--oci-helper-socket", "--oci-helper-checksum", "--oci-probe-image", "--oci-probe-digest",
			"--oci-lima-instance", "--oci-lima-host-mount-root", "--oci-minimal-doctor-facts", "--oci-intent-file",
			"--oci-control-socket", "--oci-probe-archive", "--oci-memory-capacity-bytes", "--oci-memory-reserve-bytes",
			"--oci-setup-state",
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
	if !filepath.IsAbs(*controlSocket) || !filepath.IsAbs(*nodeConfig) || !filepath.IsAbs(*setupState) {
		return errors.New("--control-socket, --node-config, and --setup-state must be absolute")
	}
	operator, err := user.Lookup(*operatorUser)
	if err != nil {
		return fmt.Errorf("resolve operator for installed node configuration: %w", err)
	}
	operatorUID, uidErr := strconv.Atoi(operator.Uid)
	operatorGID, gidErr := strconv.Atoi(operator.Gid)
	if uidErr != nil || gidErr != nil {
		return errors.New("operator UID and GID must be numeric")
	}
	bootstrapID, err := agent.NewBootSessionID()
	if err != nil {
		return err
	}
	defaultSizing, sizingErr := limarunner.HostDefaultSizing()
	if sizingErr != nil {
		return fmt.Errorf("resolve default Lima sizing for capacity setup: %w", sizingErr)
	}
	defaultCapacityBytes, defaultReserveBytes, err := limarunner.DefaultMacComputerCapacity(defaultSizing)
	if err != nil {
		return fmt.Errorf("resolve Computer capacity from default Lima sizing: %w", err)
	}
	if *memoryCapacityBytes == 0 {
		*memoryCapacityBytes = defaultCapacityBytes
	}
	configuredReserveBytes := *memoryReserveBytes
	if configuredReserveBytes == 0 {
		configuredReserveBytes = defaultReserveBytes
	}
	guestConfig := limarunner.GuestHelperInstallConfig{
		Instance: *instance, Limactl: *limactl, GuestUser: *guestUser, GuestUID: uint32(*guestUID),
		HelperBinary: *linuxHelper, ExpectedVersion: version, ExpectedChecksum: *helperChecksum,
		HostMountRoot: *hostMountRoot, HelperSocket: helperSocket,
		ProbeArchive: *probeArchive, ProbeReference: *probeReference, ProbeDigest: *probeDigest,
		NodeID: *nodeID, BootSessionID: bootstrapID,
		MemoryCapacityBytes: *memoryCapacityBytes, MemoryReserveBytes: configuredReserveBytes,
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
		"--oci-control-socket="+*controlSocket,
		"--oci-probe-archive="+*probeArchive,
		"--oci-setup-state="+*setupState,
		"--oci-memory-capacity-bytes="+strconv.FormatInt(*memoryCapacityBytes, 10),
		"--oci-memory-reserve-bytes="+strconv.FormatInt(configuredReserveBytes, 10),
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
	if err := limarunner.InstallLaunchDaemon(ctx, launchConfig); err != nil {
		return err
	}
	if err := ocicontrol.WriteInstalledConfig(*nodeConfig, ocicontrol.InstalledConfig{
		Version: ocicontrol.InstalledConfigVersion, ControlSocket: *controlSocket,
	}); err != nil {
		return err
	}
	if err := os.Chown(filepath.Dir(*nodeConfig), operatorUID, operatorGID); err != nil {
		return fmt.Errorf("set installed node configuration directory owner: %w", err)
	}
	if err := os.Chown(filepath.Dir(filepath.Dir(*nodeConfig)), operatorUID, operatorGID); err != nil {
		return fmt.Errorf("set installed node configuration parent owner: %w", err)
	}
	if err := os.Chown(*nodeConfig, operatorUID, operatorGID); err != nil {
		return fmt.Errorf("set installed node configuration owner: %w", err)
	}
	return nil
}

type macBootstrapRemovalEvidence struct {
	Unit                limarunner.LaunchDaemonRemovalEvidence `json:"unit"`
	GuestHelper         limarunner.GuestHelperRemovalEvidence  `json:"guest_helper"`
	FactsAbsent         bool                                   `json:"facts_absent"`
	IntentAbsent        bool                                   `json:"intent_absent"`
	ControlSocketAbsent bool                                   `json:"control_socket_absent"`
	NodeConfigAbsent    bool                                   `json:"node_config_absent"`
	SetupStateAbsent    bool                                   `json:"setup_state_absent"`
	DesiredSetupAbsent  bool                                   `json:"desired_setup_state_absent"`
}

func removeMacBootstrap(instance, limactl, factsPath, intentPath, controlSocket, nodeConfig, setupState string) error {
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
	var factsErr, intentErr, socketErr, configErr, setupErr, desiredSetupErr error
	evidence.FactsAbsent, factsErr = removeLocal(factsPath)
	evidence.IntentAbsent, intentErr = removeLocal(intentPath)
	evidence.ControlSocketAbsent, socketErr = removeLocal(controlSocket)
	evidence.NodeConfigAbsent, configErr = removeLocal(nodeConfig)
	evidence.SetupStateAbsent, setupErr = removeLocal(setupState)
	evidence.DesiredSetupAbsent, desiredSetupErr = removeLocal(ocicontrol.DesiredSetupStatePath(setupState))
	encodeErr := json.NewEncoder(os.Stdout).Encode(evidence)
	return errors.Join(unitErr, helperErr, factsErr, intentErr, socketErr, configErr, setupErr, desiredSetupErr, encodeErr)
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
		plainFabricID         = flag.String("plain-fabric-id", os.Getenv("WEFTY_DEV_PLAIN_FABRIC_ID"), "DEVELOPMENT ONLY: shared plain Fabric ID (must start with plain-)")
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
		ociControlSocket      = flag.String("oci-control-socket", "", "operator-only node-local OCI control socket")
		ociProbeArchive       = flag.String("oci-probe-archive", "", "absolute immutable OCI archive used by setup-oci")
		ociSetupState         = flag.String("oci-setup-state", "", "absolute durable OCI setup convergence state")
		ociMemoryCapacity     = flag.Int64("oci-memory-capacity-bytes", 0, "setup-configured Computer memory ceiling used by the private helper")
		ociMemoryReserve      = flag.Int64("oci-memory-reserve-bytes", 0, "setup-configured infrastructure memory reserve used by the private helper")
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
	if *ociControlSocket != "" && !filepath.IsAbs(*ociControlSocket) {
		return errors.New("--oci-control-socket must be absolute when set")
	}
	if *ociProbeArchive != "" && !filepath.IsAbs(*ociProbeArchive) {
		return errors.New("--oci-probe-archive must be absolute when set")
	}
	if *ociSetupState != "" && !filepath.IsAbs(*ociSetupState) {
		return errors.New("--oci-setup-state must be absolute when set")
	}
	if *ociMemoryCapacity < 0 || *ociMemoryReserve < 0 {
		return errors.New("--oci-memory-capacity-bytes and --oci-memory-reserve-bytes must be nonnegative")
	}
	if *ociImageCacheMaxBytes <= 0 {
		return fmt.Errorf("--oci-image-cache-max-bytes must be positive")
	}
	identityID := *fabricIdentityID
	if identityID == "" {
		identityID = *nodeID
	}
	participant, closeFabric, err := fabricconfig.Open(fabricconfig.Config{
		Mode:          *fabricMode,
		PlainFabricID: *plainFabricID,
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
	var ociAdapter *ocirunner.Adapter
	if *ociHelperSocket != "" {
		if *ociIntentFile == "" {
			return errors.New("--oci-intent-file is required with --oci-helper-socket")
		}
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
				Logf:     log.Printf,
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
		ociAdapter = adapter
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
			intent: limarunner.FileIntentSource{Path: *ociIntentFile},
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
	if *ociControlSocket != "" {
		if *ociIntentFile == "" {
			return errors.New("--oci-intent-file is required with --oci-control-socket")
		}
		var stopCycle ocicontrol.StopCycle
		if supervisedBootBarrier != nil {
			stopCycle = supervisedBootBarrier
		}
		controller, err := ocicontrol.NewController(ocicontrol.ControllerConfig{
			IntentPath: *ociIntentFile, Runtime: nodeAgent, Images: ociAdapter, StopCycle: stopCycle,
			Doctor: func(doctorContext context.Context) (ocicontrol.DoctorResponse, error) {
				currentUser, userErr := user.Current()
				agentUser := ""
				if userErr == nil {
					agentUser = currentUser.Username
				}
				var limaFacts func() limarunner.SupervisorFacts
				if limaSupervisor != nil {
					limaFacts = limaSupervisor.Facts
				}
				report := ocicontrol.BuildDoctor(doctorContext, ocicontrol.DoctorConfig{
					HostPlatform: ocicontrol.PlatformFacts{OS: runtime.GOOS, Architecture: runtime.GOARCH},
					AgentUser:    agentUser, LaunchUnit: os.Getenv("WEFTY_LAUNCH_UNIT"),
					CapabilitySnapshot: nodeAgent.CapabilitySnapshot,
					Intent:             (limarunner.FileIntentSource{Path: *ociIntentFile}).ReadIntent,
					LimaFacts:          limaFacts, Helper: doctorHelperSource(ociAdapter), SetupStatePath: *ociSetupState,
				})
				return report, report.Validate()
			},
			Setup: func(setupContext context.Context, request ocicontrol.SetupRequest) (ocicontrol.SetupResponse, error) {
				response := ocicontrol.SetupResponse{Convergence: ocicontrol.ConvergenceLiveSafe}
				if runtime.GOOS == "linux" {
					response.ReasonCode = contract.CapabilityReasonPrerequisiteMissing
					response.MissingCapability = "privileged_linux_setup"
					response.Runbook = ocicontrol.RunbookPath
					return response, nil
				}
				probeInfo, probeErr := os.Stat(*ociProbeArchive)
				if ociAdapter == nil || *ociProbeArchive == "" || probeErr != nil || !probeInfo.Mode().IsRegular() || *ociSetupState == "" {
					response.ReasonCode = contract.CapabilityReasonPrerequisiteMissing
					response.MissingCapability = "configured_probe_archive_or_setup_state"
					response.Runbook = ocicontrol.RunbookPath
					return response, nil
				}
				desired := ocicontrol.SetupState{
					VMMemory: request.VMMemory, VMCPUs: request.VMCPUs, VMDisk: request.VMDisk,
					VMType: "vz", HostMountRoot: *ociLimaMountRoot, ProbeDigest: *ociProbeDigest,
					MemoryCapacityBytes: *ociMemoryCapacity, MemoryReserveBytes: *ociMemoryReserve,
				}
				current, stateErr := ocicontrol.ReadSetupState(*ociSetupState)
				if stateErr == nil {
					response.Convergence = ocicontrol.ClassifyConvergence(current, desired)
				} else if !errors.Is(stateErr, os.ErrNotExist) {
					return response, stateErr
				}
				if runtime.GOOS == "darwin" {
					limaHome := os.Getenv("LIMA_HOME")
					if !filepath.IsAbs(limaHome) {
						response.ReasonCode = contract.CapabilityReasonPrerequisiteMissing
						response.MissingCapability = "LIMA_HOME"
						response.Runbook = ocicontrol.RunbookPath
						return response, nil
					}
					if err := limarunner.WriteTemplate(filepath.Join(limaHome, *ociLimaInstance, "lima.yaml"), limarunner.TemplateConfig{
						Sizing: limarunner.Sizing{Memory: request.VMMemory, CPUs: request.VMCPUs, Disk: request.VMDisk}, HostAllowedMountRoot: *ociLimaMountRoot,
					}); err != nil {
						return response, err
					}
				}
				if err := ocicontrol.WriteSetupState(ocicontrol.DesiredSetupStatePath(*ociSetupState), desired); err != nil {
					return response, err
				}
				response.Configured = true
				if response.Convergence == ocicontrol.ConvergenceRestartRequired && !request.ApplyRestart {
					response.ReasonCode = contract.CapabilityReasonTemplateRestartRequired
					return response, nil
				}
				if response.Convergence == ocicontrol.ConvergenceRecreateRequired && !request.Recreate {
					response.ReasonCode = contract.CapabilityReasonTemplateRecreateRequired
					return response, nil
				}
				if authErr := ocicontrol.AuthorizeConvergence(response.Convergence, request.ApplyRestart, request.Recreate, nodeAgent.LiveOCIAttempts()); authErr != nil {
					return response, authErr
				}
				if bootBarrier != nil && bootBarrier.Ready() {
					archive, err := os.Open(*ociProbeArchive)
					if err != nil {
						return response, fmt.Errorf("open configured OCI probe archive: %w", err)
					}
					loaded, loadErr := ociAdapter.LoadImage(setupContext, *ociProbeImage, archive)
					closeErr := archive.Close()
					if loadErr != nil || closeErr != nil {
						return response, errors.Join(loadErr, closeErr)
					}
					if loaded.TopLevelDigest != *ociProbeDigest {
						return response, errors.New("configured OCI probe archive digest does not match --oci-probe-digest")
					}
					response.ProbePreloaded = true
				}
				applyCycle := response.Convergence == ocicontrol.ConvergenceRestartRequired && request.ApplyRestart ||
					response.Convergence == ocicontrol.ConvergenceRecreateRequired && request.Recreate
				if applyCycle {
					if supervisedBootBarrier == nil {
						return response, errors.New("requested Lima restart is unavailable")
					}
					template := limarunner.TemplateConfig{
						Sizing: limarunner.Sizing{Memory: request.VMMemory, CPUs: request.VMCPUs, Disk: request.VMDisk}, HostAllowedMountRoot: *ociLimaMountRoot,
					}
					var cycleErr error
					if response.Convergence == ocicontrol.ConvergenceRecreateRequired {
						cycleErr = supervisedBootBarrier.Recreate(setupContext, nodeAgent.StopOCIRuntime, nodeAgent.RecoverOCIRuntimeCapabilities, template)
					} else {
						cycleErr = supervisedBootBarrier.Restart(setupContext, nodeAgent.StopOCIRuntime, nodeAgent.RecoverOCIRuntimeCapabilities)
					}
					if cycleErr != nil {
						return response, cycleErr
					}
					response.RestartApplied = true
					response.RecreateApplied = response.Convergence == ocicontrol.ConvergenceRecreateRequired
				}
				if err := ocicontrol.WriteSetupState(*ociSetupState, desired); err != nil {
					return response, err
				}
				return response, nil
			},
		})
		if err != nil {
			return err
		}
		controlServer, err := ocicontrol.NewServer(*ociControlSocket, controller)
		if err != nil {
			return err
		}
		go func() {
			if err := controlServer.Serve(ctx); err != nil {
				nodeAgent.SuppressOCIRuntime(contract.CapabilityReasonLocalPermissionDenied, err)
				log.Printf("wefty-agent: node-local OCI control unavailable: %v", err)
			}
		}()
	}
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
		handleShutdownSignals(ctx, signals, func(drainContext context.Context) error {
			_, err := nodeAgent.Drain(drainContext)
			return err
		}, cancel, log.Printf)
	}()
	err = nodeAgent.Run(ctx)
	cancel()
	return err
}

func handleShutdownSignals(
	ctx context.Context,
	signals <-chan os.Signal,
	drain func(context.Context) error,
	cancel context.CancelFunc,
	logf func(string, ...any),
) {
	select {
	case <-ctx.Done():
		return
	case <-signals:
	}
	drainDone := make(chan error, 1)
	go func() {
		drainContext, stopDrain := context.WithTimeout(context.Background(), gracefulDrainTimeout)
		defer stopDrain()
		drainDone <- drain(drainContext)
	}()
	select {
	case <-ctx.Done():
		return
	case <-signals:
		// A second signal is cancellation authority even while graceful drain is
		// still joining a resident service. Waiting for Drain first made this
		// signal unreachable for exactly the service it was intended to stop.
		logf("wefty-agent: forced_shutdown transition=draining_to_forced reason=second_signal")
		cancel()
		return
	case err := <-drainDone:
		if err != nil {
			logf("wefty-agent: graceful drain failed: %v", err)
			cancel()
			return
		}
	}
	select {
	case <-ctx.Done():
	case <-signals:
		logf("wefty-agent: forced_shutdown transition=drained_to_forced reason=second_signal")
		cancel()
	}
}

func doctorHelperSource(adapter *ocirunner.Adapter) ocicontrol.HelperDoctorSource {
	if adapter == nil {
		return nil
	}
	return func(ctx context.Context) (ocicontrol.HelperDoctorSnapshot, error) {
		status, err := adapter.DoctorStatus(ctx)
		return ocicontrol.HelperDoctorSnapshot{
			ProtocolVersion: status.ProtocolVersion, Version: status.HelperVersion,
			Checksum: status.HelperChecksum, InstanceID: status.HelperInstanceID,
			SessionGeneration: status.SessionGeneration, Runtime: status.Runtime,
			RuntimeError: err, RuntimePlatformRecorded: status.RuntimePlatformRecorded,
			SweepReceipt: status.SweepReceipt, SweepReceiptRecorded: status.SweepReceiptRecorded,
		}, nil
	}
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
	adapter               ociCapabilityAdapter
	nodeID, bootSessionID string
	reference, digest     string
	intent                limarunner.IntentSource
}

type ociCapabilityAdapter interface {
	Probe(context.Context, string, string, string, string, time.Duration) error
}

func (probe ociCapabilityProbe) Probe(ctx context.Context) (agent.CapabilityProbeResult, error) {
	if probe.intent != nil {
		intent, err := probe.intent.ReadIntent(ctx)
		if err != nil || intent.Version != limarunner.OCIIntentVersion || intent.Revision == 0 || !intent.Enabled {
			if err == nil {
				err = errors.New("OCI intent is disabled")
			}
			return agent.CapabilityProbeResult{
				MissingCapabilities: []string{"kind:oci"}, ReasonCode: contract.CapabilityReasonOCIIntentDisabled,
			}, err
		}
	}
	if err := probe.adapter.Probe(ctx, probe.nodeID, probe.bootSessionID, probe.reference, probe.digest, l1.DefaultLeaseDuration); err != nil {
		return agent.CapabilityProbeResult{
			MissingCapabilities: []string{"kind:oci"}, ReasonCode: contract.CapabilityReasonProbeFailed,
		}, err
	}
	capabilities := map[string]bool{
		"kind:oci": true, "runtime_handler:" + ocihelper.DefaultRuntimeHandler: true, "cgroup_v2": true,
	}
	// OpenSession admits only the exact helper wire major. A successful OCI
	// functional probe therefore proves the complete protocol-v2 Computer
	// endpoint, control-state, and attachment bundle rather than a second,
	// unreachable version comparison here.
	capabilities["computer"] = true
	return agent.CapabilityProbeResult{Capabilities: capabilities}, nil
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
			snapshot.LastProbe, time.Now(),
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
