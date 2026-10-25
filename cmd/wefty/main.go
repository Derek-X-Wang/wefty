package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/internal/nodeload"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/l3"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

var version = "dev"

type execSignalResetKey struct{}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()
	ctx = context.WithValue(ctx, execSignalResetKey{}, stop)
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		report := decideCommandReport(err)
		report.writeError(os.Stderr, hasJSONFlag(os.Args[1:]))
		os.Exit(report.exitCode)
	}
}

// commandExitCodeForArgs, commandExitCode, and writeCommandError are test
// shims over decideCommandReport. main consumes the report directly.
func commandExitCodeForArgs(err error, _ []string) int {
	return commandExitCode(err)
}

func hasArg(args []string, name string) bool {
	for _, arg := range args {
		if arg == name || strings.HasPrefix(arg, name+"=") {
			return true
		}
	}
	return false
}

const (
	exitFailure                  = 1
	exitUsage                    = 2
	exitUnauthorized             = 3
	exitNotFound                 = 4
	exitConflict                 = 5
	exitCustodyImportDeferred    = 6
	exitCustodyImportQuarantined = 7
	exitCustodyImportFailed      = 8
	exitCustodyImportSuperseded  = 9
	// exitRunFailed is the typed run outcome for `wefty exec` and `wefty wait`.
	// exitWaitTimeout is `wefty wait` ending its observation window.
	// A failed run and an expired wait need different next actions.
	exitRunFailed   = 10
	exitWaitTimeout = 11
	// exitNotReady is `wefty status` answering no. The command worked; the
	// cluster cannot take work, which is a different thing from the command
	// failing and gets its own code so a script can tell them apart.
	exitNotReady = 12
	// exitUnavailable tells an operator that the transport or service may recover.
	exitUnavailable = 13
	// exitMutationWaitTimeout means accepted intent is still being observed.
	exitMutationWaitTimeout = 14
)

const errorWaitTimeout contract.ErrorCode = "wait_timeout"

// Seat refusal codes share the conflict exit. This is the explicit whitelist
// consumed by command_reporting.go and exercised through the real binary.
const (
	seatErrorRunRerun         contract.ErrorCode = contract.ErrorSeatRunRerun
	seatErrorRoomFull         contract.ErrorCode = "seat_room_full"
	seatErrorNoAnswer         contract.ErrorCode = "seat_no_answer"
	seatErrorDenied           contract.ErrorCode = "seat_denied"
	seatErrorDedicatedCap     contract.ErrorCode = "seat_dedicated_cap"
	seatErrorHostOff          contract.ErrorCode = "seat_host_off"
	seatErrorStartFailed      contract.ErrorCode = "seat_start_failed"
	seatErrorRoomUnconfigured contract.ErrorCode = "seat_room_unconfigured"
	seatErrorNodeUnavailable  contract.ErrorCode = "seat_node_unavailable"
	seatErrorWindowNotAllowed contract.ErrorCode = "seat_window_not_allowed"
	seatErrorEnded            contract.ErrorCode = "seat_ended"
	seatErrorCopyFailed       contract.ErrorCode = "seat_copy_failed"
	seatErrorPersonDriving    contract.ErrorCode = "person_driving"
	seatErrorCopyWriteDenied  contract.ErrorCode = "seat_copy_write_denied"
)

func commandExitCode(err error) int {
	return decideCommandReport(err).exitCode
}

func writeCommandError(writer io.Writer, err error, jsonOutput bool) {
	decideCommandReport(err).writeError(writer, jsonOutput)
}

// Only flag diagnostics are suppressed: command warnings retain their normal
// stderr path, while usage failures get one machine-readable error document.
func flagErrorOutput(stderr io.Writer, jsonOutput bool) io.Writer {
	if jsonOutput {
		return io.Discard
	}
	return stderr
}

func hasJSONFlag(args []string) bool {
	_, enabled, err := removeBoolFlag(args, "--json")
	return enabled || err != nil
}

type globalOptions struct {
	fabricMode         string
	l1Address          string
	l3Address          string
	plainIdentity      string
	plainUserID        string
	plainDeviceID      string
	plainFabricID      string
	fabricName         string
	stateDirectory     string
	authKey            string
	controlURL         string
	ephemeral          bool
	printEnrollmentURL bool
	jsonOutput         bool
	nodeConfigPath     string
	oneshotLoadLimit   *float64
	profilePath        string
	profileFabric      string
	participants       map[string]connprofile.Participant
	// explicitFlags names the global flags the caller passed, so the
	// connection profile never overrides one of them.
	explicitFlags map[string]bool
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	return runWithFabricOpener(ctx, args, stdout, stderr, openCLIFabric)
}

func runWithFabricOpener(ctx context.Context, args []string, stdout, stderr io.Writer, openFabric func(context.Context, fabricconfig.Config) (fabric.Fabric, fabricconfig.CloseFunc, error)) (result error) {
	if handled, err := serviceplan.ProfilePreflight(args, stdout); handled {
		return err
	}
	defer func() {
		var help *commandHelp
		if errors.As(result, &help) {
			_, result = io.WriteString(stdout, help.usage)
		}
	}()
	if len(args) == 1 && (args[0] == "--version" || args[0] == "-version") {
		_, err := fmt.Fprintln(stdout, version)
		return err
	}
	options, commandArgs, err := parseGlobalOptions(args, stderr)
	if err != nil {
		return flagUsageError(err)
	}
	if len(commandArgs) == 0 {
		return usageError("a command is required")
	}
	if commandArgs[0] == "help" {
		_, err := io.WriteString(stdout, rootUsage)
		return err
	}
	// Setup installs this machine's stack and writes the connection profile;
	// it neither reads that profile nor opens the Fabric. Its own flag parser
	// answers --help before anything is touched.
	if commandArgs[0] == "upgrade" {
		return executeUpgrade(ctx, options, commandArgs[1:], stdout, stderr)
	}
	if commandArgs[0] == "setup" {
		return executeSetup(ctx, options, commandArgs[1:], stdout, stderr)
	}
	if commandArgs[0] == "client-service" {
		return executeClientService(ctx, options, commandArgs[1:], stdout, stderr)
	}
	if wantsCommandHelp(commandArgs) {
		return executeCommandHelp(ctx, options, commandArgs, stdout, stderr)
	}
	if commandArgs[0] == "doctor" {
		return executeDoctor(ctx, options, commandArgs[1:], stdout, stderr)
	}
	if commandArgs[0] == "node" {
		return executeLocalNode(ctx, options, commandArgs[1:], stdout, stderr)
	}
	// The authoring surface runs before the fabric is opened, like `wefty
	// node`. It writes files and never speaks to the cluster: a job reporting
	// through its run mailbox holds no credential and has no identity to dial
	// with, so requiring one here would defeat the mailbox entirely.
	if handled, err := executeAuthoringCommand(options, commandArgs, stdout, stderr); handled {
		return err
	}
	if commandArgs[0] == "seat" {
		if err := validateSeatArgs(commandArgs[1:]); err != nil {
			return err
		}
	}
	if err := applyConnectionProfile(&options); err != nil {
		return err
	}
	resolveFabricEnvironment(&options)
	config, err := commandFabricConfig(options, commandArgs)
	if err != nil {
		return err
	}
	// Written unconditionally: an inherited WEFTY_FABRIC_PRINT_ENROLLMENT_URL=1
	// must not survive an explicit --fabric-print-enrollment-url=false (wefty
	// #498). The resolved boolean, not just the true case, always wins.
	if options.printEnrollmentURL {
		_ = os.Setenv("WEFTY_FABRIC_PRINT_ENROLLMENT_URL", "1")
	} else {
		_ = os.Setenv("WEFTY_FABRIC_PRINT_ENROLLMENT_URL", "0")
	}
	callerCtx := ctx
	openCtx := ctx
	if config.ExpectedIdentity != nil && commandArgs[0] == "status" {
		budget, err := parseStatusBudget(commandArgs[1:], options.jsonOutput, stderr)
		if err != nil {
			return err
		}
		bounded, cancel := context.WithTimeout(ctx, budget)
		defer cancel()
		ctx = bounded
		probe, stop := context.WithTimeout(ctx, statusProbeBudget)
		defer stop()
		openCtx = probe
	}
	participant, closeFabric, err := openFabric(openCtx, config)
	if closeFabric != nil {
		openingFailed := err != nil
		defer func() {
			if closeErr := closeFabric(); openingFailed && closeErr != nil {
				result = errors.Join(result, closeErr)
			}
		}()
	}
	if err != nil {
		if errors.Is(callerCtx.Err(), context.Canceled) {
			return callerCtx.Err()
		}
		var identity *fabric.ParticipantIdentityError
		var unreachable *fabric.ParticipantUnreachableError
		if commandArgs[0] == "open" && errors.As(err, &identity) && identity.Person {
			return fmt.Errorf("this shortcut needs the owner's person sign-in on this laptop: %w", err)
		}
		if config.ExpectedIdentity != nil && commandArgs[0] == "status" && (errors.As(err, &identity) || errors.As(err, &unreachable)) {
			return reportParticipantCheckFailure(stdout, options, config, err)
		}
		return err
	}
	clients, err := newAPIClients(participant, options.l1Address, options.l3Address)
	if err != nil {
		return err
	}
	defer clients.close()
	clients.nodeConfigPath = options.nodeConfigPath
	clients.oneshotLoadLimit = options.oneshotLoadLimit
	clients.participantProfile = len(options.participants) != 0
	if clients.participantProfile {
		clients.clientIdentityID = config.Identity.NodeID
		clients.localPersonSignIn = localPersonSignIn(options, time.Now())
	}
	if commandArgs[0] == "open" && clients.participantProfile {
		// L1 lookups keep the client participant selected above. Only the
		// publication stream uses the independently checked saved person.
		personOptions := options
		// Primary name/folder flags apply to the client. The secondary person
		// always uses its own saved record, as in the client service.
		personOptions.explicitFlags = nil
		personConfig, err := commandFabricConfig(personOptions, []string{"whoami"})
		if err != nil {
			return fmt.Errorf("this shortcut needs the owner's person sign-in on this laptop: %w", err)
		}
		person, closePerson, err := openFabric(ctx, personConfig)
		if closePerson != nil {
			defer func() { _ = closePerson() }()
		}
		if err != nil {
			var identity *fabric.ParticipantIdentityError
			if errors.As(err, &identity) && identity.Person {
				return fmt.Errorf("this shortcut needs the owner's person sign-in on this laptop: %w", err)
			}
			return err
		}
		clients.servicePerson = person
	}
	return execute(ctx, clients, options.jsonOutput, commandArgs, stdout, stderr)
}

// resolveFabricEnvironment fills coordinator defaults immediately before
// the fabric is opened. Reading them earlier would mean `wefty run` touched a
// coordination credential on a path that exists precisely because the caller
// has none.
func resolveFabricEnvironment(options *globalOptions) {
	// Legacy bootstrap callers capture an explicit credential before New
	// clears the ambient value. Enrolled participants never consume it.
	if len(options.participants) == 0 && options.authKey == "" {
		options.authKey = os.Getenv("TS_AUTHKEY")
	}
	if options.controlURL == "" {
		options.controlURL = os.Getenv("TS_CONTROL_URL")
	}
}

func usesPersonProtocol(args []string) bool {
	return (len(args) > 0 && args[0] == "whoami") || requiresPersonCommand(args) ||
		(len(args) > 1 && args[0] == "services" && args[1] == "submission")
}

func requiresPersonCommand(args []string) bool {
	if len(args) == 0 {
		return false
	}
	if args[0] == "admin" || args[0] == "admins" {
		return true
	}
	return len(args) > 1 && args[0] == "services" &&
		(args[1] == "grant" || args[1] == "grants" || args[1] == "revoke" || args[1] == "takeover")
}

func parseGlobalOptions(args []string, stderr io.Writer) (globalOptions, []string, error) {
	options := globalOptions{}
	var err error
	args, options.jsonOutput, err = removeBoolFlag(args, "--json")
	if err != nil {
		return options, nil, err
	}
	flags := flag.NewFlagSet("wefty", flag.ContinueOnError)
	flags.SetOutput(flagErrorOutput(stderr, options.jsonOutput))
	flags.StringVar(&options.fabricMode, "fabric", "plain", "fabric implementation: plain or tsnet")
	// The addresses default from the environment so a shell that exported
	// them once -- as the README and the acceptance docs do -- does not have
	// to repeat them on every call. An explicit flag always wins, including
	// an explicit --l3= choosing an L1-only installation.
	flags.StringVar(&options.l1Address, "l1", addressFromEnvironment(l1AddressEnv, l3.DefaultL1Address),
		"L1 control-plane Fabric address (default $"+l1AddressEnv+", else "+l3.DefaultL1Address+")")
	flags.StringVar(&options.l3Address, "l3", addressFromEnvironment(l3AddressEnv, l3.DefaultL3Address),
		"L3 run-ledger Fabric address (default $"+l3AddressEnv+", else "+l3.DefaultL3Address+"; pass --l3= for an L1-only installation)")
	flags.StringVar(&options.plainIdentity, "plain-identity", "wefty-cli", "plain Fabric identity node ID")
	flags.StringVar(&options.plainUserID, "plain-user-id", os.Getenv("WEFTY_DEV_PLAIN_USER_ID"), "DEVELOPMENT ONLY: self-asserted plain Fabric person user ID")
	flags.StringVar(&options.plainDeviceID, "plain-device-id", os.Getenv("WEFTY_DEV_PLAIN_DEVICE_ID"), "DEVELOPMENT ONLY: self-asserted plain Fabric person device ID")
	flags.StringVar(&options.plainFabricID, "plain-fabric-id", os.Getenv("WEFTY_DEV_PLAIN_FABRIC_ID"), "DEVELOPMENT ONLY: shared plain- prefixed Fabric authority")
	flags.StringVar(&options.fabricName, "fabric-name", "wefty-cli", fabricNameHelp)
	flags.StringVar(&options.stateDirectory, "state-dir", "", "tsnet state directory")
	// The coordinator default is resolved after authoring dispatch. Join
	// credentials belong only to setup, never an ordinary CLI participant.
	flags.StringVar(&options.authKey, "auth-key", "", "legacy tsnet join key (default $TS_AUTHKEY); participant enrolment belongs to setup")
	flags.StringVar(&options.controlURL, "control-url", "", "optional coordination URL, also selects the fleet setup coordinator (default $TS_CONTROL_URL)")
	flags.BoolVar(&options.ephemeral, "ephemeral", false, "register an ephemeral tsnet node")
	flags.BoolVar(&options.printEnrollmentURL, "fabric-print-enrollment-url", os.Getenv("WEFTY_FABRIC_PRINT_ENROLLMENT_URL") == "1", "print the raw tsnet enrollment URL instead of the wefty-owned notice on first login (also WEFTY_FABRIC_PRINT_ENROLLMENT_URL=1)")
	flags.StringVar(&options.nodeConfigPath, "node-config", defaultNodeConfigPath(), "installed node configuration used by exec, status and singular node commands")
	flags.StringVar(&options.profilePath, "profile", os.Getenv(connprofile.EnvironmentVariable),
		"connection profile (default $"+connprofile.EnvironmentVariable+", else ~/.config/wefty/"+connprofile.FileName+" when it exists)")
	flags.Usage = func() { fmt.Fprint(flags.Output(), rootUsage) }
	if err := parseCommandFlags(flags, args, true); err != nil {
		return globalOptions{}, nil, err
	}
	options.explicitFlags = make(map[string]bool)
	flags.Visit(func(set *flag.Flag) { options.explicitFlags[set.Name] = true })
	return options, flags.Args(), nil
}

// applyConnectionProfile fills the endpoint and identity options that no flag
// and no environment variable set from the connection profile `wefty setup`
// wrote. It runs after the authoring and node-local commands are dispatched:
// those never reach the cluster, and a job reporting through its run mailbox
// must not fail on a profile it does not need.
func applyConnectionProfile(options *globalOptions) error {
	fallback := ""
	if home, err := os.UserHomeDir(); err == nil {
		fallback, _ = connprofile.DefaultPath(home)
	}
	profile, _, err := connprofile.Resolve(options.profilePath, fallback)
	if err != nil {
		return err
	}
	limit, err := nodeload.ResolveLimit(profile.OneshotLoadLimit, os.Getenv(nodeload.Environment))
	if err != nil {
		return err
	}
	options.oneshotLoadLimit = &limit
	settings := profile.CLI()
	options.participants = profile.Participants
	options.profileFabric = profile.Fabric
	layer := connprofile.Layer{Explicit: options.explicitFlags}
	layer.Fill(&options.fabricMode, "fabric", "", settings.Fabric)
	layer.Fill(&options.l1Address, "l1", l1AddressEnv, settings.L1)
	layer.Fill(&options.l3Address, "l3", l3AddressEnv, settings.L3)
	layer.Fill(&options.plainIdentity, "plain-identity", "", settings.Identity)
	layer.Fill(&options.plainUserID, "plain-user-id", "WEFTY_DEV_PLAIN_USER_ID", settings.PersonUserID)
	layer.Fill(&options.plainDeviceID, "plain-device-id", "WEFTY_DEV_PLAIN_DEVICE_ID", settings.PersonDeviceID)
	layer.Fill(&options.plainFabricID, "plain-fabric-id", "WEFTY_DEV_PLAIN_FABRIC_ID", settings.PlainFabricID)
	return nil
}

// l1AddressEnv and l3AddressEnv are the variables the README and the
// acceptance docs export. They are read as the flags' defaults, never as an
// override of a flag that was passed.
const (
	l1AddressEnv = "WEFTY_L1_ADDR"
	l3AddressEnv = "WEFTY_L3_ADDR"
)

// addressFromEnvironment treats an empty or blank variable as unset. Choosing
// an L1-only installation is a decision, and it is made with an explicit
// --l3=, not by an exported variable that happens to be empty.
func addressFromEnvironment(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func defaultNodeConfigPath() string {
	if configured := os.Getenv("WEFTY_NODE_CONFIG"); configured != "" {
		return configured
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	path, err := ocicontrol.DefaultInstalledConfigPath(home)
	if err != nil {
		return ""
	}
	return path
}

const fabricNameHelp = "Fabric node name (legacy default: wefty-cli; participant profiles must match the selected participant)"

const rootUsage = `Usage: wefty [global flags] <command>

--fabric-name NAME: ` + fabricNameHelp + `
--control-url URL: Select the coordinator, including for fleet setup (default $TS_CONTROL_URL).

--json[=true|false] is accepted before or after a command and its arguments.
Every command uses typed exits: usage 2, unauthorized 3, not found 4,
conflict 5, unavailable 13, accepted-mutation wait timeout 14, other failure 1. JSON errors go to stderr.

Commands:
  upgrade [--yes] [--drain-timeout DURATION]  Build latest main and upgrade this Mac's installed stack
  setup [--proof] [--oci-config FILE] [--from DIR] [--node-id ID]
                             Install the daily plain Mac stack and connection profile
    --role control-plane|join
                             Install a fleet role on Mac or Linux
    --join-code CODE [--with-person]
                             Join a fleet; person sign-in is for the owner's laptop only
    --role join --join-code CODE --convert-plain
                             Convert this Mac's plain install, retaining read-only history
                             --node-id chooses a stable ID on first install
                             Plain Mac only: --remove keeps data and logs; --purge deletes them
                             Fleet removal is not supported yet
  seat request --network internet|none [--dedicated] [--window 1h|8h|24h]
                             Request one approved Computer Seat
  cp SRC SEAT_ID:PATH | cp SEAT_ID:PATH DEST
                             Copy files or directories into or out of a Seat
  seat wait SEAT_ID          Wait again by the original request ID
  seat window close         Close this node's approval windows early
  status [--timeout D]       Can this cluster take work? Exits 12 when it cannot
  whoami                    Observe the current Fabric-scoped person identity in L1
  admin bootstrap NONCE      Redeem a locally initiated administrator bootstrap challenge
  admin policy get           Read the current administrator policy revision and members
  admin policy add|remove    Mutate membership with --policy-revision REVISION; add accepts --device-id DEVICE_ID
  admins list|add|remove     Alias for the administrator policy commands
  node setup-oci             Configure the installed node's OCI runtime
  doctor [--print-policy]    Check the installation or print private network policy
  node doctor                Check the installed role, plain process or OCI node
  node oci start|stop        Set durable node-local OCI intent
  node oci removals          Read durable runtime removal manifests, phases, and attestations
  node load-image FILE       Import an OCI archive through the live agent
    [--reference REFERENCE]  Name an archive whose export named no artifact
  nodes list                 List node facts with filters, --cursor, --limit, --all
  nodes inspect NODE_ID      Read one node and its resident attempts
  nodes set-claims NODE_ID   Set durable claim eligibility with an observed revision
  jobs list                  List readable jobs with filters and cursor paging
    [--class CLASS --kind KIND --state STATE --submitter me --limit N --cursor CURSOR]
  computers list             List Computers with --cursor, --limit, --all
  open SERVICE [--local-port N] [--no-browser]
                             Open a published service; fleet shortcuts stop with Ctrl-C
  services <verb>            Create and operate service-class jobs
    create [--computer --name NAME --image IMAGE --node NODE_ID --argv ARG --working-directory PATH --mount SPEC --memory-bytes BYTES --cpu-millicores VALUE --runtime-handler NAME --disk-bytes BYTES --backup-cap COUNT --idempotency-key KEY]
    list [--limit COUNT --cursor CURSOR]
    status JOB_ID|COMPUTER
    start|stop COMPUTER [--intent-revision REV --storage-id ID --storage-generation GENERATION | --expect-current]
    restart COMPUTER --idempotency-key KEY [CAS flags | --expect-current]
    remove COMPUTER [CAS flags | --expect-current]
    reimage COMPUTER --image IMAGE --idempotency-key KEY [--chown --terminate-sessions] [CAS flags | --expect-current]
    reset COMPUTER --idempotency-key KEY [--terminate-sessions] [CAS flags | --expect-current]
    resize COMPUTER --disk-bytes BYTES --idempotency-key KEY [CAS flags | --expect-current]
    abort COMPUTER --idempotency-key KEY [CAS flags | --expect-current]
    logs|forget JOB_ID
    grants|grant|revoke|takeover
                             COMPUTER accepts an exact Computer ID or current Job ID before its unique friendly name
                             Take-over view prints FRIENDLY NAME first and dialable CONNECT HOST second;
                             DISPLAY ENDPOINT is a deprecated alias through 2026-10-04
  services submission <verb>
                             Enable, disable, or set Computer Run submission inflight capacity
    enable|disable COMPUTER [--policy-revision REV --submit-intent-revision REV | --expect-current] [--idempotency-key KEY]
    set-inflight COMPUTER --max-inflight COUNT [--policy-revision REV --submit-intent-revision REV | --expect-current] [--idempotency-key KEY]
  services backup <verb>     Create, list, prune, or configure cold Backups
    create|list|prune|set-cap
  services restore COMPUTER BACKUP
                             Restore a stopped Computer from one Backup
  services clone COMPUTER BACKUP
                             Clone one Backup into a new stopped Computer with no grants
  services custody <verb>    Export, import, or attest external storage custody
    export|import|attest
  runs list [--status STATUS] [--mine] [--limit LIMIT] [--cursor CURSOR] [--all]
                             List Runs, newest first; --all walks every remaining page
    --origin computer:ID     List Runs by immutable Computer origin instead
  exec [--tag TAG] [--max-runtime D] [--restartable] [--keep-running | --detach] [--json] -- <command...>
                             Run this checkout on this machine's node; exit 0 or 10
  exec --seat SEAT_ID [--max-runtime D] [--keep-running | --detach] [--json] -- <command...>
                             Run in the Seat's folder without capturing or copying a checkout
  submit [--again] [--idempotency-key KEY]
                             Submit a saved Workflow or an inline-script/image run
  rerun RUN_ID [--again] [--idempotency-key KEY]
                             Rerun a stored snapshot
  Same actor + submit/rerun request permanently replays its run. --again creates
  fresh work; an explicit --idempotency-key wins, scoped per authenticated actor.
  Output distinguishes created/replayed. Mutable references hash by name:
  pushing a new reg/app:latest still replays the old run. Pin images by digest
  (reg/app@sha256:...) or use --again; pin Workflow versions or use --again too.
  logs RUN_ID|SEAT_ID|JOB_ID [--all | [--tail N] [--follow]]
                             Read or follow Run or Seat logs
  cancel JOB_ID|RUN_ID [--include-script]
                             Cancel a run or one-shot job
  wait RUN_ID [--timeout D] [--include-script]
                             Block until a run is terminal; exit 10 if it failed, 11 on timeout
  results RUN_ID [--out FILE]
                             Read the result document the run uploaded
  inspect RUN_ID|JOB_ID [--execution] [--include-script]
                             Show run lineage, with optional L1 execution diagnostics
  JSON run records omit inline script content, keeping sha256, size_bytes, and
  content_omitted=true. --include-script restores content on wait, inspect,
  cancel RUN_ID, and runs list --origin computer:ID.
  drain NODE_ID              Disable new claims [--revision REV] [--reason REASON]
  run <envelope|step|gate|result|params>
                             Report from inside a running job by writing run
                             mailbox events; needs no credential and no cluster
    envelope --step STEP [--status succeeded|failed|partial --summary TEXT
             --payload-file FILE | --payload-json-file FILE --key KEY]
    step --name NAME [--end --summary TEXT]
    gate --name NAME --outcome pass|fail|error|skipped [--evidence-file FILE --summary TEXT]
    result --file PATH [--status succeeded|failed|partial --summary TEXT]
    params [--json] [NAME]
  workflow init NAME         Scaffold a runnable workflow starter (bash, or
    [--lang bash|ts] [--dir DIR]  TypeScript bundled to dist/NAME.mjs)

Global flags:
  --fabric plain|tsnet
  --l1 ADDRESS               default $WEFTY_L1_ADDR, else the profile, else
                             wefty://control-plane
  --l3 ADDRESS               default $WEFTY_L3_ADDR, else the profile, else
                             wefty://run-ledger;
                             pass --l3= for an L1-only installation
  --plain-user-id USER_ID    DEVELOPMENT ONLY: self-asserted plain person identity
  --plain-device-id DEVICE_ID
  --json
  --node-config PATH
  --profile PATH             connection profile; default $WEFTY_PROFILE, else
                             ~/.config/wefty/profile.json when it exists.
                             Flags, then environment, then the profile win
  --fabric-print-enrollment-url
                             Print the raw Fabric enrollment URL on first login instead of the wefty notice
`

type usageError string

func (e usageError) Error() string { return string(e) }
