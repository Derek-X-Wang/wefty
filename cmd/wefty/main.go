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

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/l3"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		writeCommandError(os.Stderr, err, hasJSONFlag(os.Args[1:]))
		os.Exit(commandExitCodeForArgs(err, os.Args[1:]))
	}
}

// commandExitCodeForArgs preserves the historical exit 1 contract for all
// pre-existing commands. Typed exits are explicit contracts only for the
// Computer lifecycle, access, and Storage surfaces introduced in M3.5.
func commandExitCodeForArgs(err error, args []string) int {
	if !isTypedExitCLIArgs(args) {
		return exitFailure
	}
	return commandExitCode(err)
}

func isTypedExitCLIArgs(args []string) bool {
	if isComputerCLIArgs(args) {
		return true
	}
	positionals := []string{}
	for _, arg := range args {
		if !strings.HasPrefix(arg, "-") {
			positionals = append(positionals, arg)
		}
	}
	if len(positionals) >= 2 && positionals[0] == "admin" && positionals[1] == "policy" {
		return true
	}
	if len(positionals) >= 1 && positionals[0] == "admins" {
		return true
	}
	return len(positionals) >= 2 && positionals[0] == "services" &&
		(positionals[1] == "grant" || positionals[1] == "grants" || positionals[1] == "revoke" || positionals[1] == "takeover" ||
			positionals[1] == "backup" || positionals[1] == "restore" || positionals[1] == "clone" || positionals[1] == "custody")
}

func isComputerCLIArgs(args []string) bool {
	positionals := make([]string, 0, 3)
	for _, arg := range args {
		if !strings.HasPrefix(arg, "-") {
			positionals = append(positionals, arg)
		}
	}
	if len(positionals) < 2 || positionals[0] != "services" {
		return false
	}
	switch positionals[1] {
	case "reimage", "reset", "resize", "abort", "submission":
		return true
	case "create":
		return hasArg(args, "--computer")
	case "status", "start", "stop", "restart", "remove":
		// These shared verbs return typed exits only when Computer-specific CAS
		// flags identify the new authority surface. Plain Job behavior remains 1.
		target := positionals[len(positionals)-1]
		return strings.HasPrefix(target, "computer_") || strings.HasPrefix(target, "computer-") || hasArg(args, "--expect-current") ||
			hasArg(args, "--intent-revision") || hasArg(args, "--storage-id") || hasArg(args, "--storage-generation")
	default:
		return false
	}
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
	exitFailure      = 1
	exitUsage        = 2
	exitUnauthorized = 3
	exitNotFound     = 4
	exitConflict     = 5
)

func commandExitCode(err error) int {
	var usage usageError
	if errors.As(err, &usage) {
		return exitUsage
	}
	var apiError contract.APIError
	var localErr *ocicontrol.ResponseError
	var responseErr *apiResponseError
	var takeoverErr *takeoverActionError
	switch {
	case errors.As(err, &localErr):
		apiError = localErr.APIError
	case errors.As(err, &responseErr):
		apiError = responseErr.APIError
	case errors.As(err, &takeoverErr):
		apiError = takeoverErr.APIError
	default:
		return exitFailure
	}
	switch apiError.Code {
	case contract.ErrorInvalidRequest:
		return exitUsage
	case contract.ErrorUnauthorized, contract.ErrorForbidden, contract.ErrorPrincipalForbidden,
		contract.ErrorPersonIdentityRequired, contract.ErrorAdminRequired, contract.ErrorControlNotAuthorized:
		return exitUnauthorized
	case contract.ErrorNotFound, contract.ErrorAttemptNotFound, contract.ErrorTakeoverSessionEnded:
		return exitNotFound
	case contract.ErrorConflict, contract.ErrorStalePolicyRevision, contract.ErrorStaleIntentRevision,
		contract.ErrorStorageReferenceConflict, contract.ErrorIdempotencyConflict, contract.ErrorDispatchKeyConflict, contract.ErrorFinalAdmin,
		contract.ErrorCapacityExhausted, contract.ErrorComputerResourceRequired, contract.ErrorComputerTraitRequired,
		contract.ErrorControllerBusy, contract.ErrorControllerAlreadyHeld:
		return exitConflict
	default:
		return exitFailure
	}
}

func writeCommandError(writer io.Writer, err error, jsonOutput bool) {
	var usage usageError
	if jsonOutput && errors.As(err, &usage) {
		_ = writeJSON(writer, contract.ErrorResponse{Error: contract.APIError{
			Code: contract.ErrorInvalidRequest, Message: usage.Error(), Retryable: false,
		}})
		return
	}
	var localErr *ocicontrol.ResponseError
	if jsonOutput && errors.As(err, &localErr) {
		_ = writeJSON(writer, contract.ErrorResponse{Error: localErr.APIError})
		return
	}
	var responseErr *apiResponseError
	if jsonOutput && errors.As(err, &responseErr) {
		_ = writeJSON(writer, contract.ErrorResponse{Error: responseErr.APIError})
		return
	}
	var takeoverErr *takeoverActionError
	if errors.As(err, &takeoverErr) {
		if jsonOutput {
			_ = writeJSON(writer, contract.ComputerControlErrorResponse{Error: takeoverErr.APIError, Receipt: takeoverErr.Receipt})
			return
		}
		_, _ = fmt.Fprintf(writer, "wefty: %v\n", err)
		if takeoverErr.Receipt != nil {
			_ = writeComputerControlReceipt(writer, *takeoverErr.Receipt, false)
		}
		return
	}
	_, _ = fmt.Fprintf(writer, "wefty: %v\n", err)
}

func hasJSONFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--json" {
			return true
		}
	}
	return false
}

type globalOptions struct {
	fabricMode     string
	l1Address      string
	l3Address      string
	plainIdentity  string
	plainUserID    string
	plainDeviceID  string
	plainFabricID  string
	fabricName     string
	stateDirectory string
	authKey        string
	controlURL     string
	ephemeral      bool
	jsonOutput     bool
	nodeConfigPath string
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	options, commandArgs, err := parseGlobalOptions(args, stderr)
	if err != nil {
		return usageError(err.Error())
	}
	if len(commandArgs) == 0 {
		return usageError("a command is required")
	}
	if commandArgs[0] == "node" {
		return executeLocalNode(ctx, options, commandArgs[1:], stdout, stderr)
	}
	plainIdentity := fabric.Identity{
		NodeID: options.plainIdentity, UserID: options.plainUserID, DeviceID: options.plainDeviceID,
		Tags: []string{l3.DefaultCallerPrincipalTag},
	}
	if usesPersonProtocol(commandArgs) {
		plainIdentity.Tags = nil
		if options.fabricMode == "plain" && (options.plainUserID == "" || options.plainDeviceID == "") {
			return usageError("plain person commands require DEVELOPMENT ONLY --plain-user-id and --plain-device-id")
		}
	}
	participant, closeFabric, err := fabricconfig.Open(fabricconfig.Config{
		Mode:           options.fabricMode,
		Identity:       plainIdentity,
		PlainFabricID:  options.plainFabricID,
		Name:           options.fabricName,
		StateDirectory: options.stateDirectory,
		AuthKey:        options.authKey,
		ControlURL:     options.controlURL,
		Ephemeral:      options.ephemeral,
	})
	if err != nil {
		return err
	}
	defer closeFabric()
	clients, err := newAPIClients(participant, options.l1Address, options.l3Address)
	if err != nil {
		return err
	}
	defer clients.close()
	return execute(ctx, clients, options.jsonOutput, commandArgs, stdout, stderr)
}

func usesPersonProtocol(args []string) bool {
	return (len(args) == 1 && args[0] == "whoami") || requiresPersonCommand(args) ||
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
	args, options.jsonOutput = removeBoolFlag(args, "--json")
	flags := flag.NewFlagSet("wefty", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&options.fabricMode, "fabric", "plain", "fabric implementation: plain or tsnet")
	flags.StringVar(&options.l1Address, "l1", l3.DefaultL1Address, "L1 control-plane Fabric address")
	flags.StringVar(&options.l3Address, "l3", l3.DefaultL3Address, "L3 run-ledger Fabric address")
	flags.StringVar(&options.plainIdentity, "plain-identity", "wefty-cli", "plain Fabric identity node ID")
	flags.StringVar(&options.plainUserID, "plain-user-id", os.Getenv("WEFTY_DEV_PLAIN_USER_ID"), "DEVELOPMENT ONLY: self-asserted plain Fabric person user ID")
	flags.StringVar(&options.plainDeviceID, "plain-device-id", os.Getenv("WEFTY_DEV_PLAIN_DEVICE_ID"), "DEVELOPMENT ONLY: self-asserted plain Fabric person device ID")
	flags.StringVar(&options.plainFabricID, "plain-fabric-id", os.Getenv("WEFTY_DEV_PLAIN_FABRIC_ID"), "DEVELOPMENT ONLY: shared plain- prefixed Fabric authority")
	flags.StringVar(&options.fabricName, "fabric-name", "wefty-cli", "tsnet logical node name")
	flags.StringVar(&options.stateDirectory, "state-dir", "", "tsnet state directory")
	flags.StringVar(&options.authKey, "auth-key", os.Getenv("TS_AUTHKEY"), "tsnet auth key")
	flags.StringVar(&options.controlURL, "control-url", os.Getenv("TS_CONTROL_URL"), "optional tsnet coordination URL")
	flags.BoolVar(&options.ephemeral, "ephemeral", false, "register an ephemeral tsnet node")
	flags.StringVar(&options.nodeConfigPath, "node-config", defaultNodeConfigPath(), "installed node configuration used by singular node commands")
	flags.Usage = func() { fmt.Fprint(stderr, rootUsage) }
	if err := flags.Parse(args); err != nil {
		return globalOptions{}, nil, err
	}
	return options, flags.Args(), nil
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

func removeBoolFlag(args []string, name string) ([]string, bool) {
	filtered := make([]string, 0, len(args))
	found := false
	for _, arg := range args {
		if arg == name {
			found = true
			continue
		}
		filtered = append(filtered, arg)
	}
	return filtered, found
}

const rootUsage = `Usage: wefty [global flags] <command>

Commands:
  whoami                    Observe the current Fabric-scoped person identity in L1
  admin bootstrap NONCE      Redeem a locally initiated administrator bootstrap challenge
  admin policy get           Read the current administrator policy revision and members
  admin policy add|remove    Mutate administrator membership with an observed revision
  admins list|add|remove     Alias for the administrator policy commands
  node setup-oci             Configure the installed node's OCI runtime
  node doctor                Read versioned node-local OCI diagnostics
  node oci start|stop        Set durable node-local OCI intent
  node load-image FILE       Import an OCI archive through the live agent
  nodes list                 List node reachability, eligibility, and capacity
  nodes set-claims NODE_ID   Set durable claim eligibility with an observed revision
  services <verb>            Create and operate service-class jobs
    create [--computer --name NAME --image IMAGE --node NODE_ID --argv ARG --working-directory PATH --mount SPEC --memory-bytes BYTES --cpu-millicores VALUE --runtime-handler NAME --disk-bytes BYTES --backup-cap COUNT --idempotency-key KEY]
    list [--limit COUNT --cursor CURSOR]
    status JOB_ID|COMPUTER_ID
    start|stop COMPUTER [--intent-revision REV --storage-id ID --storage-generation GENERATION | --expect-current]
    restart COMPUTER --idempotency-key KEY [CAS flags | --expect-current]
    remove COMPUTER [CAS flags | --expect-current]
    reimage COMPUTER --image IMAGE --idempotency-key KEY [--chown --terminate-sessions] [CAS flags | --expect-current]
    reset COMPUTER --idempotency-key KEY [--terminate-sessions] [CAS flags | --expect-current]
    resize COMPUTER --disk-bytes BYTES --idempotency-key KEY [CAS flags | --expect-current]
    abort COMPUTER --idempotency-key KEY [CAS flags | --expect-current]
    logs|forget JOB_ID
    grants|grant|revoke|takeover
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
  runs list                  List Runs by immutable Computer origin
  submit                     Submit a saved Workflow or an inline-script/image run
  rerun RUN_ID               Create a new run from a stored snapshot
  logs RUN_ID [--follow]     Read or follow run logs
  inspect RUN_ID [--execution]
                             Show run lineage, with optional L1 execution diagnostics
  drain NODE_ID              Disable new claims using the current intent revision

Global flags:
  --fabric plain|tsnet
  --l1 ADDRESS
  --l3 ADDRESS
  --plain-user-id USER_ID    DEVELOPMENT ONLY: self-asserted plain person identity
  --plain-device-id DEVICE_ID
  --json
  --node-config PATH
`

type usageError string

func (e usageError) Error() string { return string(e) }
