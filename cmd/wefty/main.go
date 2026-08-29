package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
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
		os.Exit(1)
	}
}

func writeCommandError(writer io.Writer, err error, jsonOutput bool) {
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
		return err
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
	return len(args) > 0 && (args[0] == "admin" ||
		(args[0] == "computers" && len(args) > 1 && args[1] == "submission"))
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
  admin bootstrap NONCE      Redeem a locally initiated administrator bootstrap challenge
  node setup-oci             Configure the installed node's OCI runtime
  node doctor                Read versioned node-local OCI diagnostics
  node oci start|stop        Set durable node-local OCI intent
  node load-image FILE       Import an OCI archive through the live agent
  nodes list                 List node reachability, eligibility, and capacity
  nodes set-claims NODE_ID   Set durable claim eligibility with an observed revision
  services <verb>            Create and operate service-class jobs
    create|list|status|start|stop|restart|logs|remove|forget
  computers submission <verb>
                             Enable, disable, or set Computer Run submission inflight capacity
    enable|disable|set-inflight
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
