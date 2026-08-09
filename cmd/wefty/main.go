package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "wefty: %v\n", err)
		os.Exit(1)
	}
}

type globalOptions struct {
	fabricMode     string
	l1Address      string
	l3Address      string
	plainIdentity  string
	fabricName     string
	stateDirectory string
	authKey        string
	controlURL     string
	ephemeral      bool
	jsonOutput     bool
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	options, commandArgs, err := parseGlobalOptions(args, stderr)
	if err != nil {
		return err
	}
	if len(commandArgs) == 0 {
		return usageError("a command is required")
	}
	participant, closeFabric, err := fabricconfig.Open(fabricconfig.Config{
		Mode: options.fabricMode,
		Identity: fabric.Identity{
			NodeID: options.plainIdentity,
			Tags:   []string{l1.DefaultClientPrincipalTag, l3.DefaultCallerPrincipalTag},
		},
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

func parseGlobalOptions(args []string, stderr io.Writer) (globalOptions, []string, error) {
	options := globalOptions{}
	args, options.jsonOutput = removeBoolFlag(args, "--json")
	flags := flag.NewFlagSet("wefty", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&options.fabricMode, "fabric", "plain", "fabric implementation: plain or tsnet")
	flags.StringVar(&options.l1Address, "l1", l3.DefaultL1Address, "L1 control-plane Fabric address")
	flags.StringVar(&options.l3Address, "l3", l3.DefaultL3Address, "L3 run-ledger Fabric address")
	flags.StringVar(&options.plainIdentity, "plain-identity", "wefty-cli", "plain Fabric identity node ID")
	flags.StringVar(&options.fabricName, "fabric-name", "wefty-cli", "tsnet logical node name")
	flags.StringVar(&options.stateDirectory, "state-dir", "", "tsnet state directory")
	flags.StringVar(&options.authKey, "auth-key", os.Getenv("TS_AUTHKEY"), "tsnet auth key")
	flags.StringVar(&options.controlURL, "control-url", os.Getenv("TS_CONTROL_URL"), "optional tsnet coordination URL")
	flags.BoolVar(&options.ephemeral, "ephemeral", false, "register an ephemeral tsnet node")
	flags.Usage = func() { fmt.Fprint(stderr, rootUsage) }
	if err := flags.Parse(args); err != nil {
		return globalOptions{}, nil, err
	}
	return options, flags.Args(), nil
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
  nodes list                 List node liveness and metadata
  submit                     Submit a saved or inline workflow
  rerun RUN_ID               Create a new run from a stored snapshot
  logs RUN_ID [--follow]     Read or follow run logs
  drain NODE_ID              Gracefully drain a node

Global flags:
  --fabric plain|tsnet
  --l1 ADDRESS
  --l3 ADDRESS
  --json
`

type usageError string

func (e usageError) Error() string { return string(e) }
