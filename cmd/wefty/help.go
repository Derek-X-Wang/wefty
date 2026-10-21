package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/Derek-X-Wang/wefty/internal/workflowhelper"
)

// commandHelp preserves ErrHelp and its usage until run can write it to stdout.
// It never reaches the error reporter or its typed exit-code table.
type commandHelp struct{ usage string }

func (h *commandHelp) Error() string { return flag.ErrHelp.Error() }
func (h *commandHelp) Unwrap() error { return flag.ErrHelp }

// parseCommandFlags captures help even for flag sets whose ordinary diagnostics
// are discarded (including JSON mode). Real errors keep their original output.
func parseCommandFlags(flags *flag.FlagSet, args []string, root bool) error {
	output := flags.Output()
	var diagnostics bytes.Buffer
	flags.SetOutput(&diagnostics)
	if !root {
		args = helpBeforePositionals(flags, args)
	}
	err := flags.Parse(args)
	flags.SetOutput(output)
	if errors.Is(err, flag.ErrHelp) {
		return &commandHelp{usage: diagnostics.String()}
	}
	_, _ = io.Copy(output, &diagnostics)
	return err
}

// Go's flag parser stops at the first positional argument. An explicit help
// flag after a resource ID still asks for usage. Move it before positionals,
// while preserving value-taking flags, --, and errors in preceding flags.
func helpBeforePositionals(flags *flag.FlagSet, args []string) []string {
	var flagArgs, positionals []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			return args
		}
		if isHelpFlag(arg) {
			if len(positionals) == 0 {
				return args
			}
			flagArgs = append(flagArgs, arg)
			flagArgs = append(flagArgs, positionals...)
			return append(flagArgs, args[i+1:]...)
		}
		if arg == "-" || !strings.HasPrefix(arg, "-") {
			positionals = append(positionals, arg)
			continue
		}
		flagArgs = append(flagArgs, arg)
		name, _, hasValue := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		if registered := flags.Lookup(name); registered != nil && !hasValue {
			boolean, ok := registered.Value.(interface{ IsBoolFlag() bool })
			if (!ok || !boolean.IsBoolFlag()) && i+1 < len(args) {
				i++
				flagArgs = append(flagArgs, args[i])
			}
		}
	}
	return args
}

func flagUsageError(err error) error {
	if errors.Is(err, flag.ErrHelp) {
		return err
	}
	return usageError(err.Error())
}

// Match flag.Parse's implicit help: h/help with one or two dashes, even
// with an attached value. In particular, --help=false still requests help.
func isHelpFlag(arg string) bool {
	if strings.HasPrefix(arg, "--") {
		arg = arg[2:]
	} else if strings.HasPrefix(arg, "-") {
		arg = arg[1:]
	} else {
		return false
	}
	name, _, _ := strings.Cut(arg, "=")
	return name == "h" || name == "help"
}

// A help-looking value or an argument after -- is data. Use the same audited
// value-flag vocabulary as global JSON extraction to keep that distinction.
func wantsCommandHelp(args []string) bool {
	valueFlags := make(map[string]bool)
	for _, name := range strings.Fields(jsonValueFlags) {
		valueFlags[name] = true
	}
	var command []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			break
		}
		if isHelpFlag(arg) {
			return true
		}
		if flagArgumentTakesValue(arg, command, valueFlags) && i+1 < len(args) {
			i++
		} else if !strings.HasPrefix(arg, "-") && len(command) < 2 {
			command = append(command, arg)
		}
	}
	return false
}

// These commands have no flag parser. Their help must be answered before a
// positional --help can become a resource ID or a request to open local state.
var positionalCommandUsage = map[string]string{
	"whoami":                          "usage: wefty whoami",
	"admin bootstrap":                 "usage: wefty admin bootstrap NONCE",
	"admin policy get":                "usage: wefty admin policy get",
	"admins get":                      "usage: wefty admins get",
	"admins list":                     "usage: wefty admins list",
	"node doctor":                     "usage: wefty node doctor [--print-policy]",
	"node oci start":                  "usage: wefty node oci start",
	"node oci stop":                   "usage: wefty node oci stop",
	"node oci removals":               "usage: wefty node oci removals",
	"nodes inspect":                   "usage: wefty nodes inspect NODE_ID",
	"services status":                 "usage: wefty services status JOB_ID|COMPUTER",
	"services grants":                 "usage: wefty services grants COMPUTER",
	"services takeover sessions":      "usage: wefty services takeover sessions COMPUTER",
	"services takeover sessions list": "usage: wefty services takeover sessions list COMPUTER",
	"services backup list":            "usage: wefty services backup list COMPUTER",
}

// Group help lists the command vocabulary already maintained by rootUsage.
var commandHelpGroups = map[string]bool{
	"admin": true, "admin policy": true, "admins": true,
	"node": true, "node oci": true, "nodes": true,
	"jobs": true, "computers": true, "runs": true, "services": true,
	"services submission": true, "services takeover": true,
	"services backup": true, "services custody": true,
}

// Help uses the normal dispatch and flag registrations, but needs neither a
// Fabric identity nor installed node state. Every flag-bearing command returns
// at Parse, before touching the nil client; commands without flags are above.
func executeCommandHelp(ctx context.Context, options globalOptions, args []string, stdout, stderr io.Writer) error {
	if args[0] == "doctor" {
		return executeDoctor(ctx, options, args[1:], stdout, stderr)
	}
	// The authoring helpers recognize fewer spellings than flag.Parse. The
	// routing scan has already excluded flag values and arguments after --.
	if args[0] == "run" {
		_, err := io.WriteString(stdout, workflowhelper.RunUsage)
		return err
	}
	if args[0] == "workflow" {
		_, err := io.WriteString(stdout, workflowhelper.WorkflowUsage)
		return err
	}
	for depth := len(args) - 1; depth > 0; depth-- {
		path := strings.Join(args[:depth], " ")
		if usage, ok := positionalCommandUsage[path]; ok {
			for _, arg := range args[depth:] {
				if isHelpFlag(arg) {
					break
				}
				if strings.HasPrefix(arg, "-") {
					return usageError(usage)
				}
			}
			_, err := fmt.Fprintln(stdout, usage)
			return err
		}
		if commandHelpGroups[path] && isHelpFlag(args[depth]) {
			_, err := io.WriteString(stdout, rootUsage)
			return err
		}
	}
	if args[0] == "node" && len(args) > 1 {
		if handled, err := maybeExecutePrivilegedLinuxSetup(ctx, options, args[1:], stdout, stderr); handled {
			return err
		}
		switch args[1] {
		case "setup-oci":
			return executeLocalSetupOCI(ctx, nil, options.jsonOutput, args[2:], stdout, stderr)
		case "load-image":
			return executeLocalLoadImage(ctx, nil, options.jsonOutput, args[2:], stdout, stderr)
		default:
			return usageError(fmt.Sprintf("unknown singular node command %q", args[1]))
		}
	}
	return execute(ctx, nil, options.jsonOutput, args, stdout, stderr)
}
