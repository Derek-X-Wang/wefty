package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

func executeLocalNode(ctx context.Context, options globalOptions, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usageError("usage: wefty node setup-oci | wefty node oci start|stop | wefty node load-image FILE")
	}
	if handled, err := maybeExecutePrivilegedLinuxSetup(ctx, options, args, stdout, stderr); handled {
		return err
	}
	if options.nodeConfigPath == "" {
		return errors.New("singular node commands require an installed node configuration; set --node-config or WEFTY_NODE_CONFIG")
	}
	configPath, err := filepath.Abs(options.nodeConfigPath)
	if err != nil {
		return err
	}
	config, err := ocicontrol.ReadInstalledConfig(configPath)
	if err != nil {
		return err
	}
	client, err := ocicontrol.NewClient(config.ControlSocket)
	if err != nil {
		return err
	}
	defer client.Close()
	switch args[0] {
	case "setup-oci":
		return executeLocalSetupOCI(ctx, client, options.jsonOutput, args[1:], stdout, stderr)
	case "oci":
		return executeLocalOCIIntent(ctx, client, options.jsonOutput, args[1:], stdout)
	case "load-image":
		return executeLocalLoadImage(ctx, client, options.jsonOutput, args[1:], stdout)
	default:
		return usageError(fmt.Sprintf("unknown singular node command %q", args[0]))
	}
}

func executeLocalSetupOCI(ctx context.Context, client *ocicontrol.Client, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("node setup-oci", flag.ContinueOnError)
	flags.SetOutput(stderr)
	defaults, err := lima.HostDefaultSizing()
	if err != nil {
		return err
	}
	sizing := lima.BindSizingFlags(flags, defaults)
	applyRestart := flags.Bool("apply-restart", false, "apply restart-required Lima template changes")
	recreate := flags.Bool("recreate", false, "apply recreate-required Lima template changes with zero live OCI attempts")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return usageError("wefty node setup-oci does not accept positional arguments")
	}
	resolved, err := sizing.Sizing()
	if err != nil {
		return err
	}
	response, err := client.Setup(ctx, ocicontrol.SetupRequest{
		VMMemory: resolved.Memory, VMCPUs: resolved.CPUs, VMDisk: resolved.Disk,
		ApplyRestart: *applyRestart, Recreate: *recreate,
	})
	if err != nil {
		return err
	}
	if jsonOutput {
		return writeJSON(stdout, response)
	}
	if response.MissingCapability != "" {
		_, err = fmt.Fprintf(stdout, "OCI setup not applied: missing %s; see %s\n", response.MissingCapability, response.Runbook)
		return err
	}
	_, err = fmt.Fprintf(stdout, "OCI setup configured (%s); intent enabled=%t revision=%d; probe preloaded=%t; restart applied=%t; recreate applied=%t; reason=%s\n",
		response.Convergence, response.Intent.Enabled, response.Intent.Revision, response.ProbePreloaded,
		response.RestartApplied, response.RecreateApplied, response.ReasonCode)
	return err
}

func executeLocalOCIIntent(ctx context.Context, client *ocicontrol.Client, jsonOutput bool, args []string, stdout io.Writer) error {
	if len(args) != 1 || args[0] != "start" && args[0] != "stop" {
		return usageError("usage: wefty node oci start | wefty node oci stop")
	}
	current, err := client.Intent(ctx)
	if err != nil {
		return err
	}
	var response ocicontrol.IntentResponse
	if args[0] == "start" {
		response, err = client.Start(ctx, current.Revision)
	} else {
		response, err = client.Stop(ctx, current.Revision)
	}
	if err != nil {
		return err
	}
	if jsonOutput {
		return writeJSON(stdout, response)
	}
	_, err = fmt.Fprintf(stdout, "OCI intent enabled=%t revision=%d capability_published=%t runtime_quiesced=%t\n",
		response.Intent.Enabled, response.Intent.Revision, response.CapabilityPublished, response.RuntimeQuiesced)
	return err
}

func executeLocalLoadImage(ctx context.Context, client *ocicontrol.Client, jsonOutput bool, args []string, stdout io.Writer) error {
	if len(args) != 1 {
		return usageError("usage: wefty node load-image FILE")
	}
	path, err := filepath.Abs(args[0])
	if err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("inspect OCI image archive: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("OCI image archive must be a regular file")
	}
	response, err := client.LoadImage(ctx, path)
	if err != nil {
		return err
	}
	if jsonOutput {
		return writeJSON(stdout, response)
	}
	_, err = fmt.Fprintf(stdout, "TOP-LEVEL DIGEST\t%s\nPLATFORM DIGEST\t%s\n", response.TopLevelDigest, response.PlatformDigest)
	return err
}
