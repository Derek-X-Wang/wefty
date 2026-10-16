package main

import (
	"errors"
	"io"
	"path/filepath"
	"strings"

	"github.com/Derek-X-Wang/wefty/internal/workflowhelper"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

// executeAuthoringCommand dispatches the two commands a workflow author uses
// from inside a job and at the desk: `wefty run ...`, which writes run-mailbox
// events, and `wefty workflow init`, which scaffolds a starter that uses them.
//
// Both are handled before the CLI opens a Fabric connection. That is the whole
// design of the run mailbox (docs/contracts/run-execution-context.md): a
// default job is dispatched without a credential and reports by writing files
// the node agent publishes. A `wefty run` that needed cluster identity, an
// endpoint or a token would be exactly the coupling the mailbox removed.
func executeAuthoringCommand(options globalOptions, args []string, stdout, stderr io.Writer) (bool, error) {
	switch args[0] {
	case "run":
		return true, asUsageError(workflowhelper.ExecuteRun(args[1:], options.jsonOutput, stdout, stderr))
	case "workflow":
		tag := ""
		if len(args) > 1 && args[1] == "init" && !options.jsonOutput {
			tag = workflowInitLocalTag(options.nodeConfigPath)
		}
		return true, asUsageError(workflowhelper.ExecuteWorkflowWithTag(args[1:], options.jsonOutput, stdout, tag))
	default:
		return false, nil
	}
}

// workflowInitLocalTag reads only installation metadata. Unlike exec's
// resolveLocalNode, this guidance must work offline, without checking L1.
func workflowInitLocalTag(path string) string {
	if path == "" {
		return ""
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return ""
	}
	config, err := ocicontrol.ReadInstalledConfig(path)
	if err != nil || strings.TrimSpace(config.NodeID) == "" ||
		strings.TrimSpace(config.NodeID) != config.NodeID || strings.ContainsAny(config.NodeID, "\r\n\t") {
		return ""
	}
	tags, err := pinnedImageTags(nil, config.NodeID, true)
	if err != nil {
		return ""
	}
	return tags[0]
}

// asUsageError gives an authoring mistake wefty's ordinary usage-error shape,
// so `--json` renders it as an invalid_request error like any other. The
// process exits 2, like usage errors on every other command surface.
func asUsageError(err error) error {
	var usage workflowhelper.UsageError
	if errors.As(err, &usage) {
		return usageError(usage.Error())
	}
	return err
}
