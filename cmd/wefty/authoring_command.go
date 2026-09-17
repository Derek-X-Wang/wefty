package main

import (
	"errors"
	"io"

	"github.com/Derek-X-Wang/wefty/internal/workflowhelper"
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
		return true, asUsageError(workflowhelper.ExecuteWorkflow(args[1:], options.jsonOutput, stdout))
	default:
		return false, nil
	}
}

// asUsageError gives an authoring mistake the same exit-2 contract every other
// wefty usage error has.
func asUsageError(err error) error {
	var usage workflowhelper.UsageError
	if errors.As(err, &usage) {
		return usageError(usage.Error())
	}
	return err
}
