package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func executeCancel(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("cancel", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var includeScript bool
	flags.BoolVar(&includeScript, "include-script", false, "include inline script content in JSON output for a Run")
	if err := parseCommandFlags(flags, moveFirstPositionalToEnd(args), false); err != nil {
		return flagUsageError(err)
	}
	if flags.NArg() != 1 || strings.TrimSpace(flags.Arg(0)) == "" || strings.HasPrefix(flags.Arg(0), "-") {
		return usageError("usage: wefty cancel JOB_ID|RUN_ID [--include-script]")
	}
	args = flags.Args()

	kind, err := parseTargetID(args[0], targetJob)
	if err != nil {
		return err
	}
	if kind == targetSeat {
		return usageError("cancel received a Seat ID; cancel accepts Job IDs and Run IDs")
	}
	if kind == targetRun {
		if clients.l3.address == "" {
			return usageError("this command requires --l3, which is not configured")
		}
		var run contract.RunRecord
		if err := clients.l3.do(ctx, http.MethodPost, "/v1/runs/"+url.PathEscape(args[0])+"/cancel", nil, nil, &run, http.StatusOK); err != nil {
			return clients.cancelIdentityHint(err)
		}
		if jsonOutput {
			return writeJSON(stdout, newRunRecordJSON(run, includeScript))
		}
		_, err := fmt.Fprintf(stdout, "RUN ID\tSTATUS\tREASON\n%s\t%s\t%s\n", run.RunID, run.Status, run.FailureReason)
		return err
	}
	var job l1.Job
	if err := clients.l1.do(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(args[0])+"/cancel", nil, nil, &job, http.StatusOK); err != nil {
		return clients.cancelIdentityHint(err)
	}
	if jsonOutput {
		return writeJSON(stdout, job)
	}
	_, err = fmt.Fprintf(stdout, "JOB ID\tSTATE\tOUTCOME\n%s\t%s\t%s\n", job.JobID, job.State, job.Outcome)
	return err
}

func (clients *apiClients) cancelIdentityHint(err error) error {
	if !clients.participantProfile {
		return err
	}
	var response *apiResponseError
	if errors.As(err, &response) && (response.APIError.Code == contract.ErrorNotFound || response.APIError.Code == contract.ErrorForbidden) {
		// L1 deliberately hides another submitter's job behind not_found. The
		// conditional hint does not claim the target exists and never retries.
		return fmt.Errorf("%w; if this was submitted from another machine, cancel it there; this command uses this machine's client identity", err)
	}
	return err
}
