package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func executeCancel(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout io.Writer) error {
	if len(args) != 1 || strings.TrimSpace(args[0]) == "" || strings.HasPrefix(args[0], "-") {
		return usageError("usage: wefty cancel JOB_ID|RUN_ID")
	}

	if strings.HasPrefix(args[0], "run_") || strings.HasPrefix(args[0], "run-") {
		if clients.l3.address == "" {
			return usageError("this command requires --l3, which is not configured")
		}
		var run contract.RunRecord
		if err := clients.l3.do(ctx, http.MethodPost, "/v1/runs/"+url.PathEscape(args[0])+"/cancel", nil, nil, &run, http.StatusOK); err != nil {
			return err
		}
		if jsonOutput {
			return writeJSON(stdout, run)
		}
		_, err := fmt.Fprintf(stdout, "RUN ID\tSTATUS\tREASON\n%s\t%s\t%s\n", run.RunID, run.Status, run.FailureReason)
		return err
	}
	var job l1.Job
	if err := clients.l1.do(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(args[0])+"/cancel", nil, nil, &job, http.StatusOK); err != nil {
		return err
	}
	if jsonOutput {
		return writeJSON(stdout, job)
	}
	_, err := fmt.Fprintf(stdout, "JOB ID\tSTATE\tOUTCOME\n%s\t%s\t%s\n", job.JobID, job.State, job.Outcome)
	return err
}
