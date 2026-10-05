package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/Derek-X-Wang/wefty/l1"
)

func executeCancel(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout io.Writer) error {
	if len(args) != 1 || strings.TrimSpace(args[0]) == "" || strings.HasPrefix(args[0], "-") {
		return usageError("usage: wefty cancel JOB_ID")
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
