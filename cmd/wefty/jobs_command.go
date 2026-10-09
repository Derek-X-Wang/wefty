package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"text/tabwriter"

	"github.com/Derek-X-Wang/wefty/l1"
)

func executeJobs(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "list" {
		return usageError("usage: wefty jobs list [--class one-shot|service] [--kind KIND] [--state STATE] [--submitter me] [--cursor CURSOR] [--limit N]")
	}
	flags := flag.NewFlagSet("jobs list", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	query := url.Values{}
	var class, kind, state, submitter, cursor string
	var limit int
	flags.StringVar(&class, "class", "", "job class: one-shot or service")
	flags.StringVar(&kind, "kind", "", "exact workload kind")
	flags.StringVar(&state, "state", "", "exact persisted job state")
	flags.StringVar(&submitter, "submitter", "", "me: jobs with your originating submitter identity")
	flags.StringVar(&cursor, "cursor", "", "opaque cursor from the previous page")
	flags.IntVar(&limit, "limit", l1.DefaultJobPageLimit, "jobs per page")
	if err := flags.Parse(args[1:]); err != nil {
		return usageError(err.Error())
	}
	if flags.NArg() != 0 {
		return usageError("jobs list does not accept positional arguments")
	}
	if limit < 1 || limit > l1.MaxJobPageLimit {
		return usageError(fmt.Sprintf("--limit must be between 1 and %d", l1.MaxJobPageLimit))
	}
	for name, value := range map[string]string{"class": class, "kind": kind, "state": state, "submitter": submitter, "cursor": cursor} {
		if value != "" {
			query.Set(name, value)
		}
	}
	query.Set("limit", strconv.Itoa(limit))
	var page l1.JobList
	if err := clients.l1.do(ctx, http.MethodGet, "/v1/jobs?"+query.Encode(), nil, nil, &page, http.StatusOK); err != nil {
		return err
	}
	if jsonOutput {
		return writeJSON(stdout, page)
	}
	table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "JOB ID\tCLASS\tKIND\tSTATE\tSTATUS\tSUBMITTER"); err != nil {
		return err
	}
	for _, job := range page.Jobs {
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\n", job.JobID, job.Spec.Class, job.Spec.Kind, job.State, job.Status, job.OriginatingSubmitter); err != nil {
			return err
		}
	}
	if err := table.Flush(); err != nil {
		return err
	}
	if page.NextCursor != "" {
		_, err := fmt.Fprintf(stdout, "NEXT CURSOR\t%s\n", page.NextCursor)
		return err
	}
	return nil
}
