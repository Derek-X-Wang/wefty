package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/Derek-X-Wang/wefty/l1"
)

type targetIDKind string

const (
	targetRun  targetIDKind = "run"
	targetJob  targetIDKind = "job"
	targetSeat targetIDKind = "seat"
)

// Old Run/Job handles (including hyphenated IDs) remain readable. Every
// underscore-prefixed ID declares its kind and must never use that fallback.
func parseTargetID(id string, legacy targetIDKind) (targetIDKind, error) {
	if id == "" || strings.HasPrefix(id, "-") {
		return "", usageError("an ID is required")
	}
	prefix, _, typed := strings.Cut(id, "_")
	if !typed {
		if strings.HasPrefix(id, "run-") {
			return targetRun, nil
		}
		if strings.HasPrefix(id, "job-") {
			return targetJob, nil
		}
		return legacy, nil
	}
	switch targetIDKind(prefix) {
	case targetRun, targetJob, targetSeat:
		return targetIDKind(prefix), nil
	}
	return "", usageError(fmt.Sprintf("unknown ID kind %q in %q; expected run_, job_ or seat_", prefix, id))
}

// resolveJobRun reads the Run a Job belongs to from its run_id label, so a
// command that shows a Run can take the Job ID a drain or status message
// names (#1145, #1200). A Job ID is never sent to L3 as a Run ID.
func resolveJobRun(ctx context.Context, clients *apiClients, jobID string) (string, error) {
	var job l1.Job
	if err := clients.l1.do(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(jobID), nil, nil, &job, http.StatusOK); err != nil {
		return "", err
	}
	runID := strings.TrimSpace(job.Spec.Labels["run_id"])
	if runID == "" {
		return "", usageError(fmt.Sprintf("%s is a Job ID; its Run cannot be resolved because the Job has no run_id label", jobID))
	}
	return runID, nil
}
