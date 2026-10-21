package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Read every page from the old installation's authorities, never only this
// node's resident attempts. A paused workflow or service backoff has no attempt.
func checkPlainConversionWork(ctx context.Context, host setupHost, profile connprofile.Profile) error {
	bounded, cancel := context.WithTimeout(ctx, upgradeMutationBudget)
	defer cancel()
	action := func(ctx context.Context, c *apiClients) error {
		// Ask the servers only for unfinished work, so the check stays inside its
		// budget however long the installation's history is.
		var blockers []string
		jobs := func(class string, state contract.JobState) error {
			seen := map[string]bool{}
			for cursor := ""; ; {
				query := url.Values{"limit": {"100"}, "class": {class}}
				if state != "" {
					query.Set("state", string(state))
				}
				if cursor != "" {
					query.Set("cursor", cursor)
				}
				var page l1.JobList
				if err := c.l1.do(ctx, http.MethodGet, "/v1/jobs?"+query.Encode(), nil, nil, &page, http.StatusOK); err != nil {
					return err
				}
				for _, job := range page.Jobs {
					if job.Spec.Class == contract.JobClassService && job.ServiceJob != nil && job.DesiredState == contract.ServiceDesiredRunning {
						blockers = append(blockers, "service job "+job.JobID)
					} else if job.Spec.Class == contract.JobClassOneShot && job.State == contract.JobQueued {
						blockers = append(blockers, "queued job "+job.JobID)
					}
				}
				if page.NextCursor == "" {
					return nil
				}
				if seen[page.NextCursor] {
					return errors.New("job inventory repeated a cursor; conversion refused")
				}
				seen[page.NextCursor], cursor = true, page.NextCursor
			}
		}
		if err := jobs(contract.JobClassOneShot, contract.JobQueued); err != nil {
			return err
		}
		if err := jobs(contract.JobClassService, ""); err != nil {
			return err
		}
		for _, status := range []contract.RunState{contract.RunPending, contract.RunDispatching, contract.RunQueued, contract.RunRunning, contract.RunAwaitingInput} {
			seen := map[string]bool{}
			for cursor := ""; ; {
				page, err := c.listRunsPage(ctx, string(status), 100, cursor, false)
				if err != nil {
					return err
				}
				for _, run := range page.Runs {
					if run.Status == status {
						blockers = append(blockers, "run "+run.RunID)
					}
				}
				if page.NextCursor == "" {
					break
				}
				if seen[page.NextCursor] {
					return errors.New("run inventory repeated a cursor; conversion refused")
				}
				seen[page.NextCursor], cursor = true, page.NextCursor
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		slices.Sort(blockers)
		blockers = slices.Compact(blockers)
		if len(blockers) > 0 {
			return &apiResponseError{APIError: contract.APIError{Code: contract.ErrorConflict, Message: "conversion blocked by unfinished work: " + strings.Join(blockers, ", ") + "; finish or cancel runs/jobs with wefty cancel, stop services with wefty services stop, then retry later"}}
		}
		return nil
	}
	if host.conversionClients == nil {
		return withUpgradeClients(bounded, profile, action)
	}
	clients, err := host.conversionClients(bounded, profile)
	if err != nil {
		return fmt.Errorf("unfinished work inventory unavailable: %w", err)
	}
	defer clients.close()
	return action(bounded, clients)
}
