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
	"github.com/Derek-X-Wang/wefty/internal/conversioninventory"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Read every page from the old installation's authorities, never only this
// node's resident attempts. A paused workflow or service backoff has no attempt.
func checkPlainConversionWork(ctx context.Context, host setupHost, profile connprofile.Profile) error {
	return plainConversionWorkInventory(ctx, host, profile, false)
}

func checkPlainConversionServices(ctx context.Context, host setupHost, profile connprofile.Profile) error {
	return plainConversionWorkInventory(ctx, host, profile, true)
}

func plainConversionWorkInventory(ctx context.Context, host setupHost, profile connprofile.Profile, servicesOnly bool) error {
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
					var desired contract.ServiceDesiredState
					if job.ServiceJob != nil {
						desired = job.DesiredState
					}
					if blocker := conversioninventory.JobBlocker(job.JobID, job.Spec.Class, job.State, desired); blocker != "" {
						blockers = append(blockers, blocker)
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
		if !servicesOnly {
			for _, state := range []contract.JobState{contract.JobQueued, contract.JobClaimed} {
				if err := jobs(contract.JobClassOneShot, state); err != nil {
					return err
				}
			}
		}
		if err := jobs(contract.JobClassService, ""); err != nil {
			return err
		}
		if servicesOnly {
			return refusePlainConversionWork(blockers)
		}
		for _, status := range conversioninventory.UnfinishedRunStates() {
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
		return refusePlainConversionWork(blockers)
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

const plainConversionComputerAdvice = "stop or retire these through their normal lifecycle, then retry later"

func refusePlainConversionWork(blockers []string) error {
	slices.Sort(blockers)
	blockers = slices.Compact(blockers)
	if len(blockers) > 0 {
		var computers, work bool
		for _, blocker := range blockers {
			if strings.HasPrefix(blocker, "Computer ") {
				computers = true
			} else {
				work = true
			}
		}
		var advice []string
		if computers {
			advice = append(advice, plainConversionComputerAdvice)
		}
		if work {
			advice = append(advice, "finish or cancel runs/jobs with wefty cancel, stop services with wefty services stop, then retry later")
		}
		return &apiResponseError{APIError: contract.APIError{Code: contract.ErrorConflict, Message: "conversion blocked by unfinished work: " + strings.Join(blockers, ", ") + "; " + strings.Join(advice, "; ")}}
	}
	return nil
}
