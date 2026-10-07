package main

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

// A run nothing can take used to be invisible (#604): `--tag=no-such-tag` was
// accepted, `status` said ready, `runs list` said queued, and `logs --follow`
// said nothing at all. Accepting it is still right -- a node may join later,
// and the ledger is not the place to decide it never will -- but every surface
// a person looks at next now says so.
//
// Nothing here is a second scheduler. The submit warning compares the run's
// routing with the node list L1 already publishes; the queued-run note is L1's
// own unschedulable reason, read through the run's execution view.

// routingWarning is the submit-time check. An answer that cannot be read -- no
// node list, or an identity that may not list nodes -- is no warning, never a
// guess.
func routingWarning(ctx context.Context, clients *apiClients, kind string, tags []string) string {
	if clients == nil || clients.l1 == nil || clients.l1.client == nil {
		return ""
	}
	probeCtx, cancel := context.WithTimeout(ctx, statusProbeBudget)
	defer cancel()
	nodes, err := clients.listNodes(probeCtx)
	if err != nil {
		return ""
	}
	return routingWarningFor(nodes.Nodes, kind, tags)
}

// routingWarningFor is the rule: some alive node advertises the job's kind and
// carries every routing tag. Claim eligibility and free slots are left out on
// purpose -- they change by the minute and are `wefty status`'s answer, not a
// reason to call a run unroutable.
func routingWarningFor(nodes []l1.Node, kind string, tags []string) string {
	wanted := l1NormalizedTags(tags)
	for _, node := range nodes {
		if node.State != contract.NodeAlive || !node.Capabilities["kind:"+kind] {
			continue
		}
		carried := l1NormalizedTags(node.AuthoritativeTags)
		if !slices.ContainsFunc(wanted, func(tag string) bool { return !slices.Contains(carried, tag) }) {
			return ""
		}
	}
	routing := "no routing tags"
	if len(wanted) > 0 {
		routing = "tags " + strings.Join(wanted, ",")
	}
	return fmt.Sprintf("no alive node can run kind=%s with %s right now; the run was accepted and stays queued "+
		"until one joins (see `wefty nodes list`)", kind, routing)
}

// runKind is the L1 kind a submission becomes: an image program is kind=oci,
// everything else runs as a process.
func runKind(image *contract.ImageProgram) string {
	if image != nil {
		return contract.JobKindOCI
	}
	return contract.JobKindProcess
}

// queuedRunIsWaiting reports whether a run has not been started by any node.
func queuedRunIsWaiting(status contract.RunState) bool {
	switch status {
	case contract.RunPending, contract.RunDispatching, contract.RunQueued:
		return true
	}
	return false
}

// unschedulableReason is L1's own statement that no tag-eligible node can run
// this queued run, or empty when L1 has none. The error says it could not be
// asked, so a caller asking about several runs can stop at the first.
func unschedulableReason(ctx context.Context, clients *apiClients, runID string) (string, error) {
	probeCtx, cancel := context.WithTimeout(ctx, statusProbeBudget)
	defer cancel()
	execution, err := clients.getRunExecution(probeCtx, runID)
	if err != nil {
		return "", err
	}
	if execution.Job == nil || execution.Job.State != contract.JobQueued {
		return "", nil
	}
	return execution.Job.UnschedulableReason, nil
}

// queuedRunReasons asks about each queued run under one shared budget and
// stops at the first read that fails or runs out. It is an annotation on a
// listing that already answered: an unreachable L1 must cost a listing at
// most one probe budget, never one per queued run.
func queuedRunReasons(ctx context.Context, clients *apiClients, runIDs []string) map[string]string {
	budgetCtx, cancel := context.WithTimeout(ctx, statusProbeBudget)
	defer cancel()
	reasons := make(map[string]string, len(runIDs))
	for _, runID := range runIDs {
		reason, err := unschedulableReason(budgetCtx, clients, runID)
		if err != nil {
			break
		}
		if reason != "" {
			reasons[runID] = reason
		}
	}
	return reasons
}

// maxAnnotatedQueuedRuns bounds how many queued runs one listing asks about.
// Each is one execution read; a listing is a screenful, and the head of the
// queue is where a stuck run shows.
const maxAnnotatedQueuedRuns = 20

// runListingRow is one `runs list --json` row: the ledger's summary, plus L1's
// reason when a queued run has no node that could take it.
type runListingRow struct {
	l3.RunSummary
	UnschedulableReason string `json:"unschedulable_reason,omitempty"`
}

type runListingPage struct {
	Runs       []runListingRow `json:"runs"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

func annotateRunListing(ctx context.Context, clients *apiClients, page l3.RunListPage) runListingPage {
	var queued []string
	for _, run := range page.Runs {
		if run.Status == contract.RunQueued && len(queued) < maxAnnotatedQueuedRuns {
			queued = append(queued, run.RunID)
		}
	}
	reasons := queuedRunReasons(ctx, clients, queued)
	annotated := runListingPage{Runs: make([]runListingRow, 0, len(page.Runs)), NextCursor: page.NextCursor}
	for _, run := range page.Runs {
		annotated.Runs = append(annotated.Runs, runListingRow{RunSummary: run, UnschedulableReason: reasons[run.RunID]})
	}
	return annotated
}
