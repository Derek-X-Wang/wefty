package l3

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// The general Run listing answers "what is happening on this fabric right now".
//
// The existing listing is scoped to one Computer's origin, because that is a
// membership question with a cursor and a generation. This one is the operator's
// view: the most recent runs, newest first, optionally narrowed to a status. It
// deliberately has no cursor. A person or an agent asking what is running wants
// the head of the list, and a page size is the whole of that request; a cursor
// would be a second, unused way to walk the same table.

const (
	// DefaultRunListLimit is what `wefty runs list` asks for when nothing is
	// said. It is a screenful of recent work, not a page of a pager.
	DefaultRunListLimit = 50
	// MaxRunListLimit bounds one listing. Past it a caller wants a query, not
	// a list, and should say which runs it means.
	MaxRunListLimit = 500
)

// RunSummary is one row of the listing: what a reader needs to recognise a run
// and decide whether to open it.
type RunSummary struct {
	RunID       string            `json:"run_id"`
	ParentRunID string            `json:"parent_run_id,omitempty"`
	Status      contract.RunState `json:"status"`
	Trigger     contract.Trigger  `json:"trigger"`
	// CurrentStep is the step the run is in now, derived from its own step
	// envelopes. It is empty for a run that reported none and for one whose
	// steps have all ended.
	CurrentStep string     `json:"current_step,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
}

// RunListPage is one listing. It carries no cursor for the reason above.
type RunListPage struct {
	Runs []RunSummary `json:"runs"`
}

// RunListFilter narrows the listing. An empty filter is every run.
type RunListFilter struct {
	Status contract.RunState
	Limit  int
}

// ListRuns returns the most recent runs, newest first.
func (s *Store) ListRuns(ctx context.Context, filter RunListFilter) (RunListPage, error) {
	limit := filter.Limit
	if limit == 0 {
		limit = DefaultRunListLimit
	}
	if limit < 1 || limit > MaxRunListLimit {
		return RunListPage{}, protocolError(contract.ErrorInvalidRequest,
			"limit must be between 1 and %d", MaxRunListLimit)
	}
	status := strings.TrimSpace(string(filter.Status))
	if status != "" && !validRunState(contract.RunState(status)) {
		return RunListPage{}, protocolError(contract.ErrorInvalidRequest,
			"status %q is not a Run state", status)
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT r.run_id, r.parent_run_id, r.status, r.created_ns, r.updated_ns, r.started_ns, r.finished_ns,
       t.actor, t.source, t.source_run_id, t.computer_id, t.computer_attempt_id,
       t.computer_storage_generation, t.submit_intent_revision
FROM runs r JOIN run_triggers t ON t.run_id=r.run_id
WHERE (? = '' OR r.status = ?)
ORDER BY r.created_ns DESC, r.run_id DESC
LIMIT ?`, status, status, limit)
	if err != nil {
		return RunListPage{}, internalError(err, "list Runs")
	}
	defer rows.Close()
	page := RunListPage{Runs: []RunSummary{}}
	for rows.Next() {
		var summary RunSummary
		var parent, sourceRun, computerID, computerAttemptID sql.NullString
		var computerStorageGeneration, submitIntentRevision sql.NullInt64
		var createdNS, updatedNS int64
		var startedNS, finishedNS sql.NullInt64
		if err := rows.Scan(&summary.RunID, &parent, &summary.Status, &createdNS, &updatedNS,
			&startedNS, &finishedNS, &summary.Trigger.Principal, &summary.Trigger.Type, &sourceRun,
			&computerID, &computerAttemptID, &computerStorageGeneration, &submitIntentRevision); err != nil {
			return RunListPage{}, internalError(err, "scan Run summary")
		}
		summary.ParentRunID = parent.String
		summary.Trigger.SourceRunID = sourceRun.String
		summary.Trigger.ComputerID = computerID.String
		summary.Trigger.ComputerAttemptID = computerAttemptID.String
		summary.Trigger.ComputerStorageGeneration = computerStorageGeneration.Int64
		summary.Trigger.SubmitIntentRevision = submitIntentRevision.Int64
		summary.CreatedAt = time.Unix(0, createdNS).UTC()
		summary.UpdatedAt = time.Unix(0, updatedNS).UTC()
		if startedNS.Valid {
			started := time.Unix(0, startedNS.Int64).UTC()
			summary.StartedAt = &started
		}
		if finishedNS.Valid {
			finished := time.Unix(0, finishedNS.Int64).UTC()
			summary.FinishedAt = &finished
		}
		page.Runs = append(page.Runs, summary)
	}
	if err := rows.Err(); err != nil {
		return RunListPage{}, internalError(err, "read Run listing")
	}
	// The current step is derived per run rather than joined, because it is a
	// reading of an append-only log and not a column. A terminal run is not
	// asked: it is in no step, and reading its envelopes to learn that would
	// be work for an answer already known.
	for index := range page.Runs {
		if terminalRunState(page.Runs[index].Status) {
			continue
		}
		envelopes, err := s.ListEnvelopes(ctx, page.Runs[index].RunID)
		if err != nil {
			return RunListPage{}, err
		}
		page.Runs[index].CurrentStep = DeriveRunSteps(envelopes).Current
	}
	return page, nil
}

// terminalRunState is the ledger's own definition: a state with nowhere left to
// go. It is read from the transition table rather than restated, so a state
// added there is terminal here without anyone remembering to say so.
func terminalRunState(status contract.RunState) bool {
	return len(contract.RunTransitions[status]) == 0
}

func validRunState(status contract.RunState) bool {
	_, known := contract.RunTransitions[status]
	return known
}
