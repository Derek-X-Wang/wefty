package l3

import (
	"context"
	"database/sql"
	"math"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// General listings retain newest-first ordering and use the origin listing's
// keyset paging semantics: a cursor resumes strictly after the last row.
const (
	DefaultRunListLimit = 50
	MaxRunListLimit     = 500
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

// RunListPage is a bounded listing with an opaque continuation cursor.
type RunListPage struct {
	Runs       []RunSummary `json:"runs"`
	NextCursor string       `json:"next_cursor,omitempty"`
}

// RunListFilter narrows the listing. An empty filter is every run.
type RunListFilter struct {
	Status contract.RunState
	Limit  int
	Cursor string
	// Submitter is the exact immutable submitting actor, derived by the server.
	Submitter string
}

type runListCursor struct {
	Status    string `json:"status"`
	Submitter string `json:"submitter"`
	CreatedNS int64  `json:"created_ns"`
	RunID     string `json:"run_id"`
}

func decodeRunListCursor(value, status, submitter string) (runListCursor, error) {
	var cursor runListCursor
	if value == "" {
		return cursor, nil
	}
	if err := decodeRunCursor(value, &cursor); err != nil {
		return cursor, err
	}
	if cursor.Status != status || cursor.Submitter != submitter || cursor.CreatedNS < 0 || strings.TrimSpace(cursor.RunID) == "" {
		return runListCursor{}, protocolError(contract.ErrorInvalidRequest, "cursor is invalid for this Run list scope")
	}
	return cursor, nil
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
	cursor, err := decodeRunListCursor(filter.Cursor, status, filter.Submitter)
	if err != nil {
		return RunListPage{}, err
	}
	query, args := runListQuery(status, filter.Submitter, cursor, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
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
	if err := rows.Close(); err != nil {
		return RunListPage{}, internalError(err, "close Run listing")
	}
	if len(page.Runs) > limit {
		page.Runs = page.Runs[:limit]
		last := page.Runs[len(page.Runs)-1]
		page.NextCursor = encodeRunCursor(runListCursor{Status: status, Submitter: filter.Submitter, CreatedNS: last.CreatedAt.UnixNano(), RunID: last.RunID})
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

// runListQuery keeps every page an index range walk, including the head.
func runListQuery(status, submitter string, cursor runListCursor, limit int) (string, []any) {
	if cursor.RunID == "" {
		// Generated Run IDs are ASCII, so this tuple is above every Run,
		// including one at the largest representable creation timestamp.
		cursor.CreatedNS, cursor.RunID = math.MaxInt64, "\uffff"
	}
	query := `
SELECT r.run_id, r.parent_run_id, r.status, r.created_ns, r.updated_ns, r.started_ns, r.finished_ns,
       t.actor, t.source, t.source_run_id, t.computer_id, t.computer_attempt_id,
       t.computer_storage_generation, t.submit_intent_revision
`
	var args []any
	order := "r.created_ns DESC, r.run_id DESC"
	if submitter == "" {
		query += "FROM runs r CROSS JOIN run_triggers t ON t.run_id=r.run_id WHERE "
		if status != "" {
			query += "r.status=? AND "
			args = append(args, status)
		}
		query += "(r.created_ns, r.run_id) < (?, ?)"
	} else {
		// Trigger and Run creation times are written together. Drive the
		// actor-filtered walk from provenance so it seeks within that actor,
		// even with a status filter. CROSS JOIN preserves the driving table
		// rather than letting SQLite choose a scan and sort after ANALYZE.
		query += "FROM run_triggers t CROSS JOIN runs r ON r.run_id=t.run_id WHERE t.actor=? AND (t.created_ns, t.run_id) < (?, ?)"
		args = append(args, submitter)
		order = "t.created_ns DESC, t.run_id DESC"
	}
	args = append(args, cursor.CreatedNS, cursor.RunID)
	if submitter != "" && status != "" {
		query += " AND r.status=?"
		args = append(args, status)
	}
	query += " ORDER BY " + order + " LIMIT ?"
	args = append(args, limit+1)
	return query, args
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
