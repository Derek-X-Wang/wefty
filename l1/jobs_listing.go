package l1

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
)

// All scope predicates are applied before LIMIT, never after paging.
type jobListFilters struct {
	Class       string
	Kind        string
	State       string
	Submitter   string
	ParentJobID string
}

func parseJobListFilters(r *http.Request) (jobListFilters, error) {
	query := r.URL.Query()
	for _, name := range []string{"class", "kind", "state", "submitter", "cursor", "limit"} {
		if values, present := query[name]; present && (len(values) != 1 || (values[0] == "" && name != "cursor" && name != "limit")) {
			return jobListFilters{}, protocolError(contract.ErrorInvalidRequest, "%s must have one non-empty value", name)
		}
	}
	filters := jobListFilters{Class: query.Get("class"), Kind: query.Get("kind"), State: query.Get("state")}
	if filters.Class != "" && filters.Class != contract.JobClassOneShot && filters.Class != contract.JobClassService {
		return jobListFilters{}, protocolError(contract.ErrorInvalidRequest, "class must be one-shot or service")
	}
	if filters.State != "" {
		if _, valid := contract.JobTransitions[contract.JobState(filters.State)]; !valid {
			return jobListFilters{}, protocolError(contract.ErrorInvalidRequest, "state must be a persisted job state")
		}
	}
	scope := attemptCredentialFromRequest(r)
	filters.ParentJobID = scope.JobID
	if submitter := query.Get("submitter"); submitter != "" {
		if submitter != "me" {
			return jobListFilters{}, protocolError(contract.ErrorInvalidRequest, "submitter must be me")
		}
		filters.Submitter = identityFromRequest(r).NodeID
		if scope.JobID != "" {
			filters.Submitter = scope.OriginatingSubmitter
		}
		if filters.Submitter == "" {
			return jobListFilters{}, protocolError(contract.ErrorInvalidRequest, "submitter=me requires a resolved submitter identity")
		}
	}
	return filters, nil
}

// The insertion watermark fixes collection membership for a walk, including
// when new jobs share a timestamp or the clock moves backwards. Creation/ID
// keyset ordering preserves the existing service listing order. State and
// projections are live reads; this is not a snapshot of mutable job state.
type jobCollectionCursor struct {
	Version   int    `json:"v"`
	HighWater int64  `json:"high_water"`
	CreatedNS int64  `json:"created_ns"`
	JobID     string `json:"job_id"`
	Filters   string `json:"filters"`
}

func jobFilterFingerprint(filters jobListFilters) string {
	payload, _ := json.Marshal(filters)
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func decodeJobCollectionCursor(value string, filters jobListFilters) (jobCollectionCursor, error) {
	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return jobCollectionCursor{}, protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	var cursor jobCollectionCursor
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil {
		return jobCollectionCursor{}, protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return jobCollectionCursor{}, protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	// Accept previously issued service cursors for the unchanged service query.
	if cursor.Version == 0 && filters == (jobListFilters{Class: contract.JobClassService}) {
		old, err := decodeServiceJobCursor(value)
		if err == nil {
			return jobCollectionCursor{CreatedNS: old.CreatedNS, JobID: old.JobID}, nil
		}
	}
	if cursor.Version != 1 || cursor.HighWater < 1 || cursor.JobID == "" || cursor.Filters != jobFilterFingerprint(filters) {
		return jobCollectionCursor{}, protocolError(contract.ErrorInvalidRequest, "cursor is invalid or does not match the listing filters and scope")
	}
	return cursor, nil
}

// Job insertion order is durable and never reused after removal or VACUUM.
// The trigger covers every creation path in the job's own transaction, while
// the foreign key removes per-job metadata with the ordinary job row.
func (s *Store) initializeJobListing(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "begin job listing migration")
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
CREATE INDEX IF NOT EXISTS jobs_listing_creation_order ON jobs(created_ns, job_id);
CREATE TABLE IF NOT EXISTS job_listing_order (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
 job_id TEXT NOT NULL UNIQUE REFERENCES jobs(job_id) ON DELETE CASCADE
);
CREATE TRIGGER IF NOT EXISTS jobs_listing_insert AFTER INSERT ON jobs BEGIN
 INSERT INTO job_listing_order(job_id) VALUES(NEW.job_id);
END;
INSERT INTO job_listing_order(job_id)
 SELECT jobs.job_id FROM jobs
 WHERE NOT EXISTS (SELECT 1 FROM job_listing_order WHERE job_listing_order.job_id=jobs.job_id)
 ORDER BY jobs.created_ns, jobs.job_id;
`)
	if err != nil {
		return internalError(err, "initialize job insertion order")
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit job listing migration")
	}
	return nil
}

func (s *Store) listReadableJobs(ctx context.Context, filters jobListFilters, cursorValue string, limit int) (JobList, error) {
	return s.listReadableJobsForCaller(ctx, filters, cursorValue, limit, nil)
}

func (s *Store) listReadableJobsForCaller(ctx context.Context, filters jobListFilters, cursorValue string, limit int, actor *serviceActionActor) (page JobList, err error) {
	err = s.withReadSnapshot(ctx, actor, func(ctx context.Context, reads readModel) error {
		page, err = reads.jobsPage(ctx, filters, cursorValue, limit)
		return err
	})
	return
}

func (r *databaseReads) jobsPage(ctx context.Context, filters jobListFilters, cursorValue string, limit int) (JobList, error) {
	if limit < 1 {
		return JobList{}, protocolError(contract.ErrorInvalidRequest, "limit must be positive")
	}
	limit = min(limit, MaxJobListingPageLimit)
	cursor := jobCollectionCursor{Version: 1, Filters: jobFilterFingerprint(filters)}
	if cursorValue != "" {
		var err error
		cursor, err = decodeJobCollectionCursor(cursorValue, filters)
		if err != nil {
			return JobList{}, err
		}
	}
	if err := validateReadCredential(ctx, r); err != nil {
		return JobList{}, err
	}
	if _, err := r.eligibleNodeIDs(ctx, nil); err != nil {
		return JobList{}, internalError(err, "load page node facts")
	}
	if cursor.HighWater == 0 {
		if err := r.q.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) FROM job_listing_order`).Scan(&cursor.HighWater); err != nil {
			return JobList{}, internalError(err, "read job listing watermark")
		}
		cursor.Version = 1
		cursor.Filters = jobFilterFingerprint(filters)
	}
	query, args := jobListingQuery(filters, cursor, limit)
	rows, err := r.q.QueryContext(ctx, query, args...)
	if err != nil {
		return JobList{}, internalError(err, "list readable job IDs")
	}
	type listedID struct {
		jobID     string
		createdNS int64
	}
	listed := make([]listedID, 0, limit+1)
	for rows.Next() {
		var item listedID
		if err := rows.Scan(&item.jobID, &item.createdNS); err != nil {
			rows.Close()
			return JobList{}, internalError(err, "scan readable job ID")
		}
		listed = append(listed, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return JobList{}, internalError(err, "iterate readable job IDs")
	}
	page := JobList{Jobs: []Job{}}
	hasMore := len(listed) > limit
	if hasMore {
		listed = listed[:limit]
	}
	// Read the selected rows in the same snapshot as the filter query so a
	// state transition cannot turn a state-filtered page into mismatched rows.
	for _, item := range listed {
		job, err := readJob(ctx, r, item.jobID)
		if err != nil {
			return JobList{}, err
		}
		job, err = projectJobWithReads(ctx, r, job, projectJobAll)
		if err != nil {
			return JobList{}, err
		}
		page.Jobs = append(page.Jobs, job)
	}
	if hasMore {
		last := listed[len(listed)-1]
		cursor.CreatedNS, cursor.JobID = last.createdNS, last.jobID
		payload, _ := json.Marshal(cursor)
		page.NextCursor = base64.RawURLEncoding.EncodeToString(payload)
	}
	return page, nil
}

func jobListingQuery(filters jobListFilters, cursor jobCollectionCursor, limit int) (string, []any) {
	predicates := []string{"job_listing_order.sequence<=?",
		"NOT EXISTS (SELECT 1 FROM computer_job_projections WHERE computer_job_projections.job_id=jobs.job_id AND current=0)"}
	args := []any{cursor.HighWater}
	if cursor.JobID != "" {
		predicates = append(predicates, "jobs.created_ns>=? AND (jobs.created_ns>? OR jobs.job_id>?)")
		args = append(args, cursor.CreatedNS, cursor.CreatedNS, cursor.JobID)
	}
	// Service membership is indexed; avoid decoding every candidate's spec.
	// Its complement is the one-shot set, with retired projections excluded
	// from both classes and the unfiltered collection above.
	switch filters.Class {
	case contract.JobClassService:
		predicates = append(predicates, "EXISTS (SELECT 1 FROM service_jobs WHERE service_jobs.job_id=jobs.job_id)")
	case contract.JobClassOneShot:
		predicates = append(predicates, "NOT EXISTS (SELECT 1 FROM service_jobs WHERE service_jobs.job_id=jobs.job_id)")
	}
	if filters.Kind != "" {
		predicates = append(predicates, "json_extract(jobs.spec_json, '$.kind')=?")
		args = append(args, filters.Kind)
	}
	if filters.State != "" {
		predicates = append(predicates, "jobs.state=?")
		args = append(args, filters.State)
	}
	if filters.Submitter != "" {
		predicates = append(predicates, "jobs.originating_submitter=?")
		args = append(args, filters.Submitter)
	}
	if filters.ParentJobID != "" {
		predicates = append(predicates, "(jobs.job_id=? OR jobs.parent_job_id=?)")
		args = append(args, filters.ParentJobID, filters.ParentJobID)
	}
	args = append(args, limit+1)
	// CROSS JOIN keeps jobs as the outer loop: SQLite streams the creation
	// index (seeking created_ns on continuation), probes insertion membership
	// by job_id, and stops at LIMIT without a whole-collection temp B-tree.
	// TestJobsListingPlanUsesCreationIndex pins the actual EXPLAIN plan.
	return `SELECT jobs.job_id, jobs.created_ns FROM jobs CROSS JOIN job_listing_order ON job_listing_order.job_id=jobs.job_id WHERE ` + strings.Join(predicates, " AND ") + ` ORDER BY jobs.created_ns, jobs.job_id LIMIT ?`, args
}
