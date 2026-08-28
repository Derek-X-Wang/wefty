package l1

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
)

const (
	DefaultJobPageLimit = 100
	MaxJobPageLimit     = 1000
)

type serviceJobCursor struct {
	CreatedNS int64  `json:"created_ns"`
	JobID     string `json:"job_id"`
}

func requireServiceClass(r *http.Request) error {
	values, present := r.URL.Query()["class"]
	if !present || len(values) != 1 || values[0] != contract.JobClassService {
		return protocolError(contract.ErrorInvalidRequest, "class=service is required")
	}
	return nil
}

// validateJobRouteClass preserves L3's internal one-shot reads while making a
// service impossible to address accidentally through an unscoped L1 route.
func validateJobRouteClass(r *http.Request, job Job) error {
	values, present := r.URL.Query()["class"]
	if present && (len(values) != 1 || values[0] != contract.JobClassService) {
		return protocolError(contract.ErrorInvalidRequest, "class must be service")
	}
	isService := job.ServiceJob != nil || job.Removal != nil
	if isService {
		return requireServiceClass(r)
	}
	if present {
		return protocolError(contract.ErrorNotFound, "service job %q was not found", job.JobID)
	}
	return nil
}

func parseJobLimit(value string) (int, error) {
	if value == "" {
		return DefaultJobPageLimit, nil
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit < 1 || limit > MaxJobPageLimit {
		return 0, protocolError(contract.ErrorInvalidRequest, "limit must be an integer between 1 and %d", MaxJobPageLimit)
	}
	return limit, nil
}

func encodeServiceJobCursor(cursor serviceJobCursor) string {
	payload, err := json.Marshal(cursor)
	if err != nil {
		panic("l1: encode service job cursor: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(payload)
}

func decodeServiceJobCursor(value string) (serviceJobCursor, error) {
	if value == "" {
		return serviceJobCursor{}, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return serviceJobCursor{}, protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	var cursor serviceJobCursor
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil || cursor.CreatedNS < 0 || strings.TrimSpace(cursor.JobID) == "" {
		return serviceJobCursor{}, protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return serviceJobCursor{}, protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	return cursor, nil
}

// ListServiceJobs returns only active service definitions. Removed services
// remain addressable by exact tombstone ID but no longer belong to the active
// collection.
func (s *Store) ListServiceJobs(ctx context.Context, cursorValue string, limit int) (JobList, error) {
	if limit < 1 || limit > MaxJobPageLimit {
		return JobList{}, protocolError(contract.ErrorInvalidRequest, "limit must be between 1 and %d", MaxJobPageLimit)
	}
	cursor, err := decodeServiceJobCursor(cursorValue)
	if err != nil {
		return JobList{}, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT jobs.job_id, jobs.created_ns
		FROM jobs JOIN service_jobs ON service_jobs.job_id=jobs.job_id
		LEFT JOIN computer_job_projections ON computer_job_projections.job_id=jobs.job_id
		WHERE (jobs.created_ns>? OR (jobs.created_ns=? AND jobs.job_id>?))
			AND (computer_job_projections.job_id IS NULL OR computer_job_projections.current=1)
		ORDER BY jobs.created_ns, jobs.job_id LIMIT ?`, cursor.CreatedNS, cursor.CreatedNS, cursor.JobID, limit+1)
	if err != nil {
		return JobList{}, internalError(err, "list service job IDs")
	}
	defer rows.Close()
	type listedID struct {
		jobID     string
		createdNS int64
	}
	listed := make([]listedID, 0, limit+1)
	for rows.Next() {
		var item listedID
		if err := rows.Scan(&item.jobID, &item.createdNS); err != nil {
			return JobList{}, internalError(err, "scan service job ID")
		}
		listed = append(listed, item)
	}
	if err := rows.Err(); err != nil {
		return JobList{}, internalError(err, "iterate service job IDs")
	}

	page := JobList{Jobs: []Job{}}
	hasMore := len(listed) > limit
	if hasMore {
		listed = listed[:limit]
	}
	for _, item := range listed {
		job, err := s.GetJob(ctx, item.jobID)
		if err != nil {
			return JobList{}, err
		}
		job, err = s.projectServiceJob(ctx, job)
		if err != nil {
			return JobList{}, err
		}
		page.Jobs = append(page.Jobs, job)
	}
	if hasMore && len(listed) > 0 {
		last := listed[len(listed)-1]
		page.NextCursor = encodeServiceJobCursor(serviceJobCursor{CreatedNS: last.createdNS, JobID: last.jobID})
	}
	return page, nil
}

func (s *Store) SetServiceDesiredState(ctx context.Context, jobID string, desired contract.ServiceDesiredState) (Job, error) {
	if strings.TrimSpace(jobID) == "" {
		return Job{}, protocolError(contract.ErrorInvalidRequest, "job_id is required")
	}
	if desired != contract.ServiceDesiredRunning && desired != contract.ServiceDesiredStopped {
		return Job{}, protocolError(contract.ErrorInvalidRequest, "desired_state must be %q or %q", contract.ServiceDesiredRunning, contract.ServiceDesiredStopped)
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, internalError(err, "begin service desired-state mutation")
	}
	defer tx.Rollback()
	job, err := getJobByID(ctx, tx, jobID, now)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && job.ServiceJob == nil) {
		return Job{}, protocolError(contract.ErrorNotFound, "service job %q was not found", jobID)
	}
	if err != nil {
		return Job{}, internalError(err, "read service desired-state target")
	}
	if computerID, mapped, mapErr := computerIDForJob(ctx, tx, jobID); mapErr != nil {
		return Job{}, mapErr
	} else if mapped {
		return Job{}, protocolErrorWithDetails(contract.ErrorComputerResourceRequired,
			map[string]any{"computer_id": computerID},
			"Computer %q is the sole desired-state authority for Job %q", computerID, jobID)
	}
	if job.Removal != nil {
		return Job{}, protocolError(contract.ErrorConflict, "service job %q is being removed", jobID)
	}

	switch desired {
	case contract.ServiceDesiredRunning:
		switch job.State {
		case contract.JobFailed:
			return Job{}, protocolError(contract.ErrorConflict, "service job %q is latched failed; use restart", jobID)
		case contract.JobStopped:
			if !job.HoldsSlot(job.State) {
				if err := ensureBoundServiceCapacity(ctx, tx, job); err != nil {
					return Job{}, err
				}
			}
			if err := transitionServiceJob(ctx, tx, jobID, desired, contract.JobQueued, now); err != nil {
				return Job{}, err
			}
			if _, err := tx.ExecContext(ctx, "UPDATE service_jobs SET next_restart_at=NULL WHERE job_id=?", jobID); err != nil {
				return Job{}, internalError(err, "clear service start backoff")
			}
		case contract.JobStopping:
			return Job{}, protocolError(contract.ErrorConflict, "service job %q is still stopping; wait for stopped before start", jobID)
		case contract.JobQueued, contract.JobClaimed, contract.JobRunning:
			if job.DesiredState != contract.ServiceDesiredRunning {
				return Job{}, protocolError(contract.ErrorConflict, "service job %q has inconsistent desired state", jobID)
			}
		default:
			return Job{}, protocolError(contract.ErrorConflict, "service job %q cannot be started from %q", jobID, job.State)
		}
	case contract.ServiceDesiredStopped:
		switch job.State {
		case contract.JobQueued:
			if err := transitionServiceJob(ctx, tx, jobID, desired, contract.JobStopped, now); err != nil {
				return Job{}, err
			}
		case contract.JobClaimed, contract.JobRunning:
			if err := transitionServiceJob(ctx, tx, jobID, desired, contract.JobStopping, now); err != nil {
				return Job{}, err
			}
		case contract.JobFailed:
			if _, err := tx.ExecContext(ctx, "UPDATE service_jobs SET desired_state=?, next_restart_at=NULL WHERE job_id=?", desired, jobID); err != nil {
				return Job{}, internalError(err, "stop latched service")
			}
			if _, err := tx.ExecContext(ctx, "UPDATE jobs SET updated_ns=? WHERE job_id=?", now.UnixNano(), jobID); err != nil {
				return Job{}, internalError(err, "timestamp latched service stop")
			}
		case contract.JobStopping, contract.JobStopped:
			if job.DesiredState != contract.ServiceDesiredStopped {
				return Job{}, protocolError(contract.ErrorConflict, "service job %q has inconsistent desired state", jobID)
			}
		default:
			return Job{}, protocolError(contract.ErrorConflict, "service job %q cannot be stopped from %q", jobID, job.State)
		}
		if _, err := tx.ExecContext(ctx, "UPDATE service_jobs SET next_restart_at=NULL WHERE job_id=?", jobID); err != nil {
			return Job{}, internalError(err, "clear stopped service backoff")
		}
	}

	job, err = getJobByID(ctx, tx, jobID, now)
	if err != nil {
		return Job{}, internalError(err, "read service desired-state result")
	}
	if err := tx.Commit(); err != nil {
		return Job{}, internalError(err, "commit service desired-state mutation")
	}
	return job, nil
}

func (s *Store) RestartService(ctx context.Context, jobID string, request ServiceRestartRequest) (Job, bool, error) {
	if strings.TrimSpace(jobID) == "" || strings.TrimSpace(request.IdempotencyKey) == "" {
		return Job{}, false, protocolError(contract.ErrorInvalidRequest, "job_id and idempotency_key are required")
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return Job{}, false, internalError(err, "encode service restart request")
	}
	hash := sha256.Sum256(payload)
	requestHash := hex.EncodeToString(hash[:])
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, false, internalError(err, "begin service restart")
	}
	defer tx.Rollback()
	job, err := getJobByID(ctx, tx, jobID, now)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && job.ServiceJob == nil) {
		return Job{}, false, protocolError(contract.ErrorNotFound, "service job %q was not found", jobID)
	}
	if err != nil {
		return Job{}, false, internalError(err, "read service restart target")
	}
	if computerID, mapped, mapErr := computerIDForJob(ctx, tx, jobID); mapErr != nil {
		return Job{}, false, mapErr
	} else if mapped {
		return Job{}, false, protocolErrorWithDetails(contract.ErrorComputerResourceRequired,
			map[string]any{"computer_id": computerID},
			"Computer %q is the sole lifecycle authority for Job %q", computerID, jobID)
	}
	var storedHash string
	err = tx.QueryRowContext(ctx, `SELECT request_hash FROM service_restart_requests
		WHERE job_id=? AND idempotency_key=?`, jobID, request.IdempotencyKey).Scan(&storedHash)
	if err == nil {
		if storedHash != requestHash {
			return Job{}, false, protocolError(contract.ErrorIdempotencyConflict, "restart idempotency key conflicts with the accepted request")
		}
		return job, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Job{}, false, internalError(err, "read service restart replay")
	}
	if job.Removal != nil {
		return Job{}, false, protocolError(contract.ErrorConflict, "service job %q is being removed", jobID)
	}
	if !job.HoldsSlot(job.State) {
		if err := ensureBoundServiceCapacity(ctx, tx, job); err != nil {
			return Job{}, false, err
		}
	}
	switch job.State {
	case contract.JobStopped, contract.JobFailed:
		if err := transitionServiceJob(ctx, tx, jobID, contract.ServiceDesiredRunning, contract.JobQueued, now); err != nil {
			return Job{}, false, err
		}
	case contract.JobQueued, contract.JobClaimed, contract.JobRunning:
		// An active attempt sees the durable restart request on renewal. A
		// healthy attempt remains observed running until the agent reaps it.
	case contract.JobStopping:
		return Job{}, false, protocolError(contract.ErrorConflict, "service job %q is still stopping; wait for stopped before restart", jobID)
	default:
		return Job{}, false, protocolError(contract.ErrorConflict, "service job %q cannot be restarted from %q", jobID, job.State)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE service_jobs
		SET desired_state=?, restart_streak=0, next_restart_at=NULL, last_failure=NULL,
			healthy_since_ns=NULL, published_attempt_id=NULL WHERE job_id=?`,
		contract.ServiceDesiredRunning, jobID); err != nil {
		return Job{}, false, internalError(err, "reset service restart policy")
	}
	restartCreatedNS := now.UnixNano()
	if job.CurrentAttemptID != "" {
		var attemptCreatedNS int64
		if err := tx.QueryRowContext(ctx, "SELECT created_ns FROM attempts WHERE attempt_id=?", job.CurrentAttemptID).Scan(&attemptCreatedNS); err != nil {
			return Job{}, false, internalError(err, "read restarted attempt creation time")
		}
		if attemptCreatedNS > restartCreatedNS {
			restartCreatedNS = attemptCreatedNS
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO service_restart_requests(job_id, idempotency_key, request_hash, created_ns)
		VALUES(?, ?, ?, ?)`, jobID, request.IdempotencyKey, requestHash, restartCreatedNS); err != nil {
		return Job{}, false, internalError(err, "record durable service restart")
	}
	if _, err := tx.ExecContext(ctx, "UPDATE jobs SET updated_ns=? WHERE job_id=?", now.UnixNano(), jobID); err != nil {
		return Job{}, false, internalError(err, "timestamp service restart")
	}
	job, err = getJobByID(ctx, tx, jobID, now)
	if err != nil {
		return Job{}, false, internalError(err, "read service restart result")
	}
	if err := tx.Commit(); err != nil {
		return Job{}, false, internalError(err, "commit service restart")
	}
	return job, false, nil
}

func ensureBoundServiceCapacity(ctx context.Context, tx *sql.Tx, job Job) error {
	if job.ServiceJob == nil || job.BoundNodeID == "" {
		return nil
	}
	var capacity, occupancy int
	err := tx.QueryRowContext(ctx, `SELECT nodes.max_service_slots,
		(SELECT COUNT(*) FROM service_jobs occupied_service
		 JOIN jobs occupied_job ON occupied_job.job_id=occupied_service.job_id
		 WHERE occupied_service.bound_node_id=nodes.node_id
		   AND ((occupied_job.state=? AND occupied_service.desired_state=?)
		        OR occupied_job.state IN (?, ?, ?, ?, ?)))
		FROM nodes WHERE nodes.node_id=?`, contract.JobQueued, contract.ServiceDesiredRunning,
		contract.JobClaimed, contract.JobRunning, contract.JobStopping, contract.JobRemovalPending,
		contract.JobAgentCleaned, job.BoundNodeID).Scan(&capacity, &occupancy)
	if errors.Is(err, sql.ErrNoRows) {
		return protocolError(contract.ErrorConflict, "bound node %q was not found", job.BoundNodeID)
	}
	if err != nil {
		return internalError(err, "read bound service capacity")
	}
	if occupancy >= capacity {
		return protocolErrorWithDetails(contract.ErrorCapacityExhausted, map[string]any{
			"class": contract.JobClassService, "node_id": job.BoundNodeID,
			"occupancy": occupancy, "capacity": capacity,
		}, "service capacity exhausted on bound node %q: occupancy %d/%d", job.BoundNodeID, occupancy, capacity)
	}
	return nil
}

func (s *Store) projectJob(ctx context.Context, job Job) (Job, error) {
	if job.ServiceJob != nil || job.Removal != nil {
		return s.projectServiceJob(ctx, job)
	}
	job.Status = string(job.State)
	return s.projectQueuedJobCapabilities(ctx, job)
}

func (s *Store) projectServiceJob(ctx context.Context, job Job) (Job, error) {
	if job.Removal != nil {
		job.Status = string(job.State)
		return job, nil
	}
	if job.ServiceJob == nil {
		return Job{}, protocolError(contract.ErrorNotFound, "service job %q was not found", job.JobID)
	}
	service := job.ServiceJob
	service.SlotHeld = service.HoldsSlot(job.State)
	job.Status = string(job.State)
	now := canonicalTime(s.clock.Now())
	if service.RestartPending(job.State, now) {
		job.Status = "restart-pending"
	}
	if service.Ready == nil || !*service.Ready {
		service.PublishedPort = nil
	}
	if service.DesiredState == contract.ServiceDesiredStopped {
		service.RestartSuppressed = "desired state is stopped"
	} else if job.State == contract.JobFailed {
		if job.Spec.MaxRestartStreak != nil && service.RestartStreak >= *job.Spec.MaxRestartStreak {
			service.RestartSuppressed = fmt.Sprintf("max restart streak reached: %d/%d; use restart", service.RestartStreak, *job.Spec.MaxRestartStreak)
		} else {
			service.RestartSuppressed = "failure is latched; use restart"
		}
	}
	if job.Status != "restart-pending" {
		var err error
		job, err = s.projectQueuedJobCapabilities(ctx, job)
		if err != nil {
			return Job{}, err
		}
	}
	if service.BoundNodeID != "" {
		var claimsEnabled bool
		var capabilitiesJSON []byte
		err := s.db.QueryRowContext(ctx, "SELECT state, claims_enabled, capabilities_json FROM nodes WHERE node_id=?", service.BoundNodeID).
			Scan(&service.NodeState, &claimsEnabled, &capabilitiesJSON)
		if errors.Is(err, sql.ErrNoRows) {
			if job.State == contract.JobQueued && job.Status != "restart-pending" {
				job.UnschedulableReason = fmt.Sprintf("bound node %q is not registered", service.BoundNodeID)
			}
		} else if err != nil {
			return Job{}, internalError(err, "read bound node projection")
		} else if job.State == contract.JobQueued && job.Status != "restart-pending" {
			switch {
			case service.NodeState != contract.NodeAlive:
				job.UnschedulableReason = fmt.Sprintf("bound node %q is %s", service.BoundNodeID, service.NodeState)
			case !claimsEnabled:
				job.UnschedulableReason = fmt.Sprintf("bound node %q has claims disabled", service.BoundNodeID)
			default:
				var advertised map[string]bool
				if err := json.Unmarshal(capabilitiesJSON, &advertised); err != nil {
					return Job{}, internalError(err, "decode bound node capabilities")
				}
				required, err := storedRequiredCapabilities(ctx, s.db, job.JobID)
				if err != nil {
					return Job{}, internalError(err, "read required job capabilities")
				}
				if missing := MissingCapabilities(required, advertised); len(missing) > 0 {
					job.UnschedulableReason = fmt.Sprintf("bound node %q is missing capabilities: %s", service.BoundNodeID, strings.Join(missing, ", "))
				}
			}
		}
	} else if job.State == contract.JobQueued && job.Status != "restart-pending" {
		reason, err := s.unboundServiceUnschedulableReason(ctx, job.JobID)
		if err != nil {
			return Job{}, err
		}
		job.UnschedulableReason = reason
	}
	if job.UnschedulableReason != "" {
		job.Status = "unschedulable"
	}
	return job, nil
}

func (s *Store) unboundServiceUnschedulableReason(ctx context.Context, jobID string) (string, error) {
	required, err := storedRequiredCapabilities(ctx, s.db, jobID)
	if err != nil {
		return "", internalError(err, "read required job capabilities")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT nodes.node_id, nodes.state, nodes.claims_enabled,
		nodes.capabilities_json, nodes.max_service_slots,
		(SELECT COUNT(*) FROM service_jobs occupied_service
		 JOIN jobs occupied_job ON occupied_job.job_id=occupied_service.job_id
		 WHERE occupied_service.bound_node_id=nodes.node_id
		   AND ((occupied_job.state=? AND occupied_service.desired_state=?)
		        OR occupied_job.state IN (?, ?, ?, ?, ?)))
		FROM nodes
		WHERE NOT EXISTS (
		 SELECT 1 FROM job_tags
		 WHERE job_tags.job_id=? AND NOT EXISTS (
		  SELECT 1 FROM node_tags WHERE node_tags.node_id=nodes.node_id AND node_tags.tag=job_tags.tag
		 )
		)
		ORDER BY nodes.node_id`, contract.JobQueued, contract.ServiceDesiredRunning,
		contract.JobClaimed, contract.JobRunning, contract.JobStopping, contract.JobRemovalPending,
		contract.JobAgentCleaned, jobID)
	if err != nil {
		return "", internalError(err, "read service placement candidates")
	}
	defer rows.Close()
	matched := 0
	eligible := 0
	capacityReasons := []string{}
	ineligibleReasons := []string{}
	missingRequirements := []string{}
	seenMissing := map[string]struct{}{}
	nonCapabilityIneligible := 0
	for rows.Next() {
		var nodeID string
		var state contract.NodeState
		var claimsEnabled bool
		var capabilitiesJSON []byte
		var capacity, occupancy int
		if err := rows.Scan(&nodeID, &state, &claimsEnabled, &capabilitiesJSON, &capacity, &occupancy); err != nil {
			return "", internalError(err, "scan service placement candidate")
		}
		matched++
		if state != contract.NodeAlive {
			ineligibleReasons = append(ineligibleReasons, fmt.Sprintf("%s is %s", nodeID, state))
			nonCapabilityIneligible++
			continue
		}
		if !claimsEnabled {
			ineligibleReasons = append(ineligibleReasons, fmt.Sprintf("%s has claims disabled", nodeID))
			nonCapabilityIneligible++
			continue
		}
		var advertised map[string]bool
		if err := json.Unmarshal(capabilitiesJSON, &advertised); err != nil {
			return "", internalError(err, "decode service placement capabilities")
		}
		missing := MissingCapabilities(required, advertised)
		if len(missing) > 0 {
			ineligibleReasons = append(ineligibleReasons, fmt.Sprintf("%s is missing capabilities: %s", nodeID, strings.Join(missing, ", ")))
			for _, capability := range missing {
				if _, seen := seenMissing[capability]; seen {
					continue
				}
				seenMissing[capability] = struct{}{}
				missingRequirements = append(missingRequirements, capability)
			}
			continue
		}
		eligible++
		if occupancy < capacity {
			return "", nil
		}
		capacityReasons = append(capacityReasons, fmt.Sprintf("%s occupancy %d/%d", nodeID, occupancy, capacity))
	}
	if err := rows.Err(); err != nil {
		return "", internalError(err, "iterate service placement candidates")
	}
	if matched == 0 {
		return "no registered node matches the service routing tags", nil
	}
	if eligible == 0 {
		if len(missingRequirements) > 0 && nonCapabilityIneligible == 0 {
			return "no tag-eligible node advertises required capabilities: " + strings.Join(missingRequirements, ", "), nil
		}
		return "no tag-eligible node accepts claims: " + strings.Join(ineligibleReasons, "; "), nil
	}
	return "service capacity exhausted: " + strings.Join(capacityReasons, "; "), nil
}
