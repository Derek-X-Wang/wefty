package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
)

const (
	capabilityKindPrefix           = "kind:"
	capabilityRuntimeHandlerPrefix = "runtime_handler:"
	capabilityCgroupV2             = "cgroup_v2"
)

// RequiredCapabilities derives the normalized execution eligibility set for
// a job. Capacity remains class-scoped and deliberately does not enter this
// set.
func RequiredCapabilities(spec contract.JobSpec) []string {
	required := []string{capabilityKindPrefix + spec.Kind}
	if spec.RuntimeHandler != "" {
		required = append(required, capabilityRuntimeHandlerPrefix+spec.RuntimeHandler)
	}
	if spec.Execution.OCI != nil && spec.Execution.OCI.Limits != nil &&
		(spec.Execution.OCI.Limits.MemoryBytes != nil || spec.Execution.OCI.Limits.CPUMillicores != nil) {
		required = append(required, capabilityCgroupV2)
	}
	sort.Strings(required)
	return required
}

func (s *Store) projectQueuedJobCapabilities(ctx context.Context, job Job) (Job, error) {
	if job.State != contract.JobQueued {
		return job, nil
	}
	required, err := storedRequiredCapabilities(ctx, s.db, job.JobID)
	if err != nil {
		return Job{}, internalError(err, "read required job capabilities")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT nodes.capabilities_json
		FROM nodes
		WHERE NOT EXISTS (
		 SELECT 1 FROM job_tags
		 WHERE job_tags.job_id=? AND NOT EXISTS (
		  SELECT 1 FROM node_tags WHERE node_tags.node_id=nodes.node_id AND node_tags.tag=job_tags.tag
		 )
		)`, job.JobID)
	if err != nil {
		return Job{}, internalError(err, "read capability placement candidates")
	}
	defer rows.Close()
	missingSet := make(map[string]struct{}, len(required))
	candidates := 0
	for rows.Next() {
		candidates++
		var capabilitiesJSON []byte
		if err := rows.Scan(&capabilitiesJSON); err != nil {
			return Job{}, internalError(err, "scan capability placement candidate")
		}
		var advertised map[string]bool
		if err := json.Unmarshal(capabilitiesJSON, &advertised); err != nil {
			return Job{}, internalError(err, "decode capability placement candidate")
		}
		missing := MissingCapabilities(required, advertised)
		if len(missing) == 0 {
			return job, nil
		}
		for _, capability := range missing {
			missingSet[capability] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return Job{}, internalError(err, "iterate capability placement candidates")
	}
	if candidates == 0 {
		for _, capability := range required {
			missingSet[capability] = struct{}{}
		}
	}
	missing := make([]string, 0, len(missingSet))
	for capability := range missingSet {
		missing = append(missing, capability)
	}
	sort.Strings(missing)
	job.Status = "unschedulable"
	job.UnschedulableReason = "no tag-eligible node advertises required capabilities: " + strings.Join(missing, ", ")
	return job, nil
}

// MissingCapabilities compares one normalized requirement set with a node's
// registration-advertised execution facts. Ticket #136 adds live revisions.
func MissingCapabilities(required []string, advertised map[string]bool) []string {
	missing := make([]string, 0, len(required))
	for _, capability := range required {
		if !advertised[capability] {
			missing = append(missing, capability)
		}
	}
	return missing
}

type capabilityQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func storedRequiredCapabilities(ctx context.Context, q capabilityQueryer, jobID string) ([]string, error) {
	rows, err := q.QueryContext(ctx, `SELECT capability FROM job_required_capabilities
		WHERE job_id=? ORDER BY capability`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	required := []string{}
	for rows.Next() {
		var capability string
		if err := rows.Scan(&capability); err != nil {
			return nil, err
		}
		required = append(required, capability)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return required, nil
}
