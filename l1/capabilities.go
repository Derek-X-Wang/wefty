package l1

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const (
	capabilityKindPrefix           = "kind:"
	capabilityRuntimeHandlerPrefix = "runtime_handler:"
	capabilityCgroupV2             = "cgroup_v2"
	capabilityComputer             = "computer"
	maxNodeCapabilities            = 128
	maxMissingCapabilities         = 128
	maxCapabilityNameBytes         = 128
)

type storedCapabilityObservation struct {
	observation      contract.CapabilityObservation
	capabilitiesJSON []byte
	missingJSON      []byte
}

func canonicalCapabilityObservation(observation contract.CapabilityObservation) (storedCapabilityObservation, error) {
	if observation.Revision < 1 {
		return storedCapabilityObservation{}, protocolError(contract.ErrorInvalidRequest, "capability revision must be positive")
	}
	if observation.ObservedAt.IsZero() {
		return storedCapabilityObservation{}, protocolError(contract.ErrorInvalidRequest, "capability observation time is required")
	}
	if len(observation.Capabilities) > maxNodeCapabilities {
		return storedCapabilityObservation{}, protocolError(contract.ErrorInvalidRequest, "capability set exceeds %d entries", maxNodeCapabilities)
	}
	capabilities := make(map[string]bool, len(observation.Capabilities))
	for raw, enabled := range observation.Capabilities {
		capability := strings.ToLower(strings.TrimSpace(raw))
		if capability == "" || len(capability) > maxCapabilityNameBytes {
			return storedCapabilityObservation{}, protocolError(contract.ErrorInvalidRequest, "capability names must contain 1-%d bytes", maxCapabilityNameBytes)
		}
		if previous, exists := capabilities[capability]; exists && previous != enabled {
			return storedCapabilityObservation{}, protocolError(contract.ErrorInvalidRequest, "capability %q has conflicting normalized values", capability)
		}
		capabilities[capability] = enabled
	}
	capabilities = normalizeRegistrationCapabilities(capabilities)

	if len(observation.MissingCapabilities) > maxMissingCapabilities {
		return storedCapabilityObservation{}, protocolError(contract.ErrorInvalidRequest, "missing capability set exceeds %d entries", maxMissingCapabilities)
	}
	missingSet := make(map[string]struct{}, len(observation.MissingCapabilities))
	for _, raw := range observation.MissingCapabilities {
		capability := strings.ToLower(strings.TrimSpace(raw))
		if capability == "" || len(capability) > maxCapabilityNameBytes {
			return storedCapabilityObservation{}, protocolError(contract.ErrorInvalidRequest, "missing capability names must contain 1-%d bytes", maxCapabilityNameBytes)
		}
		if capabilities[capability] {
			return storedCapabilityObservation{}, protocolError(contract.ErrorInvalidRequest, "capability %q cannot be both advertised and missing", capability)
		}
		missingSet[capability] = struct{}{}
	}
	missing := make([]string, 0, len(missingSet))
	for capability := range missingSet {
		missing = append(missing, capability)
	}
	sort.Strings(missing)
	if len(missing) == 0 && observation.ReasonCode != "" {
		return storedCapabilityObservation{}, protocolError(contract.ErrorInvalidRequest, "capability reason requires at least one missing capability")
	}
	if len(missing) > 0 {
		if !observation.ReasonCode.Valid() {
			return storedCapabilityObservation{}, protocolError(contract.ErrorInvalidRequest, "missing capabilities require a stable reason code")
		}
	}

	observation.Capabilities = capabilities
	observation.MissingCapabilities = missing
	observation.ObservedAt = canonicalTime(observation.ObservedAt)
	capabilitiesJSON, err := json.Marshal(capabilities)
	if err != nil {
		return storedCapabilityObservation{}, internalError(err, "encode node capabilities")
	}
	missingJSON, err := json.Marshal(missing)
	if err != nil {
		return storedCapabilityObservation{}, internalError(err, "encode missing node capabilities")
	}
	return storedCapabilityObservation{observation: observation, capabilitiesJSON: capabilitiesJSON, missingJSON: missingJSON}, nil
}

func legacyCapabilityObservation(capabilities map[string]bool, now time.Time) contract.CapabilityObservation {
	legacyCapabilities := make(map[string]bool, len(capabilities))
	for capability, enabled := range capabilities {
		capability = strings.ToLower(strings.TrimSpace(capability))
		if capability != "" && !isOCIProbeCapability(capability) {
			legacyCapabilities[capability] = enabled
		}
	}
	return contract.CapabilityObservation{
		Revision: 1, Capabilities: legacyCapabilities, ObservedAt: canonicalTime(now), MissingCapabilities: []string{},
	}
}

func registrationCapabilityObservation(registration contract.NodeRegistration) contract.CapabilityObservation {
	return contract.CapabilityObservation{
		Revision: registration.CapabilityRevision, Capabilities: registration.Capabilities,
		ObservedAt: registration.CapabilityObservedAt, MissingCapabilities: registration.MissingCapabilities,
		ReasonCode: registration.CapabilityReasonCode,
	}
}

func heartbeatCapabilityObservation(request HeartbeatRequest) contract.CapabilityObservation {
	return contract.CapabilityObservation{
		Revision: request.CapabilityRevision, Capabilities: request.Capabilities,
		ObservedAt: request.CapabilityObservedAt, MissingCapabilities: request.MissingCapabilities,
		ReasonCode: request.CapabilityReasonCode,
	}
}

type capabilityObservationDecision uint8

const (
	capabilityObservationStale capabilityObservationDecision = iota
	capabilityObservationReplay
	capabilityObservationReplace
)

func decideCapabilityObservation(incoming storedCapabilityObservation, capabilitiesJSON []byte, revision int64, missingJSON []byte, reason contract.CapabilityReasonCode) (capabilityObservationDecision, error) {
	switch {
	case incoming.observation.Revision < revision:
		return capabilityObservationStale, nil
	case incoming.observation.Revision > revision:
		return capabilityObservationReplace, nil
	case !sameCapabilityObservation(incoming, capabilitiesJSON, revision, missingJSON, reason):
		return capabilityObservationStale, protocolError(contract.ErrorConflict, "capability revision %d was already observed with different content", revision)
	default:
		return capabilityObservationReplay, nil
	}
}

func sameCapabilityObservation(incoming storedCapabilityObservation, capabilitiesJSON []byte, revision int64, missingJSON []byte, reason contract.CapabilityReasonCode) bool {
	return incoming.observation.Revision == revision &&
		incoming.observation.ReasonCode == reason &&
		bytes.Equal(incoming.capabilitiesJSON, capabilitiesJSON) && bytes.Equal(incoming.missingJSON, missingJSON)
}

func isOCIProbeCapability(capability string) bool {
	capability = strings.ToLower(strings.TrimSpace(capability))
	return capability == "kind:oci" || capability == "cgroup_v2" || capability == "apparmor" ||
		capability == capabilityComputer ||
		strings.HasPrefix(capability, capabilityRuntimeHandlerPrefix)
}

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
	if spec.Execution.OCI != nil && spec.Execution.OCI.Computer != nil {
		required = append(required, capabilityComputer)
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
// current full-set capability observation.
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
