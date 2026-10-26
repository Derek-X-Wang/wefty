package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
)

// NodeGenerationObservation is the public, authority-free session projection.
// It never exposes resident fences, boot credentials or promotion requests.
// Attempts include terminal residents so a healthy primary cannot hide loss.
type NodeGenerationObservation struct {
	ObservationError    string                  `json:"observation_error,omitempty"`
	PreviousSystemBoot  bool                    `json:"previous_system_boot,omitempty"`
	AgentGenerationID   string                  `json:"agent_generation_id"`
	Role                NodeGenerationRole      `json:"role"`
	Version             string                  `json:"version"`
	CreatedAt           time.Time               `json:"created_at"`
	RetirementStartedAt *time.Time              `json:"retirement_started_at"`
	SelectedProtocol    uint32                  `json:"selected_protocol"`
	Attempts            []NodeGenerationAttempt `json:"attempts"`
}

type NodeGenerationAttempt struct {
	ObservationError   string                 `json:"observation_error,omitempty"`
	JobID              string                 `json:"job_id"`
	AttemptID          string                 `json:"attempt_id"`
	RunID              string                 `json:"run_id,omitempty"`
	JobOutcome         contract.JobOutcome    `json:"job_outcome,omitempty"`
	State              *contract.AttemptState `json:"state"`
	ExitCode           *int                   `json:"exit_code,omitempty"`
	LastRenewedAt      *time.Time             `json:"last_renewed_at"`
	LeaseExpiresAt     *time.Time             `json:"lease_expires_at"`
	InterruptionReason InterruptionReason     `json:"interruption_reason,omitempty"`
}

// Called only inside the existing read snapshot, with its ReadOnly transaction.
func readNodeGenerationObservations(ctx context.Context, q queryer, nodeID string) ([]NodeGenerationObservation, error) {
	recordErrors := map[string]string{}
	inventory, err := readNodeGenerationInventory(ctx, q, nodeID, recordErrors)
	if err != nil {
		return nil, err
	}
	primaryBoot := ""
	for _, g := range inventory.Generations {
		if g.Role == NodeGenerationPrimary {
			primaryBoot = g.Registration.SystemBootID
		}
	}
	var result []NodeGenerationObservation
	for _, g := range inventory.Generations {
		observation := NodeGenerationObservation{ObservationError: recordErrors[g.AgentGenerationID], AgentGenerationID: g.AgentGenerationID, Role: g.Role, Version: g.Registration.AgentVersion, CreatedAt: g.CreatedAt, RetirementStartedAt: g.RetiringAt, SelectedProtocol: g.SelectedProtocol, Attempts: []NodeGenerationAttempt{}}
		observation.PreviousSystemBoot = g.Role != NodeGenerationPrimary && primaryBoot != "" && g.Registration.SystemBootID != "" && !hostboot.SameBoot(primaryBoot, g.Registration.SystemBootID)
		// Retiree membership is frozen at promotion; primary membership is read
		// from its current boot and authority, as in the private inventory.

		for _, resident := range g.ResidentAttempts {
			a, err := readGenerationAttemptObservation(ctx, q, nodeID, resident)
			if err != nil {
				return nil, err
			}
			observation.Attempts = append(observation.Attempts, a)
		}

		result = append(result, observation)
	}
	return result, nil
}

func readGenerationAttemptObservation(ctx context.Context, q queryer, nodeID string, resident ResidentAttempt) (NodeGenerationAttempt, error) {
	a := NodeGenerationAttempt{JobID: resident.JobID, AttemptID: resident.AttemptID}
	rows, err := q.QueryContext(ctx, `SELECT a.state,a.last_renewal_ns,a.lease_expires_ns,COALESCE(a.interruption_reason,''),j.spec_json,j.outcome,a.result_json FROM attempts a JOIN jobs j ON j.job_id=a.job_id WHERE a.attempt_id=? AND a.job_id=? AND a.node_id=?`, resident.AttemptID, resident.JobID, nodeID)
	if err != nil {
		return a, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return a, err
		}
		a.ObservationError = fmt.Sprintf("Job %s (Attempt %s): retained Attempt or Job row unavailable", resident.JobID, resident.AttemptID)
		return a, nil
	}
	var state contract.AttemptState
	var renewal sql.NullInt64
	var expiry int64
	var specJSON, resultJSON []byte
	scanErr := rows.Scan(&state, &renewal, &expiry, &a.InterruptionReason, &specJSON, &a.JobOutcome, &resultJSON)
	// Database iteration failures invalidate the snapshot; conversion failures
	// describe this one retained record and preserve its frozen identity.
	if err := rows.Err(); err != nil {
		return a, err
	}
	if scanErr != nil {
		a.ObservationError = fmt.Sprintf("Job %s (Attempt %s): unreadable retained Attempt: %v", a.JobID, a.AttemptID, scanErr)
		return a, nil
	}
	var result ProcessResult
	if len(resultJSON) > 0 {
		if err := json.Unmarshal(resultJSON, &result); err != nil {
			a.ObservationError = fmt.Sprintf("Job %s (Attempt %s): unreadable retained Attempt result: %v", a.JobID, a.AttemptID, err)
			return a, nil
		}
	}
	var spec struct {
		Labels map[string]string `json:"labels"`
	}
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		a.ObservationError = fmt.Sprintf("Job %s (Attempt %s): unreadable retained Job spec: %v", a.JobID, a.AttemptID, err)
		return a, nil
	}
	a.RunID = spec.Labels[contract.LabelRunID]
	a.State = &state
	a.ExitCode = result.ExitCode
	deadline := time.Unix(0, expiry).UTC()
	a.LeaseExpiresAt = &deadline
	if renewal.Valid {
		at := time.Unix(0, renewal.Int64).UTC()
		a.LastRenewedAt = &at
	}
	return a, nil
}
