package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/l1"
)

// These optional fields leave legacy text and JSON unchanged. Capacity is
// shared across generations; it must never be added once per generation.
type generationCapacity struct {
	ClaimsEnabled bool `json:"claims_enabled"`
	FreeOneshot   int  `json:"free_oneshot_slots"`
	TotalOneshot  int  `json:"total_oneshot_slots"`
	FreeService   int  `json:"free_service_slots"`
	TotalService  int  `json:"total_service_slots"`
}
type generationStatus struct {
	NodeID               string                     `json:"node_id"`
	ID                   string                     `json:"agent_generation_id"`
	Role                 string                     `json:"role"`
	Version              string                     `json:"version"`
	Age                  *int64                     `json:"age_ns"`
	RetirementStartedAt  *time.Time                 `json:"retirement_started_at"`
	SelectedProtocol     uint32                     `json:"selected_protocol"`
	RemainingRunIDs      []string                   `json:"remaining_run_ids"`
	RemainingAttemptIDs  []string                   `json:"remaining_attempt_ids"`
	UnledgeredAttemptIDs []string                   `json:"unledgered_attempt_ids"`
	RenewalHealth        string                     `json:"renewal_health"`
	LastRenewedAt        *time.Time                 `json:"last_renewed_at"`
	LeaseExpiresAt       *time.Time                 `json:"lease_expires_at"`
	PendingEvidence      *int                       `json:"pending_evidence_count"`
	Progress             string                     `json:"retirement_progress"`
	Capacity             *generationCapacity        `json:"shared_capacity"`
	Attempts             []l1.NodeGenerationAttempt `json:"attempts"`
	Problems             []statusReason             `json:"problems,omitempty"`
}
type maintenanceStatus struct {
	Kind      string `json:"kind"`
	Operation string `json:"operation_id"`
	Detail    string `json:"detail"`
}
type rollingStatusFields struct {
	Maintenance         *maintenanceStatus `json:"maintenance,omitempty"`
	ActiveAttemptErrors []statusReason     `json:"active_attempt_evidence_errors,omitempty"`
	UpgradeState        string             `json:"upgrade_state,omitempty"`
	UpgradeDetail       string             `json:"upgrade_detail,omitempty"`
	OperationID         string             `json:"upgrade_operation_id,omitempty"`
	Phase               string             `json:"upgrade_phase,omitempty"`
	RecoveryState       string             `json:"upgrade_recovery_state,omitempty"`
	PrimaryID           string             `json:"primary_generation_id,omitempty"`
	Generations         []generationStatus `json:"generations,omitempty"`
	EvidenceErrors      []string           `json:"generation_evidence_errors,omitempty"`
}

func liveGenerationAttempt(s contract.AttemptState) bool {
	return s == contract.AttemptClaimed || s == contract.AttemptRunning || s == contract.AttemptAwaitingInput
}

// Cancellation reserves a Job outcome before its Attempt finishes. Only a
// completed Attempt is settled; lost/interrupted residents and live Lease
// problems retain their own evidence even when cancellation was requested.
func settledGenerationCancellation(a l1.NodeGenerationAttempt) bool {
	return a.JobOutcome == contract.JobOutcomeCanceled && a.State != nil &&
		(*a.State == contract.AttemptFailed || *a.State == contract.AttemptSucceeded)
}

// Only an accepted payload-owned exit settles a failed Attempt. Agent/guardian
// stops and lost/interrupted Attempts retain their diagnosis. Owner cancellation
// is settled separately through the authoritative Job outcome.
func settledGenerationPayloadExit(a l1.NodeGenerationAttempt) bool {
	return a.ObservationError == "" && a.JobOutcome == "" && a.TerminationInitiator == "" &&
		a.State != nil && *a.State == contract.AttemptFailed && a.ExitCode != nil && *a.ExitCode != 0
}

func projectGeneration(node l1.Node, g l1.NodeGenerationObservation, now time.Time) generationStatus {
	ns := nodeStatuses(l1.NodeList{Nodes: []l1.Node{node}})[0]
	result := generationStatus{NodeID: node.NodeID, ID: g.AgentGenerationID, Role: string(g.Role), Version: g.Version, RetirementStartedAt: g.RetirementStartedAt, SelectedProtocol: g.SelectedProtocol, RemainingRunIDs: []string{}, RemainingAttemptIDs: []string{}, UnledgeredAttemptIDs: []string{}, RenewalHealth: "healthy", Progress: "evidence_pending", Attempts: g.Attempts, Capacity: &generationCapacity{node.ClaimsEnabled, ns.FreeOneshot, ns.TotalOneshot, ns.FreeService, ns.TotalService}}
	if !g.CreatedAt.IsZero() && !g.CreatedAt.After(now) {
		age := int64(now.Sub(g.CreatedAt))
		result.Age = &age
	}
	if g.Role == l1.NodeGenerationClosed {
		zero := 0
		result.PendingEvidence = &zero
		result.Progress = "complete"
	}
	missingRenewal := false
	unknownAttempts := false
	hasLive := slices.ContainsFunc(g.Attempts, func(a l1.NodeGenerationAttempt) bool { return a.State != nil && liveGenerationAttempt(*a.State) })
	for _, a := range g.Attempts {
		state := contract.AttemptState("")
		if a.State != nil {
			state = *a.State
		}
		label := a.AttemptID
		if a.RunID != "" {
			label = "Run " + a.RunID + " (Attempt " + a.AttemptID + ")"
		}
		if a.ObservationError != "" {
			result.Problems = append(result.Problems, statusReason{"generation_observation_unverified", a.ObservationError})
			unknownAttempts = true
			result.RenewalHealth = "unverified"
			result.Progress = "unverified"
		}
		if (!hasLive || liveGenerationAttempt(state)) && a.LastRenewedAt != nil && (result.LastRenewedAt == nil || a.LastRenewedAt.Before(*result.LastRenewedAt)) {
			result.LastRenewedAt = a.LastRenewedAt
		}
		if (!hasLive || liveGenerationAttempt(state)) && a.LeaseExpiresAt != nil && (result.LeaseExpiresAt == nil || a.LeaseExpiresAt.Before(*result.LeaseExpiresAt)) {
			result.LeaseExpiresAt = a.LeaseExpiresAt
		}
		switch {
		case liveGenerationAttempt(state):
			result.Progress = "finishing"
			result.RemainingAttemptIDs = append(result.RemainingAttemptIDs, a.AttemptID)
			if a.RunID != "" {
				result.RemainingRunIDs = append(result.RemainingRunIDs, a.RunID)
			} else {
				result.UnledgeredAttemptIDs = append(result.UnledgeredAttemptIDs, a.AttemptID)
			}
			if g.PreviousSystemBoot {
				result.Problems = append(result.Problems, statusReason{"retiring_attempt_interrupted", label + ": interrupted after host reboot; prior-boot Lease is not survival evidence"})
				result.RenewalHealth = "degraded"
				result.Progress = "unverified"
			} else if a.LeaseExpiresAt != nil && !a.LeaseExpiresAt.After(now) {
				result.Problems = append(result.Problems, statusReason{"retiring_lease_stale", label + ": stale renewal; Lease expired at " + a.LeaseExpiresAt.Format(time.RFC3339Nano)})
				result.RenewalHealth = "degraded"
				result.Progress = "unverified"
			} else if a.LastRenewedAt == nil || a.LeaseExpiresAt == nil {
				missingRenewal = true
				if result.RenewalHealth != "degraded" {
					result.RenewalHealth = "unverified"
				}
			}
		case settledGenerationCancellation(a) || settledGenerationPayloadExit(a):
		case state == contract.AttemptLost || state == contract.AttemptInterrupted || state == contract.AttemptFailed:
			result.Problems = append(result.Problems, statusReason{"retiring_attempt_problem", interruptionDetail(label, state, a.InterruptionReason)})
			result.RenewalHealth = "degraded"
		case state == contract.AttemptSucceeded:
		default:
			if result.RenewalHealth != "degraded" {
				result.RenewalHealth = "unverified"
			}
			result.Progress = "unverified"
			unknownAttempts = true
			result.RemainingAttemptIDs = append(result.RemainingAttemptIDs, a.AttemptID)
		}
	}
	if unknownAttempts {
		result.RemainingRunIDs = nil
	}
	if missingRenewal {
		result.LastRenewedAt = nil
	}
	slices.Sort(result.RemainingRunIDs)
	result.RemainingRunIDs = slices.Compact(result.RemainingRunIDs)
	slices.Sort(result.RemainingAttemptIDs)
	slices.Sort(result.UnledgeredAttemptIDs)
	if g.Role == l1.NodeGenerationPrimary {
		result.Progress = "complete"
		if node.State != contract.NodeAlive && node.State != contract.NodeDraining {
			if result.RenewalHealth != "degraded" {
				result.RenewalHealth = "unverified"
			}
			result.Problems = append(result.Problems, statusReason{"primary_reachability_unverified", fmt.Sprintf("primary Node is %s; reachability unverified", node.State)})
		}
	}
	if g.ObservationError != "" {
		result.RenewalHealth = "unverified"
		result.Progress = "unverified"
		result.Problems = append(result.Problems, statusReason{"generation_observation_unverified", g.ObservationError})
	}

	return result
}

// The configured installation path, rather than the diagnosing process's HOME,
// selects the local records. Custom standalone node.json files have no implicit
// installation and therefore cause no unrelated home-directory reads.
func rollingDiagnosticPaths(nodeConfig string) (setupPaths, bool) {
	if nodeConfig == "" {
		return setupPaths{}, false
	}
	absolute, err := filepath.Abs(nodeConfig)
	if err != nil {
		return setupPaths{}, false
	}
	config := filepath.Dir(absolute)
	if filepath.Base(config) != "wefty" || filepath.Base(filepath.Dir(config)) != ".config" {
		return setupPaths{}, false
	}
	home := filepath.Dir(filepath.Dir(config))
	paths, _ := resolveMaintenancePaths(home)
	// resolveMaintenancePaths confines every destination; if a relevant folder
	// is linked/unreadable, preserve the cause without following it below.
	paths.config = config
	return paths, true
}

func collectRollingStatus(nodeConfig string, nodes l1.NodeList, reachable bool, now time.Time) rollingStatusFields {
	var result rollingStatusFields
	for _, node := range nodes.Nodes {
		for _, a := range node.ActiveAttempts {
			if a.ObservationError != "" {
				result.ActiveAttemptErrors = append(result.ActiveAttemptErrors, statusReason{"active_attempt_unverified", fmt.Sprintf("Node %s Job %s Attempt %s: observation unverified: %s", node.NodeID, a.JobID, a.AttemptID, a.ObservationError)})
			}
		}
		if node.AgentGenerationID != "" && len(node.Generations) == 0 {
			result.EvidenceErrors = append(result.EvidenceErrors, fmt.Sprintf("Node %s primary generation %s: generation observations unavailable; retiring generations unknown", node.NodeID, node.AgentGenerationID))
			result.Generations = append(result.Generations, generationStatus{NodeID: node.NodeID, ID: node.AgentGenerationID, Role: "primary", RenewalHealth: "unverified", Progress: "unverified"})
		}
		for _, g := range node.Generations {
			result.Generations = append(result.Generations, projectGeneration(node, g, now))
		}
	}
	paths, local := rollingDiagnosticPaths(nodeConfig)
	var receipt *upgradeRollingReceipt
	var catalog agentinstall.Catalog
	catalogPresent := false
	active := false
	if local {
		// Resolve again to retain confinement errors only for relevant directories.
		_, issues := resolveMaintenancePaths(paths.home)
		safeConfig, safeData, safeMaintenance := true, true, true
		var pathErrors []string
		for _, issue := range issues {
			if issue.folder == setupFolders.config {
				safeConfig = false
			}
			if issue.folder == setupFolders.data || issue.folder == setupFolders.launchAgents {
				safeData = false
			}
			if issue.folder == setupFolders.data {
				safeMaintenance = false
			}
			if issue.folder == setupFolders.config || issue.folder == setupFolders.data || issue.folder == setupFolders.launchAgents {
				pathErrors = append(pathErrors, issue.err.Error())
			}
		}
		if safeMaintenance {
			m, err := (agentinstall.Store{Data: paths.data}).ReadMaintenanceDiagnostic()
			if err != nil {
				result.EvidenceErrors = append(result.EvidenceErrors, "maintenance record: "+err.Error())
			} else if m != nil {
				kind := map[string]string{"remove": "removal", "convert": "conversion"}[m.Kind]
				result.Maintenance = &maintenanceStatus{Kind: m.Kind, Operation: m.Operation, Detail: fmt.Sprintf("maintenance in progress: %s %s; re-run the same wefty setup command to finish or restore", kind, m.Operation)}
			}
		}
		if safeConfig {
			var err error
			receipt, err = readUpgradeRolling(paths)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				result.EvidenceErrors = append(result.EvidenceErrors, "rolling receipt: "+err.Error())
			}
		}
		if safeData {
			store := agentinstall.Store{Data: paths.data, LaunchAgents: paths.launchAgents}
			var err error
			_, rootErr := os.Lstat(store.Root())
			catalog, err = store.ReadDiagnostic()
			if err == nil {
				catalogPresent = true
			} else if rootErr == nil || !errors.Is(err, os.ErrNotExist) {
				result.EvidenceErrors = append(result.EvidenceErrors, "generation catalog: "+err.Error())
			}
			if catalogPresent {
				result.PrimaryID = catalog.Primary
				ids := make([]string, 0, len(catalog.Generations))
				for id := range catalog.Generations {
					ids = append(ids, id)
				}
				sort.Strings(ids)
				for _, id := range ids {
					g := catalog.Generations[id]
					if g.Role != "primary" && g.Role != "retiring" {
						continue
					}
					found := -1
					for i := range result.Generations {
						if result.Generations[i].ID == id && result.Generations[i].NodeID == catalog.NodeID {
							found = i
							break
						}
					}
					if found < 0 {
						result.Generations = append(result.Generations, generationStatus{NodeID: catalog.NodeID, ID: id, Role: g.Role, Version: g.Version, RenewalHealth: "unverified", Progress: "unverified"})
						found = len(result.Generations) - 1
						for _, a := range g.ResidentAttempts {
							result.Generations[found].RemainingAttemptIDs = append(result.Generations[found].RemainingAttemptIDs, a.AttemptID)
						}
					}
					// Local creation time is retained across primary restarts.
					if !g.CreatedAt.IsZero() && !g.CreatedAt.After(now) {
						age := int64(now.Sub(g.CreatedAt))
						result.Generations[found].Age = &age
					}
					if !g.RetiringAt.IsZero() {
						at := g.RetiringAt
						result.Generations[found].RetirementStartedAt = &at
					}
					if g.Role == "retiring" {
						ack, err := store.ReadRetirementDiagnostic(id)
						if err != nil && !errors.Is(err, os.ErrNotExist) {
							result.EvidenceErrors = append(result.EvidenceErrors, "retirement acknowledgment "+id+": "+err.Error())
						}
						if err == nil && ack.Role == l1.NodeGenerationClosed && result.Generations[found].Role == "closed" {
							zero := 0
							result.Generations[found].PendingEvidence = &zero
						}
					}
				}
			}
		}
		if receipt != nil || catalogPresent || !safeMaintenance || result.Maintenance != nil || len(result.ActiveAttemptErrors) > 0 || len(result.Generations) > 0 || len(result.EvidenceErrors) > 0 {
			result.EvidenceErrors = append(result.EvidenceErrors, pathErrors...)
		}
		if safeData && ((receipt != nil && !terminalRollingPhase(receipt.Phase)) || (catalogPresent && catalog.PendingReceipt != "")) {
			var err error
			receipt, active, err = revalidateRollingCoordinator(paths, receipt, observeSetupLock)
			if err != nil {
				result.EvidenceErrors = append(result.EvidenceErrors, err.Error())
			}
			if catalogPresent && catalog.PendingReceipt != "" && err == nil && !active {
				fresh, readErr := (agentinstall.Store{Data: paths.data, LaunchAgents: paths.launchAgents}).ReadDiagnostic()
				if readErr != nil {
					result.EvidenceErrors = append(result.EvidenceErrors, "catalog revalidation: "+readErr.Error())
				} else if fresh.Revision != catalog.Revision || fresh.PendingReceipt != catalog.PendingReceipt {
					result.EvidenceErrors = append(result.EvidenceErrors, "catalog activation changed during diagnosis; rerun to verify")
				}
			}
		}
	}
	if len(result.Generations) == 0 && receipt == nil && !catalogPresent && len(result.EvidenceErrors) == 0 {
		return result
	}
	result.UpgradeState = "complete"
	if receipt != nil {
		result.OperationID, result.Phase, result.RecoveryState = receipt.OperationID, receipt.Phase, receipt.RecoveryState
		// A terminal phase alone is not promotion evidence. Legacy first
		// enrollment has no promotion request, but still records its winner.
		if receipt.Phase == "complete" {
			winner := false
			for _, node := range nodes.Nodes {
				winner = winner || (node.NodeID == receipt.NodeID && receipt.Candidate.ID != "" && node.AgentGenerationID == receipt.Candidate.ID)
			}
			if !winner {
				result.EvidenceErrors = append(result.EvidenceErrors, "completed rolling receipt: candidate primary is unverified")
			}
			if receipt.Promotion != nil && (receipt.Promotion.OperationID != receipt.OperationID || receipt.Promotion.CandidateGenerationID != receipt.Candidate.ID || receipt.PromotionResult == nil || receipt.ActivationReceiptID == "") {
				result.EvidenceErrors = append(result.EvidenceErrors, "completed rolling receipt: activation or committed promotion evidence is missing or inconsistent")
			}
		}
		terminal := terminalRollingPhase(receipt.Phase)
		if !terminal {
			if active {
				result.UpgradeState = "cutover"
			} else {
				result.UpgradeState = "interrupted"
			}
		}
		// Reboot interruptions remain problems after recovery completes, even when
		// an old generation's local files have already been cleaned up.
		for id, a := range receipt.RebootAttempts {
			if a.State != contract.AttemptLost && a.State != contract.AttemptInterrupted && a.State != contract.AttemptFailed {
				continue
			}
			found := false
			for i := range result.Generations {
				g := &result.Generations[i]
				if g.NodeID != receipt.NodeID {
					continue
				}
				for _, observed := range g.Attempts {
					if observed.AttemptID == id {
						found = true
						settled := settledGenerationPayloadExit(observed) || settledGenerationCancellation(observed)
						if a.State == contract.AttemptFailed && settled {
							// Settlement must come from this Node's current L1 snapshot,
							// never the saved reboot receipt's local completion copy.
							continue
						}
						if observed.State != nil && !settled && (*observed.State == contract.AttemptFailed || *observed.State == contract.AttemptLost || *observed.State == contract.AttemptInterrupted) {
							continue // Current snapshot already projects the Attempt problem.
						}
						label := "Attempt " + id
						if observed.RunID != "" {
							label = fmt.Sprintf("Run %s (Attempt %s)", observed.RunID, id)
						}
						qualification := "; current Attempt does not establish settled payload completion"
						if observed.State == nil {
							qualification = "; current Attempt state unknown"
						} else {
							g.RenewalHealth = "degraded"
						}
						g.Problems = append(g.Problems, statusReason{"retiring_attempt_problem", "recorded reboot observation: " + interruptionDetail(label, a.State, a.InterruptionReason) + qualification})
					}
				}
			}
			if found {
				continue
			}
			resident := receipt.TrackedResidents[id]
			generationID := ""
			version := ""
			for _, g := range receipt.Inventory.Generations {
				for _, r := range g.ResidentAttempts {
					if r.AttemptID == id {
						resident = r
						generationID = g.AgentGenerationID
						version = g.Registration.AgentVersion
					}
				}
			}
			// The receipt has no L3 mapping. Preserve the Attempt and Job identifiers
			// rather than inventing a Run ID from either of them.
			detail := interruptionDetail(fmt.Sprintf("Attempt %s (Job %s)", id, resident.JobID), a.State, a.InterruptionReason)
			if generationID == "" {
				result.EvidenceErrors = append(result.EvidenceErrors, detail+"; generation membership unknown")
				continue
			}
			result.Generations = append(result.Generations, generationStatus{NodeID: receipt.NodeID, ID: generationID, Version: version, Role: "history", RenewalHealth: "degraded", Progress: "unverified", Attempts: []l1.NodeGenerationAttempt{{JobID: resident.JobID, AttemptID: id, State: &a.State, InterruptionReason: a.InterruptionReason}}, Problems: []statusReason{{"retiring_attempt_problem", detail}}})
		}
	}
	if result.UpgradeState == "complete" && catalogPresent && catalog.PendingReceipt != "" {
		if active {
			result.UpgradeState = "cutover"
		} else {
			result.UpgradeState = "interrupted"
		}
	}
	if !reachable || len(result.EvidenceErrors) > 0 || (receipt != nil && receipt.Phase != "rolled_back" && len(result.Generations) == 0 && result.UpgradeState == "complete") {
		result.UpgradeState = "unverified"
	}
	remaining := map[string]bool{}
	retiring := 0
	unknown := false
	degraded := false
	unverified := false
	if catalogPresent && result.PrimaryID != "" {
		matched := false
		for _, node := range nodes.Nodes {
			if node.NodeID == catalog.NodeID && node.AgentGenerationID == catalog.Primary {
				matched = true
			}
		}
		if !matched && result.UpgradeState == "complete" {
			result.UpgradeState = "unverified"
		}
	}
	for _, g := range result.Generations {
		if g.RenewalHealth == "degraded" || len(g.Problems) > 0 {
			degraded = true
		}
		if g.RenewalHealth == "unverified" {
			unverified = true
		}
		if g.Role == "primary" && result.PrimaryID == "" {
			result.PrimaryID = g.ID
		}
		if g.Role == "staged" && result.UpgradeState == "complete" {
			result.UpgradeState = "unverified"
		}
		if g.Role != "retiring" {
			continue
		}
		retiring++
		if g.RemainingRunIDs == nil {
			unknown = true
		}
		for _, id := range g.RemainingRunIDs {
			remaining[id] = true
		}
	}
	switch result.UpgradeState {
	case "cutover":
		result.UpgradeDetail = "active rolling cutover; retry in a few seconds"
	case "interrupted":
		result.UpgradeDetail = "interrupted rolling upgrade; run wefty upgrade --rolling to resume or roll back"
	case "unverified":
		result.UpgradeDetail = "rolling upgrade reachability or evidence unverified; restore access and rerun status or doctor"
		if receipt != nil && !active && !terminalRollingPhase(receipt.Phase) {
			result.UpgradeDetail += "; incomplete receipt: run wefty upgrade --rolling to resume or roll back"
		}
	case "complete":
		result.UpgradeDetail = "upgrade complete"
		if receipt != nil && receipt.Phase == "rolled_back" {
			result.UpgradeDetail = "upgrade rolled back; previous installation retained"
		}
		if retiring > 0 {
			result.UpgradeDetail = fmt.Sprintf("upgrade complete; older generations finishing %d Runs", len(remaining))
			if unknown {
				result.UpgradeDetail = "upgrade complete; remaining Runs unknown"
			}
		}
		if degraded {
			result.UpgradeDetail = "upgrade complete; retiring Attempt problems need attention"
		} else if unverified {
			result.UpgradeDetail = "upgrade complete; generation renewal evidence unverified"
		}
	}
	sort.SliceStable(result.Generations, func(i, j int) bool {
		a, b := result.Generations[i], result.Generations[j]
		if a.NodeID != b.NodeID {
			return a.NodeID < b.NodeID
		}
		if (a.Role == "primary") != (b.Role == "primary") {
			return a.Role == "primary"
		}
		return a.ID < b.ID
	})
	return result
}

func terminalRollingPhase(phase string) bool {
	return slices.Contains([]string{"complete", "rolled_back", "reboot_reconciled"}, phase)
}

// Re-read after observing a free kernel lock. A writer may have completed or
// replaced the operation since the first receipt read; that is not interruption.
func revalidateRollingCoordinator(paths setupPaths, receipt *upgradeRollingReceipt, probe func(setupPaths) setupLockObservation) (*upgradeRollingReceipt, bool, error) {
	lock := probe(paths)
	if lock.state == setupLockUnknown {
		if lock.cause == nil {
			lock.cause = errors.New("setup lock state unknown")
		}
		return receipt, false, lock.cause
	}
	if lock.state == setupLockHeldState {
		return receipt, true, nil
	}
	if receipt == nil {
		return nil, false, nil
	}
	fresh, err := readUpgradeRolling(paths)
	if err != nil {
		return receipt, false, fmt.Errorf("rolling receipt revalidation after free setup lock: %w", err)
	}
	if fresh.OperationID != receipt.OperationID || fresh.NodeID != receipt.NodeID || fresh.Candidate.ID != receipt.Candidate.ID || fresh.Phase != receipt.Phase {
		return fresh, false, errors.New("rolling receipt identity or phase changed during diagnosis; rerun to verify")
	}
	return fresh, false, nil
}

func (r rollingStatusFields) findings() []statusReason {
	var findings []statusReason
	if r.Maintenance != nil {
		findings = append(findings, statusReason{"maintenance_in_progress", r.Maintenance.Detail})
	}
	findings = append(findings, r.ActiveAttemptErrors...)
	if r.UpgradeState == "interrupted" || r.UpgradeState == "unverified" {
		findings = append(findings, statusReason{"rolling_" + r.UpgradeState, r.UpgradeDetail})
	}
	for _, cause := range r.EvidenceErrors {
		findings = append(findings, statusReason{"rolling_evidence_unverified", cause})
	}
	for _, g := range r.Generations {
		for _, problem := range g.Problems {
			problem.Detail = "generation " + g.ID + ": " + problem.Detail
			findings = append(findings, problem)
		}
		if g.RenewalHealth == "unverified" {
			findings = append(findings, statusReason{"generation_renewal_unverified", "generation " + g.ID + ": renewal evidence unknown; Attempts " + strings.Join(g.RemainingAttemptIDs, ", ")})
		}
	}
	return findings
}
func writeRollingStatus(out io.Writer, r rollingStatusFields) error {
	if r.UpgradeState != "" {
		if _, err := fmt.Fprintf(out, "%s [%s]\n", r.UpgradeDetail, r.UpgradeState); err != nil {
			return err
		}
	}
	if r.PrimaryID != "" {
		if _, err := fmt.Fprintln(out, "primary generation:", r.PrimaryID); err != nil {
			return err
		}
	}
	for _, g := range r.Generations {
		age := "unknown"
		if g.Age != nil {
			age = time.Duration(*g.Age).String()
		}
		runs := "unknown"
		if g.RemainingRunIDs != nil {
			runs = fmt.Sprintf("%d (%s)", len(g.RemainingRunIDs), strings.Join(g.RemainingRunIDs, ", "))
		}
		renewal, expiry := "unknown", "unknown"
		if g.LastRenewedAt != nil {
			renewal = g.LastRenewedAt.Format(time.RFC3339Nano)
		}
		if g.LeaseExpiresAt != nil {
			expiry = g.LeaseExpiresAt.Format(time.RFC3339Nano)
		}
		capacity := "unknown"
		if g.Capacity != nil {
			c := g.Capacity
			capacity = fmt.Sprintf("one-shot %d/%d; service %d/%d; claims %t (shared)", c.FreeOneshot, c.TotalOneshot, c.FreeService, c.TotalService, c.ClaimsEnabled)
		}
		retirement := "unknown"
		if g.RetirementStartedAt != nil {
			retirement = g.RetirementStartedAt.Format(time.RFC3339Nano)
		}
		evidence := "unknown"
		if g.PendingEvidence != nil {
			evidence = fmt.Sprint(*g.PendingEvidence)
		}
		if _, err := fmt.Fprintf(out, "generation %s %s: version %s; age %s; remaining Runs %s; renewal %s; last renewal %s; deadline %s; capacity %s; %s; pending evidence %s; protocol %d; retirement started %s\n", g.ID, g.Role, unknownGenerationVersion(g.Version), age, runs, g.RenewalHealth, renewal, expiry, capacity, g.Progress, evidence, g.SelectedProtocol, retirement); err != nil {
			return err
		}
		for _, a := range g.Attempts {
			if settledGenerationCancellation(a) {
				label := "Attempt " + a.AttemptID
				if a.RunID != "" {
					label = "Run " + a.RunID + " (Attempt " + a.AttemptID + ")"
				}
				if _, err := fmt.Fprintf(out, "  %s: canceled\n", label); err != nil {
					return err
				}
			}
		}
		if len(g.UnledgeredAttemptIDs) > 0 {
			if _, err := fmt.Fprintln(out, "  Attempts without an L3 Run:", strings.Join(g.UnledgeredAttemptIDs, ", ")); err != nil {
				return err
			}
		}
	}
	for _, f := range r.findings() {
		if _, err := fmt.Fprintln(out, "  "+f.Detail); err != nil {
			return err
		}
	}
	return nil
}

// Reuse client read authority. No node principal, private control request,
// registration, local socket, persistence, or launchd mutation is needed.
func appendDoctorRolling(ctx context.Context, host setupHost, options globalOptions, report *plainDoctorReport, observed ...l1.NodeList) {
	if host.goos != "darwin" {
		return
	}
	paths, _ := resolveMaintenancePaths(host.home)
	config := options.nodeConfigPath
	if config == "" {
		config = paths.nodeConfig
	}
	var initialNodes l1.NodeList
	if len(observed) > 0 {
		initialNodes = observed[0]
	}
	initial := collectRollingStatus(config, initialNodes, len(initialNodes.Nodes) > 0, time.Now().UTC())
	if initial.UpgradeState == "" && len(initial.findings()) == 0 {
		return
	}
	nodes := initialNodes
	var runClients *apiClients
	reachable := false
	if options.profilePath == "" {
		options.profilePath = paths.profile
	}
	options.nodeConfigPath = config
	cause := applyConnectionProfile(&options)
	if cause == nil && options.fabricMode == "plain" {
		participant, closeFabric, err := fabricconfig.Open(ctx, fabricconfig.Config{Mode: "plain", Identity: fabric.Identity{NodeID: options.plainIdentity, Tags: []string{l1.DefaultClientPrincipalTag}}, PlainFabricID: options.plainFabricID})
		if closeFabric != nil {
			defer closeFabric()
		}
		cause = err
		if err == nil {
			clients, err := newAPIClients(participant, options.l1Address, options.l3Address)
			cause = err
			if err == nil {
				defer clients.close()
				runClients = clients
				bounded, cancel := context.WithTimeout(ctx, statusProbeBudget)
				defer cancel()
				fresh, readErr := clients.listNodes(bounded)
				cause = readErr
				reachable = readErr == nil
				if reachable {
					nodes = fresh
				}
			}
		}
	} else if cause == nil {
		cause = errors.New("rolling doctor requires a verified client read path")
	}
	report.rollingStatusFields = collectRollingStatus(config, nodes, reachable, time.Now().UTC())
	verifyRollingRuns(ctx, runClients, &report.rollingStatusFields)
	if cause != nil {
		report.EvidenceErrors = append(report.EvidenceErrors, "L1 read: "+cause.Error())
	}
	findings := report.rollingStatusFields.findings()
	if len(findings) == 0 {
		report.Findings = append(report.Findings, plainDoctorFinding{"rolling-upgrade", "ok", report.UpgradeDetail, ""})
	} else {
		report.Healthy = false
		for _, f := range findings {
			severity := "error"
			if strings.Contains(f.Code, "unverified") {
				severity = "warning"
			}
			fix := "run wefty upgrade --rolling to resume or roll back an interrupted upgrade; restore missing evidence and rerun doctor"
			if f.Code == "maintenance_in_progress" {
				fix = "re-run the same wefty setup command to finish or restore"
			}
			if f.Code == "active_attempt_unverified" {
				fix = "restore the retained Attempt evidence and rerun doctor"
			}
			report.Findings = append(report.Findings, plainDoctorFinding{f.Code, severity, f.Detail, fix})
		}
	}
}

func interruptionDetail(label string, state contract.AttemptState, reason l1.InterruptionReason) string {
	detail := fmt.Sprintf("%s: %s", label, state)
	if reason != "" {
		detail += " (" + string(reason) + ")"
	}
	return detail
}

func unknownGenerationVersion(version string) string {
	if version == "" {
		return "unknown"
	}
	return version
}

// A Run label identifies a ledger record; it does not prove that record is
// nonterminal. Cancellation and result publication can settle a Run before its
// resident Attempt finishes teardown. Verify distinct Run IDs through the
// existing public L3 read API before presenting the healthy finishing count.
func verifyRollingRuns(ctx context.Context, clients *apiClients, report *rollingStatusFields) {
	ids := map[string]bool{}
	for _, g := range report.Generations {
		for _, id := range g.RemainingRunIDs {
			ids[id] = true
		}
	}
	if len(ids) == 0 {
		return
	}
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	states := map[string]contract.RunState{}
	probe, cancel := context.WithTimeout(ctx, statusProbeBudget)
	defer cancel()
	for _, id := range ordered {
		var state contract.RunState
		var cause error
		if clients == nil || clients.l3 == nil || clients.l3.client == nil {
			cause = errors.New("L3 client read path is unavailable")
		} else {
			run, err := clients.getRun(probe, id)
			cause = err
			if err == nil {
				if run.RunID != id {
					cause = errors.New("L3 returned a different or missing Run identity")
				} else if _, known := contract.RunTransitions[run.Status]; !known {
					cause = errors.New("L3 returned an unknown Run state")
				} else {
					state = run.Status
				}
			}
		}
		if cause != nil {
			report.EvidenceErrors = append(report.EvidenceErrors, "Run "+id+": nonterminal state unverified: "+cause.Error())
		} else {
			states[id] = state
		}
	}
	remaining := map[string]bool{}
	unknown := false
	for i := range report.Generations {
		g := &report.Generations[i]
		kept := []string{}
		missing := false
		for _, id := range g.RemainingRunIDs {
			state, ok := states[id]
			if !ok {
				missing = true
				unknown = true
				continue
			}
			if state != contract.RunSucceeded && state != contract.RunFailed {
				kept = append(kept, id)
				if g.Role == "retiring" {
					remaining[id] = true
				}
			}
		}
		if g.RemainingRunIDs != nil {
			g.RemainingRunIDs = kept
		}
		if missing {
			g.RemainingRunIDs = nil
		}
	}
	if unknown {
		previousState := report.UpgradeState
		report.UpgradeState = "unverified"
		report.UpgradeDetail = "rolling upgrade Run reachability unverified; restore L3 read access and rerun status or doctor"
		if previousState == "interrupted" {
			report.UpgradeDetail += "; run wefty upgrade --rolling to resume or roll back"
		}
		if previousState == "cutover" {
			report.UpgradeDetail += "; active rolling cutover; retry in a few seconds"
		}
	} else if report.UpgradeState == "complete" && strings.HasPrefix(report.UpgradeDetail, "upgrade complete; older generations finishing ") {
		report.UpgradeDetail = fmt.Sprintf("upgrade complete; older generations finishing %d Runs", len(remaining))
	}
}
