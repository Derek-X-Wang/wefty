package l1

import (
	"context"
	"encoding/json"
)

// ClaimPollResponse preserves the claim wire shape. A hint-only reply carries
// no Attempt, credential, fencing token or lease. It grants no authority.
type ClaimPollResponse struct {
	*Claim
	QueuedOCIWork bool `json:"queued_oci_work,omitempty"`
}

// ClaimJobPoll observes the hint inside the ordinary claim transaction. A hint
// failure cannot turn a committed claim into a lost HTTP error response.
func (s *Store) ClaimJobPoll(ctx context.Context, identity, node, boot, class string, excluded ...string) (ClaimPollResponse, error) {
	var waiting bool
	claim, err := s.claimJob(ctx, identity, node, boot, class, &waiting, excluded...)
	return ClaimPollResponse{Claim: claim, QueuedOCIWork: waiting}, err
}

// Both workload classes share demand, so an empty class poll cannot erase the
// other class's hint. Capacity and runtime facts remain ClaimJob's authority.
func queuedOCIWork(ctx context.Context, q queryer, node string) (bool, error) {
	var waiting bool
	var caps, missing []byte
	if err := q.QueryRowContext(ctx, `SELECT capabilities_json, missing_capabilities_json FROM nodes WHERE node_id=?`, node).Scan(&caps, &missing); err != nil {
		return false, err
	}
	var capabilities map[string]bool
	if err := json.Unmarshal(caps, &capabilities); err != nil {
		return false, err
	}
	if capabilities["kind:oci"] {
		return false, nil
	}
	var withdrawn []string
	if err := json.Unmarshal(missing, &withdrawn); err != nil {
		return false, err
	}
	restorable, err := json.Marshal(hostVMRestorableCapabilities(capabilities, withdrawn))
	if err != nil {
		return false, err
	}
	err = q.QueryRowContext(ctx, `WITH candidates AS (
 SELECT c.current_job_id AS job_id FROM computers c WHERE c.placement_node_id=? AND c.desired_state='running'
 UNION ALL
 SELECT j.job_id FROM jobs j LEFT JOIN service_jobs s ON s.job_id=j.job_id
 WHERE j.state='queued' AND j.outcome='' AND json_extract(j.spec_json,'$.kind')='oci'
 AND (s.job_id IS NULL OR (s.desired_state='running' AND (s.bound_node_id IS NULL OR s.bound_node_id=?) AND s.policy_stop_json IS NULL))
 AND NOT EXISTS(SELECT 1 FROM job_tags jt WHERE jt.job_id=j.job_id AND NOT EXISTS(SELECT 1 FROM node_tags nt WHERE nt.node_id=? AND nt.tag=jt.tag))
 ) SELECT EXISTS(SELECT 1 FROM candidates j
 WHERE NOT EXISTS(SELECT 1 FROM job_required_capabilities r WHERE r.job_id=j.job_id
 AND NOT EXISTS(SELECT 1 FROM json_each(?) c WHERE c.key=r.capability AND c.value=1)))`, node, node, node, restorable).Scan(&waiting)
	return waiting, err
}

// Idle withdrawal lists the OCI probe capabilities it removed. Do not invent
// a runtime handler or host feature that this node never advertised.
func hostVMRestorableCapabilities(current map[string]bool, withdrawn []string) map[string]bool {
	result := make(map[string]bool, len(current)+len(withdrawn)+1)
	for key, value := range current {
		result[key] = value
	}
	result["kind:oci"] = true
	for _, capability := range withdrawn {
		if isOCIProbeCapability(capability) {
			result[capability] = true
		}
	}
	return result
}
