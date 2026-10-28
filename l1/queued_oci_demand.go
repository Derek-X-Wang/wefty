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
	var caps []byte
	if err := q.QueryRowContext(ctx, `SELECT capabilities_json FROM nodes WHERE node_id=?`, node).Scan(&caps); err != nil {
		return false, err
	}
	var capabilities map[string]bool
	if err := json.Unmarshal(caps, &capabilities); err != nil {
		return false, err
	}
	if capabilities["kind:oci"] {
		return false, nil
	}
	err := q.QueryRowContext(ctx, `SELECT EXISTS(
  SELECT 1 FROM computers c WHERE c.placement_node_id=? AND c.desired_state='running'
  UNION ALL
  SELECT 1 FROM jobs j LEFT JOIN service_jobs s ON s.job_id=j.job_id
  WHERE j.state='queued' AND j.outcome='' AND json_extract(j.spec_json,'$.kind')='oci'
   AND (s.job_id IS NULL OR (s.desired_state='running' AND (s.bound_node_id IS NULL OR s.bound_node_id=?) AND s.policy_stop_json IS NULL))
   AND NOT EXISTS(SELECT 1 FROM job_tags jt WHERE jt.job_id=j.job_id AND NOT EXISTS(SELECT 1 FROM node_tags nt WHERE nt.node_id=? AND nt.tag=jt.tag))
 )`, node, node, node).Scan(&waiting)
	return waiting, err
}
