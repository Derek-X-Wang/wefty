package l1

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// emptyClaimPoll answers a claim from a read-only snapshot when the claim's
// write transaction would change nothing, so a poll that finds no work does
// not queue for SQLite's write lock ahead of lease renewals (#1686). It
// reports empty only for a registered, alive Node of this identity and boot
// session whose heartbeat needs no liveness transition, and only when no
// queued pre-start budget has expired (one-shot) and no job is claimable. In
// every other case, refusals included, the write transaction decides and
// re-checks everything, so it stays the one authority for claims and their
// errors. waiting is the queued OCI work hint, read when hint is set; it
// means nothing unless empty is true.
//
// A busy read pool falls back to the write transaction, which is what every
// claim did before this check existed. Checks run one at a time (see
// Store.emptyClaimPollMu); each one is bounded by the read snapshot's limits.
func (s *Store) emptyClaimPoll(ctx context.Context, identityNodeID, nodeID, bootSessionID, class string, hint bool, exclusionClause string, exclusionArguments []any) (empty, waiting bool, err error) {
	s.emptyClaimPollMu.Lock()
	defer s.emptyClaimPollMu.Unlock()
	err = s.withReadSnapshot(ctx, nil, func(ctx context.Context, reads readModel) error {
		q := reads.(*databaseReads).q
		now := reads.now()
		if attemptClockError(ctx) != nil {
			return nil
		}
		var storedIdentity, storedBoot string
		var state contract.NodeState
		var heartbeatNS, authorityGeneration int64
		var claimsEnabled bool
		err := q.QueryRowContext(ctx, "SELECT identity_node_id, boot_session_id, state, last_heartbeat_ns+heartbeat_sleep_ns, authority_generation, claims_enabled FROM nodes WHERE node_id=?", nodeID).
			Scan(&storedIdentity, &storedBoot, &state, &heartbeatNS, &authorityGeneration, &claimsEnabled)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if storedIdentity != identityNodeID || storedBoot != bootSessionID || state != contract.NodeAlive ||
			s.claimingNodeLiveness(ctx, state, time.Unix(0, heartbeatNS).UTC(), now) != state {
			return nil
		}
		// A claims-disabled Node is eligible for nothing and gets no hint.
		if !claimsEnabled {
			empty = true
			return nil
		}
		if hint {
			if waiting, err = queuedOCIWork(ctx, q, nodeID); err != nil {
				return err
			}
		}
		var found bool
		if class == contract.JobClassOneShot {
			if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM jobs WHERE `+expiredPrestartJobs+`)`,
				contract.JobQueued, now.UnixNano()).Scan(&found); err != nil || found {
				return err
			}
		}
		if err := q.QueryRowContext(ctx, fmt.Sprintf(`SELECT EXISTS(`+claimCandidateQuery+`)`, exclusionClause),
			claimCandidateArguments(nodeID, bootSessionID, class, now, authorityGeneration, exclusionArguments)...).Scan(&found); err != nil || found {
			return err
		}
		empty = true
		return nil
	})
	var typed *Error
	if errors.As(err, &typed) && typed.Code == contract.ErrorUnavailable {
		return false, false, nil
	}
	if err != nil {
		return false, false, internalError(err, "read claim demand")
	}
	return empty, waiting, nil
}
