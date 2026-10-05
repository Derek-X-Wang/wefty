package l1

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
)

// One additive job-row migration and one partial unique index cover both
// classes. Terminal one-shots leave the index in the state-changing transaction;
// services leave it only when removal finalization deletes their ordinary row.
// No existing CHECK constraint is widened or table rebuilt.
const liveInstanceKeyPredicate = `instance_key IS NOT NULL AND
 (instance_class='service' OR (instance_class='one-shot' AND state NOT IN ('succeeded', 'failed')))`

func (s *Store) initializeInstanceKeys(ctx context.Context) error {
	for _, column := range []struct{ name, definition string }{
		{"instance_namespace", "TEXT NOT NULL DEFAULT ''"},
		{"instance_key", "TEXT"},
		{"instance_class", "TEXT NOT NULL DEFAULT '' CHECK(instance_class IN ('', 'one-shot', 'service'))"},
	} {
		if err := s.ensureColumn(ctx, "jobs", column.name, column.definition); err != nil {
			return err
		}
	}
	_, err := s.db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS jobs_live_instance_key
 ON jobs(instance_namespace, instance_key) WHERE `+liveInstanceKeyPredicate)
	if err != nil {
		return internalError(err, "initialize instance-key uniqueness")
	}
	return nil
}

// Parent namespaces follow the authenticated job, so all of its attempts share
// a reservation. Tags keep them disjoint from arbitrary Fabric identities.
func instanceNamespace(origin JobOrigin) string {
	if origin.Parent != nil {
		return "job:" + origin.Parent.JobID
	}
	return "fabric:" + strings.TrimSpace(origin.OriginatingSubmitter)
}

func instanceClass(spec contract.JobSpec) string {
	if spec.InstanceKey == nil {
		return ""
	}
	return spec.Class
}

func instanceKeyConflict(ctx context.Context, q queryer, spec contract.JobSpec, origin JobOrigin) error {
	if spec.InstanceKey == nil {
		return nil
	}
	var holderID, parentID, submitter string
	err := q.QueryRowContext(ctx, `SELECT job_id, COALESCE(parent_job_id, ''), originating_submitter
 FROM jobs WHERE instance_namespace=? AND instance_key=? AND `+liveInstanceKeyPredicate,
		instanceNamespace(origin), *spec.InstanceKey).Scan(&holderID, &parentID, &submitter)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return internalError(err, "read instance-key holder")
	}
	details := map[string]any{"instance_key": *spec.InstanceKey}
	// Client principals can read ordinary jobs; attempt credentials can only
	// read their own job and children. Keep disclosure scoped even in recovery.
	if replayWithinScope(origin, Job{JobID: holderID, ParentJobID: parentID, OriginatingSubmitter: submitter}) {
		details["job_id"] = holderID
	}
	return protocolErrorWithDetails(contract.ErrorInstanceKeyConflict, details, "instance key is already reserved by a live job")
}
