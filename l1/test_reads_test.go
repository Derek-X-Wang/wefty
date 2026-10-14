package l1

import (
	"context"

	"github.com/Derek-X-Wang/wefty/fabric"
)

// testReadStore exposes only reads. Mutations must keep using the real Store.
// Each method retries a complete read, retaining its context and arguments.
type testReadStore struct{ store *Store }

func testReads(store *Store) testReadStore { return testReadStore{store} }

func (r testReadStore) GetAdminPolicy(ctx context.Context) (AdminPolicy, error) {
	return retryTestStatusRead(ctx, func() (AdminPolicy, error) { return r.store.GetAdminPolicy(ctx) })
}

func (r testReadStore) GetComputer(ctx context.Context, computerID string) (Computer, error) {
	return retryTestStatusRead(ctx, func() (Computer, error) { return r.store.GetComputer(ctx, computerID) })
}

func (r testReadStore) GetComputerCustodyImport(ctx context.Context, importID string) (ComputerCustodyImportObservation, error) {
	return retryTestStatusRead(ctx, func() (ComputerCustodyImportObservation, error) {
		return r.store.GetComputerCustodyImport(ctx, importID)
	})
}

func (r testReadStore) GetComputerPolicyRevocation(ctx context.Context, identity fabric.Identity, revision int64, computerID, fabricID, userID string) (ComputerPolicyRevocation, error) {
	return retryTestStatusRead(ctx, func() (ComputerPolicyRevocation, error) {
		return r.store.GetComputerPolicyRevocation(ctx, identity, revision, computerID, fabricID, userID)
	})
}

func (r testReadStore) GetComputerSubmissionState(ctx context.Context, identity fabric.Identity, computerID string) (ComputerSubmissionState, error) {
	return retryTestStatusRead(ctx, func() (ComputerSubmissionState, error) {
		return r.store.GetComputerSubmissionState(ctx, identity, computerID)
	})
}

func (r testReadStore) GetJob(ctx context.Context, jobID string) (Job, error) {
	return retryTestStatusRead(ctx, func() (Job, error) { return r.store.GetJob(ctx, jobID) })
}

func (r testReadStore) GetJobLogs(ctx context.Context, jobID, cursor string, limit int) (LogPage, error) {
	return retryTestStatusRead(ctx, func() (LogPage, error) { return r.store.GetJobLogs(ctx, jobID, cursor, limit) })
}

func (r testReadStore) GetJobResult(ctx context.Context, jobID string) (JobResult, error) {
	return retryTestStatusRead(ctx, func() (JobResult, error) { return r.store.GetJobResult(ctx, jobID) })
}

func (r testReadStore) ListChildJobs(ctx context.Context, parentJobID, cursorValue string, limit int) (JobList, error) {
	return retryTestStatusRead(ctx, func() (JobList, error) { return r.store.ListChildJobs(ctx, parentJobID, cursorValue, limit) })
}

func (r testReadStore) ListComputerBackups(ctx context.Context, computerID string) (BackupList, error) {
	return retryTestStatusRead(ctx, func() (BackupList, error) { return r.store.ListComputerBackups(ctx, computerID) })
}

func (r testReadStore) ListComputerBackupsPage(ctx context.Context, computerID, cursor, backupID string, limit int) (BackupList, error) {
	return retryTestStatusRead(ctx, func() (BackupList, error) {
		return r.store.ListComputerBackupsPage(ctx, computerID, cursor, backupID, limit)
	})
}

func (r testReadStore) ListComputerCustodyExports(ctx context.Context, computerID string) ([]ComputerCustodyExport, error) {
	return retryTestStatusRead(ctx, func() ([]ComputerCustodyExport, error) { return r.store.ListComputerCustodyExports(ctx, computerID) })
}

func (r testReadStore) ListComputerCustodyExportsPage(ctx context.Context, computerID, cursor string, limit int) (ComputerCustodyExportList, error) {
	return retryTestStatusRead(ctx, func() (ComputerCustodyExportList, error) {
		return r.store.ListComputerCustodyExportsPage(ctx, computerID, cursor, limit)
	})
}

func (r testReadStore) ListComputerGrants(ctx context.Context, identity fabric.Identity, computerID string) (ComputerGrantList, error) {
	return retryTestStatusRead(ctx, func() (ComputerGrantList, error) { return r.store.ListComputerGrants(ctx, identity, computerID) })
}

func (r testReadStore) ListComputerIntents(ctx context.Context, computerID, cursorValue string, limit int) (ComputerIntentList, error) {
	return retryTestStatusRead(ctx, func() (ComputerIntentList, error) {
		return r.store.ListComputerIntents(ctx, computerID, cursorValue, limit)
	})
}

func (r testReadStore) ListComputerPolicyAudit(ctx context.Context, identity fabric.Identity, computerID, cursor string, limit int) (ComputerPolicyAuditList, error) {
	return retryTestStatusRead(ctx, func() (ComputerPolicyAuditList, error) {
		return r.store.ListComputerPolicyAudit(ctx, identity, computerID, cursor, limit)
	})
}

func (r testReadStore) ListComputerStorageGenerations(ctx context.Context, computerID string) (ComputerStorageGenerationList, error) {
	return retryTestStatusRead(ctx, func() (ComputerStorageGenerationList, error) {
		return r.store.ListComputerStorageGenerations(ctx, computerID)
	})
}

func (r testReadStore) ListComputerStorageGenerationsPage(ctx context.Context, computerID, cursor string, limit int) (ComputerStorageGenerationList, error) {
	return retryTestStatusRead(ctx, func() (ComputerStorageGenerationList, error) {
		return r.store.ListComputerStorageGenerationsPage(ctx, computerID, cursor, limit)
	})
}

func (r testReadStore) ListComputerStorageProvenance(ctx context.Context, computerID string) (ComputerStorageProvenance, error) {
	return retryTestStatusRead(ctx, func() (ComputerStorageProvenance, error) {
		return r.store.ListComputerStorageProvenance(ctx, computerID)
	})
}

func (r testReadStore) ListComputerTakeoverAudit(ctx context.Context, identity fabric.Identity, computerID, cursor string, limit int, tail bool) (ComputerTakeoverAuditList, error) {
	return retryTestStatusRead(ctx, func() (ComputerTakeoverAuditList, error) {
		return r.store.ListComputerTakeoverAudit(ctx, identity, computerID, cursor, limit, tail)
	})
}

func (r testReadStore) ListComputerTakeoverSessions(ctx context.Context, identity fabric.Identity, computerID string) (ComputerTakeoverSessionList, error) {
	return retryTestStatusRead(ctx, func() (ComputerTakeoverSessionList, error) {
		return r.store.ListComputerTakeoverSessions(ctx, identity, computerID)
	})
}

func (r testReadStore) ListComputers(ctx context.Context, cursorValue string, limit int) (ComputerList, error) {
	return retryTestStatusRead(ctx, func() (ComputerList, error) { return r.store.ListComputers(ctx, cursorValue, limit) })
}

func (r testReadStore) ListJobAttempts(ctx context.Context, jobID string) ([]Attempt, error) {
	return retryTestStatusRead(ctx, func() ([]Attempt, error) { return r.store.ListJobAttempts(ctx, jobID) })
}

func (r testReadStore) ListNodes(ctx context.Context) ([]Node, error) {
	return retryTestStatusRead(ctx, func() ([]Node, error) { return r.store.ListNodes(ctx) })
}

func (r testReadStore) ListServiceJobs(ctx context.Context, cursorValue string, limit int) (JobList, error) {
	return retryTestStatusRead(ctx, func() (JobList, error) { return r.store.ListServiceJobs(ctx, cursorValue, limit) })
}

func (r testReadStore) ProveComputerTokenScope(ctx context.Context, computerID, attemptID, hostIdentityNodeID, hostNodeID string) (ComputerTokenScopeProof, error) {
	return retryTestStatusRead(ctx, func() (ComputerTokenScopeProof, error) {
		return r.store.ProveComputerTokenScope(ctx, computerID, attemptID, hostIdentityNodeID, hostNodeID)
	})
}

func (r testReadStore) ProveHostBootSession(ctx context.Context, hostIdentityNodeID, hostStableNodeID, bootSessionID string) error {
	_, err := retryTestStatusRead(ctx, func() (struct{}, error) {
		return struct{}{}, r.store.ProveHostBootSession(ctx, hostIdentityNodeID, hostStableNodeID, bootSessionID)
	})
	return err
}

func (r testReadStore) ProveServiceBinding(ctx context.Context, identityNodeID, jobID string, request ServiceBindingProofRequest) (bool, error) {
	return retryTestStatusRead(ctx, func() (bool, error) { return r.store.ProveServiceBinding(ctx, identityNodeID, jobID, request) })
}

func (r testReadStore) RawJobLogJSONL(ctx context.Context, jobID string) ([]byte, error) {
	return retryTestStatusRead(ctx, func() ([]byte, error) { return r.store.RawJobLogJSONL(ctx, jobID) })
}

func (r testReadStore) ResolveAttemptCredential(ctx context.Context, token, identityNodeID string) (AttemptCredentialScope, error) {
	return retryTestStatusRead(ctx, func() (AttemptCredentialScope, error) {
		return r.store.ResolveAttemptCredential(ctx, token, identityNodeID)
	})
}

func (r testReadStore) listNodesPage(ctx context.Context, filters nodeListFilters, cursorValue string, limit int) (NodeList, error) {
	return retryTestStatusRead(ctx, func() (NodeList, error) { return r.store.listNodesPage(ctx, filters, cursorValue, limit) })
}

func (r testReadStore) listReadableJobs(ctx context.Context, filters jobListFilters, cursorValue string, limit int) (JobList, error) {
	return retryTestStatusRead(ctx, func() (JobList, error) { return r.store.listReadableJobs(ctx, filters, cursorValue, limit) })
}

func (r testReadStore) listReadableJobsForCaller(ctx context.Context, filters jobListFilters, cursorValue string, limit int, actor *serviceActionActor) (JobList, error) {
	return retryTestStatusRead(ctx, func() (JobList, error) {
		return r.store.listReadableJobsForCaller(ctx, filters, cursorValue, limit, actor)
	})
}

func (r testReadStore) owedComputerRevocation(ctx context.Context, revocationID int64) (owedRevocationRow, error) {
	return retryTestStatusRead(ctx, func() (owedRevocationRow, error) { return r.store.owedComputerRevocation(ctx, revocationID) })
}

func (r testReadStore) GetNode(ctx context.Context, nodeID string) (Node, error) {
	return retryTestStatusRead(ctx, func() (Node, error) { return r.store.GetNode(ctx, nodeID) })
}
func (r testReadStore) LookupRunLedgerJob(ctx context.Context, dispatchKey string) (Job, error) {
	return retryTestStatusRead(ctx, func() (Job, error) { return r.store.LookupRunLedgerJob(ctx, dispatchKey) })
}
func (r testReadStore) listChildJobsForCaller(ctx context.Context, parentJobID, cursorValue string, limit int, actor *serviceActionActor) (JobList, error) {
	return retryTestStatusRead(ctx, func() (JobList, error) {
		return r.store.listChildJobsForCaller(ctx, parentJobID, cursorValue, limit, actor)
	})
}
func (r testReadStore) withReadSnapshot(ctx context.Context, caller *serviceActionActor, use func(context.Context, readModel) error) error {
	_, err := retryTestStatusRead(ctx, func() (struct{}, error) { return struct{}{}, r.store.withReadSnapshot(ctx, caller, use) })
	return err
}
