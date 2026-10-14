package l1

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func testReadAnswer[T any](answer T, err error) (any, error) { return answer, err }

func TestTestReadStoreRecoversFromRealSnapshotExpiry(t *testing.T) {
	h, node, computer, backup, claim := publishedBackupForStorageCopy(t, 3)
	h.stopServer()
	admin, policy := computerSnapshotAdmin(t, h)
	reads := testReads(h.store)
	for _, test := range []struct {
		name string
		read func(context.Context) (any, error)
	}{
		{"GetAdminPolicy", func(ctx context.Context) (any, error) { return testReadAnswer(reads.GetAdminPolicy(ctx)) }},
		{"GetComputer", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.GetComputer(ctx, computer.ComputerID))
		}},
		{"GetComputerCustodyImport", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.GetComputerCustodyImport(ctx, "absent"))
		}},
		{"GetComputerPolicyRevocation", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.GetComputerPolicyRevocation(ctx, admin, policy.Revision, computer.ComputerID, admin.FabricID, admin.UserID))
		}},
		{"GetComputerSubmissionState", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.GetComputerSubmissionState(ctx, admin, computer.ComputerID))
		}},
		{"GetJob", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.GetJob(ctx, computer.CurrentJobID))
		}},
		{"GetJobLogs", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.GetJobLogs(ctx, computer.CurrentJobID, "", 10))
		}},
		{"GetJobResult", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.GetJobResult(ctx, computer.CurrentJobID))
		}},
		{"ListChildJobs", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.ListChildJobs(ctx, computer.CurrentJobID, "", 10))
		}},
		{"ListComputerBackups", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.ListComputerBackups(ctx, computer.ComputerID))
		}},
		{"ListComputerBackupsPage", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.ListComputerBackupsPage(ctx, computer.ComputerID, "", backup.BackupID, 10))
		}},
		{"ListComputerCustodyExports", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.ListComputerCustodyExports(ctx, computer.ComputerID))
		}},
		{"ListComputerCustodyExportsPage", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.ListComputerCustodyExportsPage(ctx, computer.ComputerID, "", 10))
		}},
		{"ListComputerGrants", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.ListComputerGrants(ctx, admin, computer.ComputerID))
		}},
		{"ListComputerIntents", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.ListComputerIntents(ctx, computer.ComputerID, "", 10))
		}},
		{"ListComputerPolicyAudit", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.ListComputerPolicyAudit(ctx, admin, computer.ComputerID, "", 10))
		}},
		{"ListComputerStorageGenerations", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.ListComputerStorageGenerations(ctx, computer.ComputerID))
		}},
		{"ListComputerStorageGenerationsPage", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.ListComputerStorageGenerationsPage(ctx, computer.ComputerID, "", 10))
		}},
		{"ListComputerStorageProvenance", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.ListComputerStorageProvenance(ctx, computer.ComputerID))
		}},
		{"ListComputerTakeoverAudit", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.ListComputerTakeoverAudit(ctx, admin, computer.ComputerID, "", 10, false))
		}},
		{"ListComputerTakeoverSessions", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.ListComputerTakeoverSessions(ctx, admin, computer.ComputerID))
		}},
		{"ListComputers", func(ctx context.Context) (any, error) { return testReadAnswer(reads.ListComputers(ctx, "", 10)) }},
		{"ListJobAttempts", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.ListJobAttempts(ctx, computer.CurrentJobID))
		}},
		{"ListNodes", func(ctx context.Context) (any, error) { return testReadAnswer(reads.ListNodes(ctx)) }},
		{"ListServiceJobs", func(ctx context.Context) (any, error) { return testReadAnswer(reads.ListServiceJobs(ctx, "", 10)) }},
		{"ProveComputerTokenScope", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.ProveComputerTokenScope(ctx, computer.ComputerID, claim.Lease.AttemptID, "fabric-computer-node", ""))
		}},
		{"ProveHostBootSession", func(ctx context.Context) (any, error) {
			return nil, reads.ProveHostBootSession(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID)
		}},
		{"ProveServiceBinding", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.ProveServiceBinding(ctx, "fabric-computer-node", computer.CurrentJobID, ServiceBindingProofRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID}))
		}},
		{"RawJobLogJSONL", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.RawJobLogJSONL(ctx, computer.CurrentJobID))
		}},
		{"ResolveAttemptCredential", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.ResolveAttemptCredential(ctx, claim.AttemptToken, "fabric-computer-node"))
		}},
		{"listNodesPage", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.listNodesPage(ctx, nodeListFilters{}, "", 10))
		}},
		{"listReadableJobs", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.listReadableJobs(ctx, jobListFilters{}, "", 10))
		}},
		{"listReadableJobsForCaller", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.listReadableJobsForCaller(ctx, jobListFilters{}, "", 10, nil))
		}},
		{"owedComputerRevocation", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.owedComputerRevocation(ctx, computer.owedRevocationID))
		}},
		{"GetNode", func(ctx context.Context) (any, error) { return testReadAnswer(reads.GetNode(ctx, node.NodeID)) }},
		{"LookupRunLedgerJob", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.LookupRunLedgerJob(ctx, "absent-ledger-key"))
		}},
		{"listChildJobsForCaller", func(ctx context.Context) (any, error) {
			return testReadAnswer(reads.listChildJobsForCaller(ctx, computer.CurrentJobID, "", 10, nil))
		}},
		{"withReadSnapshot", func(ctx context.Context) (any, error) {
			return nil, reads.withReadSnapshot(ctx, nil, func(context.Context, readModel) error { return nil })
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			expected, expectedErr := test.read(t.Context())
			// The clock is sampled after SQLite anchors the real transaction. Hold
			// the first snapshot past a lowered test-only limit. Restore the
			// normal limit before retrying; this stopped Store has no other readers.
			// The limit also bounds connection checkout, so keep it well above a
			// loaded runner's checkout time or the answer becomes an admission expiry.
			original := h.store.clock
			originalLimit := h.store.readSnapshotHardLimitForTest
			const probeLimit = 50 * time.Millisecond
			h.store.SetReadSnapshotHardLimitForTest(probeLimit)
			calls := 0
			h.store.clock = ClockFunc(func() time.Time {
				calls++
				if calls == 1 {
					time.Sleep(2 * probeLimit)
				}
				return original.Now()
			})
			defer func() {
				h.store.clock = original
				h.store.readSnapshotHardLimitForTest = originalLimit
			}()
			var attempts []error
			ctx := context.WithValue(t.Context(), testReadAttemptObserverKey{}, func(err error) {
				attempts = append(attempts, err)
				h.store.readSnapshotHardLimitForTest = originalLimit
			})
			got, err := test.read(ctx)
			if len(attempts) != 2 {
				t.Fatalf("attempts=%v, want expiry then a completed read", attempts)
			}
			assertSnapshotUnavailable(t, attempts[0], "read_snapshot_expired")
			if !reflect.DeepEqual(got, expected) || !reflect.DeepEqual(apiErrorFromDecision(err), apiErrorFromDecision(expectedErr)) {
				t.Fatalf("retry changed answer: got=%+v err=%v expected=%+v err=%v", got, err, expected, expectedErr)
			}
		})
	}
}
