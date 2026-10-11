package l1

import (
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestCancelPendingServiceEvidenceMustNotEstablishHealth(t *testing.T) {
	for _, cause := range []string{"renewal", "logs", "publication"} {
		t.Run(cause, func(t *testing.T) {
			var port *int
			if cause == "publication" {
				value := 8080
				port = &value
			}
			f := newPublicationFixture(t, port)
			f.h.stopServer()
			store, clock := f.h.store, f.h.clock
			// The public cancel API refuses services. Seed the reserved outcome
			// to exercise the health guard independently of that admission rule.
			settleBy := clock.Now().Add(CancelSettlementTimeout)
			if _, err := store.db.ExecContext(t.Context(),
				`UPDATE jobs SET outcome='canceled', cancel_settle_by_ns=? WHERE job_id=?`,
				settleBy.UnixNano(), f.job.JobID); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			canceled, deadline, err := cancellationDeadline(t.Context(), store.db, f.job.JobID)
			if err != nil || !canceled || !deadline.Equal(settleBy) || !clock.Now().Before(deadline) {
				t.Fatalf("pending cancellation: canceled=%t deadline=%s err=%v", canceled, deadline, err)
			}
			switch cause {
			case "renewal":
				lease, err := store.RenewLease(t.Context(), "node-1", f.job.JobID, f.claim.Lease.AttemptID, f.claim.Lease.FencingToken)
				if err != nil || !lease.LeaseExpires.After(clock.Now()) || lease.LeaseExpires.After(settleBy) {
					t.Fatalf("cancel-pending renewal: lease=%+v err=%v", lease, err)
				}
			case "logs":
				if _, err := store.AppendLogs(t.Context(), "node-1", f.job.JobID, f.claim.Lease.AttemptID, AppendLogsRequest{
					FencingToken: f.claim.Lease.FencingToken,
					Events: []contract.LogEvent{{AttemptID: f.claim.Lease.AttemptID, Stream: contract.LogStdout,
						Sequence: 0, Timestamp: clock.Now(), Bytes: []byte("cancel-pending output\n")}},
				}); err != nil {
					t.Fatal(err)
				}
			case "publication":
				published, err := store.SetAttemptPublication(t.Context(), "node-1", f.job.JobID, f.claim.Lease.AttemptID, f.request(true))
				if err != nil || published.PublishedAttemptID != f.claim.Lease.AttemptID {
					t.Fatalf("cancel-pending publication: job=%+v err=%v", published, err)
				}
			}
			observed := getRestartService(t, f.h, f.job.JobID)
			if observed.HealthySinceAt != nil || observed.Outcome != "canceled" || observed.CurrentAttemptID != f.claim.Lease.AttemptID {
				t.Fatalf("%s changed cancel-pending health or binding: health=%v outcome=%q attempt=%q",
					cause, observed.HealthySinceAt, observed.Outcome, observed.CurrentAttemptID)
			}
		})
	}
}

func TestOCIPortlessLogsBeforeStartedMustNotEstablishHealth(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {}})
	registerOCIFixtureNode(t, h)
	job := createOCIFixtureJob(t, h, "oci-portless-pre-start-logs", contract.JobClassService)
	claim := claimOCIFixture(t, h, contract.JobClassService)
	h.stopServer()
	if _, err := h.store.AppendLogs(t.Context(), "agent", job.JobID, claim.Lease.AttemptID, AppendLogsRequest{
		FencingToken: claim.Lease.FencingToken,
		Events: []contract.LogEvent{{AttemptID: claim.Lease.AttemptID, Stream: contract.LogStdout,
			Sequence: 0, Timestamp: h.clock.Now(), Bytes: []byte("pre-start output\n")}},
	}); err != nil {
		t.Fatal(err)
	}
	assertOCIFixtureState(t, h, job.JobID, contract.JobClaimed, contract.AttemptClaimed)
	observed := getRestartService(t, h, job.JobID)
	if observed.HealthySinceAt != nil {
		t.Fatalf("OCI logs before Started established health: %v", observed.HealthySinceAt)
	}
}
