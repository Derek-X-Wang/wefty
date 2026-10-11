package l3

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// linkKnowledgeFixture is the sole storage-writing seam for new link tests.
// Optional identities also permit explicit legacy/corrupt shapes. Outcome and
// token history are established through the ordinary Run transitions.
type linkKnowledgeFixture struct {
	runJob, outboxJob *string
	diagnostic        *contract.APIError
	attemptedAt       *time.Time
	settled           bool
	acknowledged      bool
	cancellation      string
}

func linkID(id string) *string { return &id }

func buildLinkKnowledge(t *testing.T, s *Store, runID string, fixture linkKnowledgeFixture) {
	t.Helper()
	var attempted, dispatched any
	if fixture.attemptedAt != nil {
		attempted = fixture.attemptedAt.UnixNano()
	}
	if fixture.acknowledged {
		dispatched = s.clock.Now().UnixNano()
	}
	var diagnostic any
	if fixture.diagnostic != nil {
		payload, err := json.Marshal(fixture.diagnostic)
		if err != nil {
			t.Fatal(err)
		}
		diagnostic = string(payload)
	}
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE runs SET l1_job_id=?, job_link_settled=?, dispatch_attempt_ns=?, job_link_failures=0, job_link_retry_ns=0 WHERE run_id=?`, fixture.runJob, fixture.settled, attempted, runID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE dispatch_outbox SET job_id=?, dispatched_ns=?, last_error=? WHERE run_id=?`, fixture.outboxJob, dispatched, diagnostic, runID); err != nil {
		t.Fatal(err)
	}
	if fixture.cancellation != "" {
		var completed any
		if fixture.cancellation != "pending" {
			completed = s.clock.Now().UnixNano()
		}
		if _, err := tx.Exec(`INSERT INTO run_cancellations(run_id, requested_ns, completed_ns, refused) VALUES(?,?,?,?) ON CONFLICT(run_id) DO UPDATE SET completed_ns=excluded.completed_ns, refused=excluded.refused, retry_ns=0`, runID, s.clock.Now().UnixNano(), completed, fixture.cancellation == "refused"); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
