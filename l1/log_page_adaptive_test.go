package l1

import (
	"context"
	"testing"
	"time"
)

func TestLogPage1000AdaptiveCutoff(t *testing.T) {
	h, _, job, claim := jobProjectionFixture(t, "running")
	h.stopServer()
	if _, err := h.store.db.ExecContext(t.Context(), `WITH RECURSIVE fixture(n) AS
 (SELECT 1 UNION ALL SELECT n+1 FROM fixture WHERE n<1000)
 INSERT INTO log_events(job_id,attempt_id,stream,sequence,sequence_end,timestamp_ns,bytes,event_json)
 SELECT ?,?,'stdout',n,n,?,X'78',json_object('attempt_id',?,'stream','stdout','sequence',n,'timestamp','2026-08-09T10:00:00Z') FROM fixture`, job.JobID, claim.Lease.AttemptID, h.clock.Now().UnixNano(), claim.Lease.AttemptID); err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(t.Context(), readPageCutoffContextKey{}, time.Nanosecond)
	cursor := ""
	for i := 1; i <= 1000; i++ {
		page, err := h.store.GetJobLogs(ctx, job.JobID, cursor, 1000)
		if err != nil || len(page.Events) != 1 || page.Events[0].Sequence != uint64(i) || page.NextCursor == cursor {
			t.Fatalf("adaptive log page %d: rows=%d cursor=%q err=%v", i, len(page.Events), page.NextCursor, err)
		}
		cursor = page.NextCursor
	}
	page, err := h.store.GetJobLogs(ctx, job.JobID, cursor, 1000)
	if err != nil || len(page.Events) != 0 || page.NextCursor != cursor {
		t.Fatalf("terminal poll: %+v %v", page, err)
	}
}
