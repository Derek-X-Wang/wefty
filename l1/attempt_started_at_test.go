package l1

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func TestJobAttemptStartedAtOnlyAfterRecordedStart(t *testing.T) {
	f := newOneshotFixture(t, true)
	c := f.claim(t)
	readStart := func(want *time.Time) {
		t.Helper()
		status, _, body := f.h.do(f.client, http.MethodGet, "/v1/jobs/"+f.job.JobID, nil)
		if status != http.StatusOK {
			t.Fatalf("get Job=%d %s", status, body)
		}
		var job struct {
			Attempts []map[string]json.RawMessage `json:"attempts"`
		}
		if err := json.Unmarshal(body, &job); err != nil {
			t.Fatal(err)
		}
		if len(job.Attempts) != 1 {
			t.Fatalf("attempts=%s", body)
		}
		raw, present := job.Attempts[0]["started_at"]
		if want == nil {
			if present {
				t.Fatalf("unrecorded start has started_at=%s", raw)
			}
			return
		}
		var got time.Time
		if !present || json.Unmarshal(raw, &got) != nil || !got.Equal(*want) {
			t.Fatalf("started_at=%s, want %s", raw, want.Format(time.RFC3339Nano))
		}
	}
	readStart(nil)
	// Renewal's legacy running promotion is not durable start evidence.
	if _, err := f.h.store.RenewLease(t.Context(), "agent", f.job.JobID, c.Lease.AttemptID, c.Lease.FencingToken); err != nil {
		t.Fatal(err)
	}
	readStart(nil)
	f.h.clock.Advance(time.Second)
	started := f.h.clock.Now()
	if _, err := f.h.store.StartAttempt(t.Context(), "agent", f.job.JobID, c.Lease.AttemptID, StartedRequest{FencingToken: c.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	readStart(&started)
	f.h.clock.Advance(time.Second)
	f.disable(t)
	completion := f.interrupt(t, c)
	if _, err := f.h.store.CompleteAttempt(t.Context(), "agent", f.job.JobID, c.Lease.AttemptID, completion); err != nil {
		t.Fatal(err)
	}
	readStart(&started)
}
