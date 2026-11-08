package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
)

// committedUnreadRunCancel is L3's relay of L1's answer when the job cancel
// committed but its post-change read expired (#1511, #1558).
func committedUnreadRunCancel(retryable, applied bool) *http.Response {
	raw, _ := json.Marshal(contract.ErrorResponse{Error: contract.APIError{
		Code: contract.ErrorUnavailable, Message: "job change committed but its post-change read is unavailable", Retryable: retryable,
		Details: map[string]any{"reason": "read_snapshot_post_change_failed", "mutation_applied": applied, "read_reason": "read_snapshot_expired"},
	}})
	return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(bytes.NewReader(raw))}
}

// TestExecCancelResendsAfterCommittedReadUnavailable: the first run cancel
// reaches the real L3 and L1, so it commits, but its answer is replaced by
// the committed-unread relay. Only a retryable answer that says the change
// was applied earns one resend; the real stack answers that repeat with the
// cancelled run, so wefty reports "cancelled run" and removes the snapshot.
// Every other answer keeps today's "could not confirm" and the snapshot.
func TestExecCancelResendsAfterCommittedReadUnavailable(t *testing.T) {
	for _, test := range []struct {
		name               string
		retryable, applied bool
		wantCancels        int32
	}{
		{"committed unread resends once", true, true, 2},
		{"not applied keeps the error", true, false, 1},
		{"not retryable keeps the error", false, true, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			stack := newExecStack(t, execTestIdleLimit)
			t.Chdir(execTestRepo(t))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			// Cancel only once L3 has linked the run to its queued L1 job, so
			// both cancels reach L1's CancelJob, not L3's pre-dispatch path.
			stack.afterAccept = func() {
				deadline := time.Now().Add(testwait.Bound)
				for {
					page, err := stack.clients.listRuns(t.Context(), "", 10)
					var run contract.RunRecord
					if err == nil && len(page.Runs) == 1 {
						run, err = stack.clients.getRun(t.Context(), page.Runs[0].RunID)
					}
					if err == nil && run.L1JobID != "" {
						break
					}
					if time.Now().After(deadline) {
						t.Errorf("run was never linked to an L1 job: %+v %v", run, err)
						break
					}
					time.Sleep(20 * time.Millisecond)
				}
				cancel()
			}
			base := stack.clients.l3.client.Transport
			var cancels atomic.Int32
			stack.clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/cancel") {
					return base.RoundTrip(r)
				}
				response, err := base.RoundTrip(r)
				if cancels.Add(1) > 1 || err != nil {
					return response, err
				}
				if response.StatusCode != http.StatusOK {
					t.Errorf("first run cancel did not commit: status=%d", response.StatusCode)
				}
				_ = response.Body.Close()
				return committedUnreadRunCancel(test.retryable, test.applied), nil
			})
			holdExecNode(t, stack)
			var errOut bytes.Buffer
			err := execute(ctx, stack.clients, false, []string{"exec", "--", "true"}, io.Discard, &errOut)
			var params execParams
			_ = json.Unmarshal(stack.requests[0].Params, &params)
			t.Cleanup(func() { _ = cleanupExecStorage(params.Storage) })
			if got := cancels.Load(); got != test.wantCancels {
				t.Fatalf("run cancel requests=%d, want %d; err=%v stderr=%q", got, test.wantCancels, err, errOut.String())
			}
			if test.wantCancels == 2 {
				if err == nil || !strings.Contains(err.Error(), "cancelled run") || strings.Contains(err.Error(), "could not confirm") ||
					!strings.Contains(errOut.String(), "wefty: cancelled run ") {
					t.Fatalf("committed cancel reported as failure: %v %q", err, errOut.String())
				}
				waitExecPath(t, params.Storage, false)
				return
			}
			if err == nil || !strings.Contains(err.Error(), "could not confirm cancellation") ||
				!strings.Contains(errOut.String(), "snapshot retained at "+params.Storage) {
				t.Fatalf("unconfirmed cancel lost its error: %v %q", err, errOut.String())
			}
			if _, statErr := os.Stat(params.Storage); statErr != nil {
				t.Fatalf("unconfirmed cancel removed the snapshot: %v", statErr)
			}
		})
	}
}
