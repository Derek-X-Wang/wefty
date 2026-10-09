package l1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestJobListingReviewClampedCursorWalk(t *testing.T) {
	for _, route := range []string{"jobs", "services", "children"} {
		t.Run(route, func(t *testing.T) {
			h := newIntegrationHarness(t, nil)
			client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
			seedJobListingRows(t, h.store, 1001)
			parent, _, err := h.store.CreateJob(t.Context(), operatorServiceSpec("review-parent", nil))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := h.store.db.Exec("UPDATE jobs SET parent_job_id=? WHERE job_id<>?", parent.JobID, parent.JobID); err != nil {
				t.Fatal(err)
			}
			path := "/v1/jobs?limit=1000"
			want := 1002
			switch route {
			case "services":
				path += "&class=service"
				want = 501
			case "children":
				path = "/v1/jobs/" + parent.JobID + "/children?limit=1000"
				want = 1001
			}
			expected := map[string]bool{}
			for i := 1; i <= 1001; i++ {
				if route != "services" || i%2 == 0 {
					expected[fmt.Sprintf("listing-%06d", i)] = true
				}
			}
			if route != "children" {
				expected[parent.JobID] = true
			}
			seen := map[string]bool{}
			cursor := ""
			for pageNumber := 0; ; pageNumber++ {
				if pageNumber >= want {
					t.Fatal("cursor walk did not finish")
				}
				status, _, body := h.do(client, http.MethodGet, path+"&cursor="+url.QueryEscape(cursor), nil)
				if status != http.StatusOK {
					t.Fatalf("page %d: %d %s", pageNumber, status, body)
				}
				var page JobList
				if err := json.Unmarshal(body, &page); err != nil {
					t.Fatal(err)
				}
				if len(page.Jobs) < 1 || len(page.Jobs) > MaxJobListingPageLimit || pageNumber == 0 && page.NextCursor == "" {
					t.Fatalf("page %d: rows=%d cursor=%q", pageNumber, len(page.Jobs), page.NextCursor)
				}
				for _, job := range page.Jobs {
					if !expected[job.JobID] {
						t.Fatalf("unexpected job %s", job.JobID)
					}
					if seen[job.JobID] {
						t.Fatalf("visited %s twice", job.JobID)
					}
					seen[job.JobID] = true
				}
				cursor = page.NextCursor
				if cursor == "" {
					break
				}
			}
			if len(seen) != want {
				t.Fatalf("visited %d jobs, want %d", len(seen), want)
			}
		})
	}
}

func TestJobListingReviewStoreClamps(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	seedJobListingRows(t, h.store, 251)
	parent, _, err := h.store.CreateJob(t.Context(), operatorServiceSpec("review-parent", nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec("UPDATE jobs SET parent_job_id=? WHERE job_id<>?", parent.JobID, parent.JobID); err != nil {
		t.Fatal(err)
	}
	for _, children := range []bool{false, true} {
		t.Run(map[bool]string{false: "jobs", true: "children"}[children], func(t *testing.T) {
			var page JobList
			var err error
			if children {
				page, err = h.store.ListChildJobs(t.Context(), parent.JobID, "", 1000)
			} else {
				page, err = h.store.listReadableJobs(t.Context(), jobListFilters{}, "", 1000)
			}
			// The real door may adapt; verify the exact clamp independently of
			// wall-clock speed using the same production page method.
			if err != nil || len(page.Jobs) < 1 || len(page.Jobs) > MaxJobListingPageLimit || page.NextCursor == "" {
				t.Fatalf("rows=%d cursor=%q err=%v", len(page.Jobs), page.NextCursor, err)
			}
			err = diagnosticReadSnapshot(t, h.store, nil, func(reads readModel) (err error) {
				if children {
					page, err = reads.childrenPage(t.Context(), parent.JobID, "", 1000)
				} else {
					page, err = reads.jobsPage(t.Context(), jobListFilters{}, "", 1000)
				}
				return err
			})
			if err != nil || len(page.Jobs) != MaxJobListingPageLimit || page.NextCursor == "" {
				t.Fatalf("clamp: rows=%d cursor=%q err=%v", len(page.Jobs), page.NextCursor, err)
			}
		})
	}
}

func TestJobListingReviewSharedCollectionLimit(t *testing.T) {
	for _, input := range []string{"251", "1000"} {
		if limit, err := parseJobLimit(input); err != nil || limit < 251 {
			t.Fatalf("shared limit %s: %d %v", input, limit, err)
		}
	}
	if _, err := parseJobLimit("1001"); errorCode(err) != contract.ErrorInvalidRequest {
		t.Fatalf("shared collection accepted 1001: %v", err)
	}
}

func TestJobReadReviewPostChangeFailureReason(t *testing.T) {
	for _, failure := range []string{"credential", "internal", "snapshot"} {
		t.Run(failure, func(t *testing.T) {
			h, client, agent, node := credentialHarness(t)
			job := h.submit(client, "post-change-reason", []string{"linux"})
			claim := claimClass(t, h, agent, node, contract.JobClassOneShot)
			scope, err := h.store.ResolveAttemptCredential(t.Context(), claim.AttemptToken, node.NodeID)
			if err != nil {
				t.Fatal(err)
			}
			h.stopServer()
			ctx := t.Context()
			wantReason := string(contract.ErrorInternal)
			switch failure {
			case "credential":
				if _, err := h.store.db.Exec("UPDATE attempts SET lease_expires_ns=0 WHERE attempt_id=?", claim.Lease.AttemptID); err != nil {
					t.Fatal(err)
				}
				ctx = context.WithValue(ctx, attemptCredentialContextKey{}, scope)
				wantReason = string(contract.ErrorUnauthorized)
			case "internal":
				if err := h.store.readDB.Close(); err != nil {
					t.Fatal(err)
				}
			case "snapshot":
				// Force a genuine expiry inside the production snapshot door.
				at := h.clock.Now()
				h.store.clock = ClockFunc(func() time.Time { time.Sleep(readSnapshotHardLimit + 20*time.Millisecond); return at })
				wantReason = "read_snapshot_expired"
			}
			w := httptest.NewRecorder()
			h.server.writeChangedJob(w, jobReadRequest(ctx, http.MethodPost, "/v1/jobs", job.JobID), job, http.StatusCreated)
			var response contract.ErrorResponse
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if w.Code != http.StatusServiceUnavailable || response.Error.Code != contract.ErrorUnavailable || response.Error.Details["mutation_applied"] != true || response.Error.Details["read_reason"] != wantReason || response.Error.Retryable != (failure == "snapshot") {
				t.Fatalf("%s: %d %s", failure, w.Code, w.Body.String())
			}
		})
	}
}
