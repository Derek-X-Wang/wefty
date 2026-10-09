package l1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func listingPage(t *testing.T, h *integrationHarness, client *http.Client, path string) JobList {
	t.Helper()
	status, _, body := h.do(client, http.MethodGet, path, nil)
	if status != http.StatusOK {
		t.Fatalf("list %s = %d %s", path, status, body)
	}
	if bytes.Contains(body, []byte("LISTING_SECRET")) {
		t.Fatalf("listing leaked sensitive env: %s", body)
	}
	var page JobList
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	return page
}

func listingWalk(t *testing.T, h *integrationHarness, client *http.Client, path string) JobList {
	t.Helper()
	var combined JobList
	cursor := ""
	separator := "?"
	if strings.Contains(path, "?") {
		separator = "&"
	}
	seen := map[string]bool{}
	for {
		page := listingPage(t, h, client, path+separator+"cursor="+url.QueryEscape(cursor))
		if len(page.Jobs) == 0 && page.NextCursor != "" {
			t.Fatal("empty continuation page")
		}
		for _, job := range page.Jobs {
			if seen[job.JobID] {
				t.Fatalf("duplicate row %s", job.JobID)
			}
			seen[job.JobID] = true
		}
		combined.Jobs = append(combined.Jobs, page.Jobs...)
		cursor = page.NextCursor
		if cursor == "" {
			return combined
		}
	}
}

func jobListingIDs(page JobList) []string {
	ids := make([]string, 0, len(page.Jobs))
	for _, job := range page.Jobs {
		ids = append(ids, job.JobID)
	}
	sort.Strings(ids)
	return ids
}

func TestJobsListingFiltersAreExact(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	mine := h.client(fabric.Identity{NodeID: "mine", Tags: []string{DefaultClientPrincipalTag}})
	other := h.client(fabric.Identity{NodeID: "other", Tags: []string{DefaultClientPrincipalTag}})
	var jobs []Job
	for i, def := range []struct {
		class, kind string
		client      *http.Client
	}{
		{"one-shot", "process", mine}, {"service", "process", mine}, {"one-shot", "oci", mine}, {"service", "oci", other},
	} {
		spec := capabilityJobSpec(fmt.Sprintf("listing-%d", i), def.kind, def.class, "", nil)
		spec.Labels = nil
		spec.Execution.SensitiveEnv = map[string]string{"LISTING_SECRET": "hidden"}
		status, _, body := h.do(def.client, http.MethodPost, "/v1/jobs", spec)
		if status != http.StatusCreated {
			t.Fatalf("submit = %d %s", status, body)
		}
		jobs = append(jobs, decodeJob(t, body))
	}
	if _, err := h.store.db.Exec(`UPDATE jobs SET state='failed' WHERE job_id=?`, jobs[2].JobID); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		query   string
		indices []int
	}{
		{"", []int{0, 1, 2, 3}}, {"class=one-shot", []int{0, 2}}, {"class=service", []int{1, 3}},
		{"kind=process", []int{0, 1}}, {"kind=oci", []int{2, 3}}, {"kind=process-extra", nil},
		{"state=queued", []int{0, 1, 3}}, {"state=failed", []int{2}}, {"state=running", nil},
		{"submitter=me", []int{0, 1, 2}}, {"class=one-shot&kind=oci&state=failed&submitter=me", []int{2}},
		{"class=service&kind=oci&submitter=me", nil},
	} {
		t.Run(tc.query, func(t *testing.T) {
			// Walk each filtered set with a one-row page, to pin filtering before LIMIT.
			var got []string
			cursor := ""
			for {
				path := "/v1/jobs?limit=1&" + tc.query
				if cursor != "" {
					path += "&cursor=" + url.QueryEscape(cursor)
				}
				page := listingPage(t, h, mine, path)
				for _, job := range page.Jobs {
					got = append(got, job.JobID)
				}
				cursor = page.NextCursor
				if cursor == "" {
					break
				}
			}
			want := []string{}
			for _, i := range tc.indices {
				want = append(want, jobs[i].JobID)
			}
			sort.Strings(got)
			sort.Strings(want)
			if len(got) == 0 {
				got = []string{}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("IDs = %v, want %v", got, want)
			}
		})
	}
	listingPage(t, h, mine, "/v1/jobs/")
}

func TestJobsListingRejectsInvalidSelectors(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	client := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
	for _, query := range []string{"class=unknown", "class=service&class=one-shot", "class=", "kind=", "class=service&kind=", "state=restart-pending", "state=Queued", "submitter=other", "submitter=", "limit=0", "limit=no", "limit=1&limit=2", "cursor=bad"} {
		t.Run(query, func(t *testing.T) {
			status, _, body := h.do(client, http.MethodGet, "/v1/jobs?"+query, nil)
			assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
		})
	}
}

func TestJobsListingCursorWalkExcludesConcurrentInserts(t *testing.T) {
	for _, class := range []string{"", contract.JobClassService} {
		t.Run(class, func(t *testing.T) {
			h := newIntegrationHarness(t, nil)
			client := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
			var want []string
			create := func(key string) Job {
				t.Helper()
				spec := validJobSpec(key, nil)
				if class != "" {
					spec = operatorServiceSpec(key, nil)
				}
				status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
				if status != http.StatusCreated {
					t.Fatalf("create=%d %s", status, body)
				}
				return decodeJob(t, body)
			}
			for i := 0; i < 7; i++ {
				want = append(want, create(fmt.Sprintf("walk-%d", i)).JobID)
			}
			// All initial jobs share a timestamp, exercising the ID tie-breaker.
			base := "/v1/jobs?limit=2"
			if class != "" {
				base += "&class=" + class
			}
			page := listingPage(t, h, client, base)
			got := jobListingIDs(page)
			if page.NextCursor == "" {
				t.Fatal("missing continuation")
			}
			// Writers finish between reads while the cursor walk remains active.
			// Insert on both sides of its creation key, including equal timestamps.
			for i, shift := range []time.Duration{0, -time.Hour, 2 * time.Hour} {
				h.clock.Advance(shift)
				done := make(chan error, 1)
				spec := validJobSpec(fmt.Sprintf("during-walk-%d", i), nil)
				if class != "" {
					spec = operatorServiceSpec(fmt.Sprintf("during-walk-%d", i), nil)
				}
				go func() { _, _, err := h.store.CreateJob(t.Context(), spec); done <- err }()
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
			cursor := page.NextCursor
			for cursor != "" {
				page = listingPage(t, h, client, base+"&cursor="+url.QueryEscape(cursor))
				for _, job := range page.Jobs {
					got = append(got, job.JobID)
				}
				cursor = page.NextCursor
			}
			sort.Strings(got)
			sort.Strings(want)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("walk IDs=%v, want exactly original IDs=%v", got, want)
			}
			fresh := listingWalk(t, h, client, "/v1/jobs?limit=100")
			if len(fresh.Jobs) != 10 {
				t.Fatalf("fresh walk has %d jobs, want 10", len(fresh.Jobs))
			}
		})
	}
}

func TestJobsListingAttemptScopeAndMine(t *testing.T) {
	h, client, agent, node := credentialHarness(t)
	parent := h.submit(client, "listing-parent", []string{"linux"})
	claim := claimClass(t, h, agent, node, contract.JobClassOneShot)
	status, body := h.credentialRequest(agent, http.MethodPost, "/v1/jobs", claim.AttemptToken, validJobSpec("listing-child", nil))
	if status != http.StatusCreated {
		t.Fatalf("child=%d %s", status, body)
	}
	child := decodeJob(t, body)
	h.submit(client, "listing-unrelated", nil) // Same originating submitter, outside scope.
	grandchildSpec := validJobSpec("listing-grandchild", nil)
	// A stored grandchild and a service child exercise scope independently of class.
	grandchild, _, err := h.store.CreateJob(t.Context(), grandchildSpec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.store.db.Exec(`UPDATE jobs SET parent_job_id=?, originating_submitter='submitter' WHERE job_id=?`, child.JobID, grandchild.JobID); err != nil {
		t.Fatal(err)
	}
	status, body = h.credentialRequest(agent, http.MethodPost, "/v1/jobs", claim.AttemptToken, operatorServiceSpec("listing-service-child", nil))
	if status != http.StatusCreated {
		t.Fatalf("service child=%d %s", status, body)
	}
	service := decodeJob(t, body)
	for _, tc := range []struct {
		query string
		want  []string
	}{
		{"", []string{parent.JobID, child.JobID, service.JobID}},
		{"submitter=me", []string{parent.JobID, child.JobID, service.JobID}},
		{"class=one-shot", []string{parent.JobID, child.JobID}},
		{"state=claimed", []string{parent.JobID}},
		{"class=service&submitter=me", []string{service.JobID}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			got := []string{}
			cursor := ""
			for {
				path := "/v1/jobs?limit=1&" + tc.query
				if cursor != "" {
					path += "&cursor=" + url.QueryEscape(cursor)
				}
				status, body := h.credentialRequest(agent, http.MethodGet, path, claim.AttemptToken, nil)
				if status != http.StatusOK {
					t.Fatalf("scope listing=%d %s", status, body)
				}
				var page JobList
				if err := json.Unmarshal(body, &page); err != nil {
					t.Fatal(err)
				}
				got = append(got, jobListingIDs(page)...)
				cursor = page.NextCursor
				if cursor == "" {
					break
				}
			}
			sort.Strings(got)
			sort.Strings(tc.want)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("scope IDs=%v, want %v", got, tc.want)
			}
		})
	}
	// A cursor is a continuation, not an authorization grant.
	broad := listingPage(t, h, client, "/v1/jobs?limit=1")
	status, body = h.credentialRequest(agent, http.MethodGet, "/v1/jobs?cursor="+url.QueryEscape(broad.NextCursor), claim.AttemptToken, nil)
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
	status, body = h.credentialRequest(agent, http.MethodGet, "/v1/jobs", "invalid-token", nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("invalid credential=%d %s", status, body)
	}
	h.clock.Advance(DefaultLeaseDuration + time.Second)
	status, body = h.credentialRequest(agent, http.MethodGet, "/v1/jobs", claim.AttemptToken, nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("expired credential=%d %s", status, body)
	}
}

func TestJobsListingCursorBindsFiltersAndLegacyServiceCursor(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	client := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
	for i := 0; i < 3; i++ {
		submitOperatorService(t, h, client, fmt.Sprintf("legacy-%d", i))
	}
	first := listingPage(t, h, client, "/v1/jobs?class=service&limit=1")
	status, _, body := h.do(client, http.MethodGet, "/v1/jobs?class=one-shot&cursor="+url.QueryEscape(first.NextCursor), nil)
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
	status, _, body = h.do(client, http.MethodGet, "/v1/jobs?class=service&kind=oci&cursor="+url.QueryEscape(first.NextCursor), nil)
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
	legacy := encodeServiceJobCursor(serviceJobCursor{CreatedNS: first.Jobs[0].CreatedAt.UnixNano(), JobID: first.Jobs[0].JobID})
	page := listingPage(t, h, client, "/v1/jobs?class=service&cursor="+url.QueryEscape(legacy))
	got := jobListingIDs(page)
	for page.NextCursor != "" {
		page = listingPage(t, h, client, "/v1/jobs?class=service&cursor="+url.QueryEscape(page.NextCursor))
		got = append(got, jobListingIDs(page)...)
	}
	if len(got) != 2 || got[0] == got[1] {
		t.Fatalf("legacy walk IDs=%v, want 2 distinct jobs", got)
	}
}

func TestJobsListingWatermarkSurvivesRemovalAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "listing.sqlite")
	store, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { store.Close() }()
	create := func(key string) Job {
		t.Helper()
		job, _, err := store.CreateJob(t.Context(), operatorServiceSpec(key, nil))
		if err != nil {
			t.Fatal(err)
		}
		return job
	}
	firstJob := create("persist-first")
	secondJob := create("persist-second")
	lastJob := create("persist-last")
	// Simulate upgrading a database with ordinary jobs but no listing metadata.
	if _, err := store.db.Exec(`DROP TRIGGER IF EXISTS jobs_listing_insert; DROP TABLE IF EXISTS job_listing_order;`); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	page, err := store.ListServiceJobs(t.Context(), "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Jobs) != 1 || page.Jobs[0].JobID != firstJob.JobID || page.NextCursor == "" {
		t.Fatalf("first page=%#v", page)
	}
	if _, err := store.db.Exec(`DELETE FROM jobs WHERE job_id=?`, lastJob.JobID); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	create("persist-insert-after-removal")
	continuation, err := store.ListServiceJobs(t.Context(), page.NextCursor, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(continuation.Jobs) != 1 || continuation.Jobs[0].JobID != secondJob.JobID || continuation.NextCursor != "" {
		t.Fatalf("continuation=%#v, want only pre-existing second job", continuation)
	}
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM job_listing_order WHERE job_id=?`, lastJob.JobID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("removed job listing metadata count=%d err=%v", count, err)
	}
}

// Seed in one transaction so the regression fixtures can be large without
// thousands of HTTP submissions or timing-dependent setup.
func seedJobListingRows(t *testing.T, store *Store, count int) {
	t.Helper()
	oneShot, err := json.Marshal(validJobSpec("listing-fixture", nil))
	if err != nil {
		t.Fatal(err)
	}
	service, err := json.Marshal(operatorServiceSpec("listing-fixture", nil))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(t.Context(), `WITH RECURSIVE fixture(n) AS (
		SELECT 1 UNION ALL SELECT n+1 FROM fixture WHERE n<?
	) INSERT INTO jobs(job_id, dispatch_key, request_hash, spec_json, state, created_ns, updated_ns)
	SELECT printf('listing-%06d', n), printf('listing-%06d', n), 'fixture',
		CASE WHEN n%2=0 THEN ? ELSE ? END, 'queued', n, n FROM fixture`, count, service, oneShot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO service_jobs(job_id, desired_state)
		SELECT job_id, 'running' FROM jobs WHERE json_extract(spec_json, '$.class')='service'`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestJobsListingDoesNotBlockConcurrentWrites(t *testing.T) {
	for _, handle := range []string{"main", "settlement"} {
		t.Run(handle, func(t *testing.T) {
			store, err := OpenStore(filepath.Join(t.TempDir(), "listing.sqlite"), StoreOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			seedJobListingRows(t, store, 10000)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			now := store.clock.Now()
			// The snapshot is anchored before its clock is sampled; pause there
			// to prove a writer can commit while that view is held.
			store.clock = ClockFunc(func() time.Time {
				select {
				case <-entered:
				default:
					close(entered)
				}
				<-release
				return now
			})
			var page JobList
			var listErr error
			listed := make(chan struct{})
			go func() {
				page, listErr = store.listReadableJobs(t.Context(), jobListFilters{}, "", MaxJobListingPageLimit)
				close(listed)
			}()
			defer func() { unblock(); <-listed }()
			select {
			case <-entered:
			case <-listed:
				t.Fatalf("listing ended before the read barrier: %v", listErr)
			case <-time.After(10 * time.Second):
				t.Fatal("listing did not reach the read barrier")
			}
			db := store.db
			if handle == "settlement" {
				db = store.settlementDB
			}
			written := make(chan error, 1)
			started := time.Now()
			go func() {
				_, err := db.ExecContext(t.Context(), `UPDATE jobs SET state='failed' WHERE job_id='listing-000001'`)
				written <- err
			}()
			// The reader remains paused throughout this generous deadline. This
			// tests lock ownership, not how fast the machine reads a maximum page of jobs.
			select {
			case err := <-written:
				if err != nil {
					t.Fatalf("concurrent %s write failed while listing held its snapshot: %v", handle, err)
				}
				t.Logf("concurrent %s write completed in %s with the listing paused", handle, time.Since(started))
			case <-time.After(2 * time.Second):
				unblock()
				<-written
				t.Fatalf("concurrent %s write did not complete within 2s while the listing was paused", handle)
			}
			unblock()
			<-listed
			if listErr != nil || len(page.Jobs) < 1 || len(page.Jobs) > MaxJobListingPageLimit || page.NextCursor == "" {
				t.Fatalf("large page: jobs=%d err=%v", len(page.Jobs), listErr)
			}
			if page.Jobs[0].State != contract.JobQueued {
				t.Fatalf("listing lost its read snapshot: first state=%s", page.Jobs[0].State)
			}
			var state contract.JobState
			if err := db.QueryRow(`SELECT state FROM jobs WHERE job_id='listing-000001'`).Scan(&state); err != nil || state != contract.JobFailed {
				t.Fatalf("concurrent write was not persisted: state=%s err=%v", state, err)
			}
		})
	}
}

func TestJobsListingPlanUsesCreationIndex(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "listing.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	seedJobListingRows(t, store, 100000)
	for _, class := range []string{"", contract.JobClassOneShot, contract.JobClassService} {
		for _, continuation := range []bool{false, true} {
			t.Run(fmt.Sprintf("class=%s/continuation=%t", class, continuation), func(t *testing.T) {
				cursor := jobCollectionCursor{HighWater: 100000}
				if continuation {
					cursor.CreatedNS, cursor.JobID = 90000, "listing-090000"
				}
				query, args := jobListingQuery(jobListFilters{Class: class}, cursor, 100)
				rows, err := store.db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+query, args...)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				var plan []string
				for rows.Next() {
					var id, parent, unused int
					var detail string
					if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
						t.Fatal(err)
					}
					plan = append(plan, detail)
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				details := strings.Join(plan, "\n")
				t.Log(details)
				if len(plan) == 0 || !strings.Contains(plan[0], "jobs_listing_creation_order") || strings.Contains(details, "TEMP B-TREE") {
					t.Errorf("page must stream jobs in creation-index order without sorting the collection:\n%s", details)
				}
				if continuation && (!strings.Contains(plan[0], "SEARCH jobs") || !strings.Contains(plan[0], "created_ns>")) {
					t.Errorf("continuation must seek the creation index:\n%s", details)
				}
				if class != "" && strings.Contains(query, "json_extract(jobs.spec_json, '$.class')") {
					t.Error("class selection must use service membership, not deserialize every spec")
				}
			})
		}
	}
}

func TestJobsListingExcludesRetiredComputerProjections(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	client := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{
		Name: "listing-computer", Spec: computerCapabilityJobSpec("listing-computer-v1"), Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	computer, err = h.store.SetComputerDesiredState(t.Context(), computer.ComputerID,
		computerDesiredRequest(computer, contract.ServiceDesiredStopped, "listing-stop"))
	if err != nil {
		t.Fatal(err)
	}
	retiredID := computer.CurrentJobID
	computer, err = h.store.InstallComputerProjection(t.Context(), computer.ComputerID, ComputerProjectionRequest{
		ComputerMutationPrecondition: computerPrecondition(computer, "listing-project"),
		Spec:                         computerCapabilityJobSpec("listing-computer-v2"),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range []contract.JobSpec{validJobSpec("listing-one-shot", nil), operatorServiceSpec("listing-service", nil)} {
		if _, _, err := h.store.CreateJobAs(t.Context(), spec, JobOrigin{OriginatingSubmitter: "operator"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.store.db.Exec(`UPDATE jobs SET originating_submitter='operator' WHERE job_id IN (?, ?)`, retiredID, computer.CurrentJobID); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"", "class=service", "class=one-shot", "kind=oci", "state=stopped", "submitter=me", "kind=oci&state=stopped&submitter=me"} {
		t.Run(query, func(t *testing.T) {
			page := listingWalk(t, h, client, "/v1/jobs?"+query)
			for _, job := range page.Jobs {
				if job.JobID == retiredID {
					t.Errorf("retired Computer projection appeared in collection: %s", job.JobID)
				}
				if job.JobID == computer.CurrentJobID && job.ComputerID != computer.ComputerID {
					t.Errorf("current projection lost Computer identity: %#v", job)
				}
			}
		})
	}
	all := jobListingIDs(listingWalk(t, h, client, "/v1/jobs"))
	partition := append(jobListingIDs(listingWalk(t, h, client, "/v1/jobs?class=one-shot")),
		jobListingIDs(listingWalk(t, h, client, "/v1/jobs?class=service"))...)
	sort.Strings(partition)
	if len(all) != 3 || !reflect.DeepEqual(all, partition) {
		t.Fatalf("unfiltered=%v, want one-shot plus service=%v (3 jobs)", all, partition)
	}
}

func TestJobsListingRejectsUnresolvedMe(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	parent, _, err := h.store.CreateJob(t.Context(), validJobSpec("unowned-listing", nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range []AttemptCredentialScope{{}, {JobID: parent.JobID}} {
		t.Run(fmt.Sprintf("attempt=%t", scope.JobID != ""), func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/v1/jobs?submitter=me", nil)
			ctx := context.WithValue(r.Context(), identityContextKey{}, fabric.Identity{})
			if scope.JobID != "" {
				// An attempt's inherited identity must not fall back to the
				// holding node's identity when the inherited identity is empty.
				ctx = context.WithValue(ctx, identityContextKey{}, fabric.Identity{NodeID: "holding-node"})
				ctx = context.WithValue(ctx, attemptCredentialContextKey{}, scope)
			}
			w := httptest.NewRecorder()
			h.server.listJobs(w, r.WithContext(ctx))
			assertAPIError(t, w.Code, w.Body.Bytes(), http.StatusBadRequest, contract.ErrorInvalidRequest)
		})
	}
}
