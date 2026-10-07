package l1

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"reflect"
	"sort"
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
	for _, query := range []string{"class=unknown", "class=service&class=one-shot", "class=", "kind=", "class=service&kind=", "state=restart-pending", "state=Queued", "submitter=other", "submitter=", "limit=0", "limit=1001", "limit=no", "limit=1&limit=2", "cursor=bad"} {
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
			fresh := listingPage(t, h, client, "/v1/jobs?limit=100")
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
	if len(page.Jobs) != 2 {
		t.Fatalf("legacy cursor returns %d jobs, want 2", len(page.Jobs))
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
