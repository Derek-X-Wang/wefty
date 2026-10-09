package l1

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// Full response baselines recorded from main 8d24686. Only generated identities
// are normalized; action refusals, clocks, node facts and paging remain intact.
func assertWiderProjectionFixture(t *testing.T, name string, raw []byte, replacements ...string) {
	t.Helper()
	raw = []byte(strings.NewReplacer(replacements...).Replace(string(raw)))
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	got, err := json.MarshalIndent(decoded, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	path := filepath.Join("testdata", "read-model-"+name+".json")
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("%s changed baseline wire\ngot:\n%s\nwant:\n%s", name, got, want)
	}
}
func TestReadModelWiderPublicFixtures(t *testing.T) {
	t.Run("one-shot", func(t *testing.T) {
		h, client, agent, node := credentialHarness(t)
		job := h.submit(client, "baseline-one-shot", []string{"linux"})
		claim := claimClass(t, h, agent, node, contract.JobClassOneShot)
		status, _, raw := h.do(client, http.MethodGet, "/v1/jobs/"+job.JobID, nil)
		if status != http.StatusOK {
			t.Fatalf("%d %s", status, raw)
		}
		assertWiderProjectionFixture(t, "one-shot", raw, job.JobID, "JOB", claim.Lease.AttemptID, "ATTEMPT")
	})
	t.Run("same-node-page", func(t *testing.T) {
		h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{"node-1": {Tags: []string{"linux"}, MaxOneshotSlots: 2, MaxServiceSlots: 2}})
		client := h.client(fabric.Identity{NodeID: "submitter", Tags: []string{DefaultClientPrincipalTag}})
		agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
		node := h.register(agent, "node-1")
		submit := func(key string) Job {
			spec := capabilityJobSpec(key, "process", contract.JobClassService, "", nil)
			spec.RoutingTags = []string{"linux"}
			status, _, raw := h.do(client, http.MethodPost, "/v1/jobs", spec)
			if status != http.StatusCreated {
				t.Fatalf("%d %s", status, raw)
			}
			return decodeJob(t, raw)
		}
		first := submit("baseline-first")
		h.clock.Advance(time.Millisecond)
		second := submit("baseline-second")
		if _, err := h.store.db.ExecContext(t.Context(), "UPDATE service_jobs SET bound_node_id=?", node.NodeID); err != nil {
			t.Fatal(err)
		}
		// Compare both rows' public wire shape across any adaptive page sizes.
		// Snapshot-local memo sharing is checked separately below.
		page := listingWalk(t, h, client, "/v1/jobs?limit=2")
		raw, err := json.Marshal(page)
		if err != nil {
			t.Fatal(err)
		}
		assertWiderProjectionFixture(t, "same-node-page", raw, first.JobID, "JOB1", second.JobID, "JOB2")
		if err := h.store.withReadSnapshot(t.Context(), nil, func(ctx context.Context, reads readModel) error {
			impl := reads.(*databaseReads)
			counter := &projectionNodeCounter{queryer: impl.q}
			impl.q = counter
			for _, id := range []string{first.JobID, second.JobID} {
				job, err := reads.job(ctx, id)
				if err != nil {
					return err
				}
				if _, err := projectJobWithReads(ctx, reads, job, projectJobStatusPart); err != nil {
					return err
				}
			}
			if counter.nodes != 1 {
				t.Fatalf("same-node page used %d node queries; want one narrow cached scan", counter.nodes)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

	})
	t.Run("children", func(t *testing.T) {
		h, client, agent, node := credentialHarness(t)
		parent := h.submit(client, "baseline-parent", []string{"linux"})
		claim := claimClass(t, h, agent, node, contract.JobClassOneShot)
		status, body := h.credentialRequest(agent, http.MethodPost, "/v1/jobs", claim.AttemptToken, validJobSpec("baseline-child", []string{"linux"}))
		if status != http.StatusCreated {
			t.Fatalf("%d %s", status, body)
		}
		child := decodeJob(t, body)
		status, raw := h.credentialRequest(agent, http.MethodGet, "/v1/jobs/"+parent.JobID+"/children", claim.AttemptToken, nil)
		if status != http.StatusOK {
			t.Fatalf("%d %s", status, raw)
		}
		assertWiderProjectionFixture(t, "children", raw, parent.JobID, "PARENT", child.JobID, "CHILD", claim.Lease.AttemptID, "PARENT_ATTEMPT")
	})
	t.Run("computer", func(t *testing.T) {
		h, _, computer := backupHarness(t, 4, nil)
		caller := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
		status, _, raw := h.do(caller, http.MethodGet, "/v1/computers/"+computer.ComputerID, nil)
		if status != http.StatusOK {
			t.Fatalf("%d %s", status, raw)
		}
		assertWiderProjectionFixture(t, "computer", raw, computer.ComputerID, "COMPUTER", computer.StorageID, "STORAGE", computer.CurrentJobID, "JOB")
	})
}
