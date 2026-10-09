package l1

import (
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
		first := h.submit(client, "baseline-first", []string{"linux"})
		a := claimClass(t, h, agent, node, contract.JobClassOneShot)
		h.clock.Advance(time.Millisecond)
		second := h.submit(client, "baseline-second", []string{"linux"})
		b := claimClass(t, h, agent, node, contract.JobClassOneShot)
		status, _, raw := h.do(client, http.MethodGet, "/v1/jobs?limit=2", nil)
		if status != http.StatusOK {
			t.Fatalf("%d %s", status, raw)
		}
		assertWiderProjectionFixture(t, "same-node-page", raw, first.JobID, "JOB1", second.JobID, "JOB2", a.Lease.AttemptID, "ATTEMPT1", b.Lease.AttemptID, "ATTEMPT2")
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
