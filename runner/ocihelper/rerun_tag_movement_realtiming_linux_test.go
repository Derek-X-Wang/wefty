//go:build service_acceptance_realtiming && linux

package ocihelper_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

// exerciseOrdinaryL3OCIRerunUnderTagMovement is the live proof of the spec's
// rerun clause (§1.2, §4.1, §9.2): a tag-only one-shot resolves once at first
// launch, and a rerun reuses that resolution even after the tag has moved.
// The frozen-rerun arm next to it submits an already-pinned image, so it can
// never see a tag float. Here the source run submits the bare tag, the tag is
// then repointed at a different image that would also run, and the rerun has
// to land on the digest the source resolved -- without asking the registry
// what the tag names now.
func exerciseOrdinaryL3OCIRerunUnderTagMovement(t *testing.T, ctx context.Context, caller *http.Client, l1Store *l1.Store, registry *refloatRegistry) {
	t.Helper()
	resolved := registry.originalDigest()
	create := l3.CreateRunRequest{
		Image:  &contract.ImageProgram{Reference: registry.reference(), Argv: []string{"/usr/local/bin/wefty-echo-service", "--once"}},
		Params: json.RawMessage(`{"lane":"native-linux-rerun-tag-movement"}`), Tags: []string{"linux"},
		// The echo payload reports to L3 with the run token; see the frozen
		// rerun arm for why it must declare that.
		DispatchAuthority: true,
	}
	var accepted l3.RunAccepted
	doNativeJSON(t, caller, http.MethodPost, "/v1/runs", create,
		http.Header{"Idempotency-Key": []string{"native-rerun-tag-movement"}}, http.StatusCreated, &accepted)
	source := waitNativeRun(t, caller, accepted.RunID, contract.RunSucceeded)
	assertNativeRunLogs(t, caller, accepted.RunID)
	if source.Workflow.Image == nil || source.Workflow.Image.Digest != nil {
		t.Fatalf("source run snapshot = %+v, want a tag-only image program", source.Workflow.Image)
	}
	sourceDigests := nativeAttemptImageDigests(t, ctx, l1Store, source.L1JobID)
	if !everyDigestIs(sourceDigests, resolved) {
		t.Fatalf("tag-only source run attempts resolved %v, want every attempt on %s", sourceDigests, resolved)
	}
	if registry.observedTagRequests() < 1 {
		t.Fatal("tag-only source run never resolved its tag through the lane registry")
	}

	moved := registry.addVariant(t, "rerun-tag-moved")
	if moved == resolved {
		t.Fatalf("moved tag target %s is the resolved digest", moved)
	}
	registry.moveTagTo(t, moved)
	if answered := nativeRegistryTagDigest(t, ctx, registry); answered != moved {
		t.Fatalf("registry answers the moved tag with %s, want %s", answered, moved)
	}
	tagRequestsBefore := registry.observedTagRequests()

	var rerun l3.RunAccepted
	doNativeJSON(t, caller, http.MethodPost, "/v1/runs/"+accepted.RunID+"/rerun", nil,
		http.Header{"Idempotency-Key": []string{"native-rerun-tag-movement-rerun"}}, http.StatusCreated, &rerun)
	rerunRecord := waitNativeRun(t, caller, rerun.RunID, contract.RunSucceeded)
	assertNativeRunLogs(t, caller, rerun.RunID)
	tagRequestsAfter := registry.observedTagRequests()

	var snapshotDigest, specDigest string
	if rerunRecord.Workflow.Image != nil && rerunRecord.Workflow.Image.Digest != nil {
		snapshotDigest = *rerunRecord.Workflow.Image.Digest
	}
	rerunJob, err := l1Store.GetJob(ctx, rerunRecord.L1JobID)
	if err != nil {
		t.Fatal(err)
	}
	if oci := rerunJob.Spec.Execution.OCI; oci != nil && oci.Image.Digest != nil {
		specDigest = *oci.Image.Digest
	}
	attemptDigests := nativeAttemptImageDigests(t, ctx, l1Store, rerunRecord.L1JobID)
	originalDigest := snapshotDigest == resolved && specDigest == resolved && everyDigestIs(attemptDigests, resolved)
	tagNotRefloated := tagRequestsAfter == tagRequestsBefore

	if evidenceDirectory := os.Getenv("WEFTY_REALTIME_EVIDENCE_DIR"); evidenceDirectory != "" {
		receipt := fmt.Sprintf("rerun_under_tag_movement_original_digest=%t\nrerun_under_tag_movement_tag_not_refloated=%t\n"+
			"rerun_under_tag_movement_resolved_digest=%s\nrerun_under_tag_movement_moved_digest=%s\n"+
			"rerun_under_tag_movement_snapshot_digest=%s\nrerun_under_tag_movement_job_digest=%s\n"+
			"rerun_under_tag_movement_attempt_digests=%s\n"+
			"rerun_under_tag_movement_tag_requests_before=%d\nrerun_under_tag_movement_tag_requests_after=%d\n",
			originalDigest, tagNotRefloated, resolved, moved, snapshotDigest, specDigest,
			strings.Join(attemptDigests, ","), tagRequestsBefore, tagRequestsAfter)
		if err := os.WriteFile(filepath.Join(evidenceDirectory, "oci-rerun-tag-movement-linux.txt"), []byte(receipt), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if !originalDigest {
		t.Fatalf("rerun after tag movement ran snapshot=%s job=%s attempts=%v, want %s everywhere (moved tag names %s)",
			snapshotDigest, specDigest, attemptDigests, resolved, moved)
	}
	if !tagNotRefloated {
		t.Fatalf("rerun re-floated the tag: registry tag requests %d -> %d", tagRequestsBefore, tagRequestsAfter)
	}
}

// nativeAttemptImageDigests lists the top-level digest each retained attempt
// of an L1 job recorded, in order; an attempt without image evidence reads as
// an empty digest so it can never pass for the expected one.
func nativeAttemptImageDigests(t *testing.T, ctx context.Context, store *l1.Store, jobID string) []string {
	t.Helper()
	attempts, err := store.ListJobAttempts(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	digests := make([]string, 0, len(attempts))
	for _, attempt := range attempts {
		if attempt.Image == nil {
			digests = append(digests, "")
			continue
		}
		digests = append(digests, attempt.Image.TopLevelDigest)
	}
	return digests
}

// everyDigestIs reports whether there is at least one digest and all of them
// are want.
func everyDigestIs(digests []string, want string) bool {
	if len(digests) == 0 {
		return false
	}
	for _, value := range digests {
		if value != want {
			return false
		}
	}
	return true
}

// nativeRegistryTagDigest asks the lane registry what its tag names right now,
// so the test proves the move is visible to anyone who would re-float.
func nativeRegistryTagDigest(t *testing.T, ctx context.Context, registry *refloatRegistry) string {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodHead, registry.server.URL+"/v2/wefty/refloat/manifests/latest", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := registry.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("registry tag lookup status = %d", response.StatusCode)
	}
	return response.Header.Get("Docker-Content-Digest")
}
