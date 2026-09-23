package oci

import (
	"context"
	"strings"
	"testing"

	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// Budget eviction is the one thing the node does to the helper's handoff root,
// and it does it with an owner key and nothing else. These drive the real
// server and the real client, because what is being proved is that the key the
// agent holds reaches the handoff arm of DeleteManagedVolume unchanged, and
// that a response which does not positively verify the removal is refused
// rather than reported as bytes recovered.

type handoffEvictionEngine struct {
	adapterTestEngine
	// unverified makes the helper answer without saying the volume is gone,
	// which is exactly what a node must never treat as a reclaim.
	unverified bool
}

func (engine *handoffEvictionEngine) DeleteManagedVolume(ctx context.Context, request ocihelper.DeleteManagedVolumeRequest) (ocihelper.DeleteManagedVolumeResponse, error) {
	if engine.unverified {
		engine.mu.Lock()
		engine.volumeDeleteRequests = append(engine.volumeDeleteRequests, request)
		engine.mu.Unlock()
		return ocihelper.DeleteManagedVolumeResponse{}, nil
	}
	return engine.adapterTestEngine.DeleteManagedVolume(ctx, request)
}

func TestEvictingARetainedHandoffCarriesOnlyTheOwnerKey(t *testing.T) {
	engine := &handoffEvictionEngine{}
	adapter, stop := startAdapterTestServer(t, engine)
	defer stop()

	var evictor workloadrunner.RetainedHandoffEvictor = adapter
	if err := evictor.EvictRetainedHandoff(t.Context(), "run-alpha"); err != nil {
		t.Fatal(err)
	}

	engine.mu.Lock()
	requests := append([]ocihelper.DeleteManagedVolumeRequest(nil), engine.volumeDeleteRequests...)
	engine.mu.Unlock()
	if len(requests) != 1 {
		t.Fatalf("the helper saw %d deletions, want exactly one", len(requests))
	}
	request := requests[0]
	if request.Kind != ocihelper.ManagedVolumeHandoff || request.OwnerKey != "run-alpha" {
		t.Fatalf("the eviction reached the helper as %+v", request)
	}
	// Nothing else crosses. A path, a volume name, removal authority or
	// Computer evidence on this arm would each be a wider deletion than the
	// one the budget is allowed to ask for.
	if request.Removal != nil || request.ComputerStorage != nil || request.StorageAbsent ||
		request.QuarantineOnFailure || request.FailureAttempts != 0 {
		t.Fatalf("the eviction carried more than an owner key: %+v", request)
	}
}

func TestAnEvictionTheHelperDidNotVerifyIsNotAReclaim(t *testing.T) {
	engine := &handoffEvictionEngine{unverified: true}
	adapter, stop := startAdapterTestServer(t, engine)
	defer stop()

	err := adapter.EvictRetainedHandoff(t.Context(), "run-alpha")
	if err == nil {
		t.Fatal("a helper that did not say the volume was gone was treated as a reclaim")
	}
	if !strings.Contains(err.Error(), "did not positively verify") {
		t.Fatalf("the refusal does not say what was missing: %v", err)
	}
}

// An owner key the helper could not derive a directory from would be a request
// to delete nothing, or worse. The helper refuses one too, but the agent says
// so in its own words and without a round trip -- which is what the message
// below asserts: the helper's refusal reads differently, so a version that
// only forwarded would not pass this.
func TestAnEmptyOwnerKeyIsRefusedByTheAgentRatherThanTheHelper(t *testing.T) {
	engine := &handoffEvictionEngine{}
	adapter, stop := startAdapterTestServer(t, engine)
	defer stop()

	err := adapter.EvictRetainedHandoff(t.Context(), "   ")
	if err == nil {
		t.Fatal("an empty owner key was accepted")
	}
	if !strings.Contains(err.Error(), "evicted by its owner key") {
		t.Fatalf("the refusal is not the agent's own: %v", err)
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if len(engine.volumeDeleteRequests) != 0 {
		t.Fatalf("the helper saw %d deletions for an empty owner key", len(engine.volumeDeleteRequests))
	}
}
