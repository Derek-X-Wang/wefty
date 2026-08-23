package l1

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestCapabilityRevisionProtocol(t *testing.T) {
	assertCapabilityRevisionProtocol(t)
}

func assertCapabilityRevisionProtocol(t *testing.T) {
	t.Helper()
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{
		"node-1": {MaxOneshotSlots: 1, MaxServiceSlots: 1},
	})
	ctx := context.Background()
	identity := fabric.Identity{NodeID: "fabric-node-1"}
	policy := NodePolicy{MaxOneshotSlots: 1, MaxServiceSlots: 1}
	observedAt := h.clock.Now()
	registration := contract.NodeRegistration{
		NodeID: "node-1", BootSessionID: "boot-1", OS: "linux", Architecture: "amd64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true, "kind:oci": true}, CapabilityRevision: 7,
		CapabilityObservedAt: observedAt, MissingCapabilities: []string{},
	}
	node, err := h.store.RegisterNode(ctx, identity, registration, policy, true)
	if err != nil {
		t.Fatal(err)
	}
	if node.CapabilityRevision != 7 || !node.Capabilities["kind:oci"] || !node.ClaimsEnabled {
		t.Fatalf("registered capability state = %#v", node)
	}
	agentClient := h.client(fabric.Identity{NodeID: identity.NodeID, Tags: []string{DefaultAgentPrincipalTag}})
	status, _, body := h.do(agentClient, http.MethodPost, "/v1/agent/nodes/node-1/heartbeat", HeartbeatRequest{BootSessionID: node.BootSessionID})
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)

	h.clock.Advance(time.Second)
	withdrawn := contract.CapabilityObservation{
		Revision: 8, Capabilities: map[string]bool{"kind:process": true}, ObservedAt: observedAt.Add(time.Second),
		MissingCapabilities: []string{"kind:oci"}, ReasonCode: contract.CapabilityReasonProbeFailed,
	}
	node, err = h.store.HeartbeatNodeWithCapabilityObservation(ctx, identity.NodeID, node.NodeID, node.BootSessionID, withdrawn, policy)
	if err != nil {
		t.Fatal(err)
	}
	if node.CapabilityRevision != 8 || node.Capabilities["kind:oci"] || !node.ClaimsEnabled ||
		len(node.MissingCapabilities) != 1 || node.MissingCapabilities[0] != "kind:oci" ||
		node.CapabilityReasonCode != contract.CapabilityReasonProbeFailed || !node.CapabilityObservedAt.Equal(observedAt.Add(time.Second)) {
		t.Fatalf("withdrawn capability state = %#v", node)
	}
	job, _, err := h.store.CreateJob(ctx, capabilityJobSpec("revisioned-oci", contract.JobKindOCI, contract.JobClassOneShot, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	if claim, err := h.store.ClaimJob(ctx, identity.NodeID, node.NodeID, node.BootSessionID, contract.JobClassOneShot); err != nil || claim != nil {
		t.Fatalf("claim while OCI is withdrawn = %#v, %v", claim, err)
	}

	// An older observation still proves node liveness but cannot restore the
	// removed badge or its associated metadata.
	h.clock.Advance(time.Second)
	stale := contract.CapabilityObservation{
		Revision: 7, Capabilities: map[string]bool{"kind:process": true, "kind:oci": true},
		ObservedAt: observedAt, MissingCapabilities: []string{},
	}
	node, err = h.store.HeartbeatNodeWithCapabilityObservation(ctx, identity.NodeID, node.NodeID, node.BootSessionID, stale, policy)
	if err != nil {
		t.Fatal(err)
	}
	if node.CapabilityRevision != 8 || node.Capabilities["kind:oci"] || !node.LastHeartbeatAt.Equal(h.clock.Now()) {
		t.Fatalf("stale revision changed capability authority = %#v", node)
	}

	// Re-registration of the same boot cannot replay its original OCI-enabled
	// startup snapshot over the later withdrawal.
	node, err = h.store.RegisterNode(ctx, identity, registration, policy, true)
	if err != nil {
		t.Fatal(err)
	}
	if node.CapabilityRevision != 8 || node.Capabilities["kind:oci"] {
		t.Fatalf("startup re-registration restored stale OCI capability = %#v", node)
	}

	// Equal and identical is a replay; equal and different is a conflict whose
	// transaction cannot partially refresh liveness or capacity.
	replay := withdrawn
	replay.ObservedAt = observedAt.Add(2 * time.Second)
	if _, err := h.store.HeartbeatNodeWithCapabilityObservation(ctx, identity.NodeID, node.NodeID, node.BootSessionID, replay, policy); err != nil {
		t.Fatalf("identical replay: %v", err)
	}
	node, err = getNode(ctx, h.store.db, node.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if !node.CapabilityObservedAt.Equal(replay.ObservedAt) || node.CapabilityRevision != withdrawn.Revision {
		t.Fatalf("replay did not advance observation time without revision = %#v", node)
	}
	beforeConflict := h.clock.Now()
	h.clock.Advance(time.Second)
	conflict := replay
	conflict.Capabilities = map[string]bool{"kind:process": true, "kind:oci": true}
	conflict.MissingCapabilities = []string{}
	conflict.ReasonCode = ""
	if _, err := h.store.HeartbeatNodeWithCapabilityObservation(ctx, identity.NodeID, node.NodeID, node.BootSessionID, conflict, policy); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("equal changed observation error = %v, want conflict", err)
	}
	node, err = getNode(ctx, h.store.db, node.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if !node.LastHeartbeatAt.Equal(beforeConflict) || node.Capabilities["kind:oci"] {
		t.Fatalf("conflicting observation partially committed = %#v", node)
	}

	recovered := contract.CapabilityObservation{
		Revision: 9, Capabilities: map[string]bool{"kind:process": true, "kind:oci": true},
		ObservedAt: observedAt.Add(4 * time.Second), MissingCapabilities: []string{},
	}
	node, err = h.store.HeartbeatNodeWithCapabilityObservation(ctx, identity.NodeID, node.NodeID, node.BootSessionID, recovered, policy)
	if err != nil {
		t.Fatal(err)
	}
	if node.CapabilityRevision != 9 || !node.Capabilities["kind:oci"] || len(node.MissingCapabilities) != 0 || node.CapabilityReasonCode != "" {
		t.Fatalf("recovered capability state = %#v", node)
	}
	claim, err := h.store.ClaimJob(ctx, identity.NodeID, node.NodeID, node.BootSessionID, contract.JobClassOneShot)
	if err != nil || claim == nil || claim.Job.JobID != job.JobID {
		t.Fatalf("claim after newer recovery observation = %#v, %v; want %q", claim, err, job.JobID)
	}

	// Re-registration remains stale after recovery as well.
	node, err = h.store.RegisterNode(ctx, identity, registration, policy, true)
	if err != nil {
		t.Fatal(err)
	}
	if node.CapabilityRevision != 9 || !node.Capabilities["kind:oci"] {
		t.Fatalf("re-registration restored stale capability observation = %#v", node)
	}

	// A replacement boot starts a new revision namespace. The replaced boot can
	// no longer heartbeat even with a numerically newer old observation.
	registration.BootSessionID = "boot-2"
	registration.CapabilityRevision = 1
	registration.CapabilityObservedAt = observedAt.Add(5 * time.Second)
	registration.Capabilities = map[string]bool{"kind:process": true}
	registration.MissingCapabilities = []string{"kind:oci"}
	registration.CapabilityReasonCode = contract.CapabilityReasonProbeFailed
	node, err = h.store.RegisterNode(ctx, identity, registration, policy, true)
	if err != nil {
		t.Fatal(err)
	}
	if node.BootSessionID != "boot-2" || node.CapabilityRevision != 1 || node.Capabilities["kind:oci"] {
		t.Fatalf("replacement boot capability state = %#v", node)
	}
	_, err = h.store.HeartbeatNodeWithCapabilityObservation(ctx, identity.NodeID, node.NodeID, "boot-1", recovered, policy)
	if errorCode(err) != contract.ErrorNodeSessionReplaced {
		t.Fatalf("replaced boot heartbeat error = %v, want node_session_replaced", err)
	}
}

func TestConcurrentCapabilityHeartbeatsAlwaysKeepHighestRevision(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	ctx := context.Background()
	identity := fabric.Identity{NodeID: "fabric-concurrent"}
	policy := DefaultNodePolicy()
	registration := contract.NodeRegistration{
		NodeID: "node-concurrent", BootSessionID: "boot-concurrent", OS: "linux", Architecture: "amd64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true, "kind:oci": true}, CapabilityRevision: 4,
		CapabilityObservedAt: h.clock.Now(), MissingCapabilities: []string{},
	}
	if _, err := h.store.RegisterNode(ctx, identity, registration, policy, true); err != nil {
		t.Fatal(err)
	}
	observations := []contract.CapabilityObservation{
		{Revision: 5, Capabilities: map[string]bool{"kind:process": true}, ObservedAt: h.clock.Now().Add(time.Second), MissingCapabilities: []string{"kind:oci"}, ReasonCode: contract.CapabilityReasonProbeFailed},
		{Revision: 6, Capabilities: map[string]bool{"kind:process": true, "kind:oci": true}, ObservedAt: h.clock.Now().Add(2 * time.Second), MissingCapabilities: []string{}},
	}
	start := make(chan struct{})
	ready := sync.WaitGroup{}
	ready.Add(len(observations))
	errorsByRevision := make(chan error, len(observations))
	for _, observation := range observations {
		observation := observation
		go func() {
			ready.Done()
			<-start
			_, err := h.store.HeartbeatNodeWithCapabilityObservation(ctx, identity.NodeID, registration.NodeID, registration.BootSessionID, observation, policy)
			errorsByRevision <- err
		}()
	}
	ready.Wait()
	close(start)
	for range observations {
		if err := <-errorsByRevision; err != nil {
			t.Fatal(err)
		}
	}
	node, err := getNode(ctx, h.store.db, registration.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if node.CapabilityRevision != 6 || !node.Capabilities["kind:oci"] {
		t.Fatalf("concurrent heartbeat result = %#v, want highest revision", node)
	}
}

func TestLegacyAndLiveRegistrationsCannotMintOCIWithoutObservation(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	ctx := context.Background()
	legacyIdentity := fabric.Identity{NodeID: "fabric-legacy-oci"}
	legacy, err := h.store.RegisterNode(ctx, legacyIdentity, contract.NodeRegistration{
		NodeID: "legacy-oci", BootSessionID: "legacy-boot", OS: "linux", Architecture: "amd64", AgentVersion: "legacy",
		Capabilities: map[string]bool{"process": true, "kind:oci": true, "runtime_handler:io.containerd.runc.v2": true},
	}, DefaultNodePolicy(), true)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Capabilities["kind:oci"] || legacy.Capabilities["runtime_handler:io.containerd.runc.v2"] || !legacy.Capabilities["kind:process"] {
		t.Fatalf("legacy registration capabilities = %#v", legacy.Capabilities)
	}

	agentClient := h.client(fabric.Identity{NodeID: "fabric-live-zero", Tags: []string{DefaultAgentPrincipalTag}})
	status, _, body := h.do(agentClient, http.MethodPost, "/v1/agent/nodes/register", contract.NodeRegistration{
		NodeID: "live-zero", BootSessionID: "live-boot", OS: "linux", Architecture: "amd64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true},
	})
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
	if _, err := getNode(ctx, h.store.db, "live-zero"); err == nil {
		t.Fatal("revision-zero live registration unexpectedly created a node")
	}
}

func TestCapabilityObservationMetadataIsBounded(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	ctx := context.Background()
	identity := fabric.Identity{NodeID: "fabric-node"}
	registration := contract.NodeRegistration{
		NodeID: "node", BootSessionID: "boot", OS: "linux", Architecture: "amd64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1,
		CapabilityObservedAt: h.clock.Now(), MissingCapabilities: []string{"kind:oci"},
		CapabilityReasonCode: contract.CapabilityReasonCode("local stack trace"),
	}
	if _, err := h.store.RegisterNode(ctx, identity, registration, DefaultNodePolicy(), true); errorCode(err) != contract.ErrorInvalidRequest {
		t.Fatalf("unsanitized reason error = %v, want invalid_request", err)
	}
	registration.CapabilityReasonCode = contract.CapabilityReasonProbeFailed
	registration.MissingCapabilities = make([]string, maxMissingCapabilities+1)
	for index := range registration.MissingCapabilities {
		registration.MissingCapabilities[index] = "missing:" + time.Unix(int64(index), 0).Format("150405.000000000")
	}
	if _, err := h.store.RegisterNode(ctx, identity, registration, DefaultNodePolicy(), true); errorCode(err) != contract.ErrorInvalidRequest {
		t.Fatalf("unbounded missing metadata error = %v, want invalid_request", err)
	}
	if _, err := getNode(ctx, h.store.db, "node"); err == nil {
		t.Fatal("invalid capability metadata unexpectedly created a node")
	}
}
