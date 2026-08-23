package l1

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"sync"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestRequiredCapabilitiesDerivesNormalizedEligibility(t *testing.T) {
	memoryBytes := int64(64 << 20)
	cpuMillicores := int64(500)
	for _, test := range []struct {
		name string
		spec contract.JobSpec
		want []string
	}{
		{
			name: "process kind",
			spec: contract.JobSpec{Kind: contract.JobKindProcess, Limits: &contract.JobLimits{MaxRuntimeSeconds: 30}},
			want: []string{"kind:process"},
		},
		{
			name: "open kind",
			spec: contract.JobSpec{Kind: "future-isolation"},
			want: []string{"kind:future-isolation"},
		},
		{
			name: "explicit runtime handler",
			spec: contract.JobSpec{Kind: contract.JobKindOCI, RuntimeHandler: "runsc"},
			want: []string{"kind:oci", "runtime_handler:runsc"},
		},
		{
			name: "memory limit",
			spec: contract.JobSpec{Kind: contract.JobKindOCI, Execution: contract.ExecutionSpec{
				OCI: &contract.OCIExecutionSpec{Limits: &contract.OCILimits{MemoryBytes: &memoryBytes}},
			}},
			want: []string{"cgroup_v2", "kind:oci"},
		},
		{
			name: "cpu limit",
			spec: contract.JobSpec{Kind: contract.JobKindOCI, Execution: contract.ExecutionSpec{
				OCI: &contract.OCIExecutionSpec{Limits: &contract.OCILimits{CPUMillicores: &cpuMillicores}},
			}},
			want: []string{"cgroup_v2", "kind:oci"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := RequiredCapabilities(test.spec)
			if !slices.Equal(got, test.want) {
				t.Fatalf("RequiredCapabilities() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestClaimRequiresAdvertisedCapabilities(t *testing.T) {
	assertClaimRequiresAdvertisedCapabilities(t)
}

func TestLegacyProcessCapabilityRemainsClaimEligible(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{
		"legacy": {MaxOneshotSlots: 1, MaxServiceSlots: 1},
	})
	legacy, err := h.store.RegisterNode(context.Background(), fabric.Identity{NodeID: "fabric-legacy"}, contract.NodeRegistration{
		NodeID: "legacy", BootSessionID: "boot-legacy", OS: "linux", Architecture: "amd64", AgentVersion: "test",
		Capabilities: map[string]bool{"process": true},
	}, NodePolicy{MaxOneshotSlots: 1, MaxServiceSlots: 1}, true)
	if err != nil {
		t.Fatal(err)
	}
	if !legacy.Capabilities["kind:process"] || legacy.Capabilities["process"] {
		t.Fatalf("registered capabilities = %#v, want canonical kind:process only", legacy.Capabilities)
	}
	job, _, err := h.store.CreateJob(context.Background(), capabilityJobSpec(
		"legacy-process", contract.JobKindProcess, contract.JobClassOneShot, "", nil,
	))
	if err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.ClaimJob(context.Background(), "fabric-legacy", legacy.NodeID, legacy.BootSessionID, contract.JobClassOneShot)
	if err != nil {
		t.Fatal(err)
	}
	if claim == nil || claim.Job.JobID != job.JobID {
		t.Fatalf("legacy process claim = %#v, want job %q", claim, job.JobID)
	}
}

func TestCreateJobPersistsNormalizedExecutionIdentifiers(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{
		"oci": {MaxOneshotSlots: 1, MaxServiceSlots: 1},
	})
	node := registerCapabilityNode(t, h, "oci", map[string]bool{
		"kind:oci": true, "runtime_handler:io.containerd.runsc.v1": true,
	})
	spec := capabilityJobSpec("normalized-oci", contract.JobKindOCI, contract.JobClassOneShot, " IO.CONTAINERD.RUNSC.V1 ", nil)
	spec.Kind = " OCI "
	job, _, err := h.store.CreateJob(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if job.Spec.Kind != contract.JobKindOCI || job.Spec.RuntimeHandler != "io.containerd.runsc.v1" {
		t.Fatalf("stored identifiers = kind %q handler %q", job.Spec.Kind, job.Spec.RuntimeHandler)
	}
	claim, err := h.store.ClaimJob(context.Background(), "fabric-oci", node.NodeID, node.BootSessionID, contract.JobClassOneShot)
	if err != nil {
		t.Fatal(err)
	}
	if claim == nil || claim.Job.JobID != job.JobID {
		t.Fatalf("normalized OCI claim = %#v, want job %q", claim, job.JobID)
	}
}

func assertClaimRequiresAdvertisedCapabilities(t *testing.T) {
	t.Helper()
	memoryBytes := int64(64 << 20)
	cpuMillicores := int64(500)
	for _, test := range []struct {
		name              string
		spec              contract.JobSpec
		incapable         map[string]bool
		capable           map[string]bool
		class             string
		missingCapability string
	}{
		{
			name: "process kind one-shot", spec: capabilityJobSpec("process-one-shot", contract.JobKindProcess, contract.JobClassOneShot, "", nil),
			incapable: map[string]bool{"kind:process": false}, capable: map[string]bool{"kind:process": true},
			class: contract.JobClassOneShot, missingCapability: "kind:process",
		},
		{
			name: "empty capability set", spec: capabilityJobSpec("process-empty-capabilities", contract.JobKindProcess, contract.JobClassOneShot, "", nil),
			incapable: map[string]bool{}, capable: map[string]bool{"kind:process": true},
			class: contract.JobClassOneShot, missingCapability: "kind:process",
		},
		{
			name: "oci kind one-shot", spec: capabilityJobSpec("oci-one-shot", contract.JobKindOCI, contract.JobClassOneShot, "", nil),
			incapable: map[string]bool{"kind:process": true}, capable: map[string]bool{"kind:oci": true},
			class: contract.JobClassOneShot, missingCapability: "kind:oci",
		},
		{
			name: "oci kind service", spec: capabilityJobSpec("oci-service", contract.JobKindOCI, contract.JobClassService, "", nil),
			incapable: map[string]bool{"kind:process": true}, capable: map[string]bool{"kind:oci": true},
			class: contract.JobClassService, missingCapability: "kind:oci",
		},
		{
			name: "runtime handler", spec: capabilityJobSpec("oci-handler", contract.JobKindOCI, contract.JobClassOneShot, "runsc", nil),
			incapable: map[string]bool{"kind:oci": true}, capable: map[string]bool{"kind:oci": true, "runtime_handler:runsc": true},
			class: contract.JobClassOneShot, missingCapability: "runtime_handler:runsc",
		},
		{
			name: "memory cgroup v2", spec: capabilityJobSpec("oci-memory", contract.JobKindOCI, contract.JobClassOneShot, "", &contract.OCILimits{MemoryBytes: &memoryBytes}),
			incapable: map[string]bool{"kind:oci": true}, capable: map[string]bool{"kind:oci": true, "cgroup_v2": true},
			class: contract.JobClassOneShot, missingCapability: "cgroup_v2",
		},
		{
			name: "cpu cgroup v2", spec: capabilityJobSpec("oci-cpu", contract.JobKindOCI, contract.JobClassOneShot, "", &contract.OCILimits{CPUMillicores: &cpuMillicores}),
			incapable: map[string]bool{"kind:oci": true}, capable: map[string]bool{"kind:oci": true, "cgroup_v2": true},
			class: contract.JobClassOneShot, missingCapability: "cgroup_v2",
		},
		{
			name: "open kind", spec: capabilityJobSpec("future-kind", "future-isolation", contract.JobClassOneShot, "", nil),
			incapable: map[string]bool{"kind:process": true}, capable: map[string]bool{"kind:future-isolation": true},
			class: contract.JobClassOneShot, missingCapability: "kind:future-isolation",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{
				"incapable": {MaxOneshotSlots: 1, MaxServiceSlots: 1},
				"capable":   {MaxOneshotSlots: 1, MaxServiceSlots: 1},
			})
			incapable := registerCapabilityNode(t, h, "incapable", test.incapable)
			capable := registerCapabilityNode(t, h, "capable", test.capable)

			job, _, err := h.store.CreateJob(context.Background(), test.spec)
			if err != nil {
				t.Fatal(err)
			}
			claim, err := h.store.ClaimJob(context.Background(), "fabric-incapable", incapable.NodeID, incapable.BootSessionID, test.class)
			if err != nil {
				t.Fatal(err)
			}
			if claim != nil {
				t.Fatalf("node missing %q claimed job %q", test.missingCapability, claim.Job.JobID)
			}
			queued, err := h.store.GetJob(context.Background(), job.JobID)
			if err != nil {
				t.Fatal(err)
			}
			if queued.State != contract.JobQueued {
				t.Fatalf("job state after incapable claim = %q, want queued", queued.State)
			}

			claim, err = h.store.ClaimJob(context.Background(), "fabric-capable", capable.NodeID, capable.BootSessionID, test.class)
			if err != nil {
				t.Fatal(err)
			}
			if claim == nil || claim.Job.JobID != job.JobID {
				t.Fatalf("capable claim = %#v, want job %q", claim, job.JobID)
			}
		})
	}
}

func TestUnknownKindDiagnosticClearsWhenCapabilityAppears(t *testing.T) {
	assertUnknownKindDiagnosticClearsWhenCapabilityAppears(t)
}

func assertUnknownKindDiagnosticClearsWhenCapabilityAppears(t *testing.T) {
	t.Helper()
	for _, class := range []string{contract.JobClassOneShot, contract.JobClassService} {
		t.Run(class, func(t *testing.T) {
			h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{
				"process-node": {MaxOneshotSlots: 1, MaxServiceSlots: 1},
				"future-node":  {MaxOneshotSlots: 1, MaxServiceSlots: 1},
			})
			registerCapabilityNode(t, h, "process-node", map[string]bool{"kind:process": true})
			operator := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
			status, _, body := h.do(operator, http.MethodPost, "/v1/jobs", capabilityJobSpec(
				"future-"+class, "future-isolation", class, "", nil,
			))
			if status != http.StatusCreated {
				t.Fatalf("create unknown-kind %s status = %d body=%s", class, status, body)
			}
			var projected Job
			if err := json.Unmarshal(body, &projected); err != nil {
				t.Fatal(err)
			}
			if projected.State != contract.JobQueued || projected.Status != "unschedulable" ||
				projected.UnschedulableReason != "no tag-eligible node advertises required capabilities: kind:future-isolation" {
				t.Fatalf("unknown-kind %s diagnostic = %#v", class, projected)
			}

			futureNode := registerCapabilityNode(t, h, "future-node", map[string]bool{"kind:future-isolation": true})
			path := "/v1/jobs/" + projected.JobID
			if class == contract.JobClassService {
				path += "?class=service"
			}
			status, _, body = h.do(operator, http.MethodGet, path, nil)
			if status != http.StatusOK {
				t.Fatalf("get unknown-kind %s status = %d body=%s", class, status, body)
			}
			var refreshed Job
			if err := json.Unmarshal(body, &refreshed); err != nil {
				t.Fatal(err)
			}
			if refreshed.Status != string(contract.JobQueued) || refreshed.UnschedulableReason != "" {
				t.Fatalf("%s diagnostic after capability appears = %#v", class, refreshed)
			}
			claim, err := h.store.ClaimJob(context.Background(), "fabric-future-node", futureNode.NodeID, futureNode.BootSessionID, class)
			if err != nil {
				t.Fatal(err)
			}
			if claim == nil || claim.Job.JobID != projected.JobID {
				t.Fatalf("%s claim after capability appears = %#v, want job %q", class, claim, projected.JobID)
			}
		})
	}
}

func TestConcurrentClaimsCannotBypassCapabilities(t *testing.T) {
	assertConcurrentClaimsCannotBypassCapabilities(t)
}

func assertConcurrentClaimsCannotBypassCapabilities(t *testing.T) {
	t.Helper()
	for _, class := range []string{contract.JobClassOneShot, contract.JobClassService} {
		t.Run(class, func(t *testing.T) {
			h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{
				"incapable": {MaxOneshotSlots: 1, MaxServiceSlots: 1},
				"capable":   {MaxOneshotSlots: 1, MaxServiceSlots: 1},
			})
			incapable := registerCapabilityNode(t, h, "incapable", map[string]bool{"kind:process": true})
			capable := registerCapabilityNode(t, h, "capable", map[string]bool{"kind:oci": true})
			job, _, err := h.store.CreateJob(context.Background(), capabilityJobSpec("concurrent-"+class, contract.JobKindOCI, class, "", nil))
			if err != nil {
				t.Fatal(err)
			}

			type result struct {
				nodeID string
				claim  *Claim
				err    error
			}
			start := make(chan struct{})
			results := make(chan result, 2)
			var wait sync.WaitGroup
			for _, node := range []Node{incapable, capable} {
				node := node
				wait.Add(1)
				go func() {
					defer wait.Done()
					<-start
					claim, err := h.store.ClaimJob(context.Background(), "fabric-"+node.NodeID, node.NodeID, node.BootSessionID, class)
					results <- result{nodeID: node.NodeID, claim: claim, err: err}
				}()
			}
			close(start)
			wait.Wait()
			close(results)

			wins := 0
			for result := range results {
				if result.err != nil {
					t.Fatal(result.err)
				}
				if result.claim == nil {
					continue
				}
				wins++
				if result.nodeID != capable.NodeID || result.claim.Job.JobID != job.JobID {
					t.Fatalf("concurrent claim winner = node %q job %q, want capable node %q job %q", result.nodeID, result.claim.Job.JobID, capable.NodeID, job.JobID)
				}
			}
			if wins != 1 {
				t.Fatalf("concurrent claim winners = %d, want exactly one capable winner", wins)
			}
		})
	}
}

func registerCapabilityNode(t *testing.T, h *integrationHarness, nodeID string, capabilities map[string]bool) Node {
	t.Helper()
	node, err := h.store.RegisterNode(context.Background(), fabric.Identity{NodeID: "fabric-" + nodeID}, contract.NodeRegistration{
		NodeID: nodeID, BootSessionID: "boot-" + nodeID, RootInstanceID: "root-" + nodeID,
		OS: "linux", Architecture: "amd64", AgentVersion: "test", Capabilities: capabilities,
		CapabilityRevision: 1, CapabilityObservedAt: h.clock.Now(), MissingCapabilities: []string{},
	}, NodePolicy{MaxOneshotSlots: 1, MaxServiceSlots: 1}, true)
	if err != nil {
		t.Fatal(err)
	}
	return node
}

func capabilityJobSpec(dispatchKey, kind, class, runtimeHandler string, limits *contract.OCILimits) contract.JobSpec {
	if kind == contract.JobKindProcess {
		spec := validJobSpec(dispatchKey, nil)
		spec.Class = class
		if class == contract.JobClassService {
			spec.Execution.HandoffDirectory = ""
			spec.Restart = contract.RestartAlways
		}
		return spec
	}
	if kind != contract.JobKindOCI {
		restart := ""
		if class == contract.JobClassService {
			restart = contract.RestartAlways
		}
		return contract.JobSpec{
			SchemaVersion: contract.SchemaVersionV1,
			DispatchKey:   dispatchKey,
			Kind:          kind,
			Class:         class,
			Restart:       restart,
			Execution:     contract.ExecutionSpec{},
		}
	}
	digest := testTopDigest
	image := contract.OCIImageSpec{Reference: "ghcr.io/example/tool:latest"}
	if class == contract.JobClassService {
		image.Digest = &digest
	}
	restart := ""
	if class == contract.JobClassService {
		restart = contract.RestartAlways
	}
	return contract.JobSpec{
		SchemaVersion:  contract.SchemaVersionV1,
		DispatchKey:    dispatchKey,
		Kind:           kind,
		Class:          class,
		Restart:        restart,
		RuntimeHandler: runtimeHandler,
		Execution:      contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: image, Limits: limits}},
	}
}
