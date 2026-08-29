package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

func TestComputerAccessCLIUsesPersonAuthenticatedL1Routes(t *testing.T) {
	network := plain.NewNetwork()
	controlFabric := network.NewFabric(fabric.Identity{NodeID: "access-cli-control"})
	adminIdentity := fabric.Identity{NodeID: "admin-device", UserID: "person-admin", DeviceID: "device-admin"}
	viewerIdentity := fabric.Identity{NodeID: "viewer-device", UserID: "person-viewer", DeviceID: "device-viewer"}
	adminFabric := network.NewFabric(adminIdentity)
	viewerFabric := network.NewFabric(viewerIdentity)
	machineFabric := network.NewFabric(fabric.Identity{NodeID: "machine-principal", Kind: fabric.IdentityKindMachine})

	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	server, err := l1.NewServer(controlFabric, store, l1.ServerConfig{AllowSelfAssertedPersonIdentities: true})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := controlFabric.Listen("tcp", l3.DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := serveTestServer(ctx, func() error { return server.Serve(ctx, listener) })
	adminClients, err := newAPIClients(adminFabric, l3.DefaultL1Address, l3.DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}
	viewerClients, err := newAPIClients(viewerFabric, l3.DefaultL1Address, l3.DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}
	machineClients, err := newAPIClients(machineFabric, l3.DefaultL1Address, l3.DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		adminClients.close()
		viewerClients.close()
		machineClients.close()
		cancel()
		if err := <-done; err != nil {
			t.Errorf("L1 server: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("close L1 store: %v", err)
		}
	})

	challenge, err := store.InitiateAdminBootstrap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := runAccessCLI(t, ctx, adminClients, true, "admin", "bootstrap", challenge.Nonce)
	var policy l1.AdminPolicy
	if err := json.Unmarshal(bootstrap, &policy); err != nil || policy.Revision != 1 {
		t.Fatalf("bootstrap policy = %#v err=%v", policy, err)
	}

	human := string(runAccessCLI(t, ctx, adminClients, false, "admin", "policy", "get"))
	if !strings.Contains(human, "POLICY REVISION\t1") || !strings.Contains(human, "person-admin") {
		t.Fatalf("human admin policy omitted revision or person identity:\n%s", human)
	}
	adminsAlias := string(runAccessCLI(t, ctx, adminClients, false, "admins", "list"))
	if !strings.Contains(adminsAlias, "person-admin") {
		t.Fatalf("admins list alias omitted administrator: %s", adminsAlias)
	}
	if !requiresPersonCommand([]string{"services", "grant"}) || requiresPersonCommand([]string{"services", "status"}) {
		t.Fatal("person-command routing does not isolate Computer access policy from ordinary service commands")
	}
	var stdout, stderr bytes.Buffer
	err = execute(ctx, viewerClients, true, []string{"admin", "policy", "add", "person-viewer", "--policy-revision", "1"}, &stdout, &stderr)
	assertCLIErrorCode(t, err, contract.ErrorAdminRequired)
	err = execute(ctx, adminClients, true, []string{"admin", "policy", "remove", "person-admin", "--policy-revision", "1"}, &stdout, &stderr)
	assertCLIErrorCode(t, err, contract.ErrorFinalAdmin)
	var addedPolicy l1.AdminPolicy
	if err := json.Unmarshal(runAccessCLI(t, ctx, adminClients, true, "admins", "add", "person-second",
		"--policy-revision", "1"), &addedPolicy); err != nil || addedPolicy.Revision != 2 || len(addedPolicy.Admins) != 2 {
		t.Fatalf("added admin policy = %#v err=%v", addedPolicy, err)
	}
	removedAdmin := string(runAccessCLI(t, ctx, adminClients, false, "admin", "policy", "remove", "person-second",
		"--policy-revision", "2"))
	if !strings.Contains(removedAdmin, "POLICY REVISION\t3") || strings.Contains(removedAdmin, "person-second") {
		t.Fatalf("admin removal projection = %s", removedAdmin)
	}

	computer, _, err := store.CreateComputer(ctx, l1.CreateComputerRequest{
		Name: "access-cli-computer", Spec: accessCLIComputerSpec("access-cli-computer"), Actor: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	err = execute(ctx, viewerClients, true, []string{"services", "takeover", "view", computer.ComputerID}, &stdout, &stderr)
	assertCLIErrorCode(t, err, contract.ErrorForbidden)
	stdout.Reset()
	err = execute(ctx, machineClients, true, []string{"services", "takeover", "view", computer.ComputerID}, &stdout, &stderr)
	assertCLIErrorCode(t, err, contract.ErrorPrincipalForbidden)
	grantJSON := runAccessCLI(t, ctx, adminClients, true, "services", "grant", computer.ComputerID,
		viewerIdentity.UserID, "--permission", "control", "--policy-revision", "3", "--idempotency-key", "grant-viewer")
	var grant l1.ComputerGrantMutationResult
	if err := json.Unmarshal(grantJSON, &grant); err != nil || grant.Grant.Permission != l1.ComputerGrantControl || grant.Replayed {
		t.Fatalf("grant result = %#v err=%v", grant, err)
	}
	replayedJSON := runAccessCLI(t, ctx, adminClients, true, "services", "grant", computer.ComputerID,
		viewerIdentity.UserID, "--permission", "control", "--policy-revision", "3", "--idempotency-key", "grant-viewer")
	if err := json.Unmarshal(replayedJSON, &grant); err != nil || !grant.Replayed {
		t.Fatalf("replayed grant = %#v err=%v", grant, err)
	}
	grantsHuman := string(runAccessCLI(t, ctx, adminClients, false, "services", "grants", computer.ComputerID))
	if !strings.Contains(grantsHuman, "POLICY REVISION\t4") || !strings.Contains(grantsHuman, "person-viewer\tcontrol") {
		t.Fatalf("human grant list omitted revision or permission:\n%s", grantsHuman)
	}
	viewJSON := runAccessCLI(t, ctx, viewerClients, true, "services", "takeover", "view", computer.ComputerID)
	var view computerTakeoverActionResult
	if err := json.Unmarshal(viewJSON, &view); err != nil || view.Action != "view" ||
		view.AuthorizedRole != l1.ComputerGrantControl || view.DisplayEndpoint != nil || view.SessionBound {
		t.Fatalf("view-first access projection = %#v err=%v", view, err)
	}

	stdout.Reset()
	err = execute(ctx, adminClients, true, []string{"services", "revoke", computer.ComputerID, viewerIdentity.UserID,
		"--policy-revision", "3", "--idempotency-key", "stale-revoke"}, &stdout, &stderr)
	assertCLIErrorCode(t, err, contract.ErrorStalePolicyRevision)
	revokeJSON := runAccessCLI(t, ctx, adminClients, true, "services", "revoke", computer.ComputerID,
		viewerIdentity.UserID, "--policy-revision", "4", "--idempotency-key", "revoke-viewer")
	if err := json.Unmarshal(revokeJSON, &grant); err != nil || grant.Grant.Permission != l1.ComputerGrantNone ||
		grant.Revocation == nil || grant.Revocation.State != l1.ComputerPolicyRevocationPending {
		t.Fatalf("revoke result = %#v err=%v", grant, err)
	}
	adminClients.wait = func(context.Context, time.Duration) error { return context.Canceled }
	stdout.Reset()
	err = execute(ctx, adminClients, true, []string{"services", "revoke", computer.ComputerID, viewerIdentity.UserID,
		"--policy-revision", "4", "--idempotency-key", "revoke-viewer", "--wait"}, &stdout, &stderr)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("injected revocation wait cancellation = %v", err)
	}
	stdout.Reset()
	err = execute(ctx, viewerClients, true, []string{"services", "takeover", "view", computer.ComputerID}, &stdout, &stderr)
	assertCLIErrorCode(t, err, contract.ErrorForbidden)
	tokenPath := filepath.Join(t.TempDir(), "session-token")
	if err := os.WriteFile(tokenPath, []byte("opaque-live-session-bearer\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	err = execute(ctx, adminClients, true, []string{"services", "takeover", "take", computer.ComputerID,
		"--session-token-file", tokenPath}, &stdout, &stderr)
	assertCLIErrorCode(t, err, contract.ErrorPassUnavailable)
	stdout.Reset()
	err = execute(ctx, adminClients, true, []string{"services", "takeover", "release", computer.ComputerID,
		"--session-token-file", tokenPath}, &stdout, &stderr)
	assertCLIErrorCode(t, err, contract.ErrorPassUnavailable)
	sessionsJSON := runAccessCLI(t, ctx, adminClients, true, "services", "takeover", "sessions", "list", computer.ComputerID)
	var sessions l1.ComputerTakeoverSessionList
	if err := json.Unmarshal(sessionsJSON, &sessions); err != nil || sessions.Sessions == nil || len(sessions.Sessions) != 0 {
		t.Fatalf("empty session projection = %#v err=%v", sessions, err)
	}
	auditHuman := string(runAccessCLI(t, ctx, adminClients, false, "services", "takeover", "audit", "tail", computer.ComputerID, "--limit", "1"))
	if !strings.Contains(auditHuman, "OCCURRED") || !strings.Contains(auditHuman, "AUTHORIZED ROLE") {
		t.Fatalf("human take-over audit omitted evidence columns:\n%s", auditHuman)
	}
	allAccessOutput := bytes.Join([][]byte{bootstrap, []byte(human), grantJSON, replayedJSON, []byte(grantsHuman),
		viewJSON, revokeJSON, sessionsJSON, []byte(auditHuman)}, []byte("\n"))
	for _, forbidden := range []string{"bearer", "fencing_token", "framebuffer", "pointer", "hidden_backend", "idempotency_key"} {
		if bytes.Contains(bytes.ToLower(allAccessOutput), []byte(forbidden)) {
			t.Fatalf("access output leaked forbidden surface %q: %s", forbidden, allAccessOutput)
		}
	}
}

func runAccessCLI(t *testing.T, ctx context.Context, clients *apiClients, jsonOutput bool, args ...string) []byte {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if err := execute(ctx, clients, jsonOutput, args, &stdout, &stderr); err != nil {
		t.Fatalf("execute %q: %v stderr=%s", args, err, stderr.String())
	}
	return stdout.Bytes()
}

func assertCLIErrorCode(t *testing.T, err error, want contract.ErrorCode) {
	t.Helper()
	var got contract.ErrorCode
	switch response := err.(type) {
	case *apiResponseError:
		got = response.APIError.Code
	case *takeoverActionError:
		got = response.APIError.Code
	}
	if got != want {
		t.Fatalf("CLI error = %#v, want %s", err, want)
	}
	if want == contract.ErrorAdminRequired && commandExitCode(err) != exitUnauthorized {
		t.Fatalf("admin-required exit = %d, want %d", commandExitCode(err), exitUnauthorized)
	}
	if (want == contract.ErrorFinalAdmin || want == contract.ErrorStalePolicyRevision) && commandExitCode(err) != exitConflict {
		t.Fatalf("conflict exit = %d, want %d", commandExitCode(err), exitConflict)
	}
}

func accessCLIComputerSpec(dispatchKey string) contract.JobSpec {
	memoryBytes := int64(64 << 20)
	return contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   dispatchKey,
		Kind:          contract.JobKindOCI,
		Class:         contract.JobClassService,
		Restart:       contract.RestartAlways,
		RoutingTags:   []string{contract.StableNodeTagPrefix + "computer-node"},
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image:  contract.OCIImageSpec{Reference: "ghcr.io/example/computer", Digest: pointerTo("sha256:" + strings.Repeat("a", 64))},
			Limits: &contract.OCILimits{MemoryBytes: &memoryBytes},
			Computer: &contract.OCIComputerSpec{
				Display:   contract.OCIComputerDisplaySpec{Protocol: contract.ComputerDisplayProtocolRFBWebSocketV1},
				DiskBytes: 1 << 30,
			},
		}},
	}
}

func pointerTo(value string) *string { return &value }
