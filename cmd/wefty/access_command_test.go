package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/takeover"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

func TestTakeoverViewPolicyRetryIsTypedAndBounded(t *testing.T) {
	t.Run("typed retry succeeds", func(t *testing.T) {
		attempts := 0
		session, err := retryTakeoverViewPolicyInstallation(t.Context(), time.Second, time.Millisecond,
			func(context.Context) (*takeover.Session, error) {
				attempts++
				if attempts == 1 {
					return nil, &takeover.ActionError{APIError: contract.APIError{Code: contract.ErrorStalePolicyRevision, Retryable: true}}
				}
				return &takeover.Session{}, nil
			})
		if err != nil || session == nil || attempts != 2 {
			t.Fatalf("typed policy retry = session=%v attempts=%d err=%v", session, attempts, err)
		}
	})
	t.Run("non matching error fails immediately", func(t *testing.T) {
		attempts := 0
		want := errors.New("permanent 403 control_not_authorized")
		_, err := retryTakeoverViewPolicyInstallation(t.Context(), time.Second, time.Millisecond,
			func(context.Context) (*takeover.Session, error) { attempts++; return nil, want })
		if !errors.Is(err, want) || attempts != 1 {
			t.Fatalf("permanent refusal = attempts=%d err=%v", attempts, err)
		}
	})
	t.Run("deadline exhaustion returns typed refusal", func(t *testing.T) {
		attempts := 0
		_, err := retryTakeoverViewPolicyInstallation(t.Context(), 5*time.Millisecond, time.Millisecond,
			func(context.Context) (*takeover.Session, error) {
				attempts++
				return nil, &takeover.ActionError{APIError: contract.APIError{Code: contract.ErrorStalePolicyRevision, Retryable: true}}
			})
		var actionErr *takeover.ActionError
		if !errors.As(err, &actionErr) || actionErr.APIError.Code != contract.ErrorStalePolicyRevision || attempts < 2 {
			t.Fatalf("exhausted typed retry = attempts=%d err=%v", attempts, err)
		}
	})
}

func TestComputerTakeoverViewProjectsFriendlyNameBeforeRawConnectHost(t *testing.T) {
	result := computerTakeoverViewResult{
		FriendlyName:     "alice",
		ConnectHost:      "fabric-address.example.test",
		ComputerID:       "computer-1",
		Action:           "view",
		SessionTokenFile: "/private/session.json",
	}

	var human bytes.Buffer
	if err := writeComputerTakeoverView(&human, result, false); err != nil {
		t.Fatal(err)
	}
	wantPrefix := "FRIENDLY NAME\talice\nCONNECT HOST\tfabric-address.example.test\n"
	if !strings.HasPrefix(human.String(), wantPrefix) || strings.Contains(human.String(), "DISPLAY ENDPOINT") {
		t.Fatalf("take-over view table = %q, want friendly name primary and raw connect host secondary", human.String())
	}

	var encoded bytes.Buffer
	if err := writeComputerTakeoverView(&encoded, result, true); err != nil {
		t.Fatal(err)
	}
	wantJSONPrefix := "{\n  \"friendly_name\": \"alice\",\n  \"connect_host\": \"fabric-address.example.test\","
	if !strings.HasPrefix(encoded.String(), wantJSONPrefix) || strings.Contains(encoded.String(), "display_endpoint") {
		t.Fatalf("take-over view JSON = %s, want friendly name primary and raw connect host secondary", encoded.String())
	}
}

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
	if !usesPersonProtocol([]string{"whoami"}) {
		t.Fatal("whoami did not select the person-authenticated protocol")
	}
	if !usesPersonProtocol([]string{"whoami", "extra"}) {
		t.Fatal("invalid whoami arguments escaped person-authenticated protocol selection")
	}
	var observedViewer l1.AuthenticatedPerson
	if err := json.Unmarshal(runAccessCLI(t, ctx, viewerClients, true, "whoami"), &observedViewer); err != nil ||
		observedViewer.UserID != viewerIdentity.UserID || observedViewer.DeviceID != viewerIdentity.DeviceID || observedViewer.FabricID == "" {
		t.Fatalf("whoami observation = %#v err=%v", observedViewer, err)
	}
	viewerHuman := string(runAccessCLI(t, ctx, viewerClients, false, "whoami"))
	if !strings.Contains(viewerHuman, "FABRIC ID\tUSER ID\tDEVICE ID\tSEEN") ||
		!strings.Contains(viewerHuman, "person-viewer\tdevice-viewer") {
		t.Fatalf("human whoami output omitted typed identity fields:\n%s", viewerHuman)
	}
	var invalidWhoAmI bytes.Buffer
	err = execute(ctx, viewerClients, true, []string{"whoami", "extra"}, &invalidWhoAmI, &bytes.Buffer{})
	var whoamiUsage usageError
	if !errors.As(err, &whoamiUsage) || commandExitCodeForArgs(err, []string{"whoami", "extra"}) != exitUsage {
		t.Fatalf("whoami extra = %v exit=%d, want typed usage", err, commandExitCodeForArgs(err, []string{"whoami", "extra"}))
	}
	var machineWhoAmI bytes.Buffer
	err = execute(ctx, machineClients, true, []string{"whoami"}, &machineWhoAmI, &bytes.Buffer{})
	assertCLIErrorCode(t, err, contract.ErrorPrincipalForbidden)
	if commandExitCodeForArgs(err, []string{"whoami"}) != exitUnauthorized {
		t.Fatalf("machine whoami exit = %d, want %d", commandExitCodeForArgs(err, []string{"whoami"}), exitUnauthorized)
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
	err = execute(ctx, viewerClients, true, []string{"services", "takeover", "view", computer.ComputerID, "--session-token-file", filepath.Join(t.TempDir(), "viewer")}, &stdout, &stderr)
	assertCLIErrorCode(t, err, contract.ErrorForbidden)
	stdout.Reset()
	err = execute(ctx, machineClients, true, []string{"services", "takeover", "view", computer.ComputerID, "--session-token-file", filepath.Join(t.TempDir(), "machine")}, &stdout, &stderr)
	assertCLIErrorCode(t, err, contract.ErrorPrincipalForbidden)
	grantJSON := runAccessCLI(t, ctx, adminClients, true, "services", "grant", computer.ComputerID,
		viewerIdentity.UserID, "--permission", "control", "--policy-revision", "3", "--idempotency-key", "grant-viewer")
	var grant l1.ComputerGrantMutationResult
	if err := json.Unmarshal(grantJSON, &grant); err != nil || grant.Grant.Permission != l1.ComputerGrantControl || grant.Replayed || !grant.MutationApplied {
		t.Fatalf("grant result = %#v err=%v", grant, err)
	}
	replayedJSON := runAccessCLI(t, ctx, adminClients, true, "services", "grant", computer.ComputerID,
		viewerIdentity.UserID, "--permission", "control", "--policy-revision", "3", "--idempotency-key", "grant-viewer")
	if err := json.Unmarshal(replayedJSON, &grant); err != nil || !grant.Replayed || grant.MutationApplied {
		t.Fatalf("replayed grant = %#v err=%v", grant, err)
	}
	grantsHuman := string(runAccessCLI(t, ctx, adminClients, false, "services", "grants", computer.ComputerID))
	if !strings.Contains(grantsHuman, "POLICY REVISION\t4") || !strings.Contains(grantsHuman, "person-viewer\tcontrol") {
		t.Fatalf("human grant list omitted revision or permission:\n%s", grantsHuman)
	}
	stdout.Reset()
	err = execute(ctx, adminClients, true, []string{"services", "revoke", computer.ComputerID, viewerIdentity.UserID,
		"--policy-revision", "3", "--idempotency-key", "stale-revoke"}, &stdout, &stderr)
	assertCLIErrorCode(t, err, contract.ErrorStalePolicyRevision)
	adminClients.wait = func(context.Context, time.Duration) error { return context.Canceled }
	stdout.Reset()
	err = execute(ctx, adminClients, true, []string{"services", "revoke", computer.ComputerID, viewerIdentity.UserID,
		"--policy-revision", "4", "--idempotency-key", "revoke-viewer", "--wait"}, &stdout, &stderr)
	var observationErr *apiResponseError
	if !errors.As(err, &observationErr) || observationErr.APIError.Code != contract.ErrorRevocationObservationFailed ||
		observationErr.APIError.Details["mutation_applied"] != true || observationErr.APIError.Details["last_observed_revocation"] == nil {
		t.Fatalf("injected revocation wait cancellation = %v", err)
	}
	revokeJSON := runAccessCLI(t, ctx, adminClients, true, "services", "revoke", computer.ComputerID,
		viewerIdentity.UserID, "--policy-revision", "4", "--idempotency-key", "revoke-viewer")
	if err := json.Unmarshal(revokeJSON, &grant); err != nil || grant.Grant.Permission != l1.ComputerGrantNone ||
		grant.Revocation == nil || grant.Revocation.State != l1.ComputerPolicyRevocationPending || !grant.Replayed || grant.MutationApplied {
		t.Fatalf("revoke replay result = %#v err=%v", grant, err)
	}
	adminClients.wait = func(context.Context, time.Duration) error { return nil }
	stdout.Reset()
	err = execute(ctx, adminClients, true, []string{"services", "revoke", computer.ComputerID, viewerIdentity.UserID,
		"--policy-revision", "4", "--idempotency-key", "revoke-viewer", "--wait",
		"--poll-interval", "1ns", "--wait-timeout", "1ns"}, &stdout, &stderr)
	if !errors.As(err, &observationErr) || observationErr.APIError.Code != contract.ErrorRevocationWaitTimeout ||
		observationErr.APIError.Details["mutation_applied"] != false {
		t.Fatalf("bounded revocation timeout = %v", err)
	}
	stdout.Reset()
	err = execute(ctx, viewerClients, true, []string{"services", "takeover", "view", computer.ComputerID, "--session-token-file", filepath.Join(t.TempDir(), "revoked")}, &stdout, &stderr)
	assertCLIErrorCode(t, err, contract.ErrorForbidden)
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
		revokeJSON, sessionsJSON, []byte(auditHuman)}, []byte("\n"))
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

func TestAccessCLIErrorProjectionAndScopedExitCodes(t *testing.T) {
	usage := usageError("bad access flags")
	var usageJSON bytes.Buffer
	writeCommandError(&usageJSON, usage, true)
	var envelope contract.ErrorResponse
	if err := json.Unmarshal(usageJSON.Bytes(), &envelope); err != nil || envelope.Error.Code != contract.ErrorInvalidRequest {
		t.Fatalf("usage JSON = %s err=%v", usageJSON.String(), err)
	}
	if got := commandExitCodeForArgs(usage, []string{"services", "takeover", "view"}); got != exitUsage {
		t.Fatalf("access usage exit = %d", got)
	}
	if got := commandExitCodeForArgs(usage, []string{"services", "status"}); got != exitFailure {
		t.Fatalf("pre-existing command exit = %d, want historical %d", got, exitFailure)
	}
	for _, code := range []contract.ErrorCode{contract.ErrorPersonIdentityRequired, contract.ErrorPrincipalForbidden} {
		err := &apiResponseError{APIError: contract.APIError{Code: code}}
		if got := commandExitCodeForArgs(err, []string{"--json", "whoami"}); got != exitUnauthorized {
			t.Fatalf("whoami %s exit = %d, want %d", code, got, exitUnauthorized)
		}
	}
	receipt := contract.ComputerControlReceipt{ComputerID: "computer-1", Action: "take",
		AdmittedMode: string(l1.ComputerAdmittedView), TenureState: contract.ComputerControlTenureFree,
		PolicyRevision: 9, HumanDriving: false, SignalStayedTrue: false,
		SessionEndReason: string(l1.ComputerTakeoverRevoked)}
	actionErr := &takeoverActionError{APIError: contract.APIError{Code: contract.ErrorTenureUnavailable,
		Message: "replacement failed", Retryable: false}, Receipt: &receipt}
	var failureJSON bytes.Buffer
	writeCommandError(&failureJSON, actionErr, true)
	var failure contract.ComputerControlErrorResponse
	if err := json.Unmarshal(failureJSON.Bytes(), &failure); err != nil || failure.Receipt == nil ||
		failure.Receipt.TenureState != contract.ComputerControlTenureFree || failure.Receipt.HumanDriving ||
		failure.Receipt.SessionEndReason != string(l1.ComputerTakeoverRevoked) {
		t.Fatalf("failed replacement JSON = %s err=%v", failureJSON.String(), err)
	}
	var failureText bytes.Buffer
	writeCommandError(&failureText, actionErr, false)
	if !strings.Contains(failureText.String(), "SESSION END REASON\trevoked") {
		t.Fatalf("failed replacement text = %q", failureText.String())
	}
}

func TestAccessArgumentReorderingDoesNotConsumeBooleanFollower(t *testing.T) {
	got := moveFirstPositionalsToEnd([]string{"--wait", "computer-1", "person-1", "--wait-timeout", "3s"}, 2)
	want := []string{"--wait", "--wait-timeout", "3s", "computer-1", "person-1"}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("reordered args = %q, want %q", got, want)
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
