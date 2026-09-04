package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
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
	"github.com/coder/websocket"
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
		ConnectHost:      "fabric-address.example.test:8443",
		DisplayEndpoint:  "fabric-address.example.test:8443",
		ComputerID:       "computer-1",
		Action:           "view",
		SessionTokenFile: "/private/session.json",
	}

	t.Run("exact human labels", func(t *testing.T) {
		var human bytes.Buffer
		if err := writeComputerTakeoverView(&human, result, false); err != nil {
			t.Fatal(err)
		}
		wantHuman := "FRIENDLY NAME\talice\n" +
			"CONNECT HOST\tfabric-address.example.test:8443\n" +
			"DISPLAY ENDPOINT\tfabric-address.example.test:8443\n" +
			"COMPUTER ID\tcomputer-1\n" +
			"ACTION\tview\n" +
			"SESSION TOKEN FILE\t/private/session.json\n"
		if human.String() != wantHuman {
			t.Fatalf("take-over view table = %q, want exact compatibility labels %q", human.String(), wantHuman)
		}
	})

	t.Run("exact JSON keys", func(t *testing.T) {
		var encoded bytes.Buffer
		if err := writeComputerTakeoverView(&encoded, result, true); err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(encoded.Bytes(), &fields); err != nil {
			t.Fatal(err)
		}
		wantFields := map[string]bool{
			"friendly_name": true, "connect_host": true, "display_endpoint": true,
			"computer_id": true, "action": true, "session_token_file": true,
		}
		if len(fields) != len(wantFields) {
			t.Fatalf("take-over view JSON keys = %v, want exactly %v", fields, wantFields)
		}
		for field := range fields {
			if !wantFields[field] {
				t.Fatalf("take-over view JSON includes unexpected key %q: %s", field, encoded.String())
			}
		}
		if string(fields["connect_host"]) != `"fabric-address.example.test:8443"` ||
			string(fields["display_endpoint"]) != string(fields["connect_host"]) {
			t.Fatalf("take-over view JSON = %s, want deprecated display_endpoint alias equal to dialable connect_host", encoded.String())
		}
	})
}

func TestComputerTakeoverViewProjectsAvailabilityAndSessionAddress(t *testing.T) {
	const (
		computerID      = "computer-1"
		friendlyName    = "alice"
		localCLIHost    = "local-cli-host.example.test"
		sessionToken    = "secret-session-token"
		sessionFileName = "live-session.json"
	)
	frontDoor := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set(contract.ComputerControlTokenHeader, sessionToken)
		connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{
			Subprotocols: []string{contract.ComputerDisplayWebSocketSubprotocol},
		})
		if err != nil {
			return
		}
		defer connection.CloseNow()
		_ = connection.Write(request.Context(), websocket.MessageBinary, []byte("RFB 003.008\n"))
		<-request.Context().Done()
	}))
	defer frontDoor.Close()
	endpoint := "ws" + strings.TrimPrefix(frontDoor.URL, "http") + contract.ComputerDisplayWebSocketPath
	frontDoorHost := strings.TrimPrefix(frontDoor.URL, "http://")

	l1Transport := forwardingRoundTripper(func(request *http.Request) (*http.Response, error) {
		var payload any
		switch request.URL.Path {
		case "/v1/computers/" + computerID:
			payload = l1.Computer{ComputerID: computerID, Name: friendlyName}
		case "/v1/computers/" + computerID + "/takeover":
			payload = l1.ComputerTakeoverAvailability{ComputerID: computerID, FriendlyName: friendlyName, DisplayEndpoint: &endpoint}
		default:
			return nil, fmt.Errorf("unexpected L1 request path %q", request.URL.Path)
		}
		body, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
	})
	clients := &apiClients{
		l1:     &apiClient{name: "L1", client: &http.Client{Transport: l1Transport}},
		fabric: directDialFabric{connectHost: localCLIHost},
		wait:   waitForContext,
	}
	viewContext, cancelView := context.WithCancel(t.Context())
	defer cancelView()
	tokenFile := filepath.Join(t.TempDir(), sessionFileName)
	var stdout, stderr bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- executeComputerTakeoverAction(viewContext, clients, true, "view",
			[]string{computerID, "--session-token-file", tokenFile}, &stdout, &stderr)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if info, err := os.Stat(tokenFile); err == nil && info.Size() > 0 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("view ended before projection: %v stderr=%s", err, stderr.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for view projection: stderr=%s", stderr.String())
		}
		time.Sleep(time.Millisecond)
	}
	cancelView()
	if err := <-done; err != nil {
		t.Fatalf("view command: %v stderr=%s", err, stderr.String())
	}
	var projected computerTakeoverViewResult
	if err := json.Unmarshal(stdout.Bytes(), &projected); err != nil {
		t.Fatalf("decode projection: %v output=%s", err, stdout.String())
	}
	if projected.FriendlyName != friendlyName || projected.ConnectHost != frontDoorHost ||
		projected.DisplayEndpoint != frontDoorHost || projected.ConnectHost == localCLIHost {
		t.Fatalf("take-over projection = %#v, want friendly name %q and front door host %q, not local CLI host %q",
			projected, friendlyName, frontDoorHost, localCLIHost)
	}
}

type directDialFabric struct {
	connectHost string
}

func (f directDialFabric) Listen(network, address string) (net.Listener, error) {
	return net.Listen(network, address)
}

func (f directDialFabric) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, address)
}

func (f directDialFabric) WhoIs(context.Context, string) (fabric.Identity, error) {
	return fabric.Identity{}, nil
}

func (f directDialFabric) ConnectHost() string { return f.connectHost }

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
	resolvedByName, err := resolveComputerID(ctx, adminClients, computer.Name)
	if err != nil || resolvedByName != computer.ComputerID {
		t.Fatalf("person-authorized friendly-name resolution = %q err=%v, want %q", resolvedByName, err, computer.ComputerID)
	}
	resolution, err := adminClients.resolvePersonComputerHandle(ctx, computer.Name, false)
	if err != nil || resolution.ComputerID != computer.ComputerID || resolution.MatchedBy != "friendly_name" {
		t.Fatalf("person handle resolution = %#v err=%v", resolution, err)
	}
	collision, _, err := store.CreateComputer(ctx, l1.CreateComputerRequest{
		Name: computer.ComputerID, Spec: accessCLIComputerSpec("id-shaped-friendly-name"), Actor: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	resolution, err = adminClients.resolvePersonComputerHandle(ctx, computer.ComputerID, false)
	if err != nil || resolution.ComputerID != computer.ComputerID || resolution.ComputerID == collision.ComputerID ||
		resolution.MatchedBy != "computer_id" {
		t.Fatalf("exact person Computer ID precedence = %#v err=%v; colliding name belongs to %q",
			resolution, err, collision.ComputerID)
	}
	if _, err := resolveComputerID(ctx, viewerClients, computer.Name); err == nil {
		t.Fatal("ungranted person resolved a Computer friendly name")
	} else {
		assertCLIErrorCode(t, err, contract.ErrorForbidden)
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
	removed, err := store.RemoveComputer(ctx, computer.ComputerID, l1.ComputerRemoveRequest{
		ComputerMutationPrecondition: l1.ComputerMutationPrecondition{
			IntentRevision: computer.IntentRevision, StorageID: computer.StorageID,
			StorageGeneration: computer.StorageGeneration, Actor: "test",
		},
	})
	if err != nil || removed.DesiredState != contract.ServiceDesiredRemoved {
		t.Fatalf("remove Computer before durable take-over reads = %#v err=%v", removed, err)
	}
	removedSessionsJSON := runAccessCLI(t, ctx, adminClients, true,
		"services", "takeover", "sessions", "list", computer.CurrentJobID)
	var removedSessions l1.ComputerTakeoverSessionList
	if err := json.Unmarshal(removedSessionsJSON, &removedSessions); err != nil || removedSessions.Sessions == nil {
		t.Fatalf("removed Computer sessions by current Job ID = %#v err=%v", removedSessions, err)
	}
	removedAuditJSON := runAccessCLI(t, ctx, adminClients, true,
		"services", "takeover", "audit", "tail", computer.ComputerID, "--limit", "1")
	var removedAudit l1.ComputerTakeoverAuditList
	if err := json.Unmarshal(removedAuditJSON, &removedAudit); err != nil || removedAudit.Events == nil {
		t.Fatalf("removed Computer audit by Computer ID = %#v err=%v", removedAudit, err)
	}
	allAccessOutput := bytes.Join([][]byte{bootstrap, []byte(human), grantJSON, replayedJSON, []byte(grantsHuman),
		revokeJSON, sessionsJSON, []byte(auditHuman), removedSessionsJSON, removedAuditJSON}, []byte("\n"))
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
