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
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/local"
	"github.com/Derek-X-Wang/wefty/fabric/participantstate"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

func healthyDoctorFixture(t *testing.T, role serviceplan.Role) *setupFixture {
	t.Helper()
	var f *setupFixture
	if role == serviceplan.Join {
		f, _, _ = newJoinFixture(t)
	} else {
		f, _, _ = newFleetFixture(t)
	}
	tools := filepath.Join(f.home, "tools")
	if err := os.MkdirAll(tools, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"go", "git", "node", "gh", "claude", "codex"} {
		if err := os.WriteFile(filepath.Join(tools, name), []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	f.loginPATH = tools
	f.host.doctorCommand = func(_ context.Context, name string, args ...string) ([]byte, error) {
		switch filepath.Base(name) {
		case "pmset":
			return []byte("AC Power:\n sleep 0\n displaysleep 10\n"), nil
		case "fdesetup":
			return []byte("FileVault is Off.\n"), nil
		default:
			t.Fatalf("unexpected host probe: %s %v", name, args)
			return nil, nil
		}
	}
	request := setupRequest{role: string(role), nodeID: "studio-0123abcd", skipSmokeRun: true}
	if role == serviceplan.Join {
		request.joinCode = joinCodeForTest(t, "opaque-l1", "opaque-l3", "")
	}
	f.setup(t, request)
	return f
}

func TestFleetDoctorHealthyRoles(t *testing.T) {
	for _, role := range []serviceplan.Role{serviceplan.ControlPlane, serviceplan.Join} {
		t.Run(string(role), func(t *testing.T) {
			f := healthyDoctorFixture(t, role)
			paths, _ := resolveSetupPaths(f.home)
			// Holding all participant locks proves doctor never opens their backends.
			p := f.profile(t)
			release := holdDoctorParticipantLocks(t, p)
			defer release()
			report, err := runFleetDoctor(t.Context(), f.host, globalOptions{}, paths, role)
			if err != nil || !report.Healthy || report.Role != role {
				t.Fatalf("healthy role: %+v %v", report, err)
			}
			seen := map[string]bool{}
			for _, finding := range report.Findings {
				seen[finding.Check] = true
				if finding.Severity != "ok" {
					t.Errorf("healthy finding: %+v", finding)
				}
			}
			for _, check := range []string{"connection-profile", "node-config", "node-agent-PATH", "local-node", "client → L1", "client → L3", "participant:client", "participant:node-agent"} {
				if !seen[check] {
					t.Errorf("missing check %s", check)
				}
			}
			if role == serviceplan.Join && (seen[setupL1Label] || seen[setupL3Label] || seen["system-sleep"] || seen["FileVault"]) {
				t.Fatal("joining doctor inspected control-plane-only checks")
			}
			ownerL1 := false
			for _, e := range report.PolicyEvidence {
				if e.Check == "owner → L1 (person commands)" {
					ownerL1 = e.Status == "NOT-RUN" && strings.Contains(e.Detail, "wefty whoami")
				}
			}
			if !ownerL1 {
				t.Error("owner → L1 attended evidence is missing or overclaimed")
			}
			for _, e := range report.PolicyEvidence {
				if strings.Contains(e.Check, "published") && e.Status != "NOT-RUN" {
					t.Fatalf("deny evidence overclaimed: %+v", e)
				}
			}
		})
	}
}

func holdDoctorParticipantLocks(t *testing.T, p connprofile.Profile) func() {
	t.Helper()
	// Filled in through the existing ownership seam, not a diagnostic fake.
	var releases []func() error
	for _, part := range p.Participants {
		release, err := participantstate.Acquire(part.StateDirectory, part.Name)
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	return func() {
		for _, release := range releases {
			_ = release()
		}
	}
}

func TestFleetDoctorFailuresKeepIndependentFileChecks(t *testing.T) {
	cases := []struct {
		name, check, detail string
		exit                int
		damage              func(*testing.T, *setupFixture)
	}{
		{"service down", setupNodeAgentLabel, "not loaded", 12, func(t *testing.T, f *setupFixture) { delete(f.launchd.loaded, setupNodeAgentLabel) }},
		{"all services down", "connection-profile", "complete valid", 13, func(t *testing.T, f *setupFixture) {
			f.launchd.loaded = map[string]bool{}
			f.host.fleetClients = func(context.Context, setupPaths, connprofile.Profile) (*apiClients, error) {
				return nil, &local.UnavailableError{}
			}
		}},
		{"profile invalid", "connection-profile", "invalid", 12, func(t *testing.T, f *setupFixture) {
			mustWrite(t, filepath.Join(f.home, setupFolders.config, "profile.json"), "{")
		}},
		{"folder mode", "path:" + setupFolders.logs, "wrong mode", 12, func(t *testing.T, f *setupFixture) {
			if err := os.Chmod(filepath.Join(f.home, setupFolders.logs), 0755); err != nil {
				t.Fatal(err)
			}
		}},
		{"client service down", "client-service", "NOT-RUN", 13, func(t *testing.T, f *setupFixture) {
			f.host.fleetClients = func(context.Context, setupPaths, connprofile.Profile) (*apiClients, error) {
				return nil, &local.UnavailableError{}
			}
		}},
		{"identity refused", "client-service", "REFUSED", 3, func(t *testing.T, f *setupFixture) {
			f.host.fleetClients = func(context.Context, setupPaths, connprofile.Profile) (*apiClients, error) {
				return nil, &fabric.ParticipantIdentityError{Reason: "does not match the recorded node ID"}
			}
		}},
		{"fenced caller", "client-service", "caller network or socket restrictions", 3, func(t *testing.T, f *setupFixture) {
			f.host.fleetClients = func(context.Context, setupPaths, connprofile.Profile) (*apiClients, error) {
				return nil, &local.FencedCallerError{}
			}
		}},
		{"L1 unreachable", "client → L1", "NOT-RUN", 13, func(t *testing.T, f *setupFixture) { damageDoctorAPI(f, "/v1/nodes", 0, nil) }},
		{"L1 identity refused", "client → L1", "REFUSED", 3, func(t *testing.T, f *setupFixture) { damageDoctorAPI(f, "/v1/nodes", 403, nil) }},
		{"identity and transport", "client → L1", "REFUSED", 3, func(t *testing.T, f *setupFixture) {
			damageDoctorAPI(f, "/v1/nodes", 403, nil)
			damageDoctorAPI(f, "/v1/runs", 0, nil)
		}},
		{"L3 unreachable", "client → L3", "NOT-RUN", 13, func(t *testing.T, f *setupFixture) { damageDoctorAPI(f, "/v1/runs", 0, nil) }},
		{"node not admitted", "local-node", "not alive and admitted", 12, func(t *testing.T, f *setupFixture) {
			damageDoctorAPI(f, "/v1/nodes", 200, func(p connprofile.Profile) any {
				return l1.NodeList{Nodes: []l1.Node{{NodeRegistration: contract.NodeRegistration{NodeID: p.Identities.Node}, State: contract.NodeAlive, ClaimsEnabled: false}}}
			})
		}},
		{"saved state missing", "participant:client", "missing", 3, func(t *testing.T, f *setupFixture) {
			if err := os.RemoveAll(f.profile(t).Participants["client"].StateDirectory); err != nil {
				t.Fatal(err)
			}
		}},
		{"recorded PATH missing", "node-agent-PATH", "cannot read", 12, func(t *testing.T, f *setupFixture) {
			mustRemove(t, filepath.Join(f.home, setupFolders.launchAgents, setupNodeAgentLabel+".plist"))
		}},
		{"stale skill", "skill:.agents/skills/wefty/SKILL.md", "stale", 12, func(t *testing.T, f *setupFixture) {
			mustWrite(t, filepath.Join(f.home, ".agents/skills/wefty/SKILL.md"), "stale")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := healthyDoctorFixture(t, serviceplan.ControlPlane)
			tc.damage(t, f)
			paths, _ := resolveSetupPaths(f.home)
			before := doctorFileMetadata(t, f.home)
			report, err := runFleetDoctor(t.Context(), f.host, globalOptions{}, paths, serviceplan.ControlPlane)
			if code := classifyCommandReport(err).exitCode; code != tc.exit {
				t.Fatalf("exit=%d want=%d err=%v report=%+v", code, tc.exit, err, report)
			}
			found := false
			for _, finding := range report.Findings {
				if finding.Check == tc.check && strings.Contains(finding.Detail, tc.detail) {
					found = true
				}
				if finding.Severity != "ok" && finding.Fix == "" {
					t.Errorf("finding without advice: %+v", finding)
				}
			}
			if !found {
				t.Fatalf("missing failure %s %s: %+v", tc.check, tc.detail, report)
			}
			for _, check := range []string{"connection-profile", "node-config", "node-agent-PATH", "skill:.agents/skills/wefty/SKILL.md"} {
				if !hasDoctorFinding(report, check) {
					t.Errorf("independent file check skipped: %s", check)
				}
			}
			if !reflect.DeepEqual(before, doctorFileMetadata(t, f.home)) {
				t.Fatal("read-only doctor changed local files")
			}
			var out bytes.Buffer
			if err := writeDoctorReport(&out, report, true); err != nil {
				t.Fatal(err)
			}
			classification := classifyCommandReport(&doctorResultError{cause: err})
			if classification.exitCode != tc.exit || !classification.stdoutVerdict {
				t.Fatalf("report replaced typed exit: %+v", classification)
			}
			var diagnostics bytes.Buffer
			classification.writeError(&diagnostics, true)
			if diagnostics.Len() != 0 {
				t.Fatalf("second JSON error: %s", &diagnostics)
			}
		})
	}
}

func damageDoctorAPI(f *setupFixture, path string, status int, body func(connprofile.Profile) any) {
	original := f.host.fleetClients
	f.host.fleetClients = func(ctx context.Context, paths setupPaths, p connprofile.Profile) (*apiClients, error) {
		clients, err := original(ctx, paths, p)
		if err != nil {
			return nil, err
		}
		for _, client := range []*apiClient{clients.l1, clients.l3} {
			previous := client.client.Transport
			client.client.Transport = fleetRoundTripper(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != path {
					return previous.RoundTrip(r)
				}
				if status == 0 {
					return nil, errors.New("fixture transport cannot answer")
				}
				var payload any = contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorForbidden, Message: "fixture identity refused"}}
				if body != nil {
					payload = body(p)
				}
				b, _ := json.Marshal(payload)
				return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(b))}, nil
			})
		}
		return clients, nil
	}
}

func doctorFileMetadata(t *testing.T, root string) map[string]string {
	t.Helper()
	metadata := map[string]string{}
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		metadata[path] = fmt.Sprintf("%s %d %d", info.Mode(), info.Size(), info.ModTime().UnixNano())
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return metadata
}

func TestFleetDoctorMacHostProbes(t *testing.T) {
	for _, tc := range []struct {
		name, power, vault           string
		commandErr                   error
		sleepSeverity, vaultSeverity string
	}{
		{"sleep on", "AC Power:\n sleep 10\n", "FileVault is Off.", nil, "warning", "ok"},
		{"FileVault on", "AC Power:\n sleep 0\n", "FileVault is On.", nil, "ok", "warning"},
		{"unknown", "something new", "some new output", nil, "warning", "warning"},
		{"failure", "AC Power:\n sleep 0\n", "FileVault is Off.", errors.New("secret backend diagnostic must not appear"), "warning", "warning"},
		{"mixed profiles", "Battery Power:\n sleep 1\nAC Power:\n sleep 0\n", "FileVault is Off.", nil, "warning", "ok"},
		{"missing profile timer", "Battery Power:\nAC Power:\n sleep 0\n", "FileVault is Off.", nil, "warning", "ok"},
		{"partially unknown", "Battery Power:\n sleep 0\nAC Power:\n sleep unknown\n", "FileVault is Off.", nil, "warning", "ok"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := healthyDoctorFixture(t, serviceplan.ControlPlane)
			var commands []string
			f.host.doctorCommand = func(_ context.Context, name string, args ...string) ([]byte, error) {
				commands = append(commands, name+" "+strings.Join(args, " "))
				if filepath.Base(name) == "pmset" {
					return []byte(tc.power), tc.commandErr
				}
				return []byte(tc.vault), tc.commandErr
			}
			paths, _ := resolveSetupPaths(f.home)
			report, err := runFleetDoctor(t.Context(), f.host, globalOptions{}, paths, serviceplan.ControlPlane)
			if err != nil || !report.Healthy {
				t.Errorf("host advice changed doctor verdict: healthy=%v err=%v", report.Healthy, err)
			}
			if !reflect.DeepEqual(commands, []string{"/usr/bin/pmset -g custom", "/usr/bin/fdesetup status"}) {
				t.Fatalf("host mutations/probes: %v", commands)
			}
			for _, finding := range report.Findings {
				want := ""
				if finding.Check == "system-sleep" {
					want = tc.sleepSeverity
				}
				if finding.Check == "FileVault" {
					want = tc.vaultSeverity
				}
				if want == "warning" && finding.Fix == "" {
					t.Errorf("advisory warning lost its advice: %+v", finding)
				}
				if want != "" && finding.Severity != want {
					t.Errorf("host result: %+v", finding)
				}
				if (tc.name == "unknown" || tc.name == "failure" || tc.name == "partially unknown" && finding.Check == "system-sleep") && want != "" && !strings.Contains(finding.Detail, "unverified") {
					t.Errorf("unknown became a verified result: %+v", finding)
				}
				if strings.Contains(finding.Detail, "secret") {
					t.Fatal("probe output leaked")
				}
			}
		})
	}
	t.Run("join skips probes", func(t *testing.T) {
		f := healthyDoctorFixture(t, serviceplan.Join)
		f.host.doctorCommand = func(context.Context, string, ...string) ([]byte, error) {
			t.Fatal("joining machine ran a Mac host probe")
			return nil, nil
		}
		paths, _ := resolveSetupPaths(f.home)
		if _, err := runFleetDoctor(t.Context(), f.host, globalOptions{}, paths, serviceplan.Join); err != nil {
			t.Fatal(err)
		}
	})
}

func TestFleetDoctorCancellation(t *testing.T) {
	f := healthyDoctorFixture(t, serviceplan.ControlPlane)
	f.host.fleetClients = func(ctx context.Context, _ setupPaths, _ connprofile.Profile) (*apiClients, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	paths, _ := resolveSetupPaths(f.home)
	started := time.Now()
	_, err := runFleetDoctor(ctx, f.host, globalOptions{}, paths, serviceplan.ControlPlane)
	if !errors.Is(err, context.Canceled) || time.Since(started) > time.Second {
		t.Fatalf("cancellation=%v after %s", err, time.Since(started))
	}
}

func TestFleetDoctorUsesRunningClientServiceWithoutBackendOpen(t *testing.T) {
	f := healthyDoctorFixture(t, serviceplan.Join)
	p := f.profile(t)
	release := holdDoctorParticipantLocks(t, p)
	defer release()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("doctor mutated remote service: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(405)
			return
		}
		var payload any
		switch r.URL.Path {
		case "/v1/nodes":
			payload = l1.NodeList{Nodes: []l1.Node{{NodeRegistration: contract.NodeRegistration{NodeID: p.Identities.Node}, State: contract.NodeAlive, ClaimsEnabled: true, AutomaticallyAdmitted: true, AuthoritativeTags: []string{contract.StableNodeTagPrefix + p.Identities.Node}, MaxOneshotSlots: 4, MaxServiceSlots: 2}}}
		case "/v1/runs":
			payload = map[string]any{"runs": []any{}}
		default:
			t.Errorf("unexpected diagnostic read: %s", r.URL.Path)
			w.WriteHeader(404)
			return
		}
		_ = json.NewEncoder(w).Encode(payload)
	}))
	defer server.Close()
	var checks, l1Dials, l3Dials atomic.Int32
	backend := upgradeFixtureFabric{dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		switch address {
		case p.L1Address:
			l1Dials.Add(1)
		case p.L3Address:
			l3Dials.Add(1)
		default:
			return nil, fmt.Errorf("unexpected peer: %s", address)
		}
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}}
	socket := local.DefaultSocketPath(f.home)
	service, err := local.NewServer(socket, map[string]local.Participant{local.Client: {NodeID: p.Participants[connprofile.ParticipantClient].NodeID, Fabric: backend, Check: func(context.Context) error { checks.Add(1); return nil }}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	ready := make(chan struct{})
	go func() { done <- service.ServeReady(ctx, func() error { close(ready); return nil }) }()
	select {
	case <-ready:
	case err := <-done:
		t.Fatal(err)
	case <-time.After(3 * time.Second):
		t.Fatal("fake client service did not become ready")
	}
	f.host.fleetClients = nil
	paths, _ := resolveSetupPaths(f.home)
	report, err := runFleetDoctor(t.Context(), f.host, globalOptions{}, paths, serviceplan.Join)
	cancel()
	serveErr := <-done
	if err != nil || serveErr != nil || !report.Healthy {
		t.Fatalf("client-service path: %+v err=%v serve=%v", report, err, serveErr)
	}
	if checks.Load() < 3 || l1Dials.Load() != 1 || l3Dials.Load() != 1 {
		t.Fatalf("client-service evidence checks=%d L1=%d L3=%d", checks.Load(), l1Dials.Load(), l3Dials.Load())
	}
	// No direct-state fallback after stopping the service, with all locks held.
	report, err = runFleetDoctor(t.Context(), f.host, globalOptions{}, paths, serviceplan.Join)
	if code := classifyCommandReport(err).exitCode; code != 13 || report.Healthy {
		t.Fatalf("missing service fell back: code=%d err=%v report=%+v", code, err, report)
	}
}

func TestFleetDoctorJoinAdmissionFacts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		damage func(*l1.Node)
	}{
		{"automatic mark missing", func(n *l1.Node) { n.AutomaticallyAdmitted = false }},
		{"claims disabled", func(n *l1.Node) { n.ClaimsEnabled = false }},
		{"Pinned tag missing", func(n *l1.Node) { n.AuthoritativeTags = nil }},
		{"extra tag", func(n *l1.Node) { n.AuthoritativeTags = append(n.AuthoritativeTags, "unexpected") }},
		{"wrong one-shot slots", func(n *l1.Node) { n.MaxOneshotSlots++ }},
		{"wrong service slots", func(n *l1.Node) { n.MaxServiceSlots++ }},
		{"dead", func(n *l1.Node) { n.State = contract.NodeDead }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := healthyDoctorFixture(t, serviceplan.Join)
			damageDoctorAPI(f, "/v1/nodes", 200, func(p connprofile.Profile) any {
				node := l1.Node{NodeRegistration: contract.NodeRegistration{NodeID: p.Identities.Node}, State: contract.NodeAlive, ClaimsEnabled: true, AutomaticallyAdmitted: true, AuthoritativeTags: []string{contract.StableNodeTagPrefix + p.Identities.Node}, MaxOneshotSlots: 4, MaxServiceSlots: 2}
				tc.damage(&node)
				return l1.NodeList{Nodes: []l1.Node{node}}
			})
			paths, _ := resolveSetupPaths(f.home)
			report, err := runFleetDoctor(t.Context(), f.host, globalOptions{}, paths, serviceplan.Join)
			if classifyCommandReport(err).exitCode != 12 || report.Healthy {
				t.Fatalf("join admission accepted %s: %+v %v", tc.name, report, err)
			}
			for _, finding := range report.Findings {
				if finding.Check == "local-node" && (finding.Severity != "error" || !strings.Contains(finding.Detail, "automatically admitted")) {
					t.Fatalf("join admission evidence: %+v", finding)
				}
			}
		})
	}
}

func TestFleetDoctorProbeDeadlineIsUncertainty(t *testing.T) {
	f := healthyDoctorFixture(t, serviceplan.ControlPlane)
	f.host.fleetClients = func(ctx context.Context, _ setupPaths, _ connprofile.Profile) (*apiClients, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	defer cancel()
	paths, _ := resolveSetupPaths(f.home)
	_, err := runFleetDoctor(ctx, f.host, globalOptions{}, paths, serviceplan.ControlPlane)
	if code := classifyCommandReport(err).exitCode; code != 13 {
		t.Fatalf("probe deadline exit=%d err=%v", code, err)
	}
}

func TestFleetDoctorPreservesOCIWithoutHome(t *testing.T) {
	root := newFleetTestHome(t)
	config := filepath.Join(root, "node.json")
	payload, err := ocicontrol.EncodeInstalledConfig(ocicontrol.InstalledConfig{Version: ocicontrol.InstalledConfigVersion, NodeID: "oci-node", ControlSocket: filepath.Join(root, "absent.sock")})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config, payload, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", "")
	var out, diagnostics bytes.Buffer
	err = run(t.Context(), []string{"--node-config=" + config, "--json", "node", "doctor"}, &out, &diagnostics)
	if code := classifyCommandReport(err).exitCode; code != 13 {
		t.Fatalf("OCI doctor acquired a HOME dependency: exit=%d err=%v", code, err)
	}
}

func TestFleetDoctorUnreadableRoleReceiptReportsFinding(t *testing.T) {
	f := healthyDoctorFixture(t, serviceplan.ControlPlane)
	paths, _ := resolveSetupPaths(f.home)
	if err := os.Chmod(filepath.Join(paths.config, setupManifestName), 0000); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	handled, err := maybeExecuteFleetDoctor(t.Context(), f.host, globalOptions{jsonOutput: true}, &out)
	var report plainDoctorReport
	if !handled || classifyCommandReport(err).exitCode != 12 || json.Unmarshal(out.Bytes(), &report) != nil || report.Healthy {
		t.Fatalf("role receipt: handled=%v err=%v report=%s", handled, err, &out)
	}
	if !hasDoctorFinding(report, "role-receipt") {
		t.Fatalf("missing role-receipt finding: %s", &out)
	}
	for _, finding := range report.Findings {
		if finding.Check == "role-receipt" && (finding.Severity != "error" || finding.Fix == "") {
			t.Fatalf("receipt finding lacks advice: %+v", finding)
		}
	}
}

func TestFleetDoctorLinuxKeepsOriginalRouteRealBinary(t *testing.T) {
	f := healthyDoctorFixture(t, serviceplan.ControlPlane)
	paths, _ := resolveSetupPaths(f.home)
	if err := os.Chmod(filepath.Join(paths.config, setupManifestName), 0000); err != nil {
		t.Fatal(err)
	}
	binary := buildPlainInstallCLIForPlatform(t, "linux")
	code, out := runWeftyWithEnv(t, binary, 10*time.Second, []string{"HOME=" + f.home, "WEFTY_NODE_CONFIG="}, "node", "doctor")
	if code != 1 || !strings.Contains(out, "is a plain-process node with no OCI control socket") {
		t.Fatalf("Linux doctor changed its original route: exit=%d output=%s", code, out)
	}
	code, out = runWeftyWithEnv(t, binary, 10*time.Second, []string{"HOME=" + f.home}, "--node-config=", "node", "doctor")
	const noConfig = "singular node commands require an installed node configuration; set --node-config or WEFTY_NODE_CONFIG"
	if code != 1 || !strings.Contains(out, noConfig) {
		t.Fatalf("Linux doctor without config: exit=%d output=%s", code, out)
	}
}

func TestFleetDoctorHostAdviceKeepsOtherFindings(t *testing.T) {
	f := healthyDoctorFixture(t, serviceplan.ControlPlane)
	f.host.doctorCommand = func(_ context.Context, name string, _ ...string) ([]byte, error) {
		if filepath.Base(name) == "pmset" {
			return []byte("AC Power:\n sleep 0\n"), nil
		}
		return []byte("FileVault is On."), nil
	}
	if err := os.Remove(filepath.Join(f.home, "tools", "git")); err != nil {
		t.Fatal(err)
	}
	paths, _ := resolveSetupPaths(f.home)
	report, err := runFleetDoctor(t.Context(), f.host, globalOptions{}, paths, serviceplan.ControlPlane)
	if report.Healthy || classifyCommandReport(err).exitCode != 12 {
		t.Fatalf("host advice hid another finding: healthy=%v err=%v", report.Healthy, err)
	}
	for _, finding := range report.Findings {
		if finding.Check == "tool:git" && finding.Severity != "error" {
			t.Fatalf("git severity changed: %+v", finding)
		}
	}
}
