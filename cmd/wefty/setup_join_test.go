package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/internal/fleetjoin"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestSetupControlPlanePrintsJoinCode(t *testing.T) {
	for _, coordinator := range []string{"", "https://coordinator.example.test"} {
		t.Run(coordinator, func(t *testing.T) {
			f, _, prompts := newFleetFixture(t)
			report := f.setup(t, setupRequest{role: "control-plane", nodeID: "studio-0123abcd", coordinatorURL: coordinator})
			payload, _ := json.Marshal(report)
			var fields map[string]any
			_ = json.Unmarshal(payload, &fields)
			code, _ := fields["join_code"].(string)
			descriptor, err := fleetjoin.Parse(code)
			if err != nil || descriptor != (fleetjoin.Descriptor{L1: "opaque-l1", L3: "opaque-l3", CoordinatorURL: coordinator}) {
				t.Fatalf("control-plane code: %+v %v", descriptor, err)
			}
			again := f.setup(t, setupRequest{})
			var human bytes.Buffer
			if err := writeSetupReport(&human, again); err != nil {
				t.Fatal(err)
			}
			if again.JoinCode != code || !strings.Contains(human.String(), "Join code (non-secret): "+code) {
				t.Fatal("committed control plane did not reprint its code")
			}
			assertJoinArtifactsPrivate(t, f.home, prompts.String(), report, "secret-l1", "secret-l3", "secret-node", "secret-client")
		})
	}
}

func TestSetupJoinRequiresCodeBeforeKeys(t *testing.T) {
	f, e, prompts := newFleetFixture(t)
	_, err := runSetup(t.Context(), f.host, setupRequest{role: "join", nodeID: "studio-0123abcd"})
	if err == nil || !strings.Contains(err.Error(), "--join-code") {
		t.Fatalf("missing code advice: %v", err)
	}
	if len(e.roles) != 0 || prompts.Len() != 0 {
		t.Fatal("consumed keys before join-code validation")
	}
}

type setupPeerLookup func(context.Context, string) (fabric.Identity, error)

func (f setupPeerLookup) LookupPeer(ctx context.Context, id string) (fabric.Identity, error) {
	return f(ctx, id)
}

func joinCodeForTest(t *testing.T, l1, l3, coordinator string) string {
	t.Helper()
	code, err := fleetjoin.Encode(fleetjoin.Descriptor{L1: l1, L3: l3, CoordinatorURL: coordinator})
	if err != nil {
		t.Fatal(err)
	}
	return code
}

func newJoinFixture(t *testing.T) (*setupFixture, *fakeSetupEnroller, *bytes.Buffer) {
	t.Helper()
	f, e, prompts := newFleetFixture(t)
	f.host.enrolmentInput = strings.NewReader("secret-node\nsecret-client\n")
	f.host.readinessBudget = 10 * time.Millisecond
	f.host.peerLookup = func(ctx context.Context, c fabricconfig.Config) (fabric.PeerLookup, fabricconfig.CloseFunc, error) {
		if c.ExpectedIdentity == nil || c.ExpectedIdentity.NodeID != "opaque-node-agent" || c.AuthKey != "" {
			t.Fatalf("unsafe verifier config: %+v", c)
		}
		return setupPeerLookup(func(_ context.Context, id string) (fabric.Identity, error) {
			switch id {
			case "opaque-l1":
				return fabric.Identity{NodeID: id, Tags: []string{"tag:wefty-control"}}, nil
			case "opaque-l3":
				return fabric.Identity{NodeID: id, Tags: []string{"tag:wefty-client"}}, nil
			}
			return fabric.Identity{}, fabric.ErrIdentityNotFound
		}), func() error { return nil }, nil
	}
	baseRunner := f.host.launchctl
	f.host.launchctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		payload, err := baseRunner(ctx, name, args...)
		if err == nil && args[0] == "bootstrap" && strings.HasSuffix(args[2], "dev.wefty.client-service.plist") {
			err = os.WriteFile(filepath.Join(f.home, setupFolders.data, "setup-ready", "client-service.json"), []byte(`{"ready_at":"2026-10-20T12:00:00Z","address":"fixture-socket","pid":4242,"node_id":"opaque-client"}`), 0600)
		}
		return payload, err
	}
	return f, e, prompts
}

func TestSetupJoinPublishesTwoServicesAndResumes(t *testing.T) {
	f, e, prompts := newJoinFixture(t)
	for _, name := range []string{"wefty-l1", "wefty-l3"} {
		if err := os.Remove(filepath.Join(f.source, name)); err != nil {
			t.Fatal(err)
		}
	}
	code := joinCodeForTest(t, "opaque-l1", "opaque-l3", "")
	report := f.setup(t, setupRequest{role: "join", joinCode: code, nodeID: "studio-0123abcd"})
	if !slices.Equal(e.roles, []string{"node-agent", "client"}) || strings.Count(prompts.String(), "Single-use join key") != 2 {
		t.Fatalf("join order: %v %s", e.roles, prompts)
	}
	p := f.profile(t)
	if p.L1Address != "opaque-l1" || p.L3Address != "opaque-l3" || p.AutomaticNodeAdmission || len(p.NodeAdmission) != 0 || len(p.Participants) != 2 {
		t.Fatalf("joining profile: %+v", p)
	}
	var started []string
	for _, command := range f.launchd.commands {
		if command[1] == "bootstrap" {
			started = append(started, strings.TrimSuffix(filepath.Base(command[3]), ".plist"))
		}
	}
	if !slices.Equal(started, []string{"dev.wefty.node-agent", "dev.wefty.client-service"}) {
		t.Fatalf("start order: %v", started)
	}
	for _, check := range report.Readiness {
		if check.Status != "VERIFIED" {
			t.Fatalf("join readiness: %+v", report.Readiness)
		}
	}
	for _, name := range []string{"node-agent", "client-service"} {
		if _, err := os.Stat(filepath.Join(report.DataDir, "setup-ready", name+".json")); err != nil {
			t.Fatal(err)
		}
	}
	paths, _ := resolveSetupPaths(f.home)
	payload, err := os.ReadFile(filepath.Join(paths.config, setupManifestName))
	if err != nil {
		t.Fatal(err)
	}
	var receipt setupManifest
	if json.Unmarshal(payload, &receipt) != nil || receipt.Role != "join" {
		t.Fatalf("receipt: %s", payload)
	}
	for _, name := range []string{"wefty-l1", "wefty-l3"} {
		if _, err := os.Stat(filepath.Join(report.BinDir, name)); !os.IsNotExist(err) {
			t.Fatalf("unexpected binary %s: %v", name, err)
		}
	}
	before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f})
	prompts.Reset()
	f.host.enrolmentInput = strings.NewReader("")
	again := f.setup(t, setupRequest{})
	if prompts.Len() != 0 || len(e.roles) != 2 || len(again.Changes) != 0 || !equalFleetFiles(before, installedUpgradeFiles(t, &upgradeFixture{setupFixture: f})) {
		t.Fatal("rerun changed installation or consumed keys")
	}
	assertJoinArtifactsPrivate(t, f.home, prompts.String(), report, "secret-node", "secret-client", "https://login.invalid/one-use")
}

func assertJoinArtifactsPrivate(t *testing.T, home, output string, report setupReport, secrets ...string) {
	t.Helper()
	payload, _ := json.Marshal(report)
	var human bytes.Buffer
	if err := writeSetupReport(&human, report); err != nil {
		t.Fatal(err)
	}
	check := func(b []byte) {
		for _, secret := range secrets {
			if bytes.Contains(b, []byte(secret)) {
				t.Error("secret persisted or printed")
			}
		}
	}
	check(append(payload, []byte(output+human.String())...))
	if err := filepath.WalkDir(home, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			check(b)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSetupJoinOptionalPersonUsesBrowserWithoutKey(t *testing.T) {
	f, e, prompts := newJoinFixture(t)
	var browser []string
	f.host.ownerLogin = func(_ context.Context, value string) error { browser = append(browser, value); return nil }
	code := joinCodeForTest(t, "opaque-l1", "opaque-l3", "")
	report := f.setup(t, setupRequest{role: "join", joinCode: code, withPerson: true, nodeID: "studio-0123abcd"})
	if !slices.Equal(e.roles, []string{"node-agent", "client", "person"}) || !slices.Equal(e.keys, []string{"secret-node", "secret-client", ""}) || len(browser) != 1 || strings.Count(prompts.String(), "Single-use join key") != 2 {
		t.Fatalf("person flow: roles=%v browser count=%d prompts=%s", e.roles, len(browser), prompts)
	}
	p := f.profile(t)
	if len(p.Participants) != 3 || p.Participants["person"].UserID != "opaque-owner" || report.Participants["person"] == "" {
		t.Fatal("missing saved person")
	}
	prompts.Reset()
	f.host.enrolmentInput = strings.NewReader("")
	_ = f.setup(t, setupRequest{})
	if len(e.roles) != 3 || len(browser) != 1 || prompts.Len() != 0 {
		t.Fatal("person rerun prompted again")
	}
	assertJoinArtifactsPrivate(t, f.home, prompts.String(), report, "secret-node", "secret-client", browser[0])
}

func TestSetupJoinBrowserFailureDoesNotPersistURL(t *testing.T) {
	f, e, prompts := newJoinFixture(t)
	f.host.ownerLogin = func(_ context.Context, value string) error { return fmt.Errorf("browser failed: %s", value) }
	report, err := runSetup(t.Context(), f.host, setupRequest{role: "join", joinCode: joinCodeForTest(t, "opaque-l1", "opaque-l3", ""), withPerson: true, nodeID: "studio-0123abcd"})
	if err == nil || strings.Contains(err.Error(), "https://") || !slices.Equal(e.loggedOut, []string{"person"}) {
		t.Fatalf("unsafe browser failure: %v", err)
	}
	paths, _ := resolveSetupPaths(f.home)
	p, readErr := connprofile.Read(filepath.Join(paths.config, fleetSetupProfileName))
	if readErr != nil || len(p.Participants) != 2 {
		t.Fatalf("lost saved machines: %+v %v", p, readErr)
	}
	assertJoinArtifactsPrivate(t, f.home, prompts.String()+err.Error(), report, "secret-node", "secret-client", "https://login.invalid/one-use")
}

func TestSetupJoinPeerFailureCorrectionKeepsEnrolledParticipants(t *testing.T) {
	for _, peer := range []string{"l1", "l3"} {
		t.Run(peer, func(t *testing.T) {
			f, e, prompts := newJoinFixture(t)
			l1ID, l3ID := "opaque-l1", "opaque-l3"
			if peer == "l1" {
				l1ID = "wrong-l1"
			} else {
				l3ID = "wrong-l3"
			}
			wrong := joinCodeForTest(t, l1ID, l3ID, "")
			report, err := runSetup(t.Context(), f.host, setupRequest{role: "join", joinCode: wrong, nodeID: "studio-0123abcd"})
			if err == nil || !strings.Contains(err.Error(), "join peer verification failed") || len(e.roles) != 2 || len(f.launchd.mutating()) != 0 {
				t.Fatalf("verification: %v %+v", err, report)
			}
			paths, _ := resolveSetupPaths(f.home)
			record, err := readFleetSetupRecord(paths)
			if err != nil || record.PeerVerificationFailure != peer+":peer_not_found" || record.JoinCode != wrong {
				t.Fatalf("failure record: %+v %v", record, err)
			}
			for _, path := range []string{paths.profile, filepath.Join(paths.config, setupManifestName)} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatal("published before verification")
				}
			}
			prompts.Reset()
			f.host.enrolmentInput = strings.NewReader("")
			correct := joinCodeForTest(t, "opaque-l1", "opaque-l3", "")
			report = f.setup(t, setupRequest{role: "join", joinCode: correct})
			if len(e.roles) != 2 || prompts.Len() != 0 {
				t.Fatal("corrected code consumed keys")
			}
			record, err = readFleetSetupRecord(paths)
			if err != nil || record.PeerVerificationFailure != "" || record.JoinCode != correct || !record.Committed {
				t.Fatalf("success record: %+v %v", record, err)
			}
			assertJoinArtifactsPrivate(t, f.home, prompts.String(), report, "secret-node", "secret-client")
		})
	}
}

func TestSetupJoinRefusesCodeChangesOutsideFailedVerification(t *testing.T) {
	for _, boundary := range []string{"stage-record", "enrolled:node-agent", "peer-verification", "profile", "receipt", "committed"} {
		t.Run(boundary, func(t *testing.T) {
			f, e, prompts := newJoinFixture(t)
			code := joinCodeForTest(t, "opaque-l1", "opaque-l3", "")
			_, err := runSetup(t.Context(), f.host, setupRequest{role: "join", joinCode: code, nodeID: "studio-0123abcd", afterFleetStep: func(step string) error {
				if step == boundary {
					return errors.New("interrupted")
				}
				return nil
			}})
			if err == nil || !strings.Contains(err.Error(), "interrupted") {
				t.Fatalf("missing boundary: %v", err)
			}
			before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f})
			count := len(e.roles)
			prompts.Reset()
			f.launchd.commands = nil
			_, err = runSetup(t.Context(), f.host, setupRequest{role: "join", joinCode: joinCodeForTest(t, "other-l1", "opaque-l3", "")})
			if err == nil || !strings.Contains(err.Error(), "different join code") || len(e.roles) != count || prompts.Len() != 0 || len(f.launchd.mutating()) != 0 || !equalFleetFiles(before, installedUpgradeFiles(t, &upgradeFixture{setupFixture: f})) {
				t.Fatalf("code change accepted at %s: %v", boundary, err)
			}
		})
	}
}

func TestSetupJoinFailureCorrectionCannotChangeCoordinatorOrReuseOldFailure(t *testing.T) {
	f, e, prompts := newJoinFixture(t)
	wrong := joinCodeForTest(t, "wrong-l1", "opaque-l3", "")
	_, err := runSetup(t.Context(), f.host, setupRequest{role: "join", joinCode: wrong, nodeID: "studio-0123abcd"})
	if err == nil {
		t.Fatal("missing verification failure")
	}
	prompts.Reset()
	f.host.enrolmentInput = strings.NewReader("")
	_, err = runSetup(t.Context(), f.host, setupRequest{role: "join", joinCode: joinCodeForTest(t, "opaque-l1", "opaque-l3", "https://other.example.test")})
	if err == nil || !strings.Contains(err.Error(), "coordinator") || len(e.roles) != 2 || prompts.Len() != 0 {
		t.Fatalf("changed coordinator: %v", err)
	}
	_, err = runSetup(t.Context(), f.host, setupRequest{role: "join", joinCode: joinCodeForTest(t, "opaque-l1", "opaque-l3", ""), afterFleetStep: func(step string) error {
		if step == "stage-record" {
			return errors.New("interrupted correction")
		}
		return nil
	}})
	if err == nil {
		t.Fatal("missing interruption")
	}
	_, err = runSetup(t.Context(), f.host, setupRequest{role: "join", joinCode: joinCodeForTest(t, "another-l1", "opaque-l3", "")})
	if err == nil || !strings.Contains(err.Error(), "different join code") || len(e.roles) != 2 || prompts.Len() != 0 {
		t.Fatalf("old failure authorized new code: %v", err)
	}
}

func TestSetupJoinResumesEveryStepBoundary(t *testing.T) {
	for _, boundary := range []string{"stage-record", "stage-profile", "enrolled:node-agent", "enrolled:client", "enrolled:person", "peer-verification", "profile", "receipt", "committed", "service:0", "service:1"} {
		t.Run(boundary, func(t *testing.T) {
			f, e, prompts := newJoinFixture(t)
			f.host.ownerLogin = func(context.Context, string) error { return nil }
			stop := func(step string) error {
				if step == boundary {
					return errors.New("interrupted")
				}
				return nil
			}
			code := joinCodeForTest(t, "opaque-l1", "opaque-l3", "")
			_, err := runSetup(t.Context(), f.host, setupRequest{role: "join", joinCode: code, withPerson: true, nodeID: "studio-0123abcd", afterFleetStep: stop, afterService: func(i int, _ launchagent.Action) error { return stop(fmt.Sprintf("service:%d", i)) }})
			if err == nil || !strings.Contains(err.Error(), "interrupted") {
				t.Fatalf("missing interruption: %v", err)
			}
			paths, _ := resolveSetupPaths(f.home)
			if _, err := os.Stat(filepath.Join(paths.config, setupManifestName)); err == nil {
				p, err := connprofile.Read(paths.profile)
				if err != nil || serviceplan.ValidateProfile(serviceplan.Join, p) != nil || len(p.Participants) != 3 {
					t.Fatalf("receipt with incomplete profile: %+v %v", p, err)
				}
			}
			savedCount := len(e.roles)
			prompts.Reset()
			f.host.enrolmentInput = strings.NewReader(strings.Repeat("remaining-key\n", 2-savedMachineCount(e.roles)))
			_ = f.setup(t, setupRequest{role: "join", joinCode: code, withPerson: true})
			if len(e.roles) != 3 || strings.Count(prompts.String(), "Single-use join key") != 2-savedMachineCount(e.roles[:savedCount]) {
				t.Fatalf("resume duplicated enrolment: %v %s", e.roles, prompts)
			}
			assertJoinArtifactsPrivate(t, f.home, prompts.String(), setupReport{}, "secret-node", "secret-client", "remaining-key", "https://login.invalid/one-use")
		})
	}
}
func savedMachineCount(roles []string) int {
	count := 0
	for _, role := range roles {
		if role != "person" {
			count++
		}
	}
	return count
}

func TestSetupJoinRoleConflictsAndInvalidCodeBeforeKeys(t *testing.T) {
	for _, existing := range []string{"plain", "control-plane", "invalid-code", "person-added", "coordinator-override"} {
		t.Run(existing, func(t *testing.T) {
			f, e, prompts := newJoinFixture(t)
			request := setupRequest{role: "join", joinCode: joinCodeForTest(t, "opaque-l1", "opaque-l3", ""), nodeID: "studio-0123abcd"}
			switch existing {
			case "plain":
				f.host.launchctl = f.launchd.run
				_ = f.setup(t, setupRequest{nodeID: "studio-0123abcd"})
			case "control-plane":
				f.host.enrolmentInput = strings.NewReader("one\ntwo\nthree\nfour\n")
				_ = f.setup(t, setupRequest{role: "control-plane", nodeID: "studio-0123abcd"})
			case "invalid-code":
				request.joinCode += "typo"
			case "person-added":
				_, _ = runSetup(t.Context(), f.host, setupRequest{role: "join", joinCode: request.joinCode, nodeID: request.nodeID, afterFleetStep: func(string) error { return errors.New("stop") }})
				request.withPerson = true
			case "coordinator-override":
				request.coordinatorURL = "https://other.example.test"
			}
			before := len(e.roles)
			prompts.Reset()
			f.launchd.commands = nil
			_, err := runSetup(t.Context(), f.host, request)
			if err == nil || len(e.roles) != before || prompts.Len() != 0 || len(f.launchd.mutating()) != 0 {
				t.Fatalf("conflict consumed keys: %v", err)
			}
		})
	}
}

func TestSetupJoinAdmissionFailureNeverEnablesClaims(t *testing.T) {
	for _, condition := range []string{"claims disabled", "not automatic", "extra tag", "wrong slots", "missing node", "wrong boot"} {
		t.Run(condition, func(t *testing.T) {
			f, e, prompts := newJoinFixture(t)
			open := f.host.fleetClients
			f.host.fleetClients = func(ctx context.Context, paths setupPaths, p connprofile.Profile) (*apiClients, error) {
				clients, err := open(ctx, paths, p)
				if err != nil {
					return nil, err
				}
				rt := clients.l1.client.Transport
				clients.l1.client.Transport = fleetRoundTripper(func(r *http.Request) (*http.Response, error) {
					if r.Method != "GET" {
						t.Fatalf("joining readiness tried mutation: %s %s", r.Method, r.URL.Path)
					}
					response, err := rt.RoundTrip(r)
					if err != nil {
						return nil, err
					}
					if r.URL.Path == "/v1/nodes" {
						var nodes l1.NodeList
						_ = json.NewDecoder(response.Body).Decode(&nodes)
						_ = response.Body.Close()
						node := &nodes.Nodes[0]
						switch condition {
						case "claims disabled":
							node.ClaimsEnabled = false
						case "not automatic":
							node.AutomaticallyAdmitted = false
						case "extra tag":
							node.AuthoritativeTags = append(node.AuthoritativeTags, "extra")
						case "wrong slots":
							node.MaxServiceSlots++
						case "missing node":
							nodes.Nodes = nil
						case "wrong boot":
							node.BootSessionID = "old-boot"
						}
						payload, _ := json.Marshal(nodes)
						response.Body = io.NopCloser(bytes.NewReader(payload))
					}
					return response, nil
				})
				return clients, nil
			}
			report, err := runSetup(t.Context(), f.host, setupRequest{role: "join", joinCode: joinCodeForTest(t, "opaque-l1", "opaque-l3", ""), nodeID: "studio-0123abcd"})
			var notReady *notReadyError
			if !errors.As(err, &notReady) {
				t.Fatalf("admission should fail: %v", err)
			}
			found := false
			for _, check := range report.Readiness {
				if check.Check == "node registration and admission" {
					found = check.Status == "FAILED" && strings.Contains(check.Detail, "set-claims") && strings.Contains(check.Detail, "revoke")
				}
			}
			if !found || len(e.roles) != 2 || strings.Count(prompts.String(), "Single-use join key") != 2 {
				t.Fatalf("admission report: %+v", report)
			}
		})
	}
}

func TestSetupJoinRefusesLeftoverControlPlaneServicesBeforeKeys(t *testing.T) {
	for _, label := range []string{"dev.wefty.l1", "dev.wefty.l3"} {
		t.Run(label, func(t *testing.T) {
			f, e, prompts := newJoinFixture(t)
			f.launchd.loaded[label] = true
			_, err := runSetup(t.Context(), f.host, setupRequest{role: "join", joinCode: joinCodeForTest(t, "opaque-l1", "opaque-l3", ""), nodeID: "studio-0123abcd"})
			if err == nil || len(e.roles) != 0 || prompts.Len() != 0 || len(f.launchd.mutating()) != 0 {
				t.Fatalf("adopted leftover control service: %v", err)
			}
		})
	}
}
