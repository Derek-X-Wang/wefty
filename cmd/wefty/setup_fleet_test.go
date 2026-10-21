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

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/l1"
)

type fleetRoundTripper func(*http.Request) (*http.Response, error)

func (f fleetRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func newFleetFixture(t *testing.T) (*setupFixture, *fakeSetupEnroller, *bytes.Buffer) {
	t.Helper()
	f := newSetupFixture(t)
	home := newFleetTestHome(t)
	f.home, f.host.home = home, home
	e := &fakeSetupEnroller{}
	output := &bytes.Buffer{}
	f.host.bootTime = func() (time.Time, error) { return time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), nil }
	f.host.enroller, f.host.enrolmentInput, f.host.enrolmentOutput = e, strings.NewReader("secret-l1\nsecret-l3\nsecret-node\nsecret-client\n"), output
	f.host.fleetClients = func(_ context.Context, _ setupPaths, p connprofile.Profile) (*apiClients, error) {
		rt := fleetRoundTripper(func(r *http.Request) (*http.Response, error) {
			var payload any
			code := 200
			switch {
			case r.URL.Path == "/v1/nodes":
				payload = l1.NodeList{Nodes: []l1.Node{{NodeRegistration: contract.NodeRegistration{NodeID: p.Identities.Node, BootSessionID: "boot-fixture"}, State: contract.NodeAlive, ClaimsEnabled: true, AutomaticallyAdmitted: len(p.NodeAdmission) == 0, AuthoritativeTags: []string{contract.StableNodeTagPrefix + p.Identities.Node}, MaxOneshotSlots: 4, MaxServiceSlots: 2}}}
			case r.URL.Path == "/v1/runs" && r.Method == "GET":
				payload = map[string]any{"runs": []any{}}
			case r.URL.Path == "/v1/runs" && r.Method == "POST":
				var request map[string]any
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Fatal(err)
				}
				tags, _ := request["tags"].([]any)
				if len(tags) != 1 || tags[0] != contract.StableNodeTagPrefix+p.Identities.Node {
					t.Fatalf("smoke is not pinned: %v", request)
				}
				payload, code = map[string]any{"run_id": "run-setup-smoke"}, 201
			case r.URL.Path == "/v1/runs/run-setup-smoke":
				payload = contract.RunRecord{RunID: "run-setup-smoke", Status: contract.RunSucceeded, NodeID: p.Identities.Node}
			default:
				return nil, fmt.Errorf("unexpected readiness request: %s %s", r.Method, r.URL.Path)
			}
			body, _ := json.Marshal(payload)
			return &http.Response{StatusCode: code, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(body))}, nil
		})
		return &apiClients{l1: &apiClient{name: "L1", address: p.L1Address, client: &http.Client{Transport: rt}}, l3: &apiClient{name: "L3", address: p.L3Address, client: &http.Client{Transport: rt}}, wait: waitForContext}, nil
	}
	f.host.launchctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		result, err := f.launchd.run(ctx, name, args...)
		if err == nil && args[0] == "bootstrap" {
			service := strings.TrimSuffix(filepath.Base(args[2]), ".plist")
			if service != "dev.wefty.client-service" {
				path := filepath.Join(f.home, setupFolders.data, "setup-ready", strings.TrimPrefix(service, "dev.wefty.")+".json")
				body := []byte(`{"ready_at":"2026-10-20T12:00:00Z","address":"opaque-listener","pid":4242,"node_id":"studio-0123abcd","boot_session_id":"boot-fixture"}`)
				if err := os.WriteFile(path, body, 0600); err != nil {
					return nil, err
				}
			}
		}
		return result, err
	}
	return f, e, output
}

func TestSetupControlPlanePublishesAndResumesWithoutKeys(t *testing.T) {
	f, e, prompts := newFleetFixture(t)
	report := f.setup(t, setupRequest{role: "control-plane", nodeID: "studio-0123abcd"})
	if !slices.Equal(e.roles, []string{"l1", "l3", "node-agent", "client"}) {
		t.Fatalf("enrolment order: %v", e.roles)
	}
	if strings.Count(prompts.String(), "Single-use join key") != 4 {
		t.Fatalf("prompts: %s", prompts)
	}
	p := f.profile(t)
	if report.Role != serviceplan.ControlPlane || p.L1Address != "opaque-l1" || p.L3Address != "opaque-l3" || !p.AutomaticNodeAdmission || len(p.Participants) != 4 {
		t.Fatalf("incomplete fleet: %+v %+v", report, p)
	}
	for role, saved := range p.Participants {
		if saved.NetworkName == "" {
			t.Fatalf("missing network name: %s", role)
		}
	}
	var started []string
	for _, command := range f.launchd.commands {
		if command[1] == "bootstrap" {
			started = append(started, strings.TrimSuffix(filepath.Base(command[3]), ".plist"))
		}
	}
	if !slices.Equal(started, []string{"dev.wefty.l1", "dev.wefty.l3", "dev.wefty.node-agent", "dev.wefty.client-service"}) {
		t.Fatalf("start order: %v", started)
	}
	paths, _ := resolveSetupPaths(f.home)
	payload, err := os.ReadFile(filepath.Join(paths.config, setupManifestName))
	if err != nil {
		t.Fatal(err)
	}
	var receipt setupManifest
	if err := json.Unmarshal(payload, &receipt); err != nil || receipt.Role != serviceplan.ControlPlane {
		t.Fatalf("receipt: %s %v", payload, err)
	}
	var text bytes.Buffer
	if err := writeSetupReport(&text, report); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text.String(), "L1 → L3 direction: NOT-RUN") || !strings.Contains(text.String(), "smoke run: VERIFIED") {
		t.Fatalf("readiness report: %s", &text)
	}
	before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f})
	prompts.Reset()
	f.host.enrolmentInput = strings.NewReader("")
	again := f.setup(t, setupRequest{})
	if len(again.Changes) != 0 || prompts.Len() != 0 || len(e.roles) != 4 || len(f.launchd.mutating()) != 0 {
		t.Fatalf("rerun changed installation or asked keys: %+v %s %v", again, prompts, f.launchd.commands)
	}
	after := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f})
	if !equalFleetFiles(before, after) {
		t.Fatal("rerun changed installed bytes")
	}
	artifacts, _ := json.Marshal(report)
	for _, key := range []string{"secret-l1", "secret-l3", "secret-node", "secret-client"} {
		if bytes.Contains(artifacts, []byte(key)) || strings.Contains(text.String()+prompts.String(), key) {
			t.Fatal("secret in report")
		}
		if err := filepath.WalkDir(f.home, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				b, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				if bytes.Contains(b, []byte(key)) {
					t.Errorf("secret in %s", path)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func equalFleetFiles(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if v != b[k] {
			return false
		}
	}
	return true
}

func TestSetupControlPlaneResumesEveryDurableStep(t *testing.T) {
	steps := []string{"stage-record", "stage-profile", "enrolled:l1", "enrolled:l3", "enrolled:node-agent", "enrolled:client", "profile", "receipt", "committed"}
	for _, step := range steps {
		t.Run(step, func(t *testing.T) {
			f, e, prompts := newFleetFixture(t)
			_, err := runSetup(t.Context(), f.host, setupRequest{role: "control-plane", nodeID: "studio-0123abcd", afterFleetStep: func(got string) error {
				if got == step {
					return errors.New("injected interruption")
				}
				return nil
			}})
			if err == nil || !strings.Contains(err.Error(), "injected") {
				t.Fatalf("missing failure at %s: %v", step, err)
			}
			first := slices.Clone(e.roles)
			if len(f.launchd.mutating()) != 0 {
				t.Fatal("services touched before publication completed")
			}
			paths, _ := resolveSetupPaths(f.home)
			if step == "profile" {
				p, err := connprofile.Read(paths.profile)
				if err != nil || len(p.Participants) != 4 || !p.AutomaticNodeAdmission {
					t.Fatalf("profile incomplete in publication window: %+v %v", p, err)
				}
				if _, err := os.Stat(filepath.Join(paths.config, setupManifestName)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("receipt committed early: %v", err)
				}
				before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f})
				if _, err := runSetup(t.Context(), f.host, setupRequest{}); err == nil {
					t.Fatal("plain setup accepted fleet publication window")
				}
				if _, err := runUpgrade(t.Context(), f.host, false, io.Discard); err == nil {
					t.Fatal("upgrade accepted fleet publication window")
				}
				if !equalFleetFiles(before, installedUpgradeFiles(t, &upgradeFixture{setupFixture: f})) {
					t.Fatal("legacy refusal changed publication window")
				}
			}
			remaining := []string{"secret-l1", "secret-l3", "secret-node", "secret-client"}[len(first):]
			f.host.enrolmentInput = strings.NewReader(strings.Join(remaining, "\n") + "\n")
			prompts.Reset()
			report := f.setup(t, setupRequest{role: "control-plane"})
			if !slices.Equal(e.roles, []string{"l1", "l3", "node-agent", "client"}) {
				t.Fatalf("participants repeated or skipped: %v", e.roles)
			}
			if strings.Count(prompts.String(), "Single-use join key") != 4-len(first) {
				t.Fatalf("saved participant key asked again: %s", prompts)
			}
			if len(report.Services) != 4 {
				t.Fatal("resume did not start four services")
			}
		})
	}
}

func TestSetupControlPlaneRefusesRoleConflictsAndLostState(t *testing.T) {
	t.Run("plain conversion", func(t *testing.T) {
		f := newSetupFixture(t)
		f.setup(t, setupRequest{})
		before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f})
		f.launchd.commands = nil
		if _, err := runSetup(t.Context(), f.host, setupRequest{role: "control-plane"}); err == nil || !strings.Contains(err.Error(), "plain") {
			t.Fatalf("plain conversion accepted: %v", err)
		}
		if len(f.launchd.commands) != 0 || !equalFleetFiles(before, installedUpgradeFiles(t, &upgradeFixture{setupFixture: f})) {
			t.Fatal("role conflict changed install")
		}
	})
	t.Run("different recorded role", func(t *testing.T) {
		f, _, _ := newFleetFixture(t)
		f.setup(t, setupRequest{role: "control-plane", nodeID: "studio-0123abcd"})
		before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f})
		f.launchd.commands = nil
		if _, err := runSetup(t.Context(), f.host, setupRequest{role: "join"}); err == nil || !strings.Contains(err.Error(), "recorded fleet role") {
			t.Fatalf("role switch accepted: %v", err)
		}
		if len(f.launchd.commands) != 0 || !equalFleetFiles(before, installedUpgradeFiles(t, &upgradeFixture{setupFixture: f})) {
			t.Fatal("role conflict changed fleet")
		}
	})
	t.Run("backend state missing", func(t *testing.T) {
		f, e, prompts := newFleetFixture(t)
		_, _ = runSetup(t.Context(), f.host, setupRequest{role: "control-plane", nodeID: "studio-0123abcd", afterFleetStep: func(step string) error {
			if step == "enrolled:l1" {
				return errors.New("interrupt")
			}
			return nil
		}})
		path := filepath.Join(f.home, setupFolders.data, "fabric", "l1")
		if err := os.RemoveAll(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "only-a-log"), []byte("not identity"), 0600); err != nil {
			t.Fatal(err)
		}
		prompts.Reset()
		if _, err := runSetup(t.Context(), f.host, setupRequest{role: "control-plane"}); err == nil || !strings.Contains(err.Error(), "state is missing") {
			t.Fatalf("missing state reenrolled: %v", err)
		}
		if len(e.roles) != 1 || prompts.Len() != 0 {
			t.Fatal("missing state consumed another key")
		}
	})
}

func TestSetupControlPlaneReadinessFailureKeepsPublishedInstall(t *testing.T) {
	f, e, prompts := newFleetFixture(t)
	f.host.readinessBudget = 20 * time.Millisecond
	f.host.fleetClients = func(context.Context, setupPaths, connprofile.Profile) (*apiClients, error) {
		return nil, errors.New("fixture service is down")
	}
	report, err := runSetup(t.Context(), f.host, setupRequest{role: "control-plane", nodeID: "studio-0123abcd"})
	var notReady *notReadyError
	if !errors.As(err, &notReady) || len(report.Readiness) == 0 {
		t.Fatalf("missing readiness refusal: %+v %v", report, err)
	}
	paths, _ := resolveSetupPaths(f.home)
	if _, err := os.Stat(filepath.Join(paths.config, setupManifestName)); err != nil {
		t.Fatal("readiness failure removed committed install")
	}
	f.host.enrolmentInput = strings.NewReader("")
	prompts.Reset()
	_, err = runSetup(t.Context(), f.host, setupRequest{})
	if !errors.As(err, &notReady) || len(e.roles) != 4 || prompts.Len() != 0 {
		t.Fatalf("readiness retry enrolled again: %v %v %s", err, e.roles, prompts)
	}
}

func TestSetupControlPlaneCoordinatorAndCollisionReport(t *testing.T) {
	f, e, prompts := newFleetFixture(t)
	e.collisionRole = "node-agent"
	report := f.setup(t, setupRequest{role: "control-plane", nodeID: "studio-0123abcd", coordinatorURL: "https://coordinator.example.test"})
	if !slices.Equal(e.coordinators, []string{"https://coordinator.example.test", "https://coordinator.example.test", "https://coordinator.example.test", "https://coordinator.example.test"}) {
		t.Fatalf("coordinator lost: %v", e.coordinators)
	}
	for _, part := range f.profile(t).Participants {
		if part.CoordinatorURL != "https://coordinator.example.test" {
			t.Fatalf("coordinator not saved: %+v", part)
		}
	}
	var output bytes.Buffer
	if err := writeSetupReport(&output, report); err != nil {
		t.Fatal(err)
	}
	notice := "The network gave wefty-studio-2 because wefty-studio is taken; retire the stale device to free the name."
	if strings.Count(output.String(), notice) != 1 || strings.Contains(prompts.String(), notice) {
		t.Fatalf("collision notice missing or duplicated: %s %s", &output, prompts)
	}
	prompts.Reset()
	report = f.setup(t, setupRequest{})
	output.Reset()
	_ = writeSetupReport(&output, report)
	if prompts.Len() != 0 || strings.Contains(output.String(), "The network gave") {
		t.Fatal("saved rerun repeated enrolment notice")
	}
	f.launchd.commands = nil
	if _, err := runSetup(t.Context(), f.host, setupRequest{coordinatorURL: "https://other.example.test"}); err == nil {
		t.Fatal("conflicting coordinator accepted")
	}
	if len(f.launchd.commands) != 0 {
		t.Fatal("coordinator conflict reached services")
	}
}

func TestSetupControlPlaneRejectsSecretCoordinatorBeforeStaging(t *testing.T) {
	f, e, prompts := newFleetFixture(t)
	_, err := runSetup(t.Context(), f.host, setupRequest{role: "control-plane", coordinatorURL: "https://owner:secret-password@example.test"})
	if err == nil || strings.Contains(err.Error(), "secret-password") || len(e.roles) != 0 || prompts.Len() != 0 {
		t.Fatalf("unsafe coordinator was not refused privately: %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.home, setupFolders.config, fleetSetupRecordName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsafe coordinator created a stage: %v", err)
	}
}

func TestSetupControlPlaneServiceStartFailureReportsAllChecksAndResumes(t *testing.T) {
	f, e, prompts := newFleetFixture(t)
	f.host.readinessBudget = 20 * time.Millisecond
	f.launchd.failOnce["bootstrap"] = errors.New("fixture activation failure")
	report, err := runSetup(t.Context(), f.host, setupRequest{role: "control-plane", nodeID: "studio-0123abcd"})
	var notReady *notReadyError
	if !errors.As(err, &notReady) || len(report.Readiness) != 9 || report.Readiness[0].Status != "FAILED" {
		t.Fatalf("service failure lost report: %+v %v", report, err)
	}
	var text bytes.Buffer
	if err := writeSetupReport(&text, report); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text.String(), "fixture activation failure") {
		t.Fatalf("activation cause lost from report: %s", &text)
	}
	prompts.Reset()
	f.host.enrolmentInput = strings.NewReader("")
	report = f.setup(t, setupRequest{})
	if len(e.roles) != 4 || prompts.Len() != 0 || len(report.Services) != 4 {
		t.Fatalf("service retry re-enrolled: %v %s", e.roles, prompts)
	}
}

func TestSetupControlPlaneRefusesStaleReadyPID(t *testing.T) {
	f, _, _ := newFleetFixture(t)
	f.host.readinessBudget = 20 * time.Millisecond
	report, err := runSetup(t.Context(), f.host, setupRequest{role: "control-plane", nodeID: "studio-0123abcd", afterService: func(i int, _ launchagent.Action) error {
		if i == 0 {
			return os.WriteFile(filepath.Join(f.home, setupFolders.data, "setup-ready", "l1.json"), []byte(`{"pid":1111,"address":"old-listener"}`), 0600)
		}
		return nil
	}})
	var notReady *notReadyError
	if !errors.As(err, &notReady) || report.Readiness[0].Status != "FAILED" || report.Readiness[4].Status != "VERIFIED" || report.Readiness[8].Status != "NOT-RUN" {
		t.Fatalf("client reachability hid stale PID: %+v %v", report.Readiness, err)
	}
}

func TestSetupControlPlaneChecksConfiguredNodeAdmission(t *testing.T) {
	for _, issue := range []string{"disabled", "automatic", "wrong tag", "wrong capacity", "stale", "other boot"} {
		t.Run(issue, func(t *testing.T) {
			f, _, _ := newFleetFixture(t)
			f.host.readinessBudget = 20 * time.Millisecond
			original := f.host.fleetClients
			f.host.fleetClients = func(ctx context.Context, paths setupPaths, p connprofile.Profile) (*apiClients, error) {
				clients, err := original(ctx, paths, p)
				if err != nil {
					return nil, err
				}
				base := clients.l1.client.Transport
				clients.l1.client.Transport = fleetRoundTripper(func(r *http.Request) (*http.Response, error) {
					response, err := base.RoundTrip(r)
					if err != nil {
						return response, err
					}
					var nodes l1.NodeList
					if err := json.NewDecoder(response.Body).Decode(&nodes); err != nil {
						return nil, err
					}
					_ = response.Body.Close()
					switch issue {
					case "disabled":
						nodes.Nodes[0].ClaimsEnabled = false
					case "automatic":
						nodes.Nodes[0].AutomaticallyAdmitted = true
					case "wrong tag":
						nodes.Nodes[0].AuthoritativeTags = []string{"other"}
					case "wrong capacity":
						nodes.Nodes[0].MaxServiceSlots = 99
					case "stale":
						nodes.Nodes[0].State = contract.NodeDead
					case "other boot":
						nodes.Nodes[0].BootSessionID = "previous-boot"
					}
					b, _ := json.Marshal(nodes)
					response.Body = io.NopCloser(bytes.NewReader(b))
					return response, nil
				})
				return clients, nil
			}
			report, err := runSetup(t.Context(), f.host, setupRequest{role: "control-plane", nodeID: "studio-0123abcd"})
			var notReady *notReadyError
			if !errors.As(err, &notReady) || report.Readiness[6].Status != "FAILED" || report.Readiness[8].Status != "NOT-RUN" {
				t.Fatalf("bad admission accepted: %+v %v", report.Readiness, err)
			}
		})
	}
}

func TestSetupControlPlaneRollsBackOnlyFailingParticipant(t *testing.T) {
	roles := []string{"l1", "l3", "node-agent", "client"}
	for i, role := range roles {
		t.Run(role, func(t *testing.T) {
			f, e, prompts := newFleetFixture(t)
			e.wrongName = role
			_, err := runSetup(t.Context(), f.host, setupRequest{role: "control-plane", nodeID: "studio-0123abcd"})
			if err == nil || !slices.Equal(e.loggedOut, []string{role}) || !slices.Equal(e.roles, roles[:i+1]) {
				t.Fatalf("wrong rollback scope: %v %v %v", err, e.roles, e.loggedOut)
			}
			paths, _ := resolveSetupPaths(f.home)
			if _, err := os.Stat(filepath.Join(paths.data, "fabric", role)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed participant retained: %v", err)
			}
			if _, err := os.Stat(paths.profile); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("partial profile activated")
			}
			p, err := connprofile.Read(filepath.Join(paths.config, fleetSetupProfileName))
			if err != nil || len(p.Participants) != i {
				t.Fatalf("prior records lost: %+v %v", p, err)
			}
			e.wrongName = ""
			prompts.Reset()
			keys := []string{"retry-l1", "retry-l3", "retry-node", "retry-client"}[i:]
			f.host.enrolmentInput = strings.NewReader(strings.Join(keys, "\n") + "\n")
			f.setup(t, setupRequest{role: "control-plane"})
			if len(e.roles) != 5 || strings.Count(prompts.String(), "Single-use join key") != 4-i {
				t.Fatalf("saved participant enrolled again: %v %s", e.roles, prompts)
			}
		})
	}
}

func TestSetupControlPlaneRemovalRefusesBeforeAnyChange(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprint(committed), func(t *testing.T) {
			f, _, _ := newFleetFixture(t)
			request := setupRequest{role: "control-plane", nodeID: "studio-0123abcd"}
			if !committed {
				request.afterFleetStep = func(step string) error {
					if step == "enrolled:l1" {
						return errors.New("interrupt")
					}
					return nil
				}
			}
			_, _ = runSetup(t.Context(), f.host, request)
			before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f})
			f.launchd.commands = nil
			if _, err := runSetupRemove(t.Context(), f.host, true); err == nil {
				t.Fatal("legacy removal accepted fleet state")
			}
			if len(f.launchd.commands) != 0 || !equalFleetFiles(before, installedUpgradeFiles(t, &upgradeFixture{setupFixture: f})) {
				t.Fatal("removal changed fleet before refusing")
			}
		})
	}
}

// Keep the default client socket within the platform's 100-byte bound. The
// worker's short TMPDIR stays under its Go cache; ordinary CI may need /tmp.
func newFleetTestHome(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(root) > 45 {
		root = "/tmp"
	}
	home, err := os.MkdirTemp(root, "wf-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	home, err = filepath.EvalSymlinks(home)
	if err != nil {
		t.Fatal(err)
	}
	return home
}

func TestSetupControlPlaneMissingCommittedReceiptRefuses(t *testing.T) {
	f, _, prompts := newFleetFixture(t)
	f.setup(t, setupRequest{role: "control-plane", nodeID: "studio-0123abcd"})
	path := filepath.Join(f.home, setupFolders.config, setupManifestName)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f})
	f.launchd.commands = nil
	prompts.Reset()
	_, err := runSetup(t.Context(), f.host, setupRequest{role: "control-plane"})
	if err == nil || !strings.Contains(err.Error(), "receipt is missing") {
		t.Fatalf("lost commit receipt reconstructed implicitly: %v", err)
	}
	if prompts.Len() != 0 || len(f.launchd.commands) != 0 || !equalFleetFiles(before, installedUpgradeFiles(t, &upgradeFixture{setupFixture: f})) {
		t.Fatal("missing receipt changed committed install")
	}
}

func TestSetupControlPlanePreflightsServicePlanBeforeKeys(t *testing.T) {
	f, e, prompts := newFleetFixture(t)
	longHome := filepath.Join(f.home, strings.Repeat("x", 70))
	if err := os.Mkdir(longHome, 0700); err != nil {
		t.Fatal(err)
	}
	f.home, f.host.home = longHome, longHome
	_, err := runSetup(t.Context(), f.host, setupRequest{role: "control-plane", nodeID: "studio-0123abcd"})
	if err == nil || !strings.Contains(err.Error(), "100 bytes") {
		t.Fatalf("invalid client socket path accepted: %v", err)
	}
	if len(e.roles) != 0 || prompts.Len() != 0 {
		t.Fatalf("service-plan refusal consumed keys: %v %s", e.roles, prompts)
	}
	if _, err := os.Stat(filepath.Join(longHome, setupFolders.config, fleetSetupRecordName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("service-plan refusal staged an installation: %v", err)
	}
}

func TestSetupControlPlaneRefusesUnrecordedAuthorityAndServices(t *testing.T) {
	for _, kind := range []string{"l1.sqlite-wal", "l3.sqlite-shm", "l1.sqlite-journal", "plist", "loaded service"} {
		t.Run(kind, func(t *testing.T) {
			f, e, prompts := newFleetFixture(t)
			switch kind {
			case "loaded service":
				f.launchd.loaded[setupL1Label] = true
			case "plist":
				folder := filepath.Join(f.home, setupFolders.launchAgents)
				if err := os.MkdirAll(folder, 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(folder, setupL1Label+".plist"), []byte("old definition"), 0600); err != nil {
					t.Fatal(err)
				}
			default:
				folder := filepath.Join(f.home, setupFolders.data)
				if err := os.MkdirAll(folder, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(folder, kind), []byte("old authority"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			_, err := runSetup(t.Context(), f.host, setupRequest{role: "control-plane", nodeID: "studio-0123abcd"})
			if err == nil || len(e.roles) != 0 || prompts.Len() != 0 || len(f.launchd.mutating()) != 0 {
				t.Fatalf("unrecorded %s was adopted or converted: %v roles=%v", kind, err, e.roles)
			}
		})
	}
}

func TestSetupControlPlanePreflightsEveryParticipantFolder(t *testing.T) {
	for _, populated := range []bool{false, true} {
		t.Run(fmt.Sprint(populated), func(t *testing.T) {
			f, e, prompts := newFleetFixture(t)
			folder := filepath.Join(f.home, setupFolders.data, "fabric", "client")
			if err := os.MkdirAll(folder, 0700); err != nil {
				t.Fatal(err)
			}
			if populated {
				if err := os.WriteFile(filepath.Join(folder, "foreign"), []byte("foreign state"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			_, err := runSetup(t.Context(), f.host, setupRequest{role: "control-plane", nodeID: "studio-0123abcd"})
			if err == nil || !strings.Contains(err.Error(), "unrecorded") || len(e.roles) != 0 || prompts.Len() != 0 {
				t.Fatalf("foreign folder was not refused before keys: %v enrolled=%v prompts=%s", err, e.roles, prompts)
			}
		})
	}
}

func TestSetupControlPlaneRejectsAmbientCoordinatorConflictBeforeKeys(t *testing.T) {
	for _, recorded := range []bool{false, true} {
		t.Run(fmt.Sprint(recorded), func(t *testing.T) {
			f, e, prompts := newFleetFixture(t)
			request := setupRequest{role: "control-plane", nodeID: "studio-0123abcd", fleetCoordinatorURL: "https://coordinator.example.test"}
			if recorded {
				request.afterFleetStep = func(step string) error {
					if step == "stage-profile" {
						return errors.New("interrupted")
					}
					return nil
				}
				if _, err := runSetup(t.Context(), f.host, request); err == nil {
					t.Fatal("expected interruption")
				}
				request = setupRequest{role: "control-plane"}
			}
			t.Setenv("TS_CONTROL_URL", "https://other.example.test")
			_, err := runSetup(t.Context(), f.host, request)
			if err == nil || !strings.Contains(err.Error(), "coordinator") || len(e.roles) != 0 || prompts.Len() != 0 {
				t.Fatalf("coordinator conflict consumed keys: %v enrolled=%v prompts=%s", err, e.roles, prompts)
			}
		})
	}
}

func TestSetupControlPlaneRefusesOrphanStagedProfile(t *testing.T) {
	f, e, prompts := newFleetFixture(t)
	_, err := runSetup(t.Context(), f.host, setupRequest{role: "control-plane", nodeID: "studio-0123abcd", afterFleetStep: func(step string) error {
		if step == "stage-profile" {
			return errors.New("interrupted")
		}
		return nil
	}})
	if err == nil {
		t.Fatal("expected interruption")
	}
	record := filepath.Join(f.home, setupFolders.config, fleetSetupRecordName)
	if err := os.Remove(record); err != nil {
		t.Fatal(err)
	}
	if _, err := runSetup(t.Context(), f.host, setupRequest{}); err == nil || !strings.Contains(err.Error(), "resume record") {
		t.Fatalf("plain setup adopted orphan stage: %v", err)
	}
	_, err = runSetup(t.Context(), f.host, setupRequest{role: "control-plane", nodeID: "studio-0123abcd"})
	if err == nil || !strings.Contains(err.Error(), "resume record") || len(e.roles) != 0 || prompts.Len() != 0 {
		t.Fatalf("orphan stage adopted: %v enrolled=%v prompts=%s", err, e.roles, prompts)
	}
	if _, err := os.Stat(record); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan stage rewritten: %v", err)
	}
}

func TestSetupControlPlaneRefusesCommittedProfileEditsBeforeServices(t *testing.T) {
	for _, change := range []string{"policy", "format", "missing digest"} {
		t.Run(change, func(t *testing.T) {
			f, _, prompts := newFleetFixture(t)
			f.setup(t, setupRequest{role: "control-plane", nodeID: "studio-0123abcd"})
			paths, _ := resolveSetupPaths(f.home)
			if change == "missing digest" {
				path := filepath.Join(paths.config, setupManifestName)
				payload, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var manifest setupManifest
				if err := json.Unmarshal(payload, &manifest); err != nil {
					t.Fatal(err)
				}
				delete(manifest.Files, filepath.Join(paths.configRel, connprofile.FileName))
				payload, _ = json.Marshal(manifest)
				if err := os.WriteFile(path, payload, 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				p := f.profile(t)
				if change == "policy" {
					p.NodeAdmission[0].MaxOneshotSlots++
				}
				payload, err := connprofile.Encode(p)
				if err != nil {
					t.Fatal(err)
				}
				if change == "format" {
					payload = append(payload, '\n')
				}
				if err := os.WriteFile(paths.profile, payload, 0600); err != nil {
					t.Fatal(err)
				}
			}
			f.launchd.commands = nil
			prompts.Reset()
			f.host.readinessBudget = 20 * time.Millisecond
			_, err := runSetup(t.Context(), f.host, setupRequest{})
			if err == nil || !strings.Contains(err.Error(), "profile") || !strings.Contains(err.Error(), "#901") || len(f.launchd.mutating()) != 0 || prompts.Len() != 0 {
				t.Fatalf("changed profile was not refused before services: %v commands=%v prompts=%s", err, f.launchd.commands, prompts)
			}
		})
	}
}

func TestSetupControlPlaneRecreatesMissingReadyFolderOnRerun(t *testing.T) {
	f, e, prompts := newFleetFixture(t)
	f.setup(t, setupRequest{role: "control-plane", nodeID: "studio-0123abcd"})
	ready := filepath.Join(f.home, setupFolders.data, "setup-ready")
	if err := os.RemoveAll(ready); err != nil {
		t.Fatal(err)
	}
	f.launchd.loaded = map[string]bool{}
	prompts.Reset()
	f.launchd.commands = nil
	f.host.readinessBudget = 20 * time.Millisecond
	report, err := runSetup(t.Context(), f.host, setupRequest{})
	if err != nil || len(report.Services) != 4 || len(e.roles) != 4 || prompts.Len() != 0 {
		t.Fatalf("missing ready folder prevented restart: %v %+v", err, report)
	}
	info, err := os.Lstat(ready)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		t.Fatalf("ready folder is not private: %v %v", info, err)
	}
}

func TestSetupControlPlaneRefusesLinkedReadyFolderBeforeKeys(t *testing.T) {
	f, e, prompts := newFleetFixture(t)
	ready := filepath.Join(f.home, setupFolders.data, "setup-ready")
	if err := os.MkdirAll(filepath.Dir(ready), 0700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(f.home, "ready-target")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(filepath.Dir(ready), target)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(relative, ready); err != nil {
		t.Fatal(err)
	}
	_, err = runSetup(t.Context(), f.host, setupRequest{role: "control-plane", nodeID: "studio-0123abcd"})
	if err == nil || !strings.Contains(err.Error(), "unlinked") || len(e.roles) != 0 || prompts.Len() != 0 {
		t.Fatalf("linked ready folder not refused before keys: %v roles=%v prompts=%s", err, e.roles, prompts)
	}
	entries, err := os.ReadDir(target)
	if err != nil || len(entries) != 0 {
		t.Fatalf("linked folder changed: %v %v", entries, err)
	}
}

func TestSetupControlPlaneRequiresReadyPointAfterSystemBoot(t *testing.T) {
	for _, issue := range []string{"previous boot", "at boot", "missing timestamp", "boot time unavailable", "empty boot time", "current boot"} {
		t.Run(issue, func(t *testing.T) {
			f, _, prompts := newFleetFixture(t)
			f.setup(t, setupRequest{role: "control-plane", nodeID: "studio-0123abcd"})
			f.host.readinessBudget = 20 * time.Millisecond
			readyAt := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
			f.host.bootTime = func() (time.Time, error) {
				switch issue {
				case "previous boot":
					return readyAt.Add(time.Hour), nil
				case "at boot":
					return readyAt, nil
				case "boot time unavailable":
					return time.Time{}, errors.New("fixture boot unavailable")
				case "empty boot time":
					return time.Time{}, nil
				default:
					return readyAt.Add(-time.Hour), nil
				}
			}
			if issue == "missing timestamp" {
				path := filepath.Join(f.home, setupFolders.data, "setup-ready", "l1.json")
				if err := os.WriteFile(path, []byte(`{"pid":4242,"address":"old-listener"}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			prompts.Reset()
			f.launchd.commands = nil
			report, err := runSetup(t.Context(), f.host, setupRequest{skipSmokeRun: true})
			if issue == "current boot" {
				if err != nil || report.Readiness[0].Status != "VERIFIED" {
					t.Fatalf("fresh record refused: %v %+v", err, report.Readiness)
				}
				return
			}
			var notReady *notReadyError
			if !errors.As(err, &notReady) || report.Readiness[0].Status != "FAILED" || !strings.Contains(report.Readiness[0].Detail, "boot") || len(f.launchd.mutating()) != 0 || prompts.Len() != 0 {
				t.Fatalf("same PID hid stale boot evidence: %v %+v commands=%v", err, report.Readiness, f.launchd.commands)
			}
		})
	}
}
