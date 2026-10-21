package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/l1"
)

type conversionFixture struct {
	*setupFixture
	enroller     *fakeSetupEnroller
	prompts      *bytes.Buffer
	node         l1.Node
	now          time.Time
	computers    []l1.Computer
	waits        int
	finishOnWait bool
	originals    map[string]string
}

func newConversionFixture(t *testing.T) *conversionFixture {
	t.Helper()
	f, e, prompts := newJoinFixture(t)
	plainHost := f.host
	plainHost.launchctl = f.launchd.run
	if _, err := runSetup(t.Context(), plainHost, setupRequest{nodeID: "studio-0123abcd"}); err != nil {
		t.Fatal(err)
	}
	paths, _ := resolveSetupPaths(f.home)
	if err := os.MkdirAll(filepath.Join(paths.data, "setup-ready"), 0700); err != nil {
		t.Fatal(err)
	}
	x := &conversionFixture{setupFixture: f, enroller: e, prompts: prompts, now: time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC), node: l1.Node{NodeRegistration: contract.NodeRegistration{NodeID: "studio-0123abcd"}, ClaimsEnabled: true}, originals: map[string]string{}}
	for _, name := range append(slices.Clone(plainHistoryEntries[:len(plainHistoryEntries)-1]), "exec-captures/a.bundle", "exec-captures/.upload-partial", "exec-captures/nested/b.bundle") {
		path := filepath.Join(paths.data, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		payload := "closed-history-" + name
		if err := os.WriteFile(path, []byte(payload), 0600); err != nil {
			t.Fatal(err)
		}
		x.originals[name] = payload
	}
	if err := os.WriteFile(filepath.Join(paths.logs, "l1.log"), []byte("keep-log-in-place"), 0600); err != nil {
		t.Fatal(err)
	}
	observe := func(context.Context, connprofile.Profile) (l1.Node, error) { return x.node, nil }
	f.host.upgrade = &upgradeHost{now: func() time.Time { return x.now }, wait: func(_ context.Context, d time.Duration) error {
		x.now = x.now.Add(d)
		x.waits++
		if x.finishOnWait {
			x.node.ActiveAttempts = nil
		}
		return nil
	}, observe: observe, readNode: observe, setClaims: func(_ context.Context, _ connprofile.Profile, r l1.NodeIntentRequest) (l1.Node, error) {
		if r.IntentRevision != x.node.IntentRevision {
			return l1.Node{}, errors.New("fixture intent revision conflict")
		}
		x.node.IntentRevision++
		x.node.ClaimsEnabled = r.ClaimsEnabled
		x.node.IntentReason = r.Reason
		return x.node, nil
	}}
	f.host.conversionFilesClosed = func(context.Context, []string) error { return nil }
	f.host.conversionClients = func(context.Context, connprofile.Profile) (*apiClients, error) {
		return conversionInventoryClient(nil, nil), nil
	}
	f.host.conversionComputers = func(context.Context, connprofile.Profile) ([]l1.Computer, error) { return x.computers, nil }
	f.host.ownerLogin = func(context.Context, string) error { return nil }
	f.launchd.commands = nil
	t.Cleanup(func() {
		// History intentionally has no write bits. Release only this temp fixture.
		_ = filepath.WalkDir(f.home, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return os.Chmod(path, 0700)
			}
			return nil
		})
	})
	return x
}

func (f *conversionFixture) request(t *testing.T) setupRequest {
	return setupRequest{role: "join", convertPlain: true, joinCode: joinCodeForTest(t, "opaque-l1", "opaque-l3", "")}
}

func (f *conversionFixture) assertPlain(t *testing.T, before map[string]string) {
	t.Helper()
	if !f.node.ClaimsEnabled {
		t.Fatal("claims left disabled")
	}
	if after := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f.setupFixture}); !reflect.DeepEqual(before, after) {
		t.Fatalf("plain installation bytes changed: before=%v after=%v", before, after)
	}
	for _, label := range []string{setupL1Label, setupL3Label, setupNodeAgentLabel} {
		if !f.launchd.loaded[label] || f.launchd.stopped[label] {
			t.Fatalf("plain service not restored: %s", label)
		}
	}
	paths, _ := resolveSetupPaths(f.home)
	for name, want := range f.originals {
		got, err := os.ReadFile(filepath.Join(paths.data, name))
		if err != nil || string(got) != want {
			t.Fatalf("active dataset %s not restored: %s %v", name, got, err)
		}
	}
	if _, err := os.Stat(filepath.Join(paths.data, plainHistoryFolder)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("partial history left behind: %v", err)
	}
}

func TestSetupConvertPlainFullHistoryAndJoin(t *testing.T) {
	for _, person := range []bool{false, true} {
		t.Run(map[bool]string{false: "machine", true: "owner"}[person], func(t *testing.T) {
			f := newConversionFixture(t)
			r := f.request(t)
			r.withPerson = person
			f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "finish-first", AttemptID: "one-shot", Class: contract.JobClassOneShot}}
			f.finishOnWait = true
			report, err := runSetup(t.Context(), f.host, r)
			if err != nil {
				t.Fatal(err)
			}
			if f.waits == 0 {
				t.Fatal("did not drain the one-shot")
			}
			roles := []string{"node-agent", "client"}
			if person {
				roles = append(roles, "person")
			}
			if !slices.Equal(f.enroller.roles, roles) || strings.Count(f.prompts.String(), "Single-use join key") != 2 {
				t.Fatalf("wrong identities or keys: %v %s", f.enroller.roles, f.prompts)
			}
			if report.History == nil || !report.History.ReadOnly || !report.History.NeverRestore || report.History.Size == 0 {
				t.Fatalf("history report: %+v", report)
			}
			var size int64
			for name, want := range f.originals {
				size += int64(len(want))
				path := filepath.Join(report.History.Directory, name)
				got, err := os.ReadFile(path)
				if err != nil || string(got) != want {
					t.Fatalf("lost %s: %v", name, err)
				}
				info, _ := os.Stat(path)
				if info.Mode().Perm()&0222 != 0 {
					t.Fatalf("writable history: %s", name)
				}
				sum := sha256.Sum256([]byte(want))
				found := false
				for _, saved := range report.History.Files {
					if saved.Name == name {
						found = saved.Size == int64(len(want)) && saved.SHA256 == hex.EncodeToString(sum[:])
						break
					}
				}
				if !found {
					t.Fatalf("history receipt size or hash differs from original bytes: %s", name)
				}
			}
			if report.History.Size != size {
				t.Fatalf("history size %d want %d", report.History.Size, size)
			}
			paths, _ := resolveSetupPaths(f.home)
			root, _ := os.OpenRoot(f.home)
			defer root.Close()
			if err := verifyPlainHistory(root, filepath.Join(paths.dataRel, plainHistoryFolder, report.NodeID), *report.History, true); err != nil {
				t.Fatal(err)
			}
			for _, label := range []string{setupL1Label, setupL3Label} {
				if f.launchd.loaded[label] {
					t.Fatalf("old authority loaded: %s", label)
				}
				if _, err := os.Stat(filepath.Join(paths.launchAgents, label+".plist")); !errors.Is(err, fs.ErrNotExist) {
					t.Fatal("old authority definition remains")
				}
			}
			for _, label := range []string{setupNodeAgentLabel, "dev.wefty.client-service"} {
				if !f.launchd.loaded[label] {
					t.Fatalf("joining service not running: %s", label)
				}
			}
			if len(f.launchd.loaded) != 2 {
				t.Fatalf("extra services: %v", f.launchd.loaded)
			}
			for _, entry := range plainHistoryEntries {
				if _, err := os.Stat(filepath.Join(paths.data, entry)); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("active authority left: %s", entry)
				}
			}
			if b, err := os.ReadFile(filepath.Join(paths.logs, "l1.log")); err != nil || string(b) != "keep-log-in-place" {
				t.Fatal("log moved")
			}
			var receipt setupManifest
			b, _ := os.ReadFile(filepath.Join(paths.config, setupManifestName))
			if json.Unmarshal(b, &receipt) != nil || !reflect.DeepEqual(receipt.History, report.History) {
				t.Fatal("history missing from role receipt")
			}
			if f.profile(t).Identities.Node != "studio-0123abcd" {
				t.Fatal("stable node ID changed")
			}
			var text bytes.Buffer
			if err := writeSetupReport(&text, report); err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{"Retained history:", "bytes). Read-only; never restored (ADR-0003)", "Installation role: join", "#901"} {
				if !strings.Contains(text.String(), want) {
					t.Fatalf("report omitted %q: %s", want, &text)
				}
			}
			assertJoinArtifactsPrivate(t, f.home, f.prompts.String(), report, "secret-node", "secret-client", "https://login.invalid/one-use")
			f.host.enrolmentInput = strings.NewReader("")
			if _, err := runSetup(t.Context(), f.host, setupRequest{}); err != nil {
				t.Fatalf("committed rerun: %v", err)
			}
			if len(f.enroller.roles) != len(roles) {
				t.Fatal("rerun re-enrolled")
			}
		})
	}
}

func TestSetupConvertPlainRunningWorkRefusedAndClaimsRestored(t *testing.T) {
	for _, kind := range []string{"one-shot", "service", "computer"} {
		t.Run(kind, func(t *testing.T) {
			f := newConversionFixture(t)
			switch kind {
			case "one-shot":
				f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "long-one-shot", AttemptID: "busy", Class: contract.JobClassOneShot}}
			case "service":
				f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "long-service", AttemptID: "resident", Class: contract.JobClassService}}
			case "computer":
				f.computers = []l1.Computer{{ComputerID: "resident-computer", BoundNodeID: f.node.NodeID}}
			}
			before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f.setupFixture})
			_, err := runSetup(t.Context(), f.host, f.request(t))
			if err == nil || commandExitCode(err) != exitConflict || !strings.Contains(err.Error(), "retry later") {
				t.Fatalf("wrong refusal: %v", err)
			}
			if kind == "service" && !strings.Contains(err.Error(), "long-service") {
				t.Fatal("service not listed")
			}
			if kind == "computer" && !strings.Contains(err.Error(), "resident-computer") {
				t.Fatal("Computer not listed")
			}
			if len(f.launchd.mutating()) != 0 || len(f.enroller.roles) != 0 {
				t.Fatal("stopped work or enrolled before drain")
			}
			f.assertPlain(t, before)
		})
	}
}

func TestSetupConvertPlainFailureAndCrashRecovery(t *testing.T) {
	beforeSteps := []string{"convert:record", "convert:claims", "convert:drain", "convert:stop", "convert:inventory", "convert:readonly:exec-captures/nested/b.bundle", "convert:readonly"}
	for _, label := range []string{setupNodeAgentLabel, setupL3Label, setupL1Label} {
		beforeSteps = append(beforeSteps, "convert:stopped:"+label)
	}
	for _, entry := range plainHistoryEntries {
		beforeSteps = append(beforeSteps, "convert:moved:"+entry)
	}
	afterSteps := []string{"convert:history", "convert:joining", "stage-record", "stage-profile", "enrolled:node-agent", "enrolled:client", "peer-verification", "profile", "receipt", "committed"}
	for _, crash := range []bool{false, true} {
		for _, stage := range append(slices.Clone(beforeSteps), afterSteps...) {
			t.Run(map[bool]string{false: "failure/", true: "crash/"}[crash]+stage, func(t *testing.T) {
				f := newConversionFixture(t)
				before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f.setupFixture})
				r := f.request(t)
				r.afterFleetStep = func(got string) error {
					if got == stage {
						if crash {
							panic("fixture process death")
						}
						return errors.New("injected interruption")
					}
					return nil
				}
				func() {
					defer func() {
						if v := recover(); v != nil && (!crash || v != "fixture process death") {
							panic(v)
						}
					}()
					_, err := runSetup(t.Context(), f.host, r)
					if err == nil {
						t.Fatal("missing interruption")
					}
				}()
				if !crash && slices.Contains(beforeSteps, stage) {
					f.assertPlain(t, before)
				}
				if slices.Contains(afterSteps, stage) {
					if f.launchd.loaded[setupL1Label] || f.launchd.loaded[setupL3Label] {
						t.Fatal("old authority running after history boundary")
					}
					paths, _ := resolveSetupPaths(f.home)
					saved, err := readPlainConversion(paths)
					if err != nil || saved == nil || !conversionForward(saved) {
						t.Fatalf("unrecorded history: %+v %v", saved, err)
					}
				}
				r.afterFleetStep = nil
				keys := []string{"secret-node", "secret-client"}
				f.host.enrolmentInput = strings.NewReader(strings.Join(keys[len(f.enroller.roles):], "\n") + "\n")
				report, err := runSetup(t.Context(), f.host, r)
				if err != nil {
					t.Fatalf("resume %s: %v", stage, err)
				}
				if report.History == nil || len(f.enroller.roles) != 2 || f.launchd.loaded[setupL1Label] || f.launchd.loaded[setupL3Label] {
					t.Fatal("resume reopened history or replaced identities")
				}
			})
		}
	}
}

func TestSetupConvertPlainActivationResumes(t *testing.T) {
	for _, index := range []int{0, 1} {
		t.Run(string(rune('0'+index)), func(t *testing.T) {
			f := newConversionFixture(t)
			r := f.request(t)
			r.afterService = func(i int, _ launchagent.Action) error {
				if i == index {
					return errors.New("activation interrupted")
				}
				return nil
			}
			if _, err := runSetup(t.Context(), f.host, r); err == nil {
				t.Fatal("missing activation failure")
			}
			r.afterService = nil
			if _, err := runSetup(t.Context(), f.host, r); err != nil {
				t.Fatal(err)
			}
			if len(f.enroller.roles) != 2 || len(f.launchd.loaded) != 2 {
				t.Fatal("activation changed identities or service set")
			}
		})
	}
}

func TestSetupConvertPlainPurgeNeverDeletesHistory(t *testing.T) {
	f := newConversionFixture(t)
	report, err := runSetup(t.Context(), f.host, f.request(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, purge := range []bool{false, true} {
		if _, err := runSetupRemove(t.Context(), f.host, purge); err == nil {
			t.Fatal("fleet removal fence changed")
		}
	}
	paths, _ := resolveSetupPaths(f.home)
	root, _ := os.OpenRoot(f.home)
	defer root.Close()
	var removal setupRemoveReport
	if err := purgeSetupEntries(root, paths, &removal); err != nil {
		t.Fatal(err)
	}
	if err := verifyPlainHistory(root, filepath.Join(paths.dataRel, plainHistoryFolder, report.NodeID), *report.History, true); err != nil {
		t.Fatal(err)
	}
	for _, name := range removal.Changes {
		if strings.Contains(name, plainHistoryFolder) {
			t.Fatal("history included in purge allowlist")
		}
	}
}

func TestSetupConvertPlainExplicitOnlyAndStableNode(t *testing.T) {
	for _, r := range []setupRequest{{role: "join"}, {role: "control-plane"}, {role: "control-plane", convertPlain: true}, {role: "join", convertPlain: true, nodeID: "another-node"}} {
		t.Run(r.role+"/"+r.nodeID, func(t *testing.T) {
			f := newConversionFixture(t)
			r.joinCode = f.request(t).joinCode
			before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f.setupFixture})
			if _, err := runSetup(t.Context(), f.host, r); err == nil {
				t.Fatal("implicit conversion permitted")
			}
			if len(f.launchd.mutating()) != 0 {
				t.Fatal("invalid conversion touched services")
			}
			f.assertPlain(t, before)
		})
	}
}

func TestSetupConvertPlainRetainsOperatorClaimsDisable(t *testing.T) {
	f := newConversionFixture(t)
	f.node.ClaimsEnabled = false
	f.node.IntentRevision = 9
	f.node.IntentReason = "owner maintenance"
	r := f.request(t)
	r.afterFleetStep = func(step string) error {
		if step == "convert:stop" {
			return errors.New("stop")
		}
		return nil
	}
	if _, err := runSetup(t.Context(), f.host, r); err == nil {
		t.Fatal("missing failure")
	}
	if f.node.ClaimsEnabled || f.node.IntentRevision != 9 || f.node.IntentReason != "owner maintenance" {
		t.Fatal("conversion replaced the owner's intent")
	}
}

func TestSetupConvertPlainStopMustConfirmAbsence(t *testing.T) {
	for _, failure := range []string{"bootout-refused", "still-loaded"} {
		t.Run(failure, func(t *testing.T) {
			f := newConversionFixture(t)
			before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f.setupFixture})
			original := f.host.launchctl
			f.host.unloadBound = time.Millisecond
			once := true
			f.host.launchctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if args[0] == "bootout" && strings.HasSuffix(args[1], setupL3Label) && once {
					once = false
					if failure == "bootout-refused" {
						return nil, errors.New("fixture refused bootout")
					}
					return nil, nil // Successful command is not proof that the process exited.
				}
				return original(ctx, name, args...)
			}
			if _, err := runSetup(t.Context(), f.host, f.request(t)); err == nil {
				t.Fatal("converted without confirmed absence")
			}
			f.assertPlain(t, before)
			if len(f.enroller.roles) != 0 {
				t.Fatal("enrolled before verifying service absence")
			}
		})
	}
}

func TestSetupConvertPlainLostDisableReplyRestoresClaims(t *testing.T) {
	f := newConversionFixture(t)
	before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f.setupFixture})
	original := f.host.upgrade.setClaims
	f.host.upgrade.setClaims = func(ctx context.Context, p connprofile.Profile, r l1.NodeIntentRequest) (l1.Node, error) {
		node, err := original(ctx, p, r)
		if err == nil && !r.ClaimsEnabled {
			return node, errors.New("fixture lost disable response")
		}
		return node, err
	}
	if _, err := runSetup(t.Context(), f.host, f.request(t)); err == nil {
		t.Fatal("lost reply was ignored")
	}
	f.assertPlain(t, before)
}

func TestSetupConvertPlainLateWorkBlocksStop(t *testing.T) {
	for _, kind := range []string{"one-shot", "service", "computer"} {
		t.Run(kind, func(t *testing.T) {
			f := newConversionFixture(t)
			before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f.setupFixture})
			r := f.request(t)
			r.afterFleetStep = func(step string) error {
				if step == "convert:drain" {
					switch kind {
					case "one-shot":
						f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "late", AttemptID: "late", Class: contract.JobClassOneShot}}
					case "service":
						f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "late", AttemptID: "late", Class: contract.JobClassService}}
					case "computer":
						f.computers = []l1.Computer{{ComputerID: "late", PlacementNodeID: f.node.NodeID}}
					}
				}
				return nil
			}
			if _, err := runSetup(t.Context(), f.host, r); err == nil || commandExitCode(err) != exitConflict {
				t.Fatalf("late work not refused: %v", err)
			}
			if len(f.launchd.mutating()) != 0 {
				t.Fatal("late work was stopped")
			}
			f.assertPlain(t, before)
		})
	}
}

func TestSetupConvertPlainHistoryTamperFailsForward(t *testing.T) {
	f := newConversionFixture(t)
	r := f.request(t)
	r.afterFleetStep = func(step string) error {
		if step == "convert:history" {
			return errors.New("before join")
		}
		return nil
	}
	if _, err := runSetup(t.Context(), f.host, r); err == nil {
		t.Fatal("missing interruption")
	}
	paths, _ := resolveSetupPaths(f.home)
	saved, err := readPlainConversion(paths)
	if err != nil {
		t.Fatal(err)
	}
	history := filepath.Join(saved.History.Directory, "l1.sqlite")
	if err := os.Chmod(history, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(history, []byte("changed"), 0400); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(history, 0400); err != nil {
		t.Fatal(err)
	}
	r.afterFleetStep = nil
	if _, err := runSetup(t.Context(), f.host, r); err == nil || !strings.Contains(err.Error(), "history hash or size") {
		t.Fatalf("tampered history accepted: %v", err)
	}
	if f.launchd.loaded[setupL1Label] || f.launchd.loaded[setupL3Label] || len(f.enroller.roles) != 0 {
		t.Fatal("history reopened or join started after tamper")
	}
}

func TestSetupConvertPlainRejectsLinkedHistory(t *testing.T) {
	for _, hard := range []bool{false, true} {
		t.Run(map[bool]string{false: "symlink", true: "hardlink"}[hard], func(t *testing.T) {
			f := newConversionFixture(t)
			paths, _ := resolveSetupPaths(f.home)
			outside := filepath.Join(f.home, "outside-capture")
			if err := os.WriteFile(outside, []byte("private fixture"), 0600); err != nil {
				t.Fatal(err)
			}
			link := filepath.Join(paths.data, "exec-captures", "linked")
			var err error
			if hard {
				err = os.Link(outside, link)
			} else {
				err = os.Symlink(outside, link)
			}
			if err != nil {
				t.Fatal(err)
			}
			before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f.setupFixture})
			if _, err := runSetup(t.Context(), f.host, f.request(t)); err == nil || !strings.Contains(err.Error(), "link or special file") {
				t.Fatalf("linked history accepted: %v", err)
			}
			info, _ := os.Stat(outside)
			if info.Mode().Perm() != 0600 {
				t.Fatal("conversion changed a file outside its dataset")
			}
			f.assertPlain(t, before)
		})
	}
}

func TestSetupConvertPlainPersonEnrollmentResumes(t *testing.T) {
	f := newConversionFixture(t)
	r := f.request(t)
	r.withPerson = true
	r.afterFleetStep = func(step string) error {
		if step == "enrolled:person" {
			return errors.New("after person saved")
		}
		return nil
	}
	if _, err := runSetup(t.Context(), f.host, r); err == nil {
		t.Fatal("missing interruption")
	}
	f.host.enrolmentInput = strings.NewReader("")
	r.afterFleetStep = nil
	if _, err := runSetup(t.Context(), f.host, r); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(f.enroller.roles, []string{"node-agent", "client", "person"}) {
		t.Fatal("re-enrolled a saved participant")
	}
}

func TestSetupConvertPlainCrashDuringPlainRestoration(t *testing.T) {
	f := newConversionFixture(t)
	r := f.request(t)
	r.afterFleetStep = func(step string) error {
		if step == "convert:moved:l1.sqlite" {
			return errors.New("restore before history boundary")
		}
		return nil
	}
	runner := f.host.launchctl
	paths, _ := resolveSetupPaths(f.home)
	f.host.launchctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		payload, err := runner(ctx, name, args...)
		if err == nil && args[0] == "bootstrap" && strings.HasSuffix(args[2], setupL1Label+".plist") {
			if err := os.WriteFile(filepath.Join(paths.data, "l1.sqlite"), []byte("new plain authority writes"), 0600); err != nil {
				t.Fatal(err)
			}
			panic("fixture dies during restore")
		}
		return payload, err
	}
	func() {
		defer func() {
			if v := recover(); v != "fixture dies during restore" {
				t.Fatalf("missing crash: %v", v)
			}
		}()
		_, _ = runSetup(t.Context(), f.host, r)
	}()
	saved, err := readPlainConversion(paths)
	if err != nil || saved.Phase != "restoring" {
		t.Fatalf("restore checkpoint missing: %+v %v", saved, err)
	}
	f.host.launchctl = runner
	r.afterFleetStep = nil
	report, err := runSetup(t.Context(), f.host, r)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(filepath.Join(report.History.Directory, "l1.sqlite"))
	if err != nil || string(payload) != "new plain authority writes" {
		t.Fatal("recovery rewound the restored authority")
	}
}

func TestSetupConvertPlainUnknownComputerInventoryRefuses(t *testing.T) {
	f := newConversionFixture(t)
	before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f.setupFixture})
	f.host.conversionComputers = func(context.Context, connprofile.Profile) ([]l1.Computer, error) {
		return nil, errors.New("fixture Computer inventory unavailable")
	}
	if _, err := runSetup(t.Context(), f.host, f.request(t)); err == nil || !strings.Contains(err.Error(), "inventory unavailable") {
		t.Fatalf("unknown inventory accepted: %v", err)
	}
	f.assertPlain(t, before)
	if len(f.launchd.mutating()) != 0 {
		t.Fatal("unknown inventory stopped the plain stack")
	}
}

func TestSetupConvertPlainCorrectedJoinCodeSurvivesRerun(t *testing.T) {
	f := newConversionFixture(t)
	r := f.request(t)
	r.joinCode = joinCodeForTest(t, "wrong-peer", "opaque-l3", "")
	if _, err := runSetup(t.Context(), f.host, r); err == nil || !strings.Contains(err.Error(), "peer verification failed") {
		t.Fatalf("wrong peers accepted: %v", err)
	}
	r.joinCode = f.request(t).joinCode
	f.host.enrolmentInput = strings.NewReader("")
	if _, err := runSetup(t.Context(), f.host, r); err != nil {
		t.Fatal(err)
	}
	if _, err := runSetup(t.Context(), f.host, setupRequest{}); err != nil {
		t.Fatalf("rerun forgot corrected code: %v", err)
	}
	if len(f.enroller.roles) != 2 {
		t.Fatal("code correction re-enrolled participants")
	}
}

func TestSetupConvertPlainHistoryReceiptCannotBeDropped(t *testing.T) {
	f := newConversionFixture(t)
	if _, err := runSetup(t.Context(), f.host, f.request(t)); err != nil {
		t.Fatal(err)
	}
	paths, _ := resolveSetupPaths(f.home)
	name := filepath.Join(paths.config, setupManifestName)
	payload, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var receipt setupManifest
	if err := json.Unmarshal(payload, &receipt); err != nil {
		t.Fatal(err)
	}
	receipt.History = nil
	payload, _ = json.Marshal(receipt)
	if err := os.WriteFile(name, payload, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := runSetup(t.Context(), f.host, setupRequest{}); err == nil || !strings.Contains(err.Error(), "missing history metadata") {
		t.Fatalf("missing history receipt accepted: %v", err)
	}
}

func TestSetupConvertPlainResumeFailureReportsHistory(t *testing.T) {
	f := newConversionFixture(t)
	r := f.request(t)
	r.afterFleetStep = func(step string) error {
		if step == "convert:history" {
			return errors.New("before join")
		}
		return nil
	}
	initial, err := runSetup(t.Context(), f.host, r)
	if err == nil || initial.History == nil {
		t.Fatalf("missing retained history on interruption: %+v %v", initial, err)
	}
	r.afterFleetStep = nil
	r.joinCode = "invalid-code"
	report, err := runSetup(t.Context(), f.host, r)
	if err == nil || report.JoinResult != "incomplete" || !reflect.DeepEqual(report.History, initial.History) {
		t.Fatalf("resume validation lost retained history: %+v %v", report, err)
	}
	if !strings.Contains(strings.Join(report.Notes, " "), "#901") {
		t.Fatal("resume failure omitted fleet upgrade boundary")
	}
	if f.launchd.loaded[setupL1Label] || f.launchd.loaded[setupL3Label] {
		t.Fatal("validation failure restarted historical authority")
	}
}

func TestSetupConvertPlainRetainsAuthorityIdentityStagingFile(t *testing.T) {
	f := newConversionFixture(t)
	paths, _ := resolveSetupPaths(f.home)
	const name = "l1.sqlite.authority-instance.tmp-0123456789abcdef"
	const payload = "closed authority identity creation staging file"
	if err := os.WriteFile(filepath.Join(paths.data, name), []byte(payload), 0600); err != nil {
		t.Fatal(err)
	}
	const foreign = "l1.sqlite.authority-instance.tmp-owner-notes"
	if err := os.WriteFile(filepath.Join(paths.data, foreign), []byte("keep in place"), 0600); err != nil {
		t.Fatal(err)
	}
	report, err := runSetup(t.Context(), f.host, f.request(t))
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(report.History.Directory, name))
	if err != nil || string(got) != payload {
		t.Fatalf("store-owned identity staging file was not retained: %s %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(paths.data, name)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("identity staging file left in active data")
	}
	if got, err := os.ReadFile(filepath.Join(paths.data, foreign)); err != nil || string(got) != "keep in place" {
		t.Fatal("unowned similarly named file moved")
	}
}

func TestSetupConvertPlainRefusesUnrecordedHistoryFile(t *testing.T) {
	f := newConversionFixture(t)
	r := f.request(t)
	r.afterFleetStep = func(step string) error {
		if step == "convert:history" {
			return errors.New("before join")
		}
		return nil
	}
	report, err := runSetup(t.Context(), f.host, r)
	if err == nil || report.History == nil {
		t.Fatal("missing interruption")
	}
	folder := report.History.Directory
	if err := os.Chmod(folder, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(folder, "unrecorded.sqlite"), []byte("unrecorded authority"), 0400); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(folder, 0500); err != nil {
		t.Fatal(err)
	}
	r.afterFleetStep = nil
	if _, err := runSetup(t.Context(), f.host, r); err == nil || !strings.Contains(err.Error(), "unrecorded file") {
		t.Fatalf("accepted unrecorded history: %v", err)
	}
	if len(f.enroller.roles) != 0 || f.launchd.loaded[setupL1Label] || f.launchd.loaded[setupL3Label] {
		t.Fatal("continued conversion with unrecorded history")
	}
}

// Recovery may die between removing the empty per-node folder and its parent.
// That remaining empty container is recorded conversion state, not an unknown
// history installation, and must not make every later recovery refuse.
func TestSetupConvertPlainCrashAfterHistoryFolderRemovalResumes(t *testing.T) {
	f := newConversionFixture(t)
	r := f.request(t)
	r.afterFleetStep = func(step string) error {
		if step == "convert:inventory" {
			panic("fixture interrupted before moving")
		}
		return nil
	}
	func() {
		defer func() {
			if got := recover(); got != "fixture interrupted before moving" {
				t.Fatalf("missing interruption: %v", got)
			}
		}()
		_, _ = runSetup(t.Context(), f.host, r)
	}()
	paths, _ := resolveSetupPaths(f.home)
	if err := os.Mkdir(filepath.Join(paths.data, plainHistoryFolder), 0700); err != nil {
		t.Fatal(err)
	}
	r.afterFleetStep = nil
	if _, err := runSetup(t.Context(), f.host, r); err != nil {
		t.Fatalf("empty history container blocked recovery: %v", err)
	}
}
