package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/participantstate"
	"github.com/Derek-X-Wang/wefty/fabric/tsnet"
	"github.com/Derek-X-Wang/wefty/fabric/tsnet/teststate"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
)

type fakeSetupEnroller struct {
	roles                []string
	coordinators         []string
	collisionRole        string
	keys                 []string
	loggedOut            []string
	wrongTag, wrongName  string
	rejectedNetworkGrant string
	afterEnrol           func() error
}

func (e *fakeSetupEnroller) Enrol(_ context.Context, spec fabric.EnrolSpec) (fabric.SelfIdentity, error) {
	role := filepath.Base(spec.StateDirectory)
	e.roles = append(e.roles, role)
	e.coordinators = append(e.coordinators, spec.CoordinatorURL)
	e.keys = append(e.keys, spec.Credential.Value)
	if err := teststate.Write(spec.StateDirectory); err != nil {
		return fabric.SelfIdentity{}, err
	}
	networkName, err := fabric.RequestedNetworkName(spec.Name)
	if err != nil {
		return fabric.SelfIdentity{}, err
	}
	if role == e.collisionRole {
		networkName += "-2"
	}
	identity := fabric.SelfIdentity{Identity: fabric.Identity{NodeID: "opaque-" + role, DeviceID: "opaque-" + role, Kind: fabric.IdentityKindMachine}, Name: spec.Name, NetworkName: networkName}
	switch role {
	case "node-agent":
		identity.Tags = []string{"tag:wefty-agent"}
	case "client", "l3":
		identity.Tags = []string{"tag:wefty-client"}
	case "l1":
		identity.Tags = []string{"tag:wefty-control"}
	case "person":
		identity.Kind, identity.UserID = "", "opaque-owner"
		if spec.LoginURL == nil {
			return fabric.SelfIdentity{}, errors.New("person login callback missing")
		}
		spec.LoginURL("https://login.invalid/one-use")
	}
	if role == e.rejectedNetworkGrant {
		return fabric.SelfIdentity{}, fabric.ErrGrantedNameMismatch
	}
	if role == e.wrongTag {
		identity.Tags = []string{"tag:wefty-control"}
	}
	if role == e.wrongName {
		identity.Name = ""
	}
	if e.afterEnrol != nil {
		if err := e.afterEnrol(); err != nil {
			return fabric.SelfIdentity{}, err
		}
	}
	return identity, nil
}

func TestSetupRollsBackParticipantWhenProfileCannotBePublished(t *testing.T) {
	f := newSetupFixture(t)
	paths, err := resolveSetupPaths(f.home)
	if err != nil {
		t.Fatal(err)
	}
	e := &fakeSetupEnroller{afterEnrol: func() error { return os.MkdirAll(paths.profile, 0o700) }}
	_, err = enrolSetupParticipants(t.Context(), setupEnrolmentRequest{home: f.home, data: paths.data, profilePath: paths.profile,
		participants: []setupParticipant{{"client", "wefty://client/machine-1"}}, enroller: e, input: strings.NewReader("single-use-key\n"), output: &bytes.Buffer{}})
	if err == nil || !slices.Equal(e.loggedOut, []string{"client"}) {
		t.Fatalf("publication failure=%v logout=%v", err, e.loggedOut)
	}
	if _, err := os.Lstat(filepath.Join(paths.data, "fabric", "client")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unrecorded enrolment state remains: %v", err)
	}
}

func (e *fakeSetupEnroller) Logout(_ context.Context, spec fabric.EnrolSpec) error {
	if spec.Credential.Value != "" {
		return errors.New("logout received join key")
	}
	e.loggedOut = append(e.loggedOut, filepath.Base(spec.StateDirectory))
	return nil
}

func TestSetupEnrolsParticipantsWithoutPersistingJoinKeys(t *testing.T) {
	f := newSetupFixture(t)
	f.setup(t, setupRequest{})
	paths, err := resolveSetupPaths(f.home)
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{"join-node-single-use", "join-client-single-use", "join-control-single-use", "join-ledger-single-use"}
	t.Setenv("TS_AUTHKEY", "ambient-must-not-survive")
	e := &fakeSetupEnroller{}
	var prompt bytes.Buffer
	keysInput := strings.NewReader(strings.Join(keys, "\n") + "\n")
	request := setupEnrolmentRequest{home: f.home, data: paths.data, profilePath: paths.profile,
		participants: []setupParticipant{{"node-agent", "wefty://node/machine-1"}, {"client", "wefty://client/machine-1"}, {"l1", "wefty://control-plane"}, {"l3", "wefty://run-ledger"}, {"person", "wefty://person/machine-1"}},
		enroller:     e, input: keysInput, output: &prompt, loginURL: func(string) {},
	}
	profile, err := enrolSetupParticipants(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(e.keys, append(keys, "")) || len(profile.Participants) != 5 {
		t.Fatalf("keys were not read once per machine participant; roles=%v", e.roles)
	}
	if os.Getenv("TS_AUTHKEY") != "" {
		t.Fatal("enrolment left an ambient key for later processes")
	}
	if profile.L1Address != "opaque-l1" || profile.L3Address != "opaque-l3" {
		t.Fatalf("opaque endpoints = %q / %q", profile.L1Address, profile.L3Address)
	}
	for role, participant := range profile.Participants {
		want := filepath.Join(paths.data, "fabric", role)
		info, err := os.Stat(want)
		if err != nil || info.Mode().Perm() != 0o700 || participant.StateDirectory != want {
			t.Fatalf("%s folder = %v %v", role, info, err)
		}
	}
	services, err := setupServices(paths, profile, map[string]string{}, "/usr/bin:/bin")
	if err != nil {
		t.Fatal(err)
	}
	for _, service := range services {
		payload, err := launchagent.Render(service)
		if err != nil {
			t.Fatal(err)
		}
		environment, err := json.Marshal(service.Environment)
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			if bytes.Contains(payload, []byte(key)) || bytes.Contains(environment, []byte(key)) {
				t.Fatal("service contains a join key")
			}
		}
	}
	if err := filepath.WalkDir(f.home, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		payload, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, key := range append(keys, "ambient-must-not-survive") {
			if bytes.Contains(payload, []byte(key)) {
				t.Fatalf("join key persisted in %s", path)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(prompt.String(), "join-") {
		t.Fatal("key prompt echoed a join key")
	}
	// A rerun must not touch the input, start participants, or change the profile.
	request.input = failingSetupKeyInput{t}
	before := len(e.roles)
	if _, err := enrolSetupParticipants(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if len(e.roles) != before {
		t.Fatal("rerun started an enrolled participant")
	}
}

type failingSetupKeyInput struct{ t *testing.T }

func (r failingSetupKeyInput) Read([]byte) (int, error) {
	r.t.Fatal("rerun asked for a join key")
	return 0, errors.New("unexpected key read")
}

func TestSetupRollsBackWrongParticipantTagsOrGrantedName(t *testing.T) {
	for _, mismatch := range []string{"tags", "name", "network-grant"} {
		t.Run(mismatch, func(t *testing.T) {
			f := newSetupFixture(t)
			paths, err := resolveSetupPaths(f.home)
			if err != nil {
				t.Fatal(err)
			}
			e := &fakeSetupEnroller{}
			if mismatch == "tags" {
				e.wrongTag = "client"
			} else if mismatch == "network-grant" {
				e.rejectedNetworkGrant = "client"
			} else {
				e.wrongName = "client"
			}
			_, err = enrolSetupParticipants(t.Context(), setupEnrolmentRequest{home: f.home, data: paths.data, profilePath: paths.profile,
				participants: []setupParticipant{{"node-agent", "wefty://node/machine-1"}, {"client", "wefty://client/machine-1"}, {"l3", "wefty://run-ledger"}},
				enroller:     e, input: strings.NewReader("first-key\nwrong-role-key\nnever-read-key\n"), output: &bytes.Buffer{},
			})
			if err == nil {
				t.Fatal("mismatch accepted")
			}
			if mismatch == "network-grant" && !errors.Is(err, fabric.ErrGrantedNameMismatch) {
				t.Fatalf("network rejection lost: %v", err)
			}
			if !slices.Equal(e.roles, []string{"node-agent", "client"}) || !slices.Equal(e.loggedOut, []string{"client"}) {
				t.Fatalf("rollback roles=%v logout=%v", e.roles, e.loggedOut)
			}
			if _, err := os.Lstat(filepath.Join(paths.data, "fabric", "client")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("wrong participant state remains: %v", err)
			}
			profile, err := connprofile.Read(paths.profile)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := profile.Participants["client"]; ok || len(profile.Participants) != 1 {
				t.Fatalf("mismatched participant saved: %+v", profile.Participants)
			}
		})
	}
}

func TestSetupRejectsMalformedLogicalParticipantBeforeEnrolment(t *testing.T) {
	for _, name := range []string{"wefty://control-plane-x", "wefty://control-plane?extra=1", "wefty://person/one"} {
		t.Run(name, func(t *testing.T) {
			f := newSetupFixture(t)
			paths, err := resolveSetupPaths(f.home)
			if err != nil {
				t.Fatal(err)
			}
			e := &fakeSetupEnroller{}
			_, err = enrolSetupParticipants(t.Context(), setupEnrolmentRequest{home: f.home, data: paths.data, profilePath: paths.profile, participants: []setupParticipant{{"l1", name}}, enroller: e, input: strings.NewReader("one-use-key\n")})
			if err == nil || len(e.roles) != 0 {
				t.Fatalf("enrolled=%v err=%v", e.roles, err)
			}
		})
	}
}

func TestSetupMissingRecordedParticipantExplainsRecovery(t *testing.T) {
	f := newSetupFixture(t)
	paths, err := resolveSetupPaths(f.home)
	if err != nil {
		t.Fatal(err)
	}
	e := &fakeSetupEnroller{}
	request := setupEnrolmentRequest{home: f.home, data: paths.data, profilePath: paths.profile, participants: []setupParticipant{{"client", "wefty://client/one"}}, enroller: e, input: strings.NewReader("one-use-key\n")}
	if _, err := enrolSetupParticipants(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(paths.data, "fabric", "client")); err != nil {
		t.Fatal(err)
	}
	_, err = enrolSetupParticipants(t.Context(), request)
	if err == nil || !strings.Contains(err.Error(), "restore") || !strings.Contains(err.Error(), "participant record") || len(e.roles) != 1 {
		t.Fatalf("roles=%v recovery=%v", e.roles, err)
	}
}

func TestSetupEnrolLockRefusalPreservesOwnerFolder(t *testing.T) {
	f := newSetupFixture(t)
	paths, err := resolveSetupPaths(f.home)
	if err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(paths.data, "fabric", "client")
	release, err := participantstate.Acquire(state, "wefty://client/test")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	input, output := strings.NewReader("unused-key\n"), &bytes.Buffer{}
	_, err = enrolSetupParticipants(t.Context(), setupEnrolmentRequest{home: f.home, data: paths.data, profilePath: paths.profile, participants: []setupParticipant{{"client", "wefty://client/test"}}, enroller: tsnet.Enroller{}, input: input, output: output})
	if err == nil || !strings.Contains(err.Error(), "unrecorded identity folder") || !strings.Contains(err.Error(), "remove that folder and rerun setup") || input.Len() != len("unused-key\n") || output.Len() != 0 {
		t.Fatalf("owned unrecorded folder was not preflighted: %v prompts=%s", err, output)
	}
	var inUse *fabric.ParticipantInUseError
	other, err := participantstate.Acquire(state, "wefty://client/test")
	if other != nil {
		_ = other()
	}
	if !errors.As(err, &inUse) {
		t.Fatalf("setup disturbed the original participant lock: %v", err)
	}
	if _, err := os.Stat(filepath.Join(state, participantstate.LockName)); err != nil {
		t.Fatalf("deleted another owner's folder: %v", err)
	}
}

func TestSetupRecordedLockFileIsNotSavedIdentity(t *testing.T) {
	f := newSetupFixture(t)
	paths, err := resolveSetupPaths(f.home)
	if err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(paths.data, "fabric", "client")
	release, err := participantstate.Acquire(state, "wefty://client/test")
	if err != nil {
		t.Fatal(err)
	}
	_ = release()
	if err := connprofile.Write(paths.profile, connprofile.Profile{Version: 1, Fabric: "tsnet", Participants: map[string]connprofile.Participant{"client": {Name: "wefty://client/test", NodeID: "client", StateDirectory: state}}}); err != nil {
		t.Fatal(err)
	}
	enroller := &fakeSetupEnroller{}
	_, err = enrolSetupParticipants(t.Context(), setupEnrolmentRequest{home: f.home, data: paths.data, profilePath: paths.profile, participants: []setupParticipant{{"client", "wefty://client/test"}}, enroller: enroller, input: strings.NewReader("unused-key\n")})
	if err == nil || !strings.Contains(err.Error(), "state is missing") || len(enroller.roles) != 0 {
		t.Fatalf("lock-only record accepted: %v", err)
	}
}

type busyRollbackEnroller struct{ fakeSetupEnroller }

func (e *busyRollbackEnroller) Logout(ctx context.Context, spec fabric.EnrolSpec) error {
	return (tsnet.Enroller{}).Logout(ctx, spec)
}
func TestSetupRollbackLockRefusalPreservesOwnerState(t *testing.T) {
	f := newSetupFixture(t)
	paths, err := resolveSetupPaths(f.home)
	if err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(paths.data, "fabric", "client")
	enroller := &busyRollbackEnroller{fakeSetupEnroller{wrongTag: "client", afterEnrol: func() error {
		release, err := participantstate.Acquire(state, "wefty://client/test")
		if err == nil {
			t.Cleanup(func() { _ = release() })
		}
		return err
	}}}
	_, err = enrolSetupParticipants(t.Context(), setupEnrolmentRequest{home: f.home, data: paths.data, profilePath: paths.profile, participants: []setupParticipant{{"client", "wefty://client/test"}}, enroller: enroller, input: strings.NewReader("unused-key\n")})
	var inUse *fabric.ParticipantInUseError
	if !errors.As(err, &inUse) {
		t.Fatalf("rollback refusal=%v", err)
	}
	if !tsnet.HasSavedState(state) {
		t.Fatal("deleted the new owner's saved state")
	}
}

type newOwnerAfterLogoutEnroller struct {
	fakeSetupEnroller
	state   string
	release func() error
}

func (e *newOwnerAfterLogoutEnroller) Logout(context.Context, fabric.EnrolSpec) error {
	release, err := participantstate.Acquire(e.state, "wefty://client/test")
	if err != nil {
		return err
	}
	e.release = release
	return os.WriteFile(filepath.Join(e.state, "new-owner"), []byte("keep"), 0o600)
}
func TestRollbackRefusesNewOwnerAfterLogoutCloses(t *testing.T) {
	f := newSetupFixture(t)
	paths, err := resolveSetupPaths(f.home)
	if err != nil {
		t.Fatal(err)
	}
	e := &newOwnerAfterLogoutEnroller{fakeSetupEnroller: fakeSetupEnroller{wrongTag: "client"}, state: filepath.Join(paths.data, "fabric", "client")}
	defer func() {
		if e.release != nil {
			_ = e.release()
		}
	}()
	_, err = enrolSetupParticipants(t.Context(), setupEnrolmentRequest{home: f.home, data: paths.data, profilePath: paths.profile, participants: []setupParticipant{{"client", "wefty://client/test"}}, enroller: e, input: strings.NewReader("unused-key\n")})
	var inUse *fabric.ParticipantInUseError
	if !errors.As(err, &inUse) {
		t.Fatalf("rollback did not refuse new owner: %v", err)
	}
	if _, err := os.Stat(filepath.Join(e.state, "new-owner")); err != nil {
		t.Fatalf("rollback deleted new owner's state: %v", err)
	}
}

// A grant is authenticated and checked by the enroller. Setup must persist
// the returned grant, including the coordinator's collision suffix.
func TestSetupRecordsGrantedNetworkNameAndPreservesLegacy(t *testing.T) {
	for _, grant := range []string{"wefty-studio-client", "wefty-studio-client-2", "", "Bad.Name"} {
		t.Run(grant, func(t *testing.T) {
			f := newSetupFixture(t)
			paths, err := resolveSetupPaths(f.home)
			if err != nil {
				t.Fatal(err)
			}
			e := &networkNameSetupEnroller{grant: grant}
			var report bytes.Buffer
			request := setupEnrolmentRequest{home: f.home, data: paths.data, profilePath: paths.profile, participants: []setupParticipant{{"client", "wefty://client/studio-0123abcd"}}, enroller: e, input: strings.NewReader("fake-key\n"), output: &report}
			profile, err := enrolSetupParticipants(t.Context(), request)
			if grant == "" || grant == "Bad.Name" {
				if !errors.Is(err, fabric.ErrGrantedNameMismatch) || !slices.Equal(e.loggedOut, []string{"client"}) {
					t.Fatalf("invalid grant: err=%v logout=%v", err, e.loggedOut)
				}
				if len(profile.Participants) != 0 {
					t.Fatal("invalid grant recorded")
				}
				if _, err := os.Stat(filepath.Join(paths.data, "fabric", "client")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("invalid participant state remains: %v", err)
				}
				if _, err := os.Stat(paths.profile); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("invalid profile published: %v", err)
				}
				if strings.Contains(report.String(), "The network gave") {
					t.Fatal("invalid grant reported as success")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if grant == "wefty-studio-client-2" {
				want := "The network gave wefty-studio-client-2 because wefty-studio-client is taken; retire the stale device to free the name, or choose a --node-id with a different machine name, such as studio-desk, for a new installation.\n"
				if strings.Count(report.String(), want) != 1 || strings.Contains(report.String(), "fake-key") {
					t.Fatalf("collision report=%q", report.String())
				}
			} else if strings.Contains(report.String(), "The network gave") {
				t.Fatalf("exact grant reported as collision: %q", report.String())
			}
			// Observe the persisted public format, not the fake's returned identity.
			raw, err := os.ReadFile(paths.profile)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(raw, []byte(`"network_name": "`+grant+`"`)) {
				t.Fatalf("grant missing from saved profile: %s", raw)
			}
			// A saved record from before readable names must remain absent on rerun.
			saved := profile.Participants["client"]
			saved.NetworkName = ""
			profile.Participants["client"] = saved
			if err := connprofile.Write(paths.profile, profile); err != nil {
				t.Fatal(err)
			}
			before, _ := os.ReadFile(paths.profile)
			report.Reset()
			request.input = failingSetupKeyInput{t}
			if _, err := enrolSetupParticipants(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			after, _ := os.ReadFile(paths.profile)
			if !bytes.Equal(before, after) || len(e.roles) != 1 || report.Len() != 0 {
				t.Fatal("rerun renamed or reopened saved participant")
			}
		})
	}
}

type networkNameSetupEnroller struct {
	fakeSetupEnroller
	grant string
}

func (e *networkNameSetupEnroller) Enrol(ctx context.Context, spec fabric.EnrolSpec) (fabric.SelfIdentity, error) {
	self, err := e.fakeSetupEnroller.Enrol(ctx, spec)
	self.NetworkName = e.grant
	return self, err
}

type progressSetupEnroller struct {
	fakeSetupEnroller
	failRole string
	states   []fabric.EnrolProgress
}

func (e *progressSetupEnroller) Enrol(ctx context.Context, spec fabric.EnrolSpec) (fabric.SelfIdentity, error) {
	states := e.states
	if states == nil {
		states = []fabric.EnrolProgress{fabric.EnrolStarting, fabric.EnrolWaitingForSignIn, fabric.EnrolWaitingForDeviceApproval, fabric.EnrolConnecting}
	}
	for _, state := range states {
		if spec.Progress != nil {
			spec.Progress(state)
		}
	}
	self, err := e.fakeSetupEnroller.Enrol(ctx, spec)
	if err == nil && filepath.Base(spec.StateDirectory) == e.failRole {
		state := fabric.EnrolWaitingForDeviceApproval
		if e.failRole == "person" {
			state = fabric.EnrolWaitingForSignIn
		}
		return fabric.SelfIdentity{}, &fabric.EnrolTimeoutError{State: state, NeedsJoinKey: spec.Credential.Value != ""}
	}
	return self, err
}

func TestSetupEnrolmentProgressNamesEveryParticipantAndHidesSecrets(t *testing.T) {
	f := newSetupFixture(t)
	paths, _ := resolveSetupPaths(f.home)
	e := &progressSetupEnroller{}
	var output bytes.Buffer
	participants := []setupParticipant{{"l1", "wefty://control-plane"}, {"l3", "wefty://run-ledger"}, {"node-agent", "wefty://node/studio"}, {"client", "wefty://client/studio"}, {"person", "wefty://person/studio"}}
	_, err := enrolSetupParticipants(t.Context(), setupEnrolmentRequest{home: f.home, data: paths.data, profilePath: paths.profile, participants: participants, enroller: e, input: strings.NewReader("secret-progress-l1\nsecret-progress-l3\nsecret-progress-node\nsecret-progress-client\n"), output: &output, loginURL: func(string) {}})
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range participants {
		for _, text := range []string{"starting network enrolment", "waiting for browser sign-in", "waiting for the network to approve this device", "connecting to the network"} {
			if !strings.Contains(output.String(), text+" ("+part.role+" participant)") {
				t.Errorf("missing progress %s/%s", part.role, text)
			}
		}
	}
	for _, secret := range []string{"secret-progress-l1", "secret-progress-l3", "secret-progress-node", "secret-progress-client", "https://login.invalid/one-use"} {
		if bytes.Contains(output.Bytes(), []byte(secret)) {
			t.Fatal("secret in progress output")
		}
	}
}

func TestSetupFleetEnrolmentTimeoutResumes(t *testing.T) {
	for _, person := range []bool{false, true} {
		t.Run(map[bool]string{false: "l3", true: "person"}[person], func(t *testing.T) {
			f, base, output := newFleetFixture(t)
			request := setupRequest{role: "control-plane", nodeID: "studio-0123abcd"}
			failRole := "l3"
			if person {
				f, base, output = newJoinFixture(t)
				request = setupRequest{role: "join", nodeID: "studio-0123abcd", joinCode: joinCodeForTest(t, "opaque-l1", "opaque-l3", ""), withPerson: true}
				f.host.ownerLogin = func(context.Context, string) error { return nil }
				failRole = "person"
			}
			e := &progressSetupEnroller{fakeSetupEnroller: *base, failRole: failRole}
			f.host.enroller = e
			_, err := runSetup(t.Context(), f.host, request)
			var timeout *fabric.EnrolTimeoutError
			if !errors.As(err, &timeout) || commandExitCode(err) != exitFailure || !strings.Contains(err.Error(), failRole+" participant") || !strings.Contains(err.Error(), "rerun setup") {
				t.Fatalf("typed timeout lost: %v", err)
			}
			paths, _ := resolveSetupPaths(f.home)
			stage, readErr := connprofile.Read(filepath.Join(paths.config, fleetSetupProfileName))
			savedCount := 1
			if person {
				savedCount = 2
			}
			if readErr != nil || len(stage.Participants) != savedCount {
				t.Fatalf("lost earlier progress: %+v %v", stage, readErr)
			}
			if _, err := os.Stat(paths.profile); !os.IsNotExist(err) {
				t.Fatal("published profile before completion")
			}
			if _, err := os.Stat(filepath.Join(paths.config, setupManifestName)); !os.IsNotExist(err) {
				t.Fatal("published receipt before completion")
			}
			if len(f.launchd.mutating()) != 0 {
				t.Fatal("started service before completion")
			}
			if _, err := os.Stat(filepath.Join(paths.data, "fabric", failRole)); !os.IsNotExist(err) {
				t.Fatal("failed enrolment folder prevents resume")
			}
			failedOutput := append([]byte(nil), output.Bytes()...)
			for _, secret := range []string{"secret-l1", "secret-l3", "secret-node", "secret-client", "https://login.invalid/one-use"} {
				if bytes.Contains(failedOutput, []byte(secret)) || strings.Contains(err.Error(), secret) {
					t.Fatal("secret in timeout output")
				}
			}
			e.failRole = ""
			output.Reset()
			if person {
				f.host.enrolmentInput = failingSetupKeyInput{t}
			} else {
				f.host.enrolmentInput = strings.NewReader("retry-l3\nretry-node\nretry-client\n")
			}
			request.nodeID = ""
			report, err := runSetup(t.Context(), f.host, request)
			if err != nil || len(report.Services) != map[bool]int{false: 4, true: 2}[person] {
				t.Fatalf("resume failed: %+v %v", report, err)
			}
			want := []string{"l1", "l3", "l3", "node-agent", "client"}
			if person {
				want = []string{"node-agent", "client", "person", "person"}
			}
			if !slices.Equal(e.roles, want) {
				t.Fatal("saved participants reenrolled", e.roles)
			}
		})
	}
}

func TestSetupDefaultNodeIDsCanShareNetworkName(t *testing.T) {
	first, err := newSetupNodeID("studio.local", bytes.NewReader([]byte{1, 2, 3, 4}))
	if err != nil {
		t.Fatal(err)
	}
	second, err := newSetupNodeID("studio.local", bytes.NewReader([]byte{5, 6, 7, 8}))
	if err != nil {
		t.Fatal(err)
	}
	if first != "studio-01020304" || second != "studio-05060708" {
		t.Fatal(first, second)
	}
	distinct, err := fabric.RequestedNetworkName("wefty://node/studio-desk")
	if err != nil || distinct != "wefty-studio-desk" {
		t.Fatalf("different machine name=%q err=%v", distinct, err)
	}
	for _, id := range []string{first, second} {
		name, err := fabric.RequestedNetworkName("wefty://node/" + id)
		if err != nil || name != "wefty-studio" {
			t.Fatalf("network name=%q err=%v", name, err)
		}
	}
}

func TestSetupEnrolmentFixedServiceCollisionKeepsRelevantAdvice(t *testing.T) {
	for _, part := range []setupParticipant{{"l1", "wefty://control-plane"}, {"l3", "wefty://run-ledger"}} {
		t.Run(part.role, func(t *testing.T) {
			f := newSetupFixture(t)
			paths, _ := resolveSetupPaths(f.home)
			name, _ := fabric.RequestedNetworkName(part.name)
			e := &networkNameSetupEnroller{grant: name + "-2"}
			var output bytes.Buffer
			_, err := enrolSetupParticipants(t.Context(), setupEnrolmentRequest{home: f.home, data: paths.data, profilePath: paths.profile, participants: []setupParticipant{part}, enroller: e, input: strings.NewReader("secret-fixed-name-key\n"), output: &output})
			if err != nil || !strings.Contains(output.String(), "retire the stale device") || strings.Contains(output.String(), "--node-id") {
				t.Fatalf("fixed service name advice=%q err=%v", output.String(), err)
			}
		})
	}
}

func TestSetupEnrolmentTimeoutKeepsTypeWhenBrowserIsCancelled(t *testing.T) {
	f := newSetupFixture(t)
	paths, _ := resolveSetupPaths(f.home)
	e := &progressSetupEnroller{failRole: "person"}
	var output bytes.Buffer
	_, err := enrolSetupParticipants(t.Context(), setupEnrolmentRequest{home: f.home, data: paths.data, profilePath: paths.profile, participants: []setupParticipant{{"person", "wefty://person/studio"}}, enroller: e, output: &output, openLoginURL: func(context.Context, string) error { return context.Canceled }})
	var timeout *fabric.EnrolTimeoutError
	if !errors.As(err, &timeout) || timeout.State != fabric.EnrolWaitingForSignIn {
		t.Fatalf("browser cancellation hid the enrolment time limit: %v", err)
	}
	if bytes.Contains(output.Bytes(), []byte("https://login.invalid/one-use")) || strings.Contains(err.Error(), "https://login.invalid/one-use") {
		t.Fatal("browser URL exposed")
	}
}

func TestSetupEnrolmentProgressKeepsHealthyStatesShortAndRepeatAdviceUseful(t *testing.T) {
	f := newSetupFixture(t)
	paths, _ := resolveSetupPaths(f.home)
	e := &progressSetupEnroller{states: []fabric.EnrolProgress{fabric.EnrolStarting, fabric.EnrolStarting, fabric.EnrolConnecting, fabric.EnrolConnecting, fabric.EnrolWaitingForDeviceApproval, fabric.EnrolWaitingForSignIn}}
	var output bytes.Buffer
	_, err := enrolSetupParticipants(t.Context(), setupEnrolmentRequest{home: f.home, data: paths.data, profilePath: paths.profile, participants: []setupParticipant{{"l3", "wefty://run-ledger"}}, enroller: e, input: strings.NewReader("secret-quiet-key\n"), output: &output})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"starting network enrolment (l3 participant)\n",
		"starting network enrolment (l3 participant); check network access, the coordinator, and the join key if used\n",
		"connecting to the network (l3 participant)\n",
		"connecting to the network (l3 participant); check network access, the coordinator, and the join key if used\n",
		"waiting for the network to approve this device (l3 participant); approve it in your network's admin console, or use a pre-approved key\n",
		"waiting for browser sign-in (l3 participant); complete sign-in in the browser\n",
	} {
		if strings.Count(output.String(), want) != 1 {
			t.Errorf("expected one progress line %q: %q", want, output.String())
		}
	}
	if bytes.Contains(output.Bytes(), []byte("secret-quiet-key")) {
		t.Fatal("progress exposed key")
	}
}
