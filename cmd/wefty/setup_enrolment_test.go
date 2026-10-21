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
				want := "The network gave wefty-studio-client-2 because wefty-studio-client is taken; retire the stale device to free the name.\n"
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
