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
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
)

type fakeSetupEnroller struct {
	roles               []string
	keys                []string
	loggedOut           []string
	wrongTag, wrongName string
	afterEnrol          func() error
}

func (e *fakeSetupEnroller) Enrol(_ context.Context, spec fabric.EnrolSpec) (fabric.SelfIdentity, error) {
	role := filepath.Base(spec.StateDirectory)
	e.roles = append(e.roles, role)
	e.keys = append(e.keys, spec.Credential.Value)
	if err := os.WriteFile(filepath.Join(spec.StateDirectory, "saved-identity"), []byte("opaque-saved-state"), 0o600); err != nil {
		return fabric.SelfIdentity{}, err
	}
	identity := fabric.SelfIdentity{Identity: fabric.Identity{NodeID: "opaque-" + role, DeviceID: "opaque-" + role, Kind: fabric.IdentityKindMachine}, Name: spec.Name}
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
	for _, mismatch := range []string{"tags", "name"} {
		t.Run(mismatch, func(t *testing.T) {
			f := newSetupFixture(t)
			paths, err := resolveSetupPaths(f.home)
			if err != nil {
				t.Fatal(err)
			}
			e := &fakeSetupEnroller{}
			if mismatch == "tags" {
				e.wrongTag = "client"
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
