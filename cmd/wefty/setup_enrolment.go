package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/participantstate"
	"github.com/Derek-X-Wang/wefty/fabric/tsnet"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/durable"
	"golang.org/x/term"
)

type setupParticipant struct{ role, name string }

type setupEnrolmentRequest struct {
	home, data, profilePath string
	participants            []setupParticipant
	enroller                fabric.Enroller
	input                   io.Reader
	output                  io.Writer
	loginURL                func(string)
	controlURL              string
}

// enrolSetupParticipants is the setup step fleet role wiring (#900) can call.
// It starts no services and no local listener. The caller must stop existing
// services before a cutover; a rerun never starts an enrolled participant.
func enrolSetupParticipants(ctx context.Context, request setupEnrolmentRequest) (connprofile.Profile, error) {
	if err := tsnet.ClearAmbientCredentials(); err != nil {
		return connprofile.Profile{}, err
	}
	profile, err := readSetupProfile(request.profilePath)
	if err != nil {
		return profile, err
	}
	if profile.Version == 0 {
		profile.Version = connprofile.Version
	}
	profile.Fabric = "tsnet"
	if profile.Participants == nil {
		profile.Participants = make(map[string]connprofile.Participant)
	}
	if !filepath.IsAbs(request.home) || !filepath.IsAbs(request.data) || !filepath.IsAbs(request.profilePath) {
		return profile, errors.New("enrolment needs absolute home, data and profile paths")
	}
	relative := func(path string) (string, error) {
		rel, err := filepath.Rel(request.home, path)
		if err != nil || !filepath.IsLocal(rel) || rel == "." {
			return "", errors.New("enrolment destinations must be inside the setup home")
		}
		return canonicalHomeFolder(request.home, rel)
	}
	dataRel, err := relative(request.data)
	if err != nil {
		return profile, err
	}
	profileFolder, err := relative(filepath.Dir(request.profilePath))
	if err != nil {
		return profile, err
	}
	seen := make(map[string]bool)
	for _, participant := range request.participants {
		_, _, ok := setupParticipantPolicy(participant.role)
		if !ok || seen[participant.role] || connprofile.ValidateParticipantName(participant.role, participant.name) != nil {
			return profile, fmt.Errorf("invalid or duplicate setup participant %q", participant.role)
		}
		seen[participant.role] = true
		if participant.role == connprofile.ParticipantPerson && request.loginURL == nil {
			return profile, errors.New("person enrolment needs an owner login-URL callback")
		}
	}
	root, err := os.OpenRoot(request.home)
	if err != nil {
		return profile, err
	}
	defer root.Close()
	if request.enroller == nil {
		request.enroller = tsnet.Enroller{}
	}
	if request.input == nil {
		request.input = os.Stdin
	}
	if request.output == nil {
		request.output = io.Discard
	}
	reader := bufio.NewReader(request.input)
	for _, participant := range request.participants {
		if err := ctx.Err(); err != nil {
			return profile, err
		}
		stateRel := filepath.Join(dataRel, "fabric", participant.role)
		stateDir := filepath.Join(request.home, stateRel)
		// Reject a linked participant folder even when its target is inside
		// home: two participants must never share one identity state folder.
		for path := stateRel; path != "."; path = filepath.Dir(path) {
			info, err := root.Lstat(path)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return profile, err
			}
			if !info.IsDir() {
				return profile, fmt.Errorf("participant state path %s is not an unlinked folder", path)
			}
		}
		if saved, ok := profile.Participants[participant.role]; ok {
			if saved.Name != participant.name || saved.StateDirectory != stateDir {
				return profile, fmt.Errorf("saved %s participant differs; setup will not rebind its identity", participant.role)
			}
			if !hasSavedParticipantState(stateDir) {
				return profile, fmt.Errorf("saved %s participant state is missing; restore its original saved state, or retire the old device and remove its stale participant record before enrolling a new identity", participant.role)
			}
			if err := root.Chmod(stateRel, 0o700); err != nil {
				return profile, err
			}
			continue
		}
		if _, err := os.ReadDir(stateDir); err == nil && hasSavedParticipantState(stateDir) {
			return profile, fmt.Errorf("unrecorded state for %s; inspect it before enrolling", participant.role)
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return profile, err
		}
		if err := root.MkdirAll(stateRel, 0o700); err != nil {
			return profile, err
		}
		if err := root.Chmod(stateRel, 0o700); err != nil {
			return profile, err
		}
		spec := fabric.EnrolSpec{Name: participant.name, StateDirectory: stateDir, CoordinatorURL: request.controlURL}
		if participant.role == connprofile.ParticipantPerson {
			spec.LoginURL = request.loginURL
		} else {
			key, err := readSetupJoinKey(ctx, request.input, reader, request.output, participant.role)
			if err != nil {
				return profile, err
			}
			spec.Credential.Value = key
		}
		self, enrolErr := request.enroller.Enrol(ctx, spec)
		key := spec.Credential.Value
		spec.Credential = fabric.Credential{}
		rollback := func(cause error) error {
			// Cleanup has its own bounded context so Ctrl-C cannot skip logout.
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
			logoutErr := request.enroller.Logout(cleanupCtx, spec)
			cancel()
			var inUse *fabric.ParticipantInUseError
			if errors.As(logoutErr, &inUse) {
				return scrubSetupEnrolmentError(errors.Join(cause, logoutErr), key)
			}
			release, lockErr := participantstate.Acquire(stateDir, spec.Name)
			if lockErr != nil {
				return scrubSetupEnrolmentError(errors.Join(cause, logoutErr, lockErr), key)
			}
			removeErr := root.RemoveAll(stateRel)
			removeErr = errors.Join(removeErr, release())
			return scrubSetupEnrolmentError(errors.Join(cause, logoutErr, removeErr), key)
		}
		if enrolErr == nil {
			_, expected, _ := setupParticipantPolicy(participant.role)
			actual := slices.Clone(self.Tags)
			slices.Sort(actual)
			slices.Sort(expected)
			if !slices.Equal(actual, expected) {
				enrolErr = errors.New("granted participant tags do not match the role")
			}
			if self.Name != participant.name {
				enrolErr = fabric.ErrGrantedNameMismatch
			}
			if self.NodeID == "" || participant.role == connprofile.ParticipantPerson && (self.UserID == "" || self.Kind == fabric.IdentityKindMachine) {
				enrolErr = errors.New("enrolled identity is incomplete or is not a person")
			}
		}
		if enrolErr != nil {
			var inUse *fabric.ParticipantInUseError
			if errors.As(enrolErr, &inUse) {
				return profile, enrolErr
			}
			return profile, rollback(enrolErr)
		}
		previousL1, previousL3 := profile.L1Address, profile.L3Address
		profile.Participants[participant.role] = connprofile.Participant{Name: self.Name, StateDirectory: stateDir, NodeID: self.NodeID, UserID: self.UserID, KeyExpiry: self.KeyExpiry}
		if participant.role == connprofile.ParticipantL1 {
			profile.L1Address = self.NodeID
		}
		if participant.role == connprofile.ParticipantL3 {
			profile.L3Address = self.NodeID
		}
		payload, err := connprofile.Encode(profile)
		if err == nil {
			err = root.MkdirAll(profileFolder, 0o700)
		}
		if err == nil {
			var folder *os.Root
			folder, err = root.OpenRoot(profileFolder)
			if err == nil {
				err = durable.WriteFile(folder, filepath.Base(request.profilePath), payload, 0o600)
				_ = folder.Close()
			}
		}
		if err != nil {
			// A durability error can arrive after rename. Keep state when the
			// published profile already records it; otherwise roll it back.
			published, readErr := root.ReadFile(filepath.Join(profileFolder, filepath.Base(request.profilePath)))
			if readErr == nil && len(payload) != 0 && bytes.Equal(published, payload) {
				return profile, err
			}
			delete(profile.Participants, participant.role)
			profile.L1Address, profile.L3Address = previousL1, previousL3
			return profile, rollback(err)
		}
	}
	return profile, nil
}

func scrubSetupEnrolmentError(err error, key string) error {
	if key == "" || !strings.Contains(err.Error(), key) {
		return err
	}
	return errors.New(strings.ReplaceAll(err.Error(), key, "[redacted]"))
}

func setupParticipantPolicy(role string) (string, []string, bool) {
	switch role {
	case connprofile.ParticipantClient:
		return "wefty://client/", connprofile.ParticipantRoleTags(role), true
	case connprofile.ParticipantPerson:
		return "wefty://person/", connprofile.ParticipantRoleTags(role), true
	case connprofile.ParticipantNodeAgent:
		return "wefty://node/", connprofile.ParticipantRoleTags(role), true
	case connprofile.ParticipantL1:
		return "wefty://control-plane", connprofile.ParticipantRoleTags(role), true
	case connprofile.ParticipantL3:
		return "wefty://run-ledger", connprofile.ParticipantRoleTags(role), true
	default:
		return "", nil, false
	}
}

func readSetupJoinKey(ctx context.Context, input io.Reader, reader *bufio.Reader, output io.Writer, role string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if _, err := fmt.Fprintf(output, "Single-use join key for %s: ", role); err != nil {
		return "", err
	}
	var key string
	if file, ok := input.(*os.File); ok && term.IsTerminal(int(file.Fd())) {
		payload, err := term.ReadPassword(int(file.Fd()))
		if err != nil {
			return "", err
		}
		key = string(payload)
		if _, err := fmt.Fprintln(output); err != nil {
			return "", err
		}
	} else {
		line, err := reader.ReadString('\n')
		if err != nil && (err != io.EOF || line == "") {
			return "", fmt.Errorf("read join key for %s: %w", role, err)
		}
		key = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return "", fmt.Errorf("join key for %s is empty", role)
	}
	return key, nil
}
