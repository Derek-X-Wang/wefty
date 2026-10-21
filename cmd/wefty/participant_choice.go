package main

import (
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/participantstate"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/l3"
)

// personSignInRequiredError is an authentication refusal, never a usage
// failure or permission to retry with the machine participant.
type personSignInRequiredError struct{}

func (*personSignInRequiredError) Error() string {
	return "person sign-in is missing or expired; restore this participant's saved state or retire the old device and enrol a new person identity through fleet setup"
}

// commandFabricConfig is the one identity choice for both Fabric modes.
// Legacy profiles retain their complete request shape. Participant profiles
// select exactly one enrolled identity, without carrying legacy person fields.
func commandFabricConfig(options globalOptions, args []string) (fabricconfig.Config, error) {
	person := usesPersonProtocol(args)
	config := fabricconfig.Config{
		Mode: options.fabricMode, PlainFabricID: options.plainFabricID,
		Name: options.fabricName, StateDirectory: options.stateDirectory,
		ControlURL: options.controlURL, Ephemeral: options.ephemeral,
		Identity: fabric.Identity{NodeID: options.plainIdentity, UserID: options.plainUserID, DeviceID: options.plainDeviceID, Tags: []string{l3.DefaultCallerPrincipalTag}},
	}
	if len(options.participants) == 0 {
		config.AuthKey = options.authKey
		if person {
			config.Identity.Tags = nil
			if options.fabricMode == "plain" && (options.plainUserID == "" || options.plainDeviceID == "") {
				return fabricconfig.Config{}, usageError("plain person commands require DEVELOPMENT ONLY --plain-user-id and --plain-device-id")
			}
		}
		return config, nil
	}
	role := connprofile.ParticipantClient
	if person {
		role = connprofile.ParticipantPerson
		if localPersonSignIn(options, time.Now()) != "signed_in" {
			return fabricconfig.Config{}, &personSignInRequiredError{}
		}
	}
	participant, ok := options.participants[role]
	if !ok || participant.NodeID == "" {
		if person {
			return fabricconfig.Config{}, &personSignInRequiredError{}
		}
		return fabricconfig.Config{}, &fabric.ParticipantIdentityError{Reason: "is missing from the profile"}
	}
	if !fabricconfig.SavedStateAvailable(options.fabricMode, participant.StateDirectory) {
		if person {
			return fabricconfig.Config{}, &personSignInRequiredError{}
		}
		return fabricconfig.Config{}, &fabric.ParticipantIdentityError{Reason: "state is missing; restore this participant's original saved state"}
	}
	if options.fabricMode == "tsnet" {
		if options.authKey != "" || options.ephemeral {
			return fabricconfig.Config{}, usageError("enrolled participants start from saved state without a join key or --ephemeral; use wefty setup for enrolment")
		}
		if options.explicitFlags["fabric-name"] && options.fabricName != participant.Name || options.explicitFlags["state-dir"] && options.stateDirectory != participant.StateDirectory {
			return fabricconfig.Config{}, usageError("--fabric-name and --state-dir must match the participant selected for this command")
		}
	}
	config.Name, config.StateDirectory = participant.Name, participant.StateDirectory
	config.NetworkName = participant.NetworkName
	if options.controlURL != "" && options.controlURL != participant.CoordinatorURL {
		return fabricconfig.Config{}, &fabric.ParticipantIdentityError{Person: person, CoordinatorConflict: true, Reason: "coordinator URL override conflicts with its record"}
	}
	config.ControlURL = participant.CoordinatorURL
	config.Identity = fabric.Identity{NodeID: participant.NodeID, Tags: []string{l3.DefaultCallerPrincipalTag}}
	if person {
		config.Identity = fabric.Identity{NodeID: participant.NodeID, DeviceID: participant.NodeID, UserID: participant.UserID}
	}
	config.ExpectedIdentity = &config.Identity
	config.Ephemeral = false
	return config, nil
}

// This is local saved sign-in state, not a live authentication probe. Status
// can report it without starting the person participant or requiring sign-in.
func localPersonSignIn(options globalOptions, now time.Time) string {
	if len(options.participants) == 0 {
		if options.plainUserID != "" && options.plainDeviceID != "" {
			return "signed_in"
		}
		return "not_signed_in"
	}
	person, ok := options.participants[connprofile.ParticipantPerson]
	if !ok || person.NodeID == "" || person.UserID == "" {
		return "not_signed_in"
	}
	if !fabricconfig.SavedStateAvailable(options.fabricMode, person.StateDirectory) {
		return "not_signed_in"
	}
	if !person.KeyExpiry.IsZero() && !now.Before(person.KeyExpiry) {
		return "expired"
	}
	return "signed_in"
}

func hasSavedParticipantState(directory string) bool {
	return participantstate.HasSavedState(directory)
}
