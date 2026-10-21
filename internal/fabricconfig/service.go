package fabricconfig

import (
	"os"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/participantstate"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
)

// SavedService selects only this process's participant. Legacy configuration
// passes through byte-for-byte; an enrolled process cannot bootstrap or use
// an override to select a different identity. Call before Open.
func SavedService(profile connprofile.Profile, role string, config Config, explicit map[string]bool) (Config, error) {
	if len(profile.Participants) == 0 {
		return config, nil
	}
	refused := func(reason string) (Config, error) {
		return Config{}, &fabric.ParticipantIdentityError{Person: role == connprofile.ParticipantPerson, Reason: reason}
	}
	saved := profile.ServiceParticipant(role)
	if saved == nil {
		return refused("is missing from the profile for " + role)
	}
	if config.AuthKey != "" || os.Getenv("TS_AUTHKEY") != "" || os.Getenv("TS_AUTH_KEY") != "" {
		return refused("cannot start with a join key override")
	}
	if config.ControlURL != "" || os.Getenv("TS_CONTROL_URL") != "" {
		return refused("cannot start with a control URL override")
	}
	if config.Ephemeral {
		return refused("cannot start with an ephemeral lifetime")
	}
	if explicit["fabric-name"] && config.Name != saved.Name {
		return refused("name override conflicts with its record")
	}
	if (config.StateDirectory != "" || explicit["state-dir"]) && config.StateDirectory != saved.StateDirectory {
		return refused("state folder override conflicts with its record")
	}
	if explicit["plain-identity"] && config.Identity.NodeID != saved.NodeID {
		return refused("node ID override conflicts with its record")
	}
	if profile.Fabric != "" && config.Mode != profile.Fabric {
		return refused("Fabric mode override conflicts with its profile")
	}
	if !participantstate.HasSavedState(saved.StateDirectory) {
		return refused("state is missing; restore this participant's original saved state")
	}
	config.Name, config.StateDirectory = saved.Name, saved.StateDirectory
	config.Identity = saved.ExpectedIdentity
	config.ExpectedIdentity = &saved.ExpectedIdentity
	config.AuthKey, config.ControlURL = "", ""
	config.Ephemeral = false
	return config, nil
}

// TrustedServicePeer resolves authorization separately from dial addresses.
// Plain development profiles may dial loopback but still trust an opaque ID.
func TrustedServicePeer(profile connprofile.Profile, role, current string, explicit bool) (string, error) {
	if len(profile.Participants) == 0 {
		return current, nil
	}
	saved := profile.PeerNodeID(role)
	if saved == "" {
		return "", &fabric.ParticipantIdentityError{Reason: "trusted peer is missing from the profile"}
	}
	if explicit && current != saved {
		return "", &fabric.ParticipantIdentityError{Reason: "trusted peer override conflicts with its record"}
	}
	return saved, nil
}
