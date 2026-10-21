package serviceplan

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
)

// ValidateProfile is the complete-role gate, distinct from partial staged
// profile validation. It starts no backend and does not consume credentials.
func ValidateProfile(role Role, p connprofile.Profile) error {
	if err := role.Validate(); err != nil {
		return err
	}
	if err := p.Validate(); err != nil {
		return err
	}
	if role == Legacy {
		return nil
	}
	if p.Fabric != "tsnet" || p.Identities.Node == "" {
		return errors.New("fleet role requires authenticated Fabric and a stable node ID")
	}
	for _, address := range []string{p.L1Address, p.L3Address} {
		if address == "" || strings.HasPrefix(address, "wefty://") {
			return errors.New("fleet endpoints must be enrolled opaque peer IDs")
		}
	}
	roles := []string{connprofile.ParticipantNodeAgent, connprofile.ParticipantClient}
	if role == ControlPlane {
		roles = append([]string{connprofile.ParticipantL1, connprofile.ParticipantL3}, roles...)
	}
	seen := map[string]bool{}
	for _, participantRole := range roles {
		part, ok := p.Participants[participantRole]
		if !ok || part.NodeID == "" || part.NetworkName == "" {
			return fmt.Errorf("fleet role is missing complete %s participant", participantRole)
		}
		if seen[part.NodeID] {
			return errors.New("fleet participants must have distinct enrolled identities")
		}
		seen[part.NodeID] = true
	}
	if role == ControlPlane {
		if p.L1Address != p.Participants[connprofile.ParticipantL1].NodeID || p.L3Address != p.Participants[connprofile.ParticipantL3].NodeID {
			return errors.New("control-plane endpoints differ from their enrolled peers")
		}
		ownAdmission := false
		for _, admission := range p.NodeAdmission {
			if admission.NodeID == p.Identities.Node {
				ownAdmission = true
				break
			}
		}
		if !ownAdmission {
			return errors.New("control-plane setup requires its own node_admission entry")
		}
		if !p.AutomaticNodeAdmission {
			return errors.New("control-plane setup requires automatic node admission")
		}
	} else {
		if p.AutomaticNodeAdmission || len(p.NodeAdmission) != 0 {
			return errors.New("joining setup cannot configure node admission")
		}
		if _, ok := p.Participants[connprofile.ParticipantL1]; ok {
			return errors.New("joining setup cannot carry an L1 participant")
		}
		if _, ok := p.Participants[connprofile.ParticipantL3]; ok {
			return errors.New("joining setup cannot carry an L3 participant")
		}
		if p.L1Address == p.L3Address || seen[p.L1Address] || seen[p.L3Address] {
			return errors.New("joining endpoints must select distinct remote peers")
		}

	}
	return nil
}
