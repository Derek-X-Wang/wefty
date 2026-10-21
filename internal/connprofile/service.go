package connprofile

import (
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

// ServiceParticipant is the saved backend and exact identity a service owns.
// Keep it separate from endpoint settings: repairing one backend must not
// restart unrelated services.
type ServiceParticipant struct {
	Participant
	ExpectedIdentity fabric.Identity `json:"expected_identity"`
}

// ServiceParticipant projects one record, without any other participant's
// machine-local state. The exact tags are Wefty policy, never profile input.
func (p Profile) ServiceParticipant(role string) *ServiceParticipant {
	saved, ok := p.Participants[role]
	if !ok {
		return nil
	}
	identity := fabric.Identity{NodeID: saved.NodeID, Tags: ParticipantRoleTags(role)}
	if role == ParticipantPerson {
		identity.UserID, identity.DeviceID = saved.UserID, saved.NodeID
	}
	return &ServiceParticipant{Participant: saved, ExpectedIdentity: identity}
}

// ParticipantRoleTags is the single role-tag policy shared by enrolment and
// saved-service verification. Each caller receives its own tag slice.
func ParticipantRoleTags(role string) []string {
	switch role {
	case ParticipantL1:
		return []string{"tag:wefty-control"}
	case ParticipantNodeAgent:
		return []string{l1.DefaultAgentPrincipalTag}
	case ParticipantL3, ParticipantClient:
		return []string{l1.DefaultClientPrincipalTag}
	default:
		return nil
	}
}

// PeerNodeID returns only enrolled control-service identity. Dial addresses
// never grant authority, and absent peer records cannot select a default.
func (p Profile) PeerNodeID(role string) string {
	return p.Participants[role].NodeID
}

// ClientServiceSettings covers the two backends owned by the per-user service.
type ClientServiceSettings struct {
	ParticipantProfile bool                `json:"participant_profile,omitempty"`
	Fabric             string              `json:"fabric,omitempty"`
	PlainFabricID      string              `json:"plain_fabric_id,omitempty"`
	Client             *ServiceParticipant `json:"client,omitempty"`
	Person             *ServiceParticipant `json:"person,omitempty"`
}

func (p Profile) ClientService() ClientServiceSettings {
	return ClientServiceSettings{ParticipantProfile: len(p.Participants) > 0, Fabric: p.Fabric, PlainFabricID: p.PlainFabricID, Client: p.ServiceParticipant(ParticipantClient), Person: p.ServiceParticipant(ParticipantPerson)}
}
