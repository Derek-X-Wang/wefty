package l1

import "github.com/Derek-X-Wang/wefty/contract"

const DefaultSharedComputerSeatLimit = 2

func ValidateComputerDefaultImage(reference string) error {
	if reference == "" {
		return nil
	}
	return contract.ValidateImageProgram(contract.ImageProgram{Reference: reference}, contract.JobClassOneShot)
}

func (s *Store) ComputerSharedSeatLimit() int { return s.computerSharedSeatLimit }

// ComputerDefaultImage supplies the current cluster setting to the trusted
// Seat-request flow. The node resolves the reference, then passes that digest
// to CreateComputer. A changed default is checked again in that transaction.
func (s *Store) ComputerDefaultImage() (string, error) {
	if s.computerDefaultImage == "" {
		return "", protocolError(contract.ErrorInvalidRequest,
			"default Computer image is not configured; set --computer-default-image on L1")
	}
	return s.computerDefaultImage, nil
}

type ComputerIdleAction string

const (
	ComputerIdleDelete  ComputerIdleAction = "delete"
	ComputerIdleStop    ComputerIdleAction = "stop"
	ComputerIdleNothing ComputerIdleAction = "nothing"
)

type ComputerNetwork string

const (
	ComputerNetworkInternet ComputerNetwork = "internet"
	ComputerNetworkNone     ComputerNetwork = "none"
)

// ComputerSettings belong to the Computer, rather than its replaceable Job.
// SeatLimit zero records an owner-made Computer that takes no Seats in stage A.
type ComputerSettings struct {
	SeatLimit  int                `json:"seat_limit"`
	IdleAction ComputerIdleAction `json:"idle_action"`
	Network    ComputerNetwork    `json:"network"`
}

func ownerComputerSettings() ComputerSettings {
	return ComputerSettings{IdleAction: ComputerIdleNothing, Network: ComputerNetworkInternet}
}

func (settings ComputerSettings) validateSeatRequest() error {
	if settings.SeatLimit < 1 {
		return protocolError(contract.ErrorInvalidRequest, "a Seat-request-made Computer needs a positive seat_limit")
	}
	if settings.IdleAction != ComputerIdleDelete {
		return protocolError(contract.ErrorInvalidRequest, "a Seat-request-made Computer requires idle_action delete")
	}
	if settings.Network != ComputerNetworkInternet && settings.Network != ComputerNetworkNone {
		return protocolError(contract.ErrorInvalidRequest, "Computer network must be internet or none")
	}
	return nil
}
