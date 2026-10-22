package l1

import (
	"fmt"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	distributionref "github.com/distribution/reference"
)

const DefaultSharedComputerSeatLimit = 2

func ValidateComputerDefaultImage(reference string) error {
	if reference == "" {
		return nil
	}
	name, pin, ok := strings.Cut(reference, "@")
	if !ok {
		return fmt.Errorf("default Computer image must be digest-pinned")
	}
	if err := contract.ValidateImageProgram(contract.ImageProgram{Reference: name, Digest: &pin}, contract.JobClassService); err != nil {
		return err
	}
	_, err := distributionref.ParseNormalizedNamed(reference)
	return err
}

// Compare repository and immutable content after Docker's name normalization.
// Tags are aliases; the configured digest is the creation authority.
func computerDefaultImageMatches(configured string, image contract.OCIImageSpec) bool {
	if image.Digest == nil {
		return false
	}
	want, err := distributionref.ParseNormalizedNamed(configured)
	if err != nil {
		return false
	}
	got, err := distributionref.ParseNormalizedNamed(image.Reference + "@" + *image.Digest)
	if err != nil {
		return false
	}
	wantPin, ok := want.(distributionref.Digested)
	if !ok {
		return false
	}
	gotPin, ok := got.(distributionref.Digested)
	return ok && want.Name() == got.Name() && wantPin.Digest() == gotPin.Digest()
}

func (s *Store) ComputerSharedSeatLimit() int { return s.computerSharedSeatLimit }

// ComputerDefaultImage supplies the current cluster setting to the trusted
// Seat-request flow. The node resolves the pinned reference, then passes the
// separate repository and digest to CreateComputer for a transactional recheck.
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
