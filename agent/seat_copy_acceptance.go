//go:build service_acceptance

package agent

import (
	"context"
	"net/http"

	"github.com/Derek-X-Wang/wefty/fabric"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

// SeatCopyAcceptanceConfig supplies an already live Seat's runtime and L1
// client. Only the container runtime is replaced in the CLI acceptance test.
type SeatCopyAcceptanceConfig struct {
	Context    context.Context
	Fabric     fabric.Fabric
	Client     *Client
	Authority  workloadrunner.AttemptAuthority
	ComputerID string
	SeatID     string
	Runtime    seatCopyRuntime
}

// NewSeatCopyAcceptanceHandler exposes the real node routes under the existing
// acceptance build tag, without making production construction public (#1340).
func NewSeatCopyAcceptanceHandler(config SeatCopyAcceptanceConfig) (http.Handler, error) {
	door := newSeatFrontDoor(config.Fabric)
	mux := http.NewServeMux()
	mux.Handle("POST /v1/seats/{seat_id}/copy", newSeatCopyHandler(config.Runtime, config.Client, config.Authority, config.ComputerID))
	registration, err := door.register(config.Context, config.ComputerID, config.Authority.AttemptID, mux)
	if err != nil {
		return nil, err
	}
	if err := registration.bind([]string{config.SeatID}); err != nil {
		return nil, err
	}
	return door, nil
}
