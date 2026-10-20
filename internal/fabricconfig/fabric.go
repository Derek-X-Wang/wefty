package fabricconfig

import (
	"context"
	"errors"
	"fmt"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/fabric/tsnet"
)

type Config struct {
	Mode             string
	Identity         fabric.Identity
	PlainFabricID    string
	Name             string
	StateDirectory   string
	AuthKey          string
	ControlURL       string
	Ephemeral        bool
	Logf             func(string, ...any)
	ExpectedIdentity *fabric.Identity
}

type CloseFunc func() error

func Open(config Config) (fabric.Fabric, CloseFunc, error) {
	switch config.Mode {
	case "plain":
		network := plain.NewNetwork()
		if config.PlainFabricID != "" {
			var err error
			network, err = plain.NewNetworkWithID(config.PlainFabricID)
			if err != nil {
				return nil, nil, fmt.Errorf("fabric config: DEVELOPMENT ONLY plain Fabric ID: %w", err)
			}
		}
		participant := network.NewFabric(config.Identity)
		return participant, func() error { return nil }, nil
	case "tsnet":
		if config.Name == "" {
			return nil, nil, errors.New("fabric config: tsnet name is required")
		}
		participant, err := tsnet.New(tsnet.Config{
			Name:             config.Name,
			StateDir:         config.StateDirectory,
			Credential:       fabric.Credential{Value: config.AuthKey},
			Ephemeral:        config.Ephemeral,
			CoordinatorURL:   config.ControlURL,
			Logf:             config.Logf,
			ExpectedIdentity: config.ExpectedIdentity,
		})
		if err != nil {
			return nil, nil, err
		}
		if config.ExpectedIdentity != nil {
			if err := participant.VerifyParticipant(context.Background()); err != nil {
				return nil, nil, errors.Join(err, participant.Close())
			}
		}
		return participant, participant.Close, nil
	default:
		return nil, nil, fmt.Errorf("fabric config: unsupported mode %q", config.Mode)
	}
}
