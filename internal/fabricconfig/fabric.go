package fabricconfig

import (
	"context"
	"errors"
	"fmt"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/participantstate"
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
	// DeferVerification is for a long-lived owner that checks on every dial.
	DeferVerification bool
}

type CloseFunc func() error

type savedParticipant interface {
	fabric.Fabric
	VerifyParticipant(context.Context) error
	Close() error
}

// Open constructs the Fabric and verifies an expected participant unless its
// long-lived owner takes responsibility through DeferVerification.
// The caller must call any non-nil CloseFunc, including on error. Returning
// cleanup separately lets diagnostics report a failed check before shutdown.
func Open(ctx context.Context, config Config) (fabric.Fabric, CloseFunc, error) {
	return open(ctx, config, func(config tsnet.Config) (savedParticipant, error) { return tsnet.New(config) })
}

func open(ctx context.Context, config Config, newParticipant func(tsnet.Config) (savedParticipant, error)) (fabric.Fabric, CloseFunc, error) {
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
		release := func() error { return nil }
		if config.ExpectedIdentity != nil {
			var err error
			release, err = participantstate.Acquire(config.StateDirectory, config.Name)
			if err != nil {
				return nil, nil, err
			}
		}
		participant := network.NewFabric(config.Identity)
		return participant, release, nil
	case "tsnet":
		if config.Name == "" {
			return nil, nil, errors.New("fabric config: tsnet name is required")
		}
		participant, err := newParticipant(tsnet.Config{
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
		if config.ExpectedIdentity != nil && !config.DeferVerification {
			if err := participant.VerifyParticipant(ctx); err != nil {
				return nil, participant.Close, err
			}
		}
		return participant, participant.Close, nil
	default:
		return nil, nil, fmt.Errorf("fabric config: unsupported mode %q", config.Mode)
	}
}
