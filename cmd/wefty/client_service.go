package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/local"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
)

// Legacy profiles retain their embedded backend. Participant profiles only
// connect to the service; there is deliberately no direct-state fallback.
func openCLIFabric(ctx context.Context, config fabricconfig.Config) (fabric.Fabric, fabricconfig.CloseFunc, error) {
	if config.ExpectedIdentity == nil {
		return fabricconfig.Open(ctx, config)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, nil, err
	}
	role := local.Client
	if config.ExpectedIdentity.UserID != "" {
		role = local.Person
	}
	dialer, err := local.New(local.SocketPath(home), role, config.ExpectedIdentity.NodeID)
	if err != nil {
		return nil, nil, err
	}
	if err := dialer.Check(ctx); err != nil {
		return nil, nil, err
	}
	return dialer, func() error { return nil }, nil
}

// client-service is an internal lifecycle entry point. #900 owns the rendered
// LaunchAgent/systemd units; it never creates or enrols a participant.
func executeClientService(ctx context.Context, options globalOptions, args []string, stdout, stderr io.Writer) error {
	return executeClientServiceWithOpener(ctx, options, args, stdout, stderr, fabricconfig.Open)
}

func executeClientServiceWithOpener(ctx context.Context, options globalOptions, args []string, stdout, stderr io.Writer, openBackend func(context.Context, fabricconfig.Config) (fabric.Fabric, fabricconfig.CloseFunc, error)) error {
	flags := flag.NewFlagSet("client-service", flag.ContinueOnError)
	flags.SetOutput(flagErrorOutput(stderr, options.jsonOutput))
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	socket := flags.String("socket", local.SocketPath(home), "private client-service socket path")
	flags.Usage = func() {
		_, _ = io.WriteString(flags.Output(), "Usage: wefty [--profile PATH] client-service [--socket PATH]\nInternal per-user Fabric client service.\n")
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return flagUsageError(err)
	}
	if flags.NArg() != 0 {
		return usageError("client-service accepts no positional arguments")
	}
	if err := applyConnectionProfile(&options); err != nil {
		return err
	}
	if len(options.participants) == 0 {
		return usageError("client-service requires a participant profile")
	}
	resolveFabricEnvironment(&options)
	participants := make(map[string]local.Participant)
	var closers []fabricconfig.CloseFunc
	defer func() {
		for i := len(closers) - 1; i >= 0; i-- {
			_ = closers[i]()
		}
	}()
	for _, role := range []string{connprofile.ParticipantClient, connprofile.ParticipantPerson} {
		saved, exists := options.participants[role]
		if !exists && role == connprofile.ParticipantPerson {
			continue
		}
		input := fabricconfig.Config{
			Mode: options.fabricMode, PlainFabricID: options.plainFabricID,
			Name: options.fabricName, StateDirectory: options.stateDirectory,
			AuthKey: options.authKey, ControlURL: options.controlURL, Ephemeral: options.ephemeral,
			Identity: fabric.Identity{NodeID: options.plainIdentity},
		}
		explicit := options.explicitFlags
		if role == connprofile.ParticipantPerson {
			// Service overrides select its primary client backend. The optional
			// person always comes from its own record, never the client's flags.
			input.Name, input.StateDirectory = "", ""
			input.Identity = fabric.Identity{}
			explicit = nil
		}
		config, configErr := fabricconfig.SavedService(connprofile.Profile{Fabric: options.profileFabric, Participants: options.participants}, role, input, explicit)
		if configErr != nil {
			var identity *fabric.ParticipantIdentityError
			if role == connprofile.ParticipantClient || !errors.As(configErr, &identity) {
				return configErr
			}
			participants[role] = missingPersonParticipant(saved.NodeID)
			continue
		}
		config.DeferVerification = role == connprofile.ParticipantPerson
		backend, closeBackend, err := openBackend(ctx, config)
		if closeBackend != nil {
			closers = append(closers, closeBackend)
		}
		if err != nil {
			var identity *fabric.ParticipantIdentityError
			if role == connprofile.ParticipantPerson && errors.As(err, &identity) {
				participants[role] = missingPersonParticipant(saved.NodeID)
				continue
			}
			return err
		}
		check := func(probe context.Context) error {
			if !hasSavedParticipantState(saved.StateDirectory) {
				return &fabric.ParticipantIdentityError{Person: role == connprofile.ParticipantPerson, Reason: "state is missing"}
			}
			if role == connprofile.ParticipantPerson && !saved.KeyExpiry.IsZero() && !time.Now().Before(saved.KeyExpiry) {
				return &fabric.ParticipantIdentityError{Person: true, Reason: "sign-in has expired"}
			}
			if verifier, ok := backend.(interface{ VerifyParticipant(context.Context) error }); ok {
				return verifier.VerifyParticipant(probe)
			}
			return probe.Err()
		}
		participants[role] = local.Participant{NodeID: saved.NodeID, Fabric: backend, Check: check}
	}
	server, err := local.NewServer(*socket, participants)
	if err != nil {
		return err
	}
	return server.Serve(ctx)
}

func missingPersonParticipant(nodeID string) local.Participant {
	return local.Participant{NodeID: nodeID, Check: func(context.Context) error {
		return &fabric.ParticipantIdentityError{Person: true, Reason: "sign-in is missing or expired"}
	}}
}
