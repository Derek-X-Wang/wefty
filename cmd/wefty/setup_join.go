package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/internal/fleetjoin"
	"github.com/Derek-X-Wang/wefty/internal/systemduser"
)

// Read-only selection runs again under the installation lock before any keys.
func selectJoinSetup(paths setupPaths, request *setupRequest, record *fleetSetupRecord) (fleetjoin.Descriptor, error) {
	if record != nil {
		if request.joinCode == "" {
			request.joinCode = record.JoinCode
		}
		if request.withPerson && !record.WithPerson {
			return fleetjoin.Descriptor{}, errors.New("--with-person differs from the staged installation; adding a person later is not available")
		}
	}
	if request.joinCode == "" {
		return fleetjoin.Descriptor{}, usageError("--role join requires --join-code CODE from the control plane setup report")
	}
	d, err := fleetjoin.Parse(request.joinCode)
	if err != nil {
		return d, usageError(err.Error())
	}
	if record != nil && request.joinCode != record.JoinCode {
		if d.CoordinatorURL != record.CoordinatorURL {
			return d, errors.New("join code coordinator differs from the recorded coordinator; setup will not rebind saved identities")
		}
		if record.PeerVerificationFailure == "" || record.Committed || record.ProfilePublished {
			return d, errors.New("different join code refused; only a recorded failed peer verification before publication permits correction")
		}
		for _, path := range []string{paths.profile, filepath.Join(paths.config, setupManifestName)} {
			if _, err := os.Lstat(path); err == nil {
				return d, errors.New("different join code refused after publication; changing control planes is not available")
			} else if !errors.Is(err, fs.ErrNotExist) {
				return d, err
			}
		}
	}
	if request.coordinatorURL != d.CoordinatorURL {
		return d, errors.New("join code coordinator does not match the owner selection; explicitly select the same coordinator with --coordinator-url or TS_CONTROL_URL, or obtain a code for the selected coordinator")
	}
	return d, nil
}

func validJoinVerificationFailure(value string) bool {
	switch value {
	case "", "l1:local_participant", "l3:local_participant", "l1:peer_not_found", "l1:role_mismatch", "l1:network_unavailable", "l3:peer_not_found", "l3:role_mismatch", "l3:network_unavailable", "network_unavailable":
		return true
	}
	return false
}

func verifySetupJoinPeers(ctx context.Context, host setupHost, profile connprofile.Profile) (string, error) {
	// Check all enrolled local participants, not just the verifier's own identity.
	// This is a peer-verification failure so a corrected code can reuse the stage.
	for _, peer := range []struct{ role, id string }{{"l1", profile.L1Address}, {"l3", profile.L3Address}} {
		for _, participant := range profile.Participants {
			if peer.id == participant.NodeID {
				failure := peer.role + ":local_participant"
				return failure, setupJoinPeerVerificationError(failure)
			}
		}
	}
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	config, err := fabricconfig.SavedService(profile, connprofile.ParticipantNodeAgent, fabricconfig.Config{Mode: profile.Fabric}, nil)
	if err != nil {
		return "network_unavailable", err
	}
	open := host.peerLookup
	if open == nil {
		open = func(ctx context.Context, c fabricconfig.Config) (fabric.PeerLookup, fabricconfig.CloseFunc, error) {
			f, close, err := fabricconfig.Open(ctx, c)
			if err != nil {
				return nil, close, err
			}
			lookup, ok := f.(fabric.PeerLookup)
			if !ok {
				return nil, close, errors.New("Fabric does not support authenticated peer lookup")
			}
			return lookup, close, nil
		}
	}
	lookup, close, openErr := open(bounded, config)
	var failure string
	if openErr != nil || lookup == nil {
		failure = "network_unavailable"
	} else {
		failure = fabricconfig.CheckFleetPeers(bounded, lookup, profile.L1Address, profile.L3Address)
	}
	if close != nil {
		if err := close(); err != nil && failure == "" {
			failure = "network_unavailable"
		}
	}
	if failure != "" {
		return failure, setupJoinPeerVerificationError(failure)
	}
	return "", nil
}

func setupJoinPeerVerificationError(failure string) error {
	return fmt.Errorf("join peer verification failed (%s); no profile or role receipt published. Check the control plane identities and copy its current join code; rerun with the corrected code on the same coordinator to reuse saved participants", failure)
}

// The owner login URL goes directly to the browser, never a report or log.
func openSetupOwnerLoginURL(ctx context.Context, value string) error {
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || strings.ContainsAny(value, "\x00\r\n\t ") {
		return errors.New("owner sign-in returned an invalid browser address")
	}
	if err := exec.CommandContext(ctx, "/usr/bin/open", value).Run(); err != nil {
		return errors.New("could not open owner sign-in in the browser; repair browser access and rerun setup")
	}
	return nil
}

// setupOwnerLoginBrowser keeps the one-use address exclusively in a browser
// argument. A headless Linux machine can finish setup without --with-person;
// neither the address nor command output is copied into an error or receipt.
// Mac's existing default opener above remains unchanged.
func setupOwnerLoginBrowser(goos string, run func(context.Context, string, ...string) error, files ...systemduser.FileSystem) func(context.Context, string) error {
	if run == nil {
		run = func(ctx context.Context, name string, args ...string) error {
			command := exec.CommandContext(ctx, name, args...)
			command.WaitDelay = time.Second
			return command.Run()
		}
	}
	return func(ctx context.Context, value string) error {
		u, err := url.Parse(value)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || strings.ContainsAny(value, "\x00\r\n\t ") {
			return errors.New("owner sign-in returned an invalid browser address")
		}
		command := "/usr/bin/open"
		if goos == "linux" {
			fileSystem := systemduser.FileSystem{}
			if len(files) > 0 {
				fileSystem = files[0]
			}
			command, err = systemduser.ResolveTool("xdg-open", fileSystem)
			if err != nil {
				return err
			}
		} else if goos != "darwin" {
			return errors.New("owner browser sign-in is not supported on this platform")
		}
		openCtx := ctx
		if goos == "linux" {
			var cancel context.CancelFunc
			openCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
		}
		if err := run(openCtx, command, value); err != nil {
			// Generic xdg-open may remain attached to a foreground browser. Its
			// deadline says nothing about sign-in: enrollment confirms the identity.
			if goos == "linux" && errors.Is(openCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
				return nil
			}
			return errors.New("could not open owner sign-in in the browser; repair browser access and rerun setup")
		}
		return nil
	}
}
