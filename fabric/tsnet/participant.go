package tsnet

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"tailscale.com/ipn"
	"tailscale.com/ipn/ipnstate"
)

const participantStartTimeout = 30 * time.Second

// VerifyParticipant waits for the saved node and checks its authenticated
// identity before the caller constructs any application clients. No login URL
// or interactive recovery is offered from this path.
func (f *Fabric) VerifyParticipant(ctx context.Context) error {
	return f.checkParticipant(ctx)
}

func (f *Fabric) checkParticipant(ctx context.Context) error {
	if f.expectedIdentity == nil {
		return nil
	}
	status, err := f.runningStatus(ctx)
	if err != nil {
		return err
	}
	expected, self := f.expectedIdentity, status.Self
	if self == nil || string(self.ID) != expected.NodeID {
		return &fabric.ParticipantIdentityError{Reason: "does not match the recorded node ID"}
	}
	if self.Expired {
		return &fabric.ParticipantIdentityError{Reason: "has an expired node identity"}
	}
	var tags []string
	if self.Tags != nil {
		tags = append(tags, self.Tags.AsSlice()...)
	}
	want := append([]string(nil), expected.Tags...)
	slices.Sort(tags)
	slices.Sort(want)
	if !slices.Equal(tags, want) || (len(want) == 0 && (expected.UserID == "" || strconv.FormatInt(int64(self.UserID), 10) != expected.UserID)) {
		return &fabric.ParticipantIdentityError{Reason: "does not match the recorded role identity"}
	}
	return nil
}

func (f *Fabric) runningStatus(ctx context.Context) (*ipnstate.Status, error) {
	ctx, cancel := context.WithTimeout(ctx, participantStartTimeout)
	defer cancel()
	client, err := f.localClient()
	if err != nil {
		return nil, err
	}
	status, err := client.Status(ctx)
	if err != nil {
		return nil, err
	}
	if err := refuseLogin(status.BackendState); err != nil {
		return nil, err
	}
	if status.BackendState == "Running" {
		return status, nil
	}
	watcher, err := client.WatchIPNBus(ctx, ipn.NotifyInitialState)
	if err != nil {
		return nil, err
	}
	defer watcher.Close()
	for {
		notice, err := watcher.Next()
		if err != nil {
			return nil, err
		}
		if notice.ErrMessage != nil {
			return nil, errors.New("Fabric could not reach running state")
		}
		if notice.State == nil {
			continue
		}
		if err := refuseLogin(notice.State.String()); err != nil {
			return nil, err
		}
		if *notice.State == ipn.Running {
			break
		}
	}
	status, err = client.Status(ctx)
	if err != nil {
		return nil, err
	}
	if err := refuseLogin(status.BackendState); err != nil {
		return nil, err
	}
	if status.BackendState != "Running" {
		return nil, errors.New("Fabric did not remain running")
	}
	return status, nil
}

func refuseLogin(state string) error {
	switch state {
	case "NeedsLogin":
		return &fabric.ParticipantIdentityError{Reason: "needs sign-in; commands cannot enrol participants"}
	case "NeedsMachineAuth":
		return &fabric.ParticipantIdentityError{Reason: "needs machine authorization; commands cannot enrol participants"}
	}
	return nil
}
