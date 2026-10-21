package tsnet

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
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
	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		return &fabric.ParticipantUnreachableError{Cause: participantStatusError{cause: err}}
	}
	status, err := f.runningStatus(ctx)
	if errors.Is(ctx.Err(), context.Canceled) {
		return ctx.Err()
	}
	if err != nil {
		var identity *fabric.ParticipantIdentityError
		if errors.As(err, &identity) {
			return err
		}
		return &fabric.ParticipantUnreachableError{Cause: participantStatusError{cause: err}}
	}
	expected, self := f.expectedIdentity, status.Self
	if self == nil || string(self.ID) != expected.NodeID {
		return f.identityError("does not match the recorded node ID")
	}
	// Only a node agent advertises its name to other machines. Elsewhere the
	// name reaches nobody, so a renamed device must not stop the service.
	if f.recordedNetworkName != "" && f.localName.AdvertisesNetworkName() {
		granted, _, _ := strings.Cut(strings.TrimSuffix(self.DNSName, "."), ".")
		if granted != f.recordedNetworkName {
			return f.identityError("network name differs from its record; restore the recorded name on the network or re-record the granted name in the connection profile")
		}
	}
	if self.Expired {
		return f.identityError("has an expired node identity")
	}
	var tags []string
	if self.Tags != nil {
		tags = append(tags, self.Tags.AsSlice()...)
	}
	want := append([]string(nil), expected.Tags...)
	slices.Sort(tags)
	slices.Sort(want)
	if !slices.Equal(tags, want) || (len(want) == 0 && (expected.UserID == "" || strconv.FormatInt(int64(self.UserID), 10) != expected.UserID)) {
		return f.identityError("does not match the recorded role identity")
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
	if err := f.refuseLogin(status.BackendState); err != nil {
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
		if err := f.refuseLogin(notice.State.String()); err != nil {
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
	if err := f.refuseLogin(status.BackendState); err != nil {
		return nil, err
	}
	if status.BackendState != "Running" {
		return nil, errors.New("Fabric did not remain running")
	}
	return status, nil
}

func (f *Fabric) refuseLogin(state string) error {
	if f.expectedIdentity == nil {
		return nil
	}
	switch state {
	case "NeedsLogin":
		return f.identityError("needs sign-in; commands cannot enrol participants")
	case "NeedsMachineAuth":
		return f.identityError("needs machine authorization; commands cannot enrol participants")
	}
	return nil
}

func (f *Fabric) identityError(reason string) error {
	return &fabric.ParticipantIdentityError{Reason: reason, Person: f.expectedIdentity != nil && len(f.expectedIdentity.Tags) == 0 && f.expectedIdentity.UserID != ""}
}

// participantStatusError keeps backend URLs and implementation names behind the
// Fabric seam. The original cause remains available for error classification.
type participantStatusError struct{ cause error }

func (e participantStatusError) Error() string {
	switch {
	case errors.Is(e.cause, context.DeadlineExceeded):
		return context.DeadlineExceeded.Error()
	case errors.Is(e.cause, context.Canceled):
		return context.Canceled.Error()
	default:
		return "backend status unavailable"
	}
}

func (e participantStatusError) Unwrap() error { return e.cause }
