//go:build !darwin

package lima

import (
	"context"
	"errors"
)

var errMacBootstrapUnsupported = errors.New("Mac bootstrap mutation is available only on macOS")

func InstallLaunchDaemon(context.Context, LaunchDaemonConfig) error {
	return errMacBootstrapUnsupported
}

func RemoveLaunchDaemon(context.Context) (LaunchDaemonRemovalEvidence, error) {
	return LaunchDaemonRemovalEvidence{}, errMacBootstrapUnsupported
}

func InstallGuestHelper(context.Context, GuestHelperInstallConfig) error {
	return errMacBootstrapUnsupported
}

func InspectGuestSystemdVersion(context.Context, string, string) (int, error) {
	return 0, errMacBootstrapUnsupported
}

func RemoveGuestHelper(context.Context, GuestHelperRemovalConfig) (GuestHelperRemovalEvidence, error) {
	return GuestHelperRemovalEvidence{}, errMacBootstrapUnsupported
}
