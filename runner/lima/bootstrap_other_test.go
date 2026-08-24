//go:build !darwin

package lima

import (
	"errors"
	"testing"
)

func TestMacBootstrapMutationStubsFailHonestly(t *testing.T) {
	if err := InstallLaunchDaemon(t.Context(), LaunchDaemonConfig{}); !errors.Is(err, errMacBootstrapUnsupported) {
		t.Fatalf("InstallLaunchDaemon stub = %v", err)
	}
	if _, err := RemoveLaunchDaemon(t.Context()); !errors.Is(err, errMacBootstrapUnsupported) {
		t.Fatalf("RemoveLaunchDaemon stub = %v", err)
	}
	if err := InstallGuestHelper(t.Context(), GuestHelperInstallConfig{}); !errors.Is(err, errMacBootstrapUnsupported) {
		t.Fatalf("InstallGuestHelper stub = %v", err)
	}
	if _, err := RemoveGuestHelper(t.Context(), GuestHelperRemovalConfig{}); !errors.Is(err, errMacBootstrapUnsupported) {
		t.Fatalf("RemoveGuestHelper stub = %v", err)
	}
}
