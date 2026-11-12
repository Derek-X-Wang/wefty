package main

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

// Under --recreate an older base turns the run into a recreate, a current
// base changes nothing, and an unreadable guest refuses before any class
// change. A recreate the setup state already requires never reads the guest
// (#1593).
func TestRecreateHostVMBaseActsOnlyOnAnObservedDifference(t *testing.T) {
	at := time.Date(2026, 11, 11, 20, 0, 0, 0, time.UTC)
	observed := func(guestOS string) func(context.Context) (limarunner.GuestOSObservation, error) {
		return func(context.Context) (limarunner.GuestOSObservation, error) {
			return limarunner.GuestOSObservation{GuestOS: guestOS, ObservedAt: at}, nil
		}
	}
	for name, tc := range map[string]struct {
		class       ocicontrol.ConvergenceClass
		observe     func(context.Context) (limarunner.GuestOSObservation, error)
		wantClass   ocicontrol.ConvergenceClass
		wantDiffers []string
		wantErr     bool
		wantBase    bool
	}{
		"older base, unchanged":        {ocicontrol.ConvergenceUnchanged, observed("ubuntu-24.04"), ocicontrol.ConvergenceRecreateRequired, []string{"vm_disk", "guest_os"}, false, true},
		"older base, restart required": {ocicontrol.ConvergenceRestartRequired, observed("ubuntu-24.04"), ocicontrol.ConvergenceRecreateRequired, []string{"vm_disk", "guest_os"}, false, true},
		"current base":                 {ocicontrol.ConvergenceLiveSafe, observed(limarunner.TemplateGuestOS), ocicontrol.ConvergenceLiveSafe, []string{"vm_disk"}, false, true},
		"unreadable guest": {ocicontrol.ConvergenceUnchanged, func(context.Context) (limarunner.GuestOSObservation, error) {
			return limarunner.GuestOSObservation{}, &limarunner.GuestOSUnreadableError{}
		}, ocicontrol.ConvergenceUnchanged, []string{"vm_disk"}, true, false},
		"recreate already required": {ocicontrol.ConvergenceRecreateRequired, func(context.Context) (limarunner.GuestOSObservation, error) {
			t.Fatal("a required recreate read the guest")
			return limarunner.GuestOSObservation{}, nil
		}, ocicontrol.ConvergenceRecreateRequired, []string{"vm_disk"}, false, false},
	} {
		t.Run(name, func(t *testing.T) {
			response := ocicontrol.SetupResponse{Convergence: tc.class, Differences: []string{"vm_disk"}}
			err := recreateHostVMBase(t.Context(), &response, tc.observe)
			var unreadable *limarunner.GuestOSUnreadableError
			if tc.wantErr != errors.As(err, &unreadable) || !tc.wantErr && err != nil {
				t.Fatalf("err = %v", err)
			}
			if response.Convergence != tc.wantClass || !slices.Equal(response.Differences, tc.wantDiffers) || (response.Base != nil) != tc.wantBase {
				t.Fatalf("response = %+v", response)
			}
		})
	}
}
