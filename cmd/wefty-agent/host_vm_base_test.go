package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

// Under --recreate an older base turns the run into a recreate, a current
// base changes nothing, and an unreadable guest refuses before any class
// change, also when the setup state already requires a recreate (#1593).
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
		"recreate required, older base":   {ocicontrol.ConvergenceRecreateRequired, observed("ubuntu-24.04"), ocicontrol.ConvergenceRecreateRequired, []string{"vm_disk", "guest_os"}, false, true},
		"recreate required, current base": {ocicontrol.ConvergenceRecreateRequired, observed(limarunner.TemplateGuestOS), ocicontrol.ConvergenceRecreateRequired, []string{"vm_disk"}, false, true},
		"recreate required, unreadable guest": {ocicontrol.ConvergenceRecreateRequired, func(context.Context) (limarunner.GuestOSObservation, error) {
			return limarunner.GuestOSObservation{}, &limarunner.GuestOSUnreadableError{}
		}, ocicontrol.ConvergenceRecreateRequired, []string{"vm_disk"}, true, false},
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

// An unreadable guest under --recreate refuses before lima.yaml changes, so a
// refused run leaves the host VM configuration byte-for-byte as it was and
// the caller never reaches the desired-state write or the recreate (#1593).
// Without --recreate the same unreadable guest is never read and sizing
// applies.
func TestUnreadableGuestUnderRecreateChangesNoConfiguration(t *testing.T) {
	fixture, err := os.ReadFile(filepath.Join("..", "..", "runner", "lima", "testdata", "lima-2.2.0-created-instance.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	limaHome := t.TempDir()
	config := filepath.Join(limaHome, "wefty-oci", "lima.yaml")
	if err := os.Mkdir(filepath.Dir(config), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config, fixture, 0o600); err != nil {
		t.Fatal(err)
	}
	reads := 0
	unreadable := func(context.Context) (limarunner.GuestOSObservation, error) {
		reads++
		return limarunner.GuestOSObservation{}, &limarunner.GuestOSUnreadableError{}
	}
	request := ocicontrol.SetupRequest{VMMemory: "6GiB", VMCPUs: 6, VMDisk: "64GiB", Recreate: true}
	response := ocicontrol.SetupResponse{Convergence: ocicontrol.ConvergenceRecreateRequired}
	_, err = setupHostVMBaseAndSizing(t.Context(), &response, request, limaHome, "wefty-oci", unreadable)
	var refusal *limarunner.GuestOSUnreadableError
	if !errors.As(err, &refusal) || reads != 1 {
		t.Fatalf("err = %v after %d reads", err, reads)
	}
	if after, err := os.ReadFile(config); err != nil || !bytes.Equal(after, fixture) {
		t.Fatalf("refused recreate changed lima.yaml (err %v)", err)
	}
	request.Recreate = false
	response = ocicontrol.SetupResponse{Convergence: ocicontrol.ConvergenceRestartRequired}
	path, err := setupHostVMBaseAndSizing(t.Context(), &response, request, limaHome, "wefty-oci", unreadable)
	if err != nil || reads != 1 || path != filepath.Join(limaHome, "wefty-oci", limarunner.GuestOSObservationFile) {
		t.Fatalf("plain setup = %q, %v after %d reads", path, err, reads)
	}
	if response.Base == nil || response.Base.Current != "" || response.Convergence != ocicontrol.ConvergenceRestartRequired {
		t.Fatalf("plain setup response = %+v", response)
	}
	if after, err := os.ReadFile(config); err != nil || bytes.Equal(after, fixture) {
		t.Fatalf("plain setup did not apply sizing (err %v)", err)
	}
}
