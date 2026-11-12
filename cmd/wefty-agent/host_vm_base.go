package main

import (
	"context"
	"path/filepath"

	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

// recreateHostVMBase decides what `setup-oci --recreate` does with the host
// VM base (#1593). Every --recreate reads the running guest rather than an
// older record, so no host VM is deleted while its base is unknown: a failed
// read refuses, even when the setup state already requires a recreate. A
// base that differs from the template makes this run a recreate under the
// unchanged #1021 guard. The caller runs this before it writes any
// configuration, so a refusal changes nothing.
func recreateHostVMBase(ctx context.Context, response *ocicontrol.SetupResponse, observe func(context.Context) (limarunner.GuestOSObservation, error)) error {
	observation, err := observe(ctx)
	if err != nil {
		return err
	}
	base := limarunner.NewHostVMBase(observation, true)
	response.Base = &base
	if base.UpgradeAvailable {
		response.Convergence = ocicontrol.ConvergenceRecreateRequired // also when the setup state already required it
		response.Differences = append(response.Differences, ocicontrol.HostVMBaseField)
	}
	return nil
}

// setupHostVMBaseAndSizing reports the host VM base and applies the requested
// sizing to the Lima instance. The base from the recorded observation is
// advisory: an unobserved or unreadable record reports no current value and
// never blocks. Under --recreate the fresh read runs first, so its refusal
// returns before lima.yaml changes and before the caller writes the desired
// setup state. It returns the observation path for the post-recreate report.
func setupHostVMBaseAndSizing(ctx context.Context, response *ocicontrol.SetupResponse, request ocicontrol.SetupRequest, limaHome, instance string, observe func(context.Context) (limarunner.GuestOSObservation, error)) (string, error) {
	observationPath, err := limarunner.GuestOSObservationPath(limaHome, instance)
	if err != nil {
		return "", err
	}
	observation, observeErr := limarunner.ReadGuestOSObservation(observationPath)
	base := limarunner.NewHostVMBase(observation, observeErr == nil)
	response.Base = &base
	if request.Recreate {
		if err := recreateHostVMBase(ctx, response, observe); err != nil {
			return "", err
		}
	}
	err = limarunner.UpdateInstanceSizing(filepath.Join(limaHome, instance, "lima.yaml"),
		limarunner.Sizing{Memory: request.VMMemory, CPUs: request.VMCPUs, Disk: request.VMDisk})
	return observationPath, err
}
