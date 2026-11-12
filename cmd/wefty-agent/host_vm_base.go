package main

import (
	"context"

	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

// recreateHostVMBase decides what `setup-oci --recreate` does with the host
// VM base (#1593). It reads the running guest rather than an older record, so
// it never acts on an unknown base: a failed read refuses. A base that differs
// from the template makes this run a recreate under the unchanged #1021
// guard. A recreate that the setup state already requires installs the
// template base anyway, so it does not depend on this read.
func recreateHostVMBase(ctx context.Context, response *ocicontrol.SetupResponse, observe func(context.Context) (limarunner.GuestOSObservation, error)) error {
	if response.Convergence == ocicontrol.ConvergenceRecreateRequired {
		return nil
	}
	observation, err := observe(ctx)
	if err != nil {
		return err
	}
	base := limarunner.NewHostVMBase(observation, true)
	response.Base = &base
	if base.UpgradeAvailable {
		response.Convergence = ocicontrol.ConvergenceRecreateRequired
		response.Differences = append(response.Differences, ocicontrol.HostVMBaseField)
	}
	return nil
}
