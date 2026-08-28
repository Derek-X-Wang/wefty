package ocicontrol

import "errors"

// SetupState is the durable, explicit node-local configuration compared by
// setup. Probe identity is live-safe; fixed VM sizing requires a restart; VM
// topology or the operator mount root requires recreation.
type SetupState struct {
	VMMemory      string `json:"vm_memory"`
	VMCPUs        int    `json:"vm_cpus"`
	VMDisk        string `json:"vm_disk"`
	VMType        string `json:"vm_type"`
	HostMountRoot string `json:"host_mount_root"`
	ProbeDigest   string `json:"probe_digest"`
}

func ClassifyConvergence(current, desired SetupState) ConvergenceClass {
	if current == desired {
		return ConvergenceUnchanged
	}
	if current.VMType != desired.VMType || current.HostMountRoot != desired.HostMountRoot {
		return ConvergenceRecreateRequired
	}
	if current.VMMemory != desired.VMMemory || current.VMCPUs != desired.VMCPUs || current.VMDisk != desired.VMDisk {
		return ConvergenceRestartRequired
	}
	return ConvergenceLiveSafe
}

func AuthorizeConvergence(class ConvergenceClass, applyRestart, recreate bool, liveOCIAttempts int) error {
	if !class.Valid() || liveOCIAttempts < 0 {
		return errors.New("invalid OCI setup convergence state")
	}
	switch class {
	case ConvergenceRecreateRequired:
		if !recreate {
			return errors.New("template_recreate_required")
		}
		if liveOCIAttempts != 0 {
			return errors.New("template recreation requires zero live OCI attempts")
		}
	case ConvergenceRestartRequired:
		if !applyRestart {
			return errors.New("template_restart_required")
		}
	}
	return nil
}
