package ocicontrol

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/systemdpolicy"
)

// SetupState is the durable, explicit node-local configuration compared by
// setup. Probe identity is live-safe; fixed VM sizing requires a restart; VM
// topology or the operator mount root requires recreation.
type SetupState struct {
	VMMemory            string `json:"vm_memory"`
	VMCPUs              int    `json:"vm_cpus"`
	VMDisk              string `json:"vm_disk"`
	VMType              string `json:"vm_type"`
	HostMountRoot       string `json:"host_mount_root"`
	ProbeDigest         string `json:"probe_digest"`
	MemoryCapacityBytes int64  `json:"memory_capacity_bytes,omitempty"`
	MemoryReserveBytes  int64  `json:"memory_reserve_bytes,omitempty"`
	SystemdVersion      int    `json:"systemd_version,omitempty"`
	HelperRestartPolicy string `json:"helper_restart_policy,omitempty"`
}

func DesiredSetupStatePath(currentPath string) string {
	if currentPath == "" {
		return ""
	}
	return currentPath + ".desired"
}

func ClassifyConvergence(current, desired SetupState) ConvergenceClass {
	if current == desired {
		return ConvergenceUnchanged
	}
	if current.VMType != desired.VMType || current.HostMountRoot != desired.HostMountRoot {
		return ConvergenceRecreateRequired
	}
	if current.VMMemory != desired.VMMemory || current.VMCPUs != desired.VMCPUs || current.VMDisk != desired.VMDisk ||
		current.MemoryCapacityBytes != desired.MemoryCapacityBytes || current.MemoryReserveBytes != desired.MemoryReserveBytes ||
		current.SystemdVersion != desired.SystemdVersion || current.HelperRestartPolicy != desired.HelperRestartPolicy {
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
			return controlError(ErrorTemplateRecreateRequired, 409, "template recreation required", nil)
		}
		if liveOCIAttempts != 0 {
			return errors.New("template recreation requires zero live OCI attempts")
		}
	case ConvergenceRestartRequired:
		if !applyRestart {
			return controlError(ErrorTemplateRestartRequired, 409, "template restart required", nil)
		}
		if liveOCIAttempts != 0 {
			return errors.New("template restart requires zero live OCI attempts")
		}
	}
	return nil
}

func ReadSetupState(path string) (SetupState, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return SetupState{}, err
	}
	var state SetupState
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return SetupState{}, errors.New("invalid OCI setup state")
	}
	if err := validateSetupState(state); err != nil {
		return SetupState{}, err
	}
	return state, nil
}

func WriteSetupState(path string, state SetupState) error {
	if !filepath.IsAbs(path) {
		return errors.New("OCI setup state path must be absolute")
	}
	if err := validateSetupState(state); err != nil {
		return err
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return writeAtomic(path, append(payload, '\n'), 0o600)
}

func validateSetupState(state SetupState) error {
	if err := (lima.Sizing{Memory: state.VMMemory, CPUs: state.VMCPUs, Disk: state.VMDisk}).Validate(); err != nil {
		return errors.New("invalid OCI setup state")
	}
	if state.VMType == "" || state.ProbeDigest == "" || !filepath.IsAbs(state.HostMountRoot) || filepath.Clean(state.HostMountRoot) == string(filepath.Separator) {
		return errors.New("invalid OCI setup state")
	}
	if state.MemoryCapacityBytes < 0 || state.MemoryReserveBytes < 0 {
		return errors.New("invalid OCI setup state")
	}
	if state.SystemdVersion < 0 ||
		state.SystemdVersion == 0 && state.HelperRestartPolicy != "" && state.HelperRestartPolicy != "conservative_fixed_1s" ||
		state.SystemdVersion > 0 && state.HelperRestartPolicy != setupStateRestartPolicy(state.SystemdVersion) {
		return errors.New("invalid OCI setup state")
	}
	return nil
}

func setupStateRestartPolicy(systemdVersion int) string {
	if systemdVersion == 0 {
		return ""
	}
	return systemdpolicy.Name(systemdVersion)
}
