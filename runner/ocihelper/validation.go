package ocihelper

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
)

var (
	digestPattern  = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	envNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

func validateWorkload(input WorkloadInput, allowedMountRoots []string) error {
	return validateWorkloadWithSource(input, allowedMountRoots, validateMountSource)
}

func validateWorkloadWithSource(input WorkloadInput, allowedMountRoots []string, validateSource func(string, []string, bool) error) error {
	if err := validateWorkloadWire(input); err != nil {
		return err
	}
	for _, mount := range input.OperatorMounts {
		if err := validateSource(mount.NodePath, allowedMountRoots, false); err != nil {
			return fmt.Errorf("operator mount source %q is not permitted: %w", mount.NodePath, err)
		}
	}
	return nil
}

// validateWorkloadWire validates only closed protocol data. Filesystem
// authority is checked after the helper translates node paths into its guest
// view; doing it here would reject every non-identity Lima translation.
func validateWorkloadWire(input WorkloadInput) error {
	if !digestPattern.MatchString(input.ImageDigest) {
		return errors.New("workload image must be an immutable sha256 digest")
	}
	if input.Argv != nil {
		if len(input.Argv) == 0 {
			return errors.New("workload argv override must not be empty")
		}
		hasNonEmptyArgument := false
		for _, argument := range input.Argv {
			if strings.IndexByte(argument, 0) >= 0 {
				return errors.New("workload argv contains NUL")
			}
			if argument != "" {
				hasNonEmptyArgument = true
			}
		}
		if !hasNonEmptyArgument {
			return errors.New("workload argv override must contain a non-empty argument")
		}
	}
	if input.WorkingDirectory != "" && !validContainerPath(input.WorkingDirectory) {
		return errors.New("working directory must be a clean absolute container path")
	}
	if err := validateEnvironmentLayer("environment", input.Environment, false); err != nil {
		return err
	}
	if err := validateEnvironmentLayer("sensitive environment", input.SensitiveEnvironment, false); err != nil {
		return err
	}
	if err := validateEnvironmentLayer("reserved environment", input.ReservedEnvironment, true); err != nil {
		return err
	}
	if input.Limits.MemoryBytes < 0 || input.Limits.CPUMillicores < 0 {
		return errors.New("workload limits must not be negative")
	}
	seenManagedKinds := make(map[ManagedVolumeKind]struct{}, len(input.ManagedVolumes))
	seenVolumes := make(map[string]struct{}, len(input.OperatorMounts))
	for _, volume := range input.ManagedVolumes {
		switch volume.Kind {
		case ManagedVolumeHandoff, ManagedVolumeServiceData, ManagedVolumeLogSegments:
		default:
			return fmt.Errorf("managed volume kind %q is not supported", volume.Kind)
		}
		if _, exists := seenManagedKinds[volume.Kind]; exists {
			return fmt.Errorf("managed volume kind %q is duplicated", volume.Kind)
		}
		seenManagedKinds[volume.Kind] = struct{}{}
	}
	for _, mount := range input.OperatorMounts {
		if !validMountSourcePath(mount.NodePath) {
			return errors.New("operator mount source must be a clean absolute non-root path")
		}
		if !validContainerPath(mount.ContainerPath) {
			return errors.New("operator mount target must be a clean absolute container path")
		}
		if conflictsWithReservedMount(mount.ContainerPath) {
			return fmt.Errorf("operator mount target %q conflicts with a helper-managed target", mount.ContainerPath)
		}
		if _, exists := seenVolumes[mount.ContainerPath]; exists {
			return fmt.Errorf("container mount target %q is duplicated", mount.ContainerPath)
		}
		seenVolumes[mount.ContainerPath] = struct{}{}
	}
	return nil
}

func validateEnvironmentLayer(layerName string, environment []EnvironmentVariable, reservedOnly bool) error {
	seen := make(map[string]struct{}, len(environment))
	for _, variable := range environment {
		if !envNamePattern.MatchString(variable.Name) {
			return fmt.Errorf("%s variable %q is invalid", layerName, variable.Name)
		}
		if reservedOnly && !contract.IsOCIReservedEnvironmentName(variable.Name) {
			return fmt.Errorf("reserved environment variable %q is not helper-managed", variable.Name)
		}
		if _, exists := seen[variable.Name]; exists {
			return fmt.Errorf("%s variable %q is duplicated", layerName, variable.Name)
		}
		seen[variable.Name] = struct{}{}
		if strings.IndexByte(variable.Value, 0) >= 0 {
			return fmt.Errorf("%s variable %q contains NUL", layerName, variable.Name)
		}
	}
	return nil
}

func conflictsWithReservedMount(target string) bool {
	for _, reserved := range []string{
		contract.OCIContainerHandoffDirectory,
		contract.OCIContainerServiceDirectory,
		"/proc",
		"/dev",
		"/sys",
		"/run",
		"/etc/hosts",
		"/etc/resolv.conf",
	} {
		if target == reserved || strings.HasPrefix(target, reserved+"/") || strings.HasPrefix(reserved, target+"/") {
			return true
		}
	}
	return false
}

func validContainerPath(value string) bool {
	return strings.HasPrefix(value, "/") && value != "/" && path.Clean(value) == value
}

func validMountSourcePath(value string) bool {
	return filepath.IsAbs(value) && filepath.Clean(value) == value && value != string(filepath.Separator)
}

func withinAllowedRoot(value string, roots []string) bool {
	return validateMountSource(value, roots, false) == nil
}

func validateMountSource(value string, roots []string, regularFileOnly bool) error {
	source, err := openValidatedMountSource(value, roots, regularFileOnly)
	if err != nil {
		return err
	}
	return source.close()
}

func rejectSymlinkComponents(value string) error {
	volume := filepath.VolumeName(value)
	remainder := strings.TrimPrefix(value, volume)
	current := volume + string(filepath.Separator)
	for _, component := range strings.Split(strings.TrimPrefix(remainder, string(filepath.Separator)), string(filepath.Separator)) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("path component %q is a symlink", current)
		}
	}
	return nil
}

func validateEnsureImageEvent(event EnsureImageEvent) error {
	switch event.Kind {
	case ImageProgress:
		if event.Progress == nil || event.Result != nil || event.Progress.Completed < 0 || event.Progress.Total < 0 || event.Progress.Completed > event.Progress.Total {
			return errors.New("invalid image progress event")
		}
	case ImageComplete:
		if event.Progress != nil || event.Result == nil || event.Result.TopLevelDigest == "" || event.Result.PlatformDigest == "" {
			return errors.New("invalid image completion event")
		}
	default:
		return errors.New("unknown image event kind")
	}
	return nil
}

func validateWatchEvent(event WatchEvent) error {
	switch event.Kind {
	case WatchProgress:
		present := 0
		if event.Status != "" {
			present++
		}
		if event.Log != nil {
			present++
		}
		if event.Seal != nil {
			present++
		}
		if event.Result != nil || present != 1 {
			return errors.New("invalid watch progress event")
		}
		if event.Log != nil {
			if event.Log.Stream != "stdout" && event.Log.Stream != "stderr" {
				return errors.New("invalid watch log frame")
			}
			if event.Log.Gap == nil {
				checksum := sha256.Sum256(event.Log.Bytes)
				if event.Log.Checksum != hex.EncodeToString(checksum[:]) {
					return errors.New("invalid watch log frame")
				}
			} else if len(event.Log.Bytes) != 0 || event.Log.Checksum != "" || event.Log.Gap.LostEventCount == 0 || event.Log.Gap.LostByteCount == 0 || event.Log.Gap.ThroughSequence < event.Log.Sequence || event.Log.Gap.Reason != "logger_source_incomplete" {
				return errors.New("invalid watch log gap")
			}
		}
		if event.Seal != nil {
			if (event.Seal.Stream != "stdout" && event.Seal.Stream != "stderr") || (event.Seal.Complete && event.Seal.Reason != "") || (!event.Seal.Complete && event.Seal.Reason == "") {
				return errors.New("invalid watch log seal")
			}
		}
	case WatchComplete:
		if event.Status != "" || event.Log != nil || event.Seal != nil || event.Result == nil {
			return errors.New("invalid watch completion event")
		}
		result := event.Result
		primary := 0
		if result.ExitCode != nil {
			primary++
		}
		if result.Signal != "" {
			primary++
		}
		if result.RuntimeFailure != "" {
			primary++
		}
		if primary != 1 || (result.Signal == "") != (result.TerminationCause == "") {
			return errors.New("watch completion must contain exactly one result arm")
		}
	default:
		return errors.New("unknown watch event kind")
	}
	return nil
}
