package ocihelper

import (
	"errors"
	"fmt"
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
	if !digestPattern.MatchString(input.ImageDigest) {
		return errors.New("workload image must be an immutable sha256 digest")
	}
	if len(input.Argv) == 0 {
		return errors.New("workload argv must not be empty")
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
		return errors.New("workload argv must contain a non-empty argument")
	}
	if input.WorkingDirectory != "" && !validContainerPath(input.WorkingDirectory) {
		return errors.New("working directory must be a clean absolute container path")
	}
	seenEnvironment := make(map[string]struct{}, len(input.Environment))
	for _, variable := range input.Environment {
		if !envNamePattern.MatchString(variable.Name) || contract.IsOCIReservedEnvironmentName(variable.Name) {
			return fmt.Errorf("environment variable %q is not permitted", variable.Name)
		}
		if _, exists := seenEnvironment[variable.Name]; exists {
			return fmt.Errorf("environment variable %q is duplicated", variable.Name)
		}
		seenEnvironment[variable.Name] = struct{}{}
		if strings.IndexByte(variable.Value, 0) >= 0 {
			return fmt.Errorf("environment variable %q contains NUL", variable.Name)
		}
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
		if !withinAllowedRoot(mount.NodePath, allowedMountRoots) {
			return fmt.Errorf("operator mount source %q is outside configured roots", mount.NodePath)
		}
	}
	return nil
}

func conflictsWithReservedMount(target string) bool {
	for _, reserved := range []string{contract.OCIContainerHandoffDirectory, contract.OCIContainerServiceDirectory} {
		if target == reserved || strings.HasPrefix(target, reserved+"/") || strings.HasPrefix(reserved, target+"/") {
			return true
		}
	}
	return false
}

func validContainerPath(value string) bool {
	return strings.HasPrefix(value, "/") && value != "/" && path.Clean(value) == value
}

func withinAllowedRoot(value string, roots []string) bool {
	if !filepath.IsAbs(value) || filepath.Clean(value) != value || len(roots) == 0 {
		return false
	}
	resolvedValue, err := filepath.EvalSymlinks(value)
	if err != nil {
		return false
	}
	for _, root := range roots {
		if !filepath.IsAbs(root) || filepath.Clean(root) != root {
			continue
		}
		resolvedRoot, err := filepath.EvalSymlinks(root)
		if err != nil {
			continue
		}
		relative, err := filepath.Rel(resolvedRoot, resolvedValue)
		if err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return true
		}
	}
	return false
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
		if event.Status == "" || event.Result != nil {
			return errors.New("invalid watch progress event")
		}
	case WatchComplete:
		if event.Status != "" || event.Result == nil {
			return errors.New("invalid watch completion event")
		}
	default:
		return errors.New("unknown watch event kind")
	}
	return nil
}
