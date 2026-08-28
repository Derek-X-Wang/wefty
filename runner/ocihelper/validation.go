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
	distributionref "github.com/distribution/reference"
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
	if len(input.ReservedEnvironment) > 0 && !input.helperMintedReserved {
		return errors.New("reserved environment values must be minted by the helper")
	}
	if input.helperMintedReserved {
		if err := validateEnvironmentLayer("reserved environment", input.ReservedEnvironment, true); err != nil {
			return err
		}
	}
	if strings.IndexByte(input.L3Endpoint, 0) >= 0 || strings.IndexByte(input.RunToken, 0) >= 0 {
		return errors.New("helper environment minting inputs contain NUL")
	}
	if input.Limits.MemoryBytes < 0 || input.Limits.CPUMillicores < 0 {
		return errors.New("workload limits must not be negative")
	}
	seenManagedKinds := make(map[ManagedVolumeKind]struct{}, len(input.ManagedVolumes))
	seenVolumes := make(map[string]struct{}, len(input.OperatorMounts))
	for _, volume := range input.ManagedVolumes {
		switch volume.Kind {
		case ManagedVolumeHandoff, ManagedVolumeServiceData, ManagedVolumeComputerDisk, ManagedVolumeLogSegments:
		default:
			return fmt.Errorf("managed volume kind %q is not supported", volume.Kind)
		}
		if _, exists := seenManagedKinds[volume.Kind]; exists {
			return fmt.Errorf("managed volume kind %q is duplicated", volume.Kind)
		}
		seenManagedKinds[volume.Kind] = struct{}{}
		if volume.Kind == ManagedVolumeHandoff {
			if volume.OwnerKey == "" || strings.TrimSpace(volume.OwnerKey) != volume.OwnerKey || len(volume.OwnerKey) > 255 || strings.IndexByte(volume.OwnerKey, 0) >= 0 {
				return errors.New("handoff managed volume requires a bounded non-empty owner key")
			}
		} else if volume.OwnerKey != "" {
			return fmt.Errorf("managed volume kind %q does not accept an owner key", volume.Kind)
		}
		if volume.Kind == ManagedVolumeComputerDisk {
			storage := volume.ComputerStorage
			if storage == nil || !boundedStorageID(storage.ComputerID) || !boundedStorageID(storage.StorageID) ||
				storage.StorageGeneration < 1 || storage.IntentRevision < 1 || storage.DiskBytes <= 0 {
				return errors.New("computer disk requires a bounded durable Storage identity and positive allocation")
			}
		} else if volume.ComputerStorage != nil {
			return fmt.Errorf("managed volume kind %q does not accept Computer Storage", volume.Kind)
		}
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
	_, computerDisk := seenManagedKinds[ManagedVolumeComputerDisk]
	if input.Computer != computerDisk {
		return errors.New("Computer trait and durable Computer disk descriptor must agree")
	}
	if input.Computer {
		for _, mount := range input.OperatorMounts {
			if !mount.ReadOnly {
				return errors.New("Computer operator mounts must be read-only so tenant writes remain bounded")
			}
		}
	}
	return nil
}

func boundedStorageID(value string) bool {
	return value != "" && strings.TrimSpace(value) == value && len(value) <= 255 && strings.IndexByte(value, 0) < 0
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
		if !reservedOnly && contract.IsOCIReservedEnvironmentName(variable.Name) {
			return fmt.Errorf("%s variable %q must use a closed helper-minting input", layerName, variable.Name)
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
		contract.OCIContainerControlDirectory,
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
		if event.Progress == nil || event.Result != nil || event.Progress.Status == "" || event.Progress.Completed < 0 || event.Progress.Total < 0 || event.Progress.Completed > event.Progress.Total ||
			(event.Progress.TopLevelDigest != "" && !digestPattern.MatchString(event.Progress.TopLevelDigest)) {
			return errors.New("invalid image progress event")
		}
	case ImageComplete:
		if event.Progress != nil || event.Result == nil || !digestPattern.MatchString(event.Result.TopLevelDigest) || !digestPattern.MatchString(event.Result.PlatformDigest) ||
			!validImageEvidence(event.Result.Evidence) || event.Result.Evidence.TopLevelDigest != event.Result.TopLevelDigest ||
			event.Result.Evidence.PlatformManifestDigest != event.Result.PlatformDigest {
			return errors.New("invalid image completion event")
		}
	default:
		return errors.New("unknown image event kind")
	}
	return nil
}

func validImageEvidence(evidence ImageEvidence) bool {
	if strings.TrimSpace(evidence.SubmittedReference) == "" || strings.TrimSpace(evidence.TopLevelMediaType) == "" ||
		strings.TrimSpace(evidence.Platform.OS) == "" || strings.TrimSpace(evidence.Platform.Architecture) == "" ||
		strings.TrimSpace(evidence.RuntimeHandler) == "" || strings.TrimSpace(evidence.Snapshotter) == "" ||
		!digestPattern.MatchString(evidence.TopLevelDigest) || !digestPattern.MatchString(evidence.PlatformManifestDigest) {
		return false
	}
	return evidence.IndexDigest == nil || digestPattern.MatchString(*evidence.IndexDigest)
}

func validateEnsureImageRequest(request EnsureImageRequest) error {
	source := request.Source
	if source == "" {
		source = ImageSourceRegistry
	}
	if request.Digest != "" && !digestPattern.MatchString(request.Digest) {
		return errors.New("image digest must be sha256 plus 64 lowercase hexadecimal characters")
	}
	if request.OperationTimeout < 0 {
		return errors.New("image operation timeout must not be negative")
	}
	if request.Pin != nil {
		if err := request.Pin.Authority.validate(); err != nil {
			return err
		}
		if request.Pin.Binding && request.Pin.Authority.Class != "service" {
			return errors.New("binding image pins require service-class authority")
		}
	}
	if strings.TrimSpace(request.Platform.OS) == "" || strings.TrimSpace(request.Platform.Architecture) == "" ||
		request.Platform.OS != strings.ToLower(strings.TrimSpace(request.Platform.OS)) ||
		request.Platform.Architecture != strings.ToLower(strings.TrimSpace(request.Platform.Architecture)) ||
		request.Platform.Variant != strings.ToLower(strings.TrimSpace(request.Platform.Variant)) {
		return errors.New("image platform must be canonical and include os and architecture")
	}
	switch source {
	case ImageSourceRegistry:
		if strings.TrimSpace(request.Reference) == "" {
			return errors.New("registry image reference is required")
		}
		named, err := distributionref.ParseNormalizedNamed(request.Reference)
		if err != nil {
			return errors.New("registry image reference is invalid")
		}
		if _, ok := named.(distributionref.Digested); ok {
			return errors.New("registry image reference must not contain a digest")
		}
	case ImageSourceArchive:
		// Archive names and expected digests are optional: the verified OCI
		// layout is authoritative and returns both selected digests.
	default:
		return fmt.Errorf("image source %q is unsupported", source)
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
