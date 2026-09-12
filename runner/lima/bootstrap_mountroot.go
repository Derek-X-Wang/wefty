// Package-internal mount-root admission: a helper unit may only be installed
// with a host mount root the Lima instance actually virtiofs-mounts, or every
// later operator mount dies as oci_spec_rejected with nothing naming the cause.
package lima

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

// HostMountRootNotMountedReason is the typed refusal code for a requested
// guest helper host mount root the Lima instance does not mount.
const HostMountRootNotMountedReason = "host_mount_root_not_mounted"

// HostMountRootNotMountedError is the typed bootstrap refusal: it names only
// the requested root and the instance's mounted locations, never secrets.
type HostMountRootNotMountedError struct {
	Root             string   `json:"root"`
	MountedLocations []string `json:"mounted_locations"`
}

func (err *HostMountRootNotMountedError) Error() string {
	return fmt.Sprintf("%s: guest helper host mount root %s is not covered by the Lima instance's mounts %s",
		HostMountRootNotMountedReason, err.Root, strings.Join(err.MountedLocations, ", "))
}

func (err *HostMountRootNotMountedError) ReasonCode() string {
	return HostMountRootNotMountedReason
}

// resolveMountPath cleans a path and resolves symlinked components so mount
// locations written with an alias match the real directories they point at.
func resolveMountPath(path string) string {
	clean := filepath.Clean(path)
	if resolved, err := filepath.EvalSymlinks(clean); err == nil {
		return resolved
	}
	return clean
}

// ValidateHostMountRootIsMounted is the platform-neutral decision: the
// requested root is accepted when, after cleaning and symlink resolution, it
// equals one of the instance's configured mount locations or lies inside one
// (a location that is a parent directory of the root mounts it).
func ValidateHostMountRootIsMounted(root string, locations []string) error {
	if filepath.IsAbs(root) {
		resolvedRoot := resolveMountPath(root)
		for _, location := range locations {
			resolvedLocation := resolveMountPath(location)
			if resolvedLocation == resolvedRoot {
				return nil
			}
			if relative, err := filepath.Rel(resolvedLocation, resolvedRoot); err == nil {
				if relative == "." || relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
					return nil
				}
			}
		}
	}
	return &HostMountRootNotMountedError{
		Root:             filepath.Clean(root),
		MountedLocations: cleanedMountLocations(locations),
	}
}

func cleanedMountLocations(locations []string) []string {
	cleaned := make([]string, 0, len(locations))
	for _, location := range locations {
		cleaned = append(cleaned, filepath.Clean(location))
	}
	return cleaned
}

// parseInstanceMountLocations reads a Lima instance's configured virtiofs
// mount locations from its own inventory (`limactl list --json` emits each
// instance's record, including its stored lima.yaml as `config`). It never
// reads the instance directory directly: the code's instance facts come from
// the inventory stream.
func parseInstanceMountLocations(payload []byte, instance string) ([]string, error) {
	if !instanceNamePattern.MatchString(instance) {
		return nil, errors.New("guest helper host mount verification requires a valid Lima instance")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	for {
		var record struct {
			Name   string `json:"name"`
			Config struct {
				Mounts []struct {
					Location string `json:"location"`
				} `json:"mounts"`
			} `json:"config"`
		}
		decodeErr := decoder.Decode(&record)
		if errors.Is(decodeErr, io.EOF) {
			break
		}
		if decodeErr != nil {
			return nil, fmt.Errorf("inspect the mounts of Lima instance %q: invalid JSON: %w", instance, decodeErr)
		}
		if record.Name != instance {
			continue
		}
		locations := make([]string, 0, len(record.Config.Mounts))
		for _, mount := range record.Config.Mounts {
			if mount.Location != "" {
				locations = append(locations, mount.Location)
			}
		}
		return locations, nil
	}
	return nil, fmt.Errorf("Lima instance %q was not found in the inventory for mount verification", instance)
}
