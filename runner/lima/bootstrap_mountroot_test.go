package lima

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The decision is the same one the bootstrap guard runs: a root matches when,
// after cleaning and symlink resolution, it equals a configured location or
// lies inside one (the location itself is a parent directory of the root).
func TestValidateHostMountRootIsMounted(t *testing.T) {
	base := t.TempDir()
	if err := os.MkdirAll(filepath.Join(base, "real"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(base, "real"), filepath.Join(base, "alias")); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		root      string
		locations []string
		want      *HostMountRootNotMountedError
	}{
		{name: "exact_match", root: "/Users/operator/wefty-mounts", locations: []string{"/Users/operator/wefty-mounts"}},
		{name: "parent_directory_match", root: "/Users/operator/wefty-mounts/run-6", locations: []string{"/Users/operator/wefty-mounts"}},
		{name: "cleaned_match", root: "/Users/operator/wefty-mounts/run-6/../", locations: []string{"/Users/operator/wefty-mounts"}},
		{name: "symlink_resolved_match", root: filepath.Join(base, "real"), locations: []string{filepath.Join(base, "alias")}},
		{name: "not_mounted", root: "/Users/operator/wefty-mounts", locations: []string{"/Users/operator/other"},
			want: &HostMountRootNotMountedError{Root: "/Users/operator/wefty-mounts", MountedLocations: []string{"/Users/operator/other"}}},
		{name: "no_mounts_configured", root: "/Users/operator/wefty-mounts", locations: nil,
			want: &HostMountRootNotMountedError{Root: "/Users/operator/wefty-mounts"}},
		{name: "location_outside_root_is_not_covered", root: "/Users/operator/wefty-mounts", locations: []string{"/Users/operator/wefty-mounts/run-4"},
			want: &HostMountRootNotMountedError{Root: "/Users/operator/wefty-mounts", MountedLocations: []string{"/Users/operator/wefty-mounts/run-4"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateHostMountRootIsMounted(test.root, test.locations)
			if test.want == nil {
				if err != nil {
					t.Fatalf("root %s with locations %v = %v, want mounted", test.root, test.locations, err)
				}
				return
			}
			var refusal *HostMountRootNotMountedError
			if !errors.As(err, &refusal) {
				t.Fatalf("root %s with locations %v = %v, want typed %s refusal", test.root, test.locations, err, HostMountRootNotMountedReason)
			}
			if refusal.ReasonCode() != HostMountRootNotMountedReason {
				t.Fatalf("reason code = %q", refusal.ReasonCode())
			}
			if refusal.Root != test.want.Root {
				t.Fatalf("refusal root = %q, want %q", refusal.Root, test.want.Root)
			}
			if len(refusal.MountedLocations) != len(test.want.MountedLocations) {
				t.Fatalf("refusal locations = %v, want %v", refusal.MountedLocations, test.want.MountedLocations)
			}
		})
	}
}

func TestParseInstanceMountLocations(t *testing.T) {
	payload := []byte(`{"name":"other"}` + "\n" +
		`{"name":"wefty-oci","config":{"mounts":[{"location":"/Users/operator/wefty-mounts","mountPoint":"/mnt/wefty-host"}]}}` + "\n")
	locations, err := parseInstanceMountLocations(payload, DefaultInstanceName)
	if err != nil {
		t.Fatal(err)
	}
	if len(locations) != 1 || locations[0] != "/Users/operator/wefty-mounts" {
		t.Fatalf("locations = %v", locations)
	}

	for _, test := range []struct {
		name, instance, wantError string
	}{
		{name: "absent_instance", instance: "missing", wantError: "was not found in the inventory"},
		{name: "malformed_payload", instance: DefaultInstanceName, wantError: "invalid JSON"},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := []byte(`{"name":`)
			if test.name == "absent_instance" {
				input = payload
			}
			if _, err := parseInstanceMountLocations(input, test.instance); err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("err = %v, want %q", err, test.wantError)
			}
		})
	}
}
