package lima

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

// limaCreatedInstance is the lima.yaml that `limactl create` (Lima 2.2.0)
// wrote for the rendered setup template: `base` is resolved into images and a
// parameter block, and the mount location is replaced with a neutral path.
const limaCreatedInstance = "testdata/lima-2.2.0-created-instance.yaml"

// requireLimaLoadable applies the two Lima 2.2.0 instance-load rules that an
// unresolved template breaks (limayaml.Validate): no `base`, and `images` set.
func requireLimaLoadable(t *testing.T, config map[string]any) {
	t.Helper()
	if _, ok := config["base"]; ok {
		t.Fatal("instance config names a base; Lima refuses to load it")
	}
	if images, ok := config["images"].([]any); !ok || len(images) == 0 {
		t.Fatal("instance config has no images; Lima refuses to load it")
	}
}

func readConfigMap(t *testing.T, path string) map[string]any {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := yaml.Unmarshal(payload, &config); err != nil {
		t.Fatal(err)
	}
	return config
}

func copyFixture(t *testing.T) string {
	t.Helper()
	payload, err := os.ReadFile(limaCreatedInstance)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "lima.yaml")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestUpdateInstanceSizingKeepsTheResolvedConfig(t *testing.T) {
	path := copyFixture(t)
	before := readConfigMap(t, path)
	requireLimaLoadable(t, before)

	if err := UpdateInstanceSizing(path, Sizing{Memory: "6GiB", CPUs: 6, Disk: "64GiB"}); err != nil {
		t.Fatal(err)
	}
	after := readConfigMap(t, path)
	requireLimaLoadable(t, after)
	if after["cpus"] != 6 || after["memory"] != "6GiB" || after["disk"] != "64GiB" {
		t.Fatalf("sizing = cpus %v memory %v disk %v", after["cpus"], after["memory"], after["disk"])
	}
	for _, key := range []string{"cpus", "memory", "disk"} {
		delete(before, key)
		delete(after, key)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("setup changed fields it does not own:\nbefore %#v\nafter  %#v", before, after)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %v", info.Mode().Perm())
	}

	first, _ := os.ReadFile(path)
	if err := UpdateInstanceSizing(path, Sizing{Memory: "6GiB", CPUs: 6, Disk: "64GiB"}); err != nil {
		t.Fatal(err)
	}
	second, _ := os.ReadFile(path)
	if !bytes.Equal(first, second) {
		t.Fatal("a repeated update with the same sizing changed the file")
	}
}

func TestUpdateInstanceSizingLeavesAMissingInstanceAlone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lima.yaml")
	if err := UpdateInstanceSizing(path, Sizing{Memory: "4GiB", CPUs: 4, Disk: "32GiB"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("a config was created for an instance that does not exist: %v", err)
	}
}

func TestUpdateInstanceSizingRefusesAnUnresolvedConfig(t *testing.T) {
	// This is what setup wrote before #1595: the rendered template itself.
	unresolved, err := RenderTemplate(TemplateConfig{Sizing: Sizing{Memory: "4GiB", CPUs: 4, Disk: "32GiB"},
		HostAllowedMountRoot: "/Users/operator/wefty-mounts"})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "lima.yaml")
	if err := os.WriteFile(path, unresolved, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := UpdateInstanceSizing(path, Sizing{Memory: "6GiB", CPUs: 6, Disk: "64GiB"}); err == nil {
		t.Fatal("an unresolved instance config was accepted")
	}
	kept, _ := os.ReadFile(path)
	if !bytes.Equal(kept, unresolved) {
		t.Fatal("a refused update changed the file")
	}
}

func TestUpdateInstanceSizingRejectsInvalidInput(t *testing.T) {
	path := copyFixture(t)
	original, _ := os.ReadFile(path)
	for name, call := range map[string]func() error{
		"relative path": func() error {
			return UpdateInstanceSizing("lima.yaml", Sizing{Memory: "4GiB", CPUs: 4, Disk: "32GiB"})
		},
		"invalid sizing": func() error { return UpdateInstanceSizing(path, Sizing{Memory: "4GiB", CPUs: 0, Disk: "32GiB"}) },
	} {
		if err := call(); err == nil {
			t.Fatalf("%s: accepted", name)
		}
	}
	if kept, _ := os.ReadFile(path); !bytes.Equal(kept, original) {
		t.Fatal("a rejected update changed the file")
	}
}
