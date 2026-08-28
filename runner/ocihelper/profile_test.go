package ocihelper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/typeurl/v2"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

func TestRuntimeSpecGoldens(t *testing.T) {
	for _, test := range runtimeSpecGoldenCases() {
		t.Run(test.name, func(t *testing.T) {
			input := goldenRuntimeSpecInput(t, test.architecture)
			test.configure(&input)
			seccompFixture := filepath.Join("testdata", "containerd-v2.3.4", "seccomp-linux-"+test.architecture+".json")
			dependencies := goldenDependencies(t, seccompFixture)
			regenerate := os.Getenv("UPDATE_OCI_PROFILE_GOLDENS") == "1" && runtime.GOOS == "linux" && runtime.GOARCH == test.architecture
			if regenerate {
				dependencies.generateSeccomp = func(facts GuestKernelFacts, spec *specs.Spec) (*specs.LinuxSeccomp, error) {
					profile, err := generateGuestSeccomp(facts, spec)
					if err == nil {
						writeJSONFixture(t, seccompFixture, profile)
					}
					return profile, err
				}
			}
			spec, err := buildRuntimeSpec(context.Background(), input, dependencies)
			if err != nil {
				t.Fatal(err)
			}
			goldenPath := filepath.Join("testdata", "containerd-v2.3.4", test.golden)
			actual := redactSensitiveEnvironment(t, marshalRuntimeSpecIndented(t, spec), input.Workload.SensitiveEnvironment)
			if regenerate {
				if err := os.WriteFile(goldenPath, actual, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			expected, err := os.ReadFile(goldenPath)
			if err != nil {
				t.Fatal(err)
			}
			if string(actual) != string(expected) {
				t.Fatalf("serialized profile changed; regenerate and review containerd %s fixtures\nwant: %s\n got: %s", ContainerdBaselineVersion, expected, actual)
			}
		})
	}
}

type runtimeSpecGoldenCase struct {
	name         string
	architecture string
	golden       string
	configure    func(*RuntimeSpecInput)
	expectedUID  uint32
	expectedGID  uint32
	expectedGIDs []uint32
	unlimited    bool
	service      bool
}

func runtimeSpecGoldenCases() []runtimeSpecGoldenCase {
	identity := func(*RuntimeSpecInput) {}
	return []runtimeSpecGoldenCase{
		{name: "native Linux amd64", architecture: "amd64", golden: "wefty-v1-linux-amd64.json", expectedUID: 1001, expectedGID: 1002, expectedGIDs: []uint32{1002, 44, 2000}, configure: func(input *RuntimeSpecInput) {
			input.Guest.AppArmorProfile = "wefty-default"
		}},
		{name: "Lima Linux arm64", architecture: "arm64", golden: "wefty-v1-lima-arm64.json", expectedUID: 1001, expectedGID: 1002, expectedGIDs: []uint32{1002, 44, 2000}, configure: identity},
		{name: "service Linux amd64", architecture: "amd64", golden: "wefty-v1-service-linux-amd64.json", expectedUID: 1001, expectedGID: 1002, expectedGIDs: []uint32{1002, 44, 2000}, service: true, configure: func(input *RuntimeSpecInput) {
			input.ContainerID = "golden-service-amd64"
			input.CgroupPath = "/wefty/golden-service-amd64"
			input.Workload.ManagedVolumes = []ManagedVolumeDescriptor{{Kind: ManagedVolumeServiceData}, {Kind: ManagedVolumeLogSegments, ReadOnly: true}}
			input.Workload.OperatorMounts = nil
			input.OperatorMountSources = nil
			input.ManagedVolumeSources = map[ManagedVolumeKind]string{
				ManagedVolumeServiceData: "/run/wefty/fixtures/service",
				ManagedVolumeLogSegments: "/run/wefty/fixtures/log-segments",
			}
			input.Workload.ReservedEnvironment = []EnvironmentVariable{
				{Name: "WEFTY_SERVICE_DIR", Value: "/wefty/service"},
				{Name: "WEFTY_SERVICE_PORT", Value: "42100"},
				{Name: "WEFTY_RUN_TOKEN", Value: "authoritative-service-token"},
			}
		}},
		{name: "default root Linux amd64", architecture: "amd64", golden: "wefty-v1-default-root-linux-amd64.json", expectedGIDs: []uint32{0}, configure: func(input *RuntimeSpecInput) {
			input.ContainerID = "golden-default-root-amd64"
			input.CgroupPath = "/wefty/golden-default-root-amd64"
			input.Image.User = ""
			input.Workload.ManagedVolumes = nil
			input.Workload.OperatorMounts = nil
			input.ManagedVolumeSources = nil
			input.OperatorMountSources = nil
		}},
		{name: "numeric user unlimited Lima arm64", architecture: "arm64", golden: "wefty-v1-numeric-user-unlimited-lima-arm64.json", expectedUID: 1001, expectedGID: 1002, expectedGIDs: []uint32{1002, 3001}, unlimited: true, configure: func(input *RuntimeSpecInput) {
			input.ContainerID = "golden-numeric-unlimited-arm64"
			input.CgroupPath = "/wefty/golden-numeric-unlimited-arm64"
			input.Image.User = "1001:1002"
			input.Workload.Limits = WorkloadLimits{}
		}},
	}
}

func TestContainerdBaselineVersionPinned(t *testing.T) {
	if err := verifyContainerdBaselineVersion(); err != nil {
		t.Fatal(err)
	}
}

func TestCanonicalDocumentCrossesContainerdBoundaryWithoutReserialization(t *testing.T) {
	input := goldenRuntimeSpecInput(t, "amd64")
	input.Workload.Limits.MemoryBytes = math.MaxInt64
	spec, err := buildRuntimeSpec(context.Background(), input,
		goldenDependencies(t, filepath.Join("testdata", "containerd-v2.3.4", "seccomp-linux-amd64.json")))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := marshalRuntimeSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	document := &RuntimeSpecDocument{payload: payload, mounts: &retainedMountSources{}}
	t.Cleanup(func() { _ = document.Close() })
	containerdSpec, err := document.ContainerdSpec()
	if err != nil {
		t.Fatal(err)
	}
	container := containers.Container{Spec: containerdSpec}
	if container.Spec.GetTypeUrl() != containerdRuntimeSpecTypeURL {
		t.Fatalf("containerd spec type URL = %q, want %q", container.Spec.GetTypeUrl(), containerdRuntimeSpecTypeURL)
	}
	encoded := typeurl.MarshalProto(container.Spec)
	if !bytes.Equal(encoded.Value, document.JSON()) {
		t.Fatal("containerd Any changed the canonical runtime-spec bytes")
	}
	if !bytes.Contains(encoded.Value, []byte(`"ambient":[]`)) || !bytes.Contains(encoded.Value, []byte(`"inheritable":[]`)) ||
		!bytes.Contains(encoded.Value, []byte(`"readonly":false`)) ||
		!bytes.Contains(encoded.Value, []byte(`"limit":9223372036854775807`)) {
		t.Fatalf("canonical fields were dropped or rounded at the containerd boundary: %s", encoded.Value)
	}
	var decoded specs.Spec
	if err := json.Unmarshal(container.Spec.GetValue(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Linux.Resources.Memory == nil || decoded.Linux.Resources.Memory.Limit == nil || *decoded.Linux.Resources.Memory.Limit != math.MaxInt64 {
		t.Fatalf("containerd decoded memory limit = %#v", decoded.Linux.Resources.Memory)
	}
}

func TestNamedAndNumericImageUsersChooseDifferentSupplementalLookup(t *testing.T) {
	input := goldenRuntimeSpecInput(t, "amd64")
	dependencies := goldenDependencies(t, filepath.Join("testdata", "containerd-v2.3.4", "seccomp-linux-amd64.json"))
	named, err := buildRuntimeSpec(context.Background(), input, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(named.Process.User.AdditionalGids, []uint32{1002, 44, 2000}) || slices.Contains(named.Process.User.AdditionalGids, 3001) {
		t.Fatalf("named user selected duplicate-UID groups: %#v", named.Process.User)
	}
	input.Image.User = "1001:1002"
	numeric, err := buildRuntimeSpec(context.Background(), input, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(numeric.Process.User.AdditionalGids, []uint32{1002, 3001}) {
		t.Fatalf("numeric user did not use UID lookup: %#v", numeric.Process.User)
	}
}

func TestComputerDiskMakesRootReadOnlyAndBoundsWritableScratch(t *testing.T) {
	input := goldenRuntimeSpecInput(t, "amd64")
	input.Workload.ManagedVolumes = []ManagedVolumeDescriptor{{Kind: ManagedVolumeComputerDisk, ComputerStorage: &ComputerStorageReference{
		ComputerID: "computer-1", StorageID: "storage-1", StorageGeneration: 1, DiskBytes: 8 << 30,
	}}}
	input.Workload.OperatorMounts = nil
	input.OperatorMountSources = nil
	input.ManagedVolumeSources = map[ManagedVolumeKind]string{ManagedVolumeComputerDisk: "/run/wefty/fixtures/computer-disk"}
	input.ComputerControlSource = "/run/wefty/fixtures/control"
	spec, err := buildRuntimeSpec(context.Background(), input,
		goldenDependencies(t, filepath.Join("testdata", "containerd-v2.3.4", "seccomp-linux-amd64.json")))
	if err != nil {
		t.Fatal(err)
	}
	if spec.Root == nil || !spec.Root.Readonly {
		t.Fatalf("Computer root = %#v, want read-only", spec.Root)
	}
	writable := map[string]string{"/wefty/service": "", "/tmp": "size=524288k", "/var/tmp": "size=65536k"}
	bounded := map[string]bool{}
	controlReadOnly := false
	for _, mount := range spec.Mounts {
		if mount.Destination == contract.OCIContainerControlDirectory {
			controlReadOnly = mount.Source == input.ComputerControlSource && slices.Contains(mount.Options, "rro")
		}
		if size, expected := writable[mount.Destination]; expected {
			bounded[mount.Destination] = mount.Destination == "/wefty/service" ||
				(mount.Type == "tmpfs" && slices.Contains(mount.Options, size))
		}
	}
	if !controlReadOnly {
		t.Fatalf("Computer control mount is absent or writable: %+v", spec.Mounts)
	}
	for path := range writable {
		if !bounded[path] {
			t.Fatalf("Computer writable path %s is absent or unbounded: %+v", path, spec.Mounts)
		}
	}
}

func TestOperatorMountsSortParentsBeforeChildren(t *testing.T) {
	input := goldenRuntimeSpecInput(t, "amd64")
	input.Workload.OperatorMounts = []OperatorMount{
		{NodePath: "/host/child", ContainerPath: "/data/sub"},
		{NodePath: "/host/parent", ContainerPath: "/data"},
	}
	input.OperatorMountSources = []string{"/mnt/wefty/operator/child", "/mnt/wefty/operator/parent"}
	spec, err := buildRuntimeSpec(context.Background(), input,
		goldenDependencies(t, filepath.Join("testdata", "containerd-v2.3.4", "seccomp-linux-amd64.json")))
	if err != nil {
		t.Fatal(err)
	}
	parentIndex, childIndex := -1, -1
	for index, mount := range spec.Mounts {
		switch mount.Destination {
		case "/data":
			parentIndex = index
			if mount.Source != "/mnt/wefty/operator/parent" {
				t.Fatalf("parent mount source = %q", mount.Source)
			}
		case "/data/sub":
			childIndex = index
			if mount.Source != "/mnt/wefty/operator/child" {
				t.Fatalf("child mount source = %q", mount.Source)
			}
		}
	}
	if parentIndex < 0 || childIndex < 0 || parentIndex >= childIndex {
		t.Fatalf("nested mount order parent=%d child=%d", parentIndex, childIndex)
	}
}

func TestHelperTranslatesOperatorMountSourcesBeforeGuestValidation(t *testing.T) {
	workload := WorkloadInput{
		ImageDigest: "sha256:" + strings.Repeat("a", 64),
		OperatorMounts: []OperatorMount{
			{NodePath: "/Users/operator/project", ContainerPath: "/workspace"},
		},
	}
	identity, err := TranslateOperatorMountSources(workload, IdentityOperatorMountSource)
	if err != nil || !slices.Equal(identity, []string{"/Users/operator/project"}) {
		t.Fatalf("native translation = %v, err=%v", identity, err)
	}
	lima, err := TranslateOperatorMountSources(workload, func(string) (string, error) {
		return "/mnt/wefty-host/project", nil
	})
	if err != nil || !slices.Equal(lima, []string{"/mnt/wefty-host/project"}) {
		t.Fatalf("Lima translation = %v, err=%v", lima, err)
	}
	if _, err := TranslateOperatorMountSources(workload, func(string) (string, error) { return "relative", nil }); err == nil {
		t.Fatal("invalid guest translation was accepted")
	}
}

func TestRuntimeSpecHasNoRawDefaultsOrEscapeHatches(t *testing.T) {
	input := goldenRuntimeSpecInput(t, "amd64")
	spec, err := buildRuntimeSpec(context.Background(), input,
		goldenDependencies(t, filepath.Join("testdata", "containerd-v2.3.4", "seccomp-linux-amd64.json")))
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(spec.Linux.Namespaces, func(namespace specs.LinuxNamespace) bool {
		return namespace.Type == specs.NetworkNamespace || namespace.Type == specs.UserNamespace
	}) {
		t.Fatalf("profile contains a private network or unsupported user namespace: %#v", spec.Linux.Namespaces)
	}
	if spec.Process.Capabilities == nil || slices.Contains(spec.Process.Capabilities.Bounding, "CAP_NET_RAW") ||
		slices.Contains(spec.Process.Capabilities.Bounding, "CAP_NET_BIND_SERVICE") {
		t.Fatalf("profile contains a forbidden network capability: %#v", spec.Process.Capabilities)
	}
	if !slices.Equal(spec.Process.Capabilities.Bounding, isolationCapabilities) ||
		!slices.Equal(spec.Process.Capabilities.Permitted, isolationCapabilities) ||
		!slices.Equal(spec.Process.Capabilities.Effective, isolationCapabilities) {
		t.Fatalf("effective capability sets differ from wefty-v1: %#v", spec.Process.Capabilities)
	}
	if spec.Process.Capabilities.Inheritable == nil || len(spec.Process.Capabilities.Inheritable) != 0 ||
		spec.Process.Capabilities.Ambient == nil || len(spec.Process.Capabilities.Ambient) != 0 {
		t.Fatalf("inheritable and ambient capability sets are not explicit empty slices: %#v", spec.Process.Capabilities)
	}
	if spec.Linux.Seccomp == nil || len(spec.Linux.Seccomp.Syscalls) == 0 || !spec.Process.NoNewPrivileges {
		t.Fatal("profile did not explicitly enable seccomp and no-new-privileges")
	}
	if got := environmentValue(spec.Process.Env, "WEFTY_RUN_TOKEN"); got != "authoritative-token" {
		t.Fatalf("reserved environment override survived: %q", got)
	}
	if environmentCount(spec.Process.Env, "WEFTY_RUN_TOKEN") != 1 {
		t.Fatalf("reserved environment was not unique: %#v", spec.Process.Env)
	}
	if got := environmentValue(spec.Process.Env, "SECRET_TOKEN"); got != "fixture-sensitive-value" {
		t.Fatalf("sensitive environment did not override public environment: %q", got)
	}
	if environmentValue(spec.Process.Env, "HOME") != "" {
		t.Fatalf("host environment escaped into the profile: %#v", spec.Process.Env)
	}
	if len(spec.Linux.Devices) != 6 || len(spec.Linux.Resources.Devices) != 7 || spec.Linux.Resources.Devices[0].Allow {
		t.Fatalf("device policy is not deny-all plus six pseudo-devices: devices=%#v rules=%#v", spec.Linux.Devices, spec.Linux.Resources.Devices)
	}
	if slices.ContainsFunc(spec.Linux.Devices, func(device specs.LinuxDevice) bool {
		return device.Path == "/dev/console" || device.Path == "/dev/ptmx"
	}) {
		t.Fatalf("unsupported device escaped the fixed profile: %#v", spec.Linux.Devices)
	}
	if spec.Linux.Resources.Memory == nil || spec.Linux.Resources.Memory.Limit == nil || spec.Linux.Resources.Memory.Swap == nil ||
		*spec.Linux.Resources.Memory.Limit != 536870912 || *spec.Linux.Resources.Memory.Swap != 536870912 {
		t.Fatalf("memory limit was not serialized: %#v", spec.Linux.Resources.Memory)
	}
	if spec.Linux.Resources.CPU == nil || spec.Linux.Resources.CPU.Quota == nil || *spec.Linux.Resources.CPU.Quota != 75000 {
		t.Fatalf("CPU limit was not serialized: %#v", spec.Linux.Resources.CPU)
	}
	assertMount(t, spec.Mounts, "/etc/resolv.conf", "/run/wefty/fixtures/resolv.conf", true)
	assertMount(t, spec.Mounts, "/etc/hosts", "/run/wefty/fixtures/hosts", true)
	assertExactMount(t, spec.Mounts, specs.Mount{Destination: "/sys/fs/cgroup", Type: "cgroup", Source: "cgroup", Options: []string{"nosuid", "noexec", "nodev", "relatime", "ro"}})
	if spec.Process.ApparmorProfile != "" {
		t.Fatal("base fixture unexpectedly applied AppArmor before case configuration")
	}
	spec.Process.Capabilities.Ambient = nil
	if _, err := marshalRuntimeSpec(spec); err == nil {
		t.Fatal("serializer accepted an implicit capability set")
	}
}

func TestWorkloadValidationRejectsReservedEnvironmentAndMaliciousMounts(t *testing.T) {
	canonicalTemporaryDirectory := func() string {
		path, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return path
	}
	root := canonicalTemporaryDirectory()
	allowed := filepath.Join(root, "allowed")
	if err := os.Mkdir(allowed, 0o700); err != nil {
		t.Fatal(err)
	}
	regular := filepath.Join(allowed, "regular")
	if err := os.WriteFile(regular, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	insideLink := filepath.Join(allowed, "inside-link")
	if err := os.Symlink(regular, insideLink); err != nil {
		t.Fatal(err)
	}
	base := WorkloadInput{ImageDigest: "sha256:" + strings.Repeat("a", 64), Argv: []string{"/bin/true"}}
	for _, test := range []struct {
		name   string
		mutate func(*WorkloadInput)
	}{
		{name: "non-reserved helper environment", mutate: func(input *WorkloadInput) {
			input.ReservedEnvironment = []EnvironmentVariable{{Name: "WEFTY_CUSTOM", Value: "attacker"}}
		}},
		{name: "symlink source", mutate: func(input *WorkloadInput) {
			input.OperatorMounts = []OperatorMount{{NodePath: insideLink, ContainerPath: "/data"}}
		}},
		{name: "device source", mutate: func(input *WorkloadInput) {
			input.OperatorMounts = []OperatorMount{{NodePath: "/dev/null", ContainerPath: "/data"}}
		}},
		{name: "resolver shadow", mutate: func(input *WorkloadInput) {
			input.OperatorMounts = []OperatorMount{{NodePath: regular, ContainerPath: "/etc/resolv.conf"}}
		}},
		{name: "runtime mount shadow", mutate: func(input *WorkloadInput) {
			input.OperatorMounts = []OperatorMount{{NodePath: regular, ContainerPath: "/dev/custom"}}
		}},
		{name: "reserved subtree", mutate: func(input *WorkloadInput) {
			input.OperatorMounts = []OperatorMount{{NodePath: regular, ContainerPath: "/wefty"}}
		}},
		{name: "control signal shadow", mutate: func(input *WorkloadInput) {
			input.OperatorMounts = []OperatorMount{{NodePath: regular, ContainerPath: contract.OCIContainerControlDirectory}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := base
			test.mutate(&input)
			roots := []string{root}
			if strings.Contains(test.name, "device") {
				roots = []string{"/dev"}
			}
			if err := validateWorkload(input, roots); err == nil {
				t.Fatal("malicious workload was accepted")
			}
		})
	}
}

func TestWorkloadWireValidationRejectsEveryInvalidBranch(t *testing.T) {
	valid := func() WorkloadInput {
		return WorkloadInput{ImageDigest: "sha256:" + strings.Repeat("a", 64), Argv: []string{"/bin/true"}}
	}
	for _, test := range []struct {
		name   string
		mutate func(*WorkloadInput)
	}{
		{name: "digest", mutate: func(input *WorkloadInput) { input.ImageDigest = "latest" }},
		{name: "empty argv override", mutate: func(input *WorkloadInput) { input.Argv = []string{} }},
		{name: "argv NUL", mutate: func(input *WorkloadInput) { input.Argv = []string{"bad\x00arg"} }},
		{name: "argv no non-empty", mutate: func(input *WorkloadInput) { input.Argv = []string{"", ""} }},
		{name: "working directory", mutate: func(input *WorkloadInput) { input.WorkingDirectory = "relative" }},
		{name: "public env name", mutate: func(input *WorkloadInput) { input.Environment = []EnvironmentVariable{{Name: "BAD-NAME"}} }},
		{name: "public env duplicate", mutate: func(input *WorkloadInput) {
			input.Environment = []EnvironmentVariable{{Name: "A"}, {Name: "A"}}
		}},
		{name: "public env NUL", mutate: func(input *WorkloadInput) { input.Environment = []EnvironmentVariable{{Name: "A", Value: "x\x00"}} }},
		{name: "sensitive env name", mutate: func(input *WorkloadInput) { input.SensitiveEnvironment = []EnvironmentVariable{{Name: "BAD-NAME"}} }},
		{name: "sensitive env duplicate", mutate: func(input *WorkloadInput) {
			input.SensitiveEnvironment = []EnvironmentVariable{{Name: "A"}, {Name: "A"}}
		}},
		{name: "sensitive env NUL", mutate: func(input *WorkloadInput) {
			input.SensitiveEnvironment = []EnvironmentVariable{{Name: "A", Value: "x\x00"}}
		}},
		{name: "reserved env name", mutate: func(input *WorkloadInput) {
			input.ReservedEnvironment = []EnvironmentVariable{{Name: "WEFTY_NOT_RESERVED"}}
		}},
		{name: "reserved env duplicate", mutate: func(input *WorkloadInput) {
			input.ReservedEnvironment = []EnvironmentVariable{{Name: "WEFTY_RUN_TOKEN"}, {Name: "WEFTY_RUN_TOKEN"}}
		}},
		{name: "reserved env NUL", mutate: func(input *WorkloadInput) {
			input.ReservedEnvironment = []EnvironmentVariable{{Name: "WEFTY_RUN_TOKEN", Value: "x\x00"}}
		}},
		{name: "negative memory", mutate: func(input *WorkloadInput) { input.Limits.MemoryBytes = -1 }},
		{name: "negative CPU", mutate: func(input *WorkloadInput) { input.Limits.CPUMillicores = -1 }},
		{name: "managed kind", mutate: func(input *WorkloadInput) {
			input.ManagedVolumes = []ManagedVolumeDescriptor{{Kind: "host_device"}}
		}},
		{name: "managed duplicate", mutate: func(input *WorkloadInput) {
			input.ManagedVolumes = []ManagedVolumeDescriptor{{Kind: ManagedVolumeHandoff, OwnerKey: "run-1"}, {Kind: ManagedVolumeHandoff, OwnerKey: "run-1"}}
		}},
		{name: "Computer disk missing Storage", mutate: func(input *WorkloadInput) {
			input.ManagedVolumes = []ManagedVolumeDescriptor{{Kind: ManagedVolumeComputerDisk}}
		}},
		{name: "Computer disk invalid generation", mutate: func(input *WorkloadInput) {
			input.ManagedVolumes = []ManagedVolumeDescriptor{{Kind: ManagedVolumeComputerDisk, ComputerStorage: &ComputerStorageReference{
				ComputerID: "computer-1", StorageID: "storage-1", StorageGeneration: 0, DiskBytes: 1024,
			}}}
		}},
		{name: "ordinary volume carries Computer Storage", mutate: func(input *WorkloadInput) {
			input.ManagedVolumes = []ManagedVolumeDescriptor{{Kind: ManagedVolumeServiceData, ComputerStorage: &ComputerStorageReference{
				ComputerID: "computer-1", StorageID: "storage-1", StorageGeneration: 1, DiskBytes: 1024,
			}}}
		}},
		{name: "Computer disk writable operator mount", mutate: func(input *WorkloadInput) {
			input.ManagedVolumes = []ManagedVolumeDescriptor{{Kind: ManagedVolumeComputerDisk, ComputerStorage: &ComputerStorageReference{
				ComputerID: "computer-1", StorageID: "storage-1", StorageGeneration: 1, DiskBytes: 1024,
			}}}
			input.OperatorMounts = []OperatorMount{{NodePath: "/host/data", ContainerPath: "/data"}}
		}},
		{name: "mount source", mutate: func(input *WorkloadInput) {
			input.OperatorMounts = []OperatorMount{{NodePath: "/", ContainerPath: "/data"}}
		}},
		{name: "mount target", mutate: func(input *WorkloadInput) {
			input.OperatorMounts = []OperatorMount{{NodePath: "/host/data", ContainerPath: "relative"}}
		}},
		{name: "reserved mount target", mutate: func(input *WorkloadInput) {
			input.OperatorMounts = []OperatorMount{{NodePath: "/host/data", ContainerPath: "/sys/fs"}}
		}},
		{name: "duplicate mount target", mutate: func(input *WorkloadInput) {
			input.OperatorMounts = []OperatorMount{
				{NodePath: "/host/one", ContainerPath: "/data"},
				{NodePath: "/host/two", ContainerPath: "/data"},
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := valid()
			test.mutate(&input)
			if err := validateWorkloadWire(input); err == nil {
				t.Fatal("invalid wire workload was accepted")
			}
		})
	}
	computerDisk := valid()
	computerDisk.ManagedVolumes = []ManagedVolumeDescriptor{{Kind: ManagedVolumeComputerDisk, ComputerStorage: &ComputerStorageReference{
		ComputerID: "computer-1", StorageID: "storage-1", StorageGeneration: 1, DiskBytes: 8 << 30,
	}}}
	if err := validateWorkloadWire(computerDisk); err != nil {
		t.Fatalf("valid Computer disk was rejected: %v", err)
	}
	reservedInOperatorLayers := valid()
	reservedInOperatorLayers.Environment = []EnvironmentVariable{{Name: "WEFTY_RUN_TOKEN", Value: "public"}}
	reservedInOperatorLayers.SensitiveEnvironment = []EnvironmentVariable{{Name: "WEFTY_RUN_TOKEN", Value: "sensitive"}}
	if err := validateWorkloadWire(reservedInOperatorLayers); err != nil {
		t.Fatalf("defensively stripped reserved operator keys were rejected on the wire: %v", err)
	}
}

func TestRuntimeSpecConstructionRejectsEveryInvalidBranch(t *testing.T) {
	for _, test := range []struct {
		name               string
		mutateInput        func(*RuntimeSpecInput)
		mutateDependencies func(*runtimeSpecDependencies)
	}{
		{name: "container ID", mutateInput: func(input *RuntimeSpecInput) { input.ContainerID = "\x00" }},
		{name: "root cgroup path", mutateInput: func(input *RuntimeSpecInput) { input.CgroupPath = "/" }},
		{name: "relative rootfs", mutateInput: func(input *RuntimeSpecInput) { input.RootfsPath = "relative" }},
		{name: "missing rootfs", mutateInput: func(input *RuntimeSpecInput) { input.RootfsPath = filepath.Join(t.TempDir(), "missing") }},
		{name: "rootfs is file", mutateInput: func(input *RuntimeSpecInput) {
			file := filepath.Join(t.TempDir(), "rootfs")
			if err := os.WriteFile(file, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			input.RootfsPath = file
		}},
		{name: "unsupported arch", mutateInput: func(input *RuntimeSpecInput) { input.Guest.Architecture = "riscv64" }},
		{name: "missing kernel", mutateInput: func(input *RuntimeSpecInput) { input.Guest.KernelRelease = "" }},
		{name: "AppArmor name", mutateInput: func(input *RuntimeSpecInput) { input.Guest.AppArmorProfile = "../../escape" }},
		{name: "managed root", mutateInput: func(input *RuntimeSpecInput) { input.ManagedRoot = "/" }},
		{name: "translation count", mutateInput: func(input *RuntimeSpecInput) { input.OperatorMountSources = nil }},
		{name: "resolver source", mutateDependencies: func(dependencies *runtimeSpecDependencies) {
			dependencies.validateSource = func(_ string, _ []string, regularOnly bool) error {
				if regularOnly {
					return errors.New("resolver rejected")
				}
				return nil
			}
		}},
		{name: "managed source missing", mutateInput: func(input *RuntimeSpecInput) { delete(input.ManagedVolumeSources, ManagedVolumeHandoff) }},
		{name: "no effective argv", mutateInput: func(input *RuntimeSpecInput) {
			input.Image.Entrypoint, input.Image.Command, input.Workload.Argv = nil, nil, nil
		}},
		{name: "invalid image working directory", mutateInput: func(input *RuntimeSpecInput) { input.Image.WorkingDirectory = "relative" }},
		{name: "invalid image environment", mutateInput: func(input *RuntimeSpecInput) { input.Image.Environment = []string{"NO_EQUALS"} }},
		{name: "CPU quota overflow", mutateInput: func(input *RuntimeSpecInput) { input.Workload.Limits.CPUMillicores = math.MaxInt64 }},
		{name: "non-Linux baseline", mutateDependencies: func(dependencies *runtimeSpecDependencies) {
			dependencies.generateBase = func(context.Context, string, string) (*specs.Spec, error) { return &specs.Spec{}, nil }
		}},
		{name: "empty seccomp", mutateDependencies: func(dependencies *runtimeSpecDependencies) {
			dependencies.generateSeccomp = func(GuestKernelFacts, *specs.Spec) (*specs.LinuxSeccomp, error) {
				return &specs.LinuxSeccomp{}, nil
			}
		}},
		{name: "containerd version", mutateDependencies: func(dependencies *runtimeSpecDependencies) {
			dependencies.verifyVersion = func() error { return errors.New("wrong containerd patch") }
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := goldenRuntimeSpecInput(t, "amd64")
			dependencies := goldenDependencies(t, filepath.Join("testdata", "containerd-v2.3.4", "seccomp-linux-amd64.json"))
			if test.mutateInput != nil {
				test.mutateInput(&input)
			}
			if test.mutateDependencies != nil {
				test.mutateDependencies(&dependencies)
			}
			if _, err := buildRuntimeSpec(context.Background(), input, dependencies); err == nil {
				t.Fatal("invalid runtime-spec input was accepted")
			}
		})
	}
}

func TestRuntimeSpecValidationUsesGuestTranslatedAndManagedRoots(t *testing.T) {
	input := goldenRuntimeSpecInput(t, "amd64")
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	input.ManagedRoot = managedRoot
	input.ResolverPath = filepath.Join(managedRoot, "resolv.conf")
	input.HostsPath = filepath.Join(managedRoot, "hosts")
	for _, fixture := range []string{input.ResolverPath, input.HostsPath} {
		if err := os.WriteFile(fixture, []byte("fixture"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	handoff := filepath.Join(managedRoot, "handoff")
	if err := os.Mkdir(handoff, 0o700); err != nil {
		t.Fatal(err)
	}
	input.ManagedVolumeSources[ManagedVolumeHandoff] = handoff

	guestMountRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	translatedSource := filepath.Join(guestMountRoot, "operator-source")
	if err := os.Mkdir(translatedSource, 0o700); err != nil {
		t.Fatal(err)
	}
	input.AllowedMountRoots = []string{guestMountRoot}
	input.OperatorMountSources = []string{translatedSource}
	input.Workload.OperatorMounts[0].NodePath = "/host/path-that-is-not-visible-in-the-guest"
	if err := validateRuntimeSpecInput(input, validateMountSource); err != nil {
		t.Fatalf("valid guest-side translations were rejected: %v", err)
	}

	input.OperatorMountSources[0] = handoff
	if err := validateRuntimeSpecInput(input, validateMountSource); err == nil {
		t.Fatal("operator mount translation outside the guest allowed root was accepted")
	}
	input.OperatorMountSources[0] = translatedSource
	input.ManagedVolumeSources[ManagedVolumeHandoff] = translatedSource
	if err := validateRuntimeSpecInput(input, validateMountSource); err == nil {
		t.Fatal("managed volume source outside the helper-managed root was accepted")
	}
}

func TestRuntimeSpecDocumentRejectsMountSwapAfterBuild(t *testing.T) {
	input := goldenRuntimeSpecInput(t, "amd64")
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	input.ManagedRoot = managedRoot
	input.ResolverPath = filepath.Join(managedRoot, "resolv.conf")
	input.HostsPath = filepath.Join(managedRoot, "hosts")
	for _, fixture := range []string{input.ResolverPath, input.HostsPath} {
		if err := os.WriteFile(fixture, []byte("fixture"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	handoff := filepath.Join(managedRoot, "handoff")
	if err := os.Mkdir(handoff, 0o700); err != nil {
		t.Fatal(err)
	}
	input.ManagedVolumeSources[ManagedVolumeHandoff] = handoff
	guestMountRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	translatedSource := filepath.Join(guestMountRoot, "operator-source")
	if err := os.Mkdir(translatedSource, 0o700); err != nil {
		t.Fatal(err)
	}
	input.AllowedMountRoots = []string{guestMountRoot}
	input.OperatorMountSources = []string{translatedSource}

	retained := &retainedMountSources{}
	dependencies := goldenDependencies(t, filepath.Join("testdata", "containerd-v2.3.4", "seccomp-linux-amd64.json"))
	dependencies.validateSource = retained.validate
	spec, err := buildRuntimeSpec(context.Background(), input, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := marshalRuntimeSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	document := &RuntimeSpecDocument{payload: payload, mounts: retained}
	t.Cleanup(func() { _ = document.Close() })
	if err := document.RevalidateMounts(); err != nil {
		t.Fatalf("unchanged mount source failed revalidation: %v", err)
	}
	if err := os.Rename(translatedSource, translatedSource+"-old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(translatedSource, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := document.RevalidateMounts(); err == nil {
		t.Fatal("swapped mount source retained pre-mount authority")
	}
	if _, err := document.ContainerdSpec(); err == nil {
		t.Fatal("containerd handoff accepted a swapped mount source")
	}
}

func goldenRuntimeSpecInput(t *testing.T, architecture string) RuntimeSpecInput {
	t.Helper()
	rootfs, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(rootfs, "etc"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootfs, "etc", "passwd"), []byte(
		"root:x:0:0:root:/root:/bin/sh\nalias:x:1001:3000:alias:/tmp:/bin/false\napp:x:1001:1002:app:/workspace:/bin/sh\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootfs, "etc", "group"), []byte(
		"root:x:0:\nvideo:x:44:app\napp:x:1002:\nbuild:x:2000:app\nrogue:x:3001:alias\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	operatorRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	operatorSource := filepath.Join(operatorRoot, "source")
	if err := os.Mkdir(operatorSource, 0o700); err != nil {
		t.Fatal(err)
	}
	readOnly := architecture == "arm64"
	return RuntimeSpecInput{
		ContainerID:  "golden-" + architecture,
		CgroupPath:   "/wefty/golden-" + architecture,
		RootfsPath:   rootfs,
		ManagedRoot:  "/run/wefty/fixtures",
		ResolverPath: "/run/wefty/fixtures/resolv.conf",
		HostsPath:    "/run/wefty/fixtures/hosts",
		Image: ImageRuntimeConfig{
			User:             "app:app",
			Environment:      []string{"PATH=/usr/local/bin:/usr/bin:/bin", "MODE=image", "WEFTY_RUN_TOKEN=stale-image-token", "WEFTY_CUSTOM=preserved"},
			Entrypoint:       []string{"/usr/local/bin/echo-service"},
			Command:          []string{"--once"},
			WorkingDirectory: "/workspace",
		},
		Workload: WorkloadInput{
			ImageDigest: "sha256:" + strings.Repeat("a", 64),
			Environment: []EnvironmentVariable{
				{Name: "MODE", Value: "operator"},
				{Name: "SECRET_TOKEN", Value: "public-value"},
				{Name: "WEFTY_RUN_TOKEN", Value: "untrusted-public-token"},
			},
			SensitiveEnvironment: []EnvironmentVariable{
				{Name: "SECRET_TOKEN", Value: "fixture-sensitive-value"},
				{Name: "WEFTY_RUN_TOKEN", Value: "untrusted-sensitive-token"},
			},
			ReservedEnvironment: []EnvironmentVariable{
				{Name: "WEFTY_HANDOFF_DIR", Value: "/wefty/handoff"},
				{Name: "WEFTY_L3_ENDPOINT", Value: "http://127.0.0.1:41000"},
				{Name: "WEFTY_RUN_TOKEN", Value: "authoritative-token"},
			},
			ManagedVolumes: []ManagedVolumeDescriptor{{Kind: ManagedVolumeHandoff, OwnerKey: "run-fixture"}},
			OperatorMounts: []OperatorMount{{NodePath: operatorSource, ContainerPath: "/workspace/input", ReadOnly: readOnly}},
			Limits:         WorkloadLimits{MemoryBytes: 536870912, CPUMillicores: 750},
		},
		Guest: GuestKernelFacts{
			Architecture:  architecture,
			KernelRelease: "6.12.0-fixture",
		},
		AllowedMountRoots:    []string{"/mnt/wefty/operator"},
		ManagedVolumeSources: map[ManagedVolumeKind]string{ManagedVolumeHandoff: "/run/wefty/fixtures/handoff"},
		OperatorMountSources: []string{"/mnt/wefty/operator/input"},
	}
}

func goldenDependencies(t *testing.T, seccompPath string) runtimeSpecDependencies {
	t.Helper()
	return runtimeSpecDependencies{
		generateBase: generateContainerdBaseline,
		generateSeccomp: func(_ GuestKernelFacts, _ *specs.Spec) (*specs.LinuxSeccomp, error) {
			payload, err := os.ReadFile(seccompPath)
			if err != nil {
				return nil, err
			}
			var profile specs.LinuxSeccomp
			if err := json.Unmarshal(payload, &profile); err != nil {
				return nil, err
			}
			return &profile, nil
		},
		validateSource: func(string, []string, bool) error { return nil },
		verifyVersion:  func() error { return nil },
	}
}

func materializeGoldenMountSources(t *testing.T, input *RuntimeSpecInput) {
	t.Helper()
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	input.ManagedRoot = managedRoot
	input.ResolverPath = filepath.Join(managedRoot, "resolv.conf")
	input.HostsPath = filepath.Join(managedRoot, "hosts")
	for _, fixture := range []string{input.ResolverPath, input.HostsPath} {
		if err := os.WriteFile(fixture, []byte("fixture"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	handoff := filepath.Join(managedRoot, "handoff")
	if err := os.Mkdir(handoff, 0o700); err != nil {
		t.Fatal(err)
	}
	input.ManagedVolumeSources[ManagedVolumeHandoff] = handoff
	guestMountRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	translatedSource := filepath.Join(guestMountRoot, "operator-source")
	if err := os.Mkdir(translatedSource, 0o700); err != nil {
		t.Fatal(err)
	}
	input.AllowedMountRoots = []string{guestMountRoot}
	input.OperatorMountSources = []string{translatedSource}
}

func writeJSONFixture(t *testing.T, path string, value any) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, marshalIndentedJSON(t, value), 0o644); err != nil {
		t.Fatal(err)
	}
}

func marshalIndentedJSON(t *testing.T, value any) []byte {
	t.Helper()
	payload, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return append(payload, '\n')
}

func marshalRuntimeSpecIndented(t *testing.T, spec *specs.Spec) []byte {
	t.Helper()
	payload, err := marshalRuntimeSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	var indented bytes.Buffer
	if err := json.Indent(&indented, payload, "", "  "); err != nil {
		t.Fatal(err)
	}
	indented.WriteByte('\n')
	return []byte(indented.String())
}

func redactSensitiveEnvironment(t *testing.T, payload []byte, sensitive []EnvironmentVariable) []byte {
	t.Helper()
	sensitiveNames := make(map[string]struct{}, len(sensitive))
	for _, variable := range sensitive {
		if !contract.IsOCIReservedEnvironmentName(variable.Name) {
			sensitiveNames[variable.Name] = struct{}{}
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var document map[string]any
	if err := decoder.Decode(&document); err != nil {
		t.Fatal(err)
	}
	process, ok := document["process"].(map[string]any)
	if !ok {
		t.Fatal("serialized profile process is absent")
	}
	environment, ok := process["env"].([]any)
	if !ok {
		t.Fatal("serialized profile environment is absent")
	}
	for index, encoded := range environment {
		value, ok := encoded.(string)
		if !ok {
			t.Fatalf("serialized environment entry = %#v", encoded)
		}
		name, _, _ := strings.Cut(value, "=")
		if _, redact := sensitiveNames[name]; redact {
			environment[index] = name + "=<redacted>"
		}
	}
	return marshalIndentedJSON(t, document)
}

func environmentValue(environment []string, name string) string {
	prefix := name + "="
	for _, value := range environment {
		if strings.HasPrefix(value, prefix) {
			return strings.TrimPrefix(value, prefix)
		}
	}
	return ""
}

func environmentCount(environment []string, name string) int {
	prefix := name + "="
	count := 0
	for _, value := range environment {
		if strings.HasPrefix(value, prefix) {
			count++
		}
	}
	return count
}

func assertMount(t *testing.T, mounts []specs.Mount, destination, source string, readOnly bool) {
	t.Helper()
	for _, mount := range mounts {
		if mount.Destination != destination {
			continue
		}
		if mount.Source != source || slices.Contains(mount.Options, "rro") != readOnly || !slices.Contains(mount.Options, "rprivate") {
			t.Fatalf("mount %q = %#v", destination, mount)
		}
		return
	}
	t.Fatalf("mount %q is absent", destination)
}

func assertExactMount(t *testing.T, mounts []specs.Mount, expected specs.Mount) {
	t.Helper()
	for _, mount := range mounts {
		if mount.Destination == expected.Destination {
			if mount.Type != expected.Type || mount.Source != expected.Source || !slices.Equal(mount.Options, expected.Options) {
				t.Fatalf("mount %q = %#v, want %#v", expected.Destination, mount, expected)
			}
			return
		}
	}
	t.Fatalf("mount %q is absent", expected.Destination)
}
