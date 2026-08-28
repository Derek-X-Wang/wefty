package ocihelper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	containerdseccomp "github.com/containerd/containerd/v2/contrib/seccomp"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	containerdoci "github.com/containerd/containerd/v2/pkg/oci"
	containerdversion "github.com/containerd/containerd/v2/version"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	IsolationProfileVersion      = "wefty-v1"
	ContainerdBaselineVersion    = "v2.3.4"
	ContainerdNamespace          = "wefty"
	DefaultRuntimeHandler        = "io.containerd.runc.v2"
	DefaultSnapshotter           = "overlayfs"
	containerRootfsPath          = "rootfs"
	containerdRuntimeSpecTypeURL = "types.containerd.io/opencontainers/runtime-spec/1/Spec"
	defaultCPUPeriodMicroseconds = uint64(100_000)
	// OWNER-CALL: Computer desktops need a browser-sized /tmp; the combined
	// cgroup-charged tmpfs ceilings must be reviewed against the 1 GiB default.
	computerTmpKilobytes    = 512 * 1024
	computerVarTmpKilobytes = 64 * 1024
)

var appArmorProfilePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

var isolationCapabilities = []string{
	"CAP_CHOWN",
	"CAP_DAC_OVERRIDE",
	"CAP_FSETID",
	"CAP_FOWNER",
	"CAP_MKNOD",
	"CAP_SETGID",
	"CAP_SETUID",
	"CAP_SETFCAP",
	"CAP_SETPCAP",
	"CAP_SYS_CHROOT",
	"CAP_KILL",
	"CAP_AUDIT_WRITE",
}

// ImageRuntimeConfig is the image metadata consumed inside the privileged
// helper after the immutable image has been selected and unpacked.
type ImageRuntimeConfig struct {
	User             string
	Environment      []string
	Entrypoint       []string
	Command          []string
	WorkingDirectory string
}

// GuestKernelFacts are collected by the helper in the same Linux guest that
// will execute runc. They are never accepted over the helper wire protocol.
type GuestKernelFacts struct {
	Architecture    string
	KernelRelease   string
	AppArmorProfile string
}

// RuntimeSpecInput combines closed agent inputs with image and guest facts
// that only the privileged helper can obtain. OperatorMountSources contains
// guest-visible translations in the same order as Workload.OperatorMounts.
type RuntimeSpecInput struct {
	ContainerID          string
	CgroupPath           string
	RootfsPath           string
	Image                ImageRuntimeConfig
	Workload             WorkloadInput
	Guest                GuestKernelFacts
	ResolverPath         string
	HostsPath            string
	ManagedRoot          string
	AllowedMountRoots    []string
	ManagedVolumeSources map[ManagedVolumeKind]string
	OperatorMountSources []string
}

// TranslateOperatorMountSources is the helper-side translation seam used by
// the engine before profile construction. Native Linux passes
// IdentityOperatorMountSource; Lima supplies its preconfigured host-to-guest
// shared-root mapping. Filesystem authority is validated only after this step.
func TranslateOperatorMountSources(workload WorkloadInput, translate func(string) (string, error)) ([]string, error) {
	if err := validateWorkloadWire(workload); err != nil {
		return nil, err
	}
	if len(workload.OperatorMounts) == 0 {
		return nil, nil
	}
	if translate == nil {
		return nil, errors.New("operator mount source translator is required")
	}
	translated := make([]string, len(workload.OperatorMounts))
	for index, mount := range workload.OperatorMounts {
		source, err := translate(mount.NodePath)
		if err != nil {
			return nil, fmt.Errorf("translate operator mount source %q: %w", mount.NodePath, err)
		}
		if !validMountSourcePath(source) {
			return nil, fmt.Errorf("translated operator mount source %q is not a clean absolute non-root path", source)
		}
		translated[index] = source
	}
	return translated, nil
}

func IdentityOperatorMountSource(source string) (string, error) { return source, nil }

type runtimeSpecDependencies struct {
	generateBase    func(context.Context, string, string) (*specs.Spec, error)
	generateSeccomp func(GuestKernelFacts, *specs.Spec) (*specs.LinuxSeccomp, error)
	validateSource  func(string, []string, bool) error
	verifyVersion   func() error
}

// RuntimeSpecDocument is the only public profile handoff. Its containerd Any
// retains the canonical JSON byte-for-byte, including explicit empty arrays
// and false values that runtime-spec's ordinary json tags omit. It also owns
// retained mount descriptors. ContainerdSpec performs the mandatory
// revalidation immediately before handoff; callers close the document after
// container creation consumes it.
type RuntimeSpecDocument struct {
	payload  []byte
	mounts   *retainedMountSources
	ownerUID uint32
	ownerGID uint32
}

func (document *RuntimeSpecDocument) JSON() []byte {
	if document == nil {
		return nil
	}
	return slices.Clone(document.payload)
}

// ProcessOwner returns the primary owner resolved while constructing the
// runtime spec. Service data initialization consumes this exact result so
// passwd/group resolution cannot drift between profile and volume setup.
func (document *RuntimeSpecDocument) ProcessOwner() (uint32, uint32, error) {
	if document == nil {
		return 0, 0, errors.New("OCI runtime spec document is absent")
	}
	return document.ownerUID, document.ownerGID, nil
}

// ContainerdSpec first revalidates every retained mount identity, then returns
// the exact Any stored by containerd's container service. Using
// containerd/client.WithSpec here would marshal the Go struct again and drop
// wefty-v1's explicit empty capability arrays.
func (document *RuntimeSpecDocument) ContainerdSpec() (*anypb.Any, error) {
	if document == nil {
		return nil, errors.New("OCI runtime spec document is absent")
	}
	if err := document.RevalidateMounts(); err != nil {
		return nil, err
	}
	return &anypb.Any{TypeUrl: containerdRuntimeSpecTypeURL, Value: slices.Clone(document.payload)}, nil
}

// RevalidateMounts is the engine-side pre-mount hook reserved for ticket #141.
func (document *RuntimeSpecDocument) RevalidateMounts() error {
	if document == nil || document.mounts == nil {
		return errors.New("OCI runtime spec document is absent")
	}
	return document.mounts.revalidate()
}

func (document *RuntimeSpecDocument) Close() error {
	if document == nil || document.mounts == nil {
		return nil
	}
	return document.mounts.close()
}

// RuntimeSpecRejectionError distinguishes a closed profile-construction
// rejection from an ambiguous engine failure at the helper RPC boundary.
type RuntimeSpecRejectionError struct{ err error }

func (rejection *RuntimeSpecRejectionError) Error() string {
	return fmt.Sprintf("OCI runtime spec rejected: %v", rejection.err)
}

func (rejection *RuntimeSpecRejectionError) Unwrap() error { return rejection.err }

// ServiceDataRejectionError is a terminal helper rejection: retrying the same
// pinned service cannot safely reconcile a directory/record owner conflict.
type ServiceDataRejectionError struct {
	Reason               string
	ActualUID, ActualGID uint32
	WantedUID, WantedGID uint32
	err                  error
}

func (rejection *ServiceDataRejectionError) Error() string {
	return fmt.Sprintf("service data rejected: %s (actual owner %d:%d, wanted %d:%d)", rejection.Reason, rejection.ActualUID, rejection.ActualGID, rejection.WantedUID, rejection.WantedGID)
}

func (rejection *ServiceDataRejectionError) Unwrap() error { return rejection.err }

// SpawnFailureForRunError preserves a helper-side spec rejection at the
// agent/runtime seam without teaching the agent about containerd errors.
func SpawnFailureForRunError(err error) *contract.SpawnFailure {
	var rpcErr *RPCError
	if !errors.As(err, &rpcErr) {
		return nil
	}
	switch rpcErr.Code {
	case CodeOCISpecRejected:
		return &contract.SpawnFailure{Code: contract.SpawnFailureOCISpecRejected, Message: rpcErr.Message}
	case CodeImageUnavailable:
		return &contract.SpawnFailure{Code: contract.SpawnFailureImageUnavailable, Message: rpcErr.Message}
	case CodeInsufficientDisk:
		failure := &contract.SpawnFailure{Code: contract.SpawnFailureInsufficientDisk, Message: rpcErr.Message}
		if rpcErr.DiskFailure != nil {
			failure.RequestedBytes = rpcErr.DiskFailure.RequestedBytes
			failure.ObservedAvailableBytes = rpcErr.DiskFailure.ObservedAvailableBytes
		}
		return failure
	default:
		return nil
	}
}

// BuildRuntimeSpec constructs and canonically serializes the complete wefty
// v1 profile. No caller-provided OCI spec, namespace, device, capability, or
// mount option is accepted.
func BuildRuntimeSpec(ctx context.Context, input RuntimeSpecInput) (*RuntimeSpecDocument, error) {
	retained := &retainedMountSources{}
	spec, err := buildRuntimeSpec(ctx, input, runtimeSpecDependencies{
		generateBase:    generateContainerdBaseline,
		generateSeccomp: generateGuestSeccomp,
		validateSource:  retained.validate,
		verifyVersion:   verifyContainerdBaselineVersion,
	})
	if err != nil {
		_ = retained.close()
		return nil, &RuntimeSpecRejectionError{err: err}
	}
	payload, err := marshalRuntimeSpec(spec)
	if err != nil {
		_ = retained.close()
		return nil, &RuntimeSpecRejectionError{err: err}
	}
	return &RuntimeSpecDocument{payload: payload, mounts: retained, ownerUID: spec.Process.User.UID, ownerGID: spec.Process.User.GID}, nil
}

// marshalRuntimeSpec serializes the profile while retaining explicit empty
// inheritable and ambient capability sets. The runtime-spec Go type marks
// those slices omitempty even though their deliberate emptiness is part of the
// wefty-v1 review surface.
func marshalRuntimeSpec(spec *specs.Spec) ([]byte, error) {
	if spec == nil {
		return nil, errors.New("OCI runtime spec is required")
	}
	payload, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var document map[string]any
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("OCI runtime spec serialization produced trailing JSON")
	}
	process, ok := document["process"].(map[string]any)
	if !ok {
		return nil, errors.New("OCI runtime spec process is absent")
	}
	capabilities, ok := process["capabilities"].(map[string]any)
	if !ok {
		return nil, errors.New("OCI runtime spec capabilities are absent")
	}
	if spec.Process == nil || spec.Process.Capabilities == nil {
		return nil, errors.New("OCI runtime spec capabilities are absent")
	}
	if spec.Process.Capabilities.Inheritable == nil || spec.Process.Capabilities.Ambient == nil {
		return nil, errors.New("OCI runtime spec inheritable and ambient capabilities must be explicit")
	}
	capabilities["inheritable"] = slices.Clone(spec.Process.Capabilities.Inheritable)
	capabilities["ambient"] = slices.Clone(spec.Process.Capabilities.Ambient)
	root, ok := document["root"].(map[string]any)
	if !ok {
		return nil, errors.New("OCI runtime spec root is absent")
	}
	root["readonly"] = spec.Root.Readonly
	return json.Marshal(document)
}

func buildRuntimeSpec(ctx context.Context, input RuntimeSpecInput, dependencies runtimeSpecDependencies) (*specs.Spec, error) {
	if dependencies.generateBase == nil || dependencies.generateSeccomp == nil || dependencies.validateSource == nil || dependencies.verifyVersion == nil {
		return nil, errors.New("OCI profile dependencies are incomplete")
	}
	if err := dependencies.verifyVersion(); err != nil {
		return nil, err
	}
	if err := validateRuntimeSpecInput(input, dependencies.validateSource); err != nil {
		return nil, err
	}

	spec, err := dependencies.generateBase(ctx, input.Guest.Architecture, input.ContainerID)
	if err != nil {
		return nil, fmt.Errorf("generate containerd %s baseline: %w", ContainerdBaselineVersion, err)
	}
	if spec.Process == nil || spec.Root == nil || spec.Linux == nil {
		return nil, errors.New("containerd baseline is not a Linux OCI spec")
	}

	if err := applyImageUser(ctx, spec, input.RootfsPath, input.Image.User); err != nil {
		return nil, fmt.Errorf("resolve image user %q: %w", input.Image.User, err)
	}
	argv := append(slices.Clone(input.Image.Entrypoint), input.Image.Command...)
	if input.Workload.Argv != nil {
		argv = slices.Clone(input.Workload.Argv)
	}
	if err := validateEffectiveArgv(argv); err != nil {
		return nil, fmt.Errorf("image and workload do not provide a runnable argv: %w", err)
	}
	cwd := input.Image.WorkingDirectory
	if input.Workload.WorkingDirectory != "" {
		cwd = input.Workload.WorkingDirectory
	}
	if cwd == "" {
		cwd = "/"
	}
	if !validContainerPathAllowRoot(cwd) {
		return nil, errors.New("effective working directory must be a clean absolute container path")
	}
	environment, err := mergeRuntimeEnvironment(input.Image.Environment, input.Workload.Environment,
		input.Workload.SensitiveEnvironment, input.Workload.ReservedEnvironment)
	if err != nil {
		return nil, err
	}

	// Ticket #141 targets containerd v2.3.4's runc v2 shim. That containerd
	// release pins runtime-spec v1.3.0, so the bundle version must remain the
	// linked specs.Version rather than being independently downgraded.
	spec.Version = specs.Version
	computerDisk := false
	for _, volume := range input.Workload.ManagedVolumes {
		computerDisk = computerDisk || volume.Kind == ManagedVolumeComputerDisk
	}
	spec.Root = &specs.Root{Path: containerRootfsPath, Readonly: computerDisk}
	spec.Hostname = ""
	spec.Domainname = ""
	spec.Hooks = nil
	spec.Annotations = nil
	spec.Process.Terminal = false
	spec.Process.ConsoleSize = nil
	spec.Process.Args = argv
	spec.Process.CommandLine = ""
	spec.Process.Env = environment
	spec.Process.Cwd = cwd
	spec.Process.NoNewPrivileges = true
	spec.Process.Capabilities = explicitIsolationCapabilities()
	// Keep containerd v2.3.4's baseline RLIMIT_NOFILE=1024 as an explicit M3
	// decision. TODO: raise it only with workload evidence and a spec amendment.
	spec.Process.ApparmorProfile = input.Guest.AppArmorProfile
	spec.Process.SelinuxLabel = ""
	spec.Process.OOMScoreAdj = nil
	spec.Process.Scheduler = nil
	spec.Process.IOPriority = nil
	spec.Process.ExecCPUAffinity = nil

	resources, err := isolationResources(input.Workload.Limits)
	if err != nil {
		return nil, err
	}
	spec.Linux = &specs.Linux{
		Devices:       isolationDevices(),
		CgroupsPath:   input.CgroupPath,
		Namespaces:    isolationNamespaces(),
		Resources:     resources,
		MaskedPaths:   isolationMaskedPaths(),
		ReadonlyPaths: isolationReadonlyPaths(),
	}
	seccompProfile, err := dependencies.generateSeccomp(input.Guest, spec)
	if err != nil {
		return nil, fmt.Errorf("generate guest seccomp profile: %w", err)
	}
	if seccompProfile == nil || len(seccompProfile.Syscalls) == 0 {
		return nil, errors.New("guest seccomp generator returned an empty profile")
	}
	spec.Linux.Seccomp = seccompProfile

	mounts, err := isolationMounts(input)
	if err != nil {
		return nil, err
	}
	spec.Mounts = mounts
	return spec, nil
}

func validateRuntimeSpecInput(input RuntimeSpecInput, validateSource func(string, []string, bool) error) error {
	if strings.TrimSpace(input.ContainerID) == "" || strings.IndexByte(input.ContainerID, 0) >= 0 {
		return errors.New("container ID is required")
	}
	if !filepath.IsAbs(input.CgroupPath) || filepath.Clean(input.CgroupPath) != input.CgroupPath || input.CgroupPath == string(filepath.Separator) {
		return errors.New("cgroup path must be clean, absolute, and not root")
	}
	if !filepath.IsAbs(input.RootfsPath) || filepath.Clean(input.RootfsPath) != input.RootfsPath {
		return errors.New("rootfs path must be clean and absolute")
	}
	rootfs, err := os.Stat(input.RootfsPath)
	if err != nil || !rootfs.IsDir() {
		return errors.New("rootfs path must be an existing directory")
	}
	if input.Guest.Architecture != "amd64" && input.Guest.Architecture != "arm64" {
		return fmt.Errorf("guest architecture %q is unsupported by the M3 profile", input.Guest.Architecture)
	}
	if strings.TrimSpace(input.Guest.KernelRelease) == "" {
		return errors.New("guest kernel release is required")
	}
	if input.Guest.AppArmorProfile != "" && !appArmorProfilePattern.MatchString(input.Guest.AppArmorProfile) {
		return errors.New("AppArmor profile name is invalid")
	}
	if !filepath.IsAbs(input.ManagedRoot) || filepath.Clean(input.ManagedRoot) != input.ManagedRoot || input.ManagedRoot == string(filepath.Separator) {
		return errors.New("managed root must be clean, absolute, and not root")
	}
	if len(input.OperatorMountSources) != len(input.Workload.OperatorMounts) {
		return errors.New("operator mount translations do not match workload mounts")
	}
	guestWorkload := input.Workload
	guestWorkload.OperatorMounts = slices.Clone(input.Workload.OperatorMounts)
	for index := range guestWorkload.OperatorMounts {
		guestWorkload.OperatorMounts[index].NodePath = input.OperatorMountSources[index]
	}
	if err := validateWorkloadWithSource(guestWorkload, input.AllowedMountRoots, validateSource); err != nil {
		return err
	}
	for _, source := range []struct {
		name string
		path string
	}{
		{name: "resolver", path: input.ResolverPath},
		{name: "hosts", path: input.HostsPath},
	} {
		if err := validateSource(source.path, []string{input.ManagedRoot}, true); err != nil {
			return fmt.Errorf("%s source is not permitted: %w", source.name, err)
		}
	}
	for _, volume := range input.Workload.ManagedVolumes {
		if volume.Kind == ManagedVolumeLogSegments {
			continue
		}
		source := input.ManagedVolumeSources[volume.Kind]
		if source == "" {
			return fmt.Errorf("managed volume %q has no guest source", volume.Kind)
		}
		if err := validateSource(source, []string{input.ManagedRoot}, false); err != nil {
			return fmt.Errorf("managed volume %q source is not permitted: %w", volume.Kind, err)
		}
	}
	return nil
}

func generateContainerdBaseline(ctx context.Context, architecture, containerID string) (*specs.Spec, error) {
	ctx = namespaces.WithNamespace(ctx, ContainerdNamespace)
	return containerdoci.GenerateSpecWithPlatform(ctx, nil, "linux/"+architecture, &containers.Container{ID: containerID})
}

func generateGuestSeccomp(facts GuestKernelFacts, spec *specs.Spec) (*specs.LinuxSeccomp, error) {
	if runtime.GOOS != "linux" {
		return nil, errors.New("seccomp profile construction must run inside the Linux guest")
	}
	if facts.Architecture != runtime.GOARCH {
		return nil, fmt.Errorf("reported guest architecture %q does not match runtime %q", facts.Architecture, runtime.GOARCH)
	}
	return containerdseccomp.DefaultProfile(spec), nil
}

func verifyContainerdBaselineVersion() error {
	want := strings.TrimPrefix(ContainerdBaselineVersion, "v")
	if containerdversion.Version != want && !strings.HasPrefix(containerdversion.Version, want+"+") {
		return fmt.Errorf("containerd baseline is %s, profile fixtures require %s", containerdversion.Version, ContainerdBaselineVersion)
	}
	return nil
}

func applyImageUser(ctx context.Context, spec *specs.Spec, rootfsPath, configuredUser string) error {
	spec.Root.Path = rootfsPath
	user := configuredUser
	if user == "" {
		user = "0:0"
	}
	container := &containers.Container{}
	if err := containerdoci.ApplyOpts(ctx, nil, container, spec, containerdoci.WithUser(user)); err != nil {
		return err
	}
	lookupUser, _, _ := strings.Cut(user, ":")
	if _, err := strconv.ParseUint(lookupUser, 10, 32); err == nil {
		lookupUser = strconv.FormatUint(uint64(spec.Process.User.UID), 10)
	}
	return containerdoci.ApplyOpts(ctx, nil, container, spec, containerdoci.WithAdditionalGIDs(lookupUser))
}

func explicitIsolationCapabilities() *specs.LinuxCapabilities {
	allowed := slices.Clone(isolationCapabilities)
	return &specs.LinuxCapabilities{
		Bounding:    slices.Clone(allowed),
		Permitted:   slices.Clone(allowed),
		Effective:   slices.Clone(allowed),
		Inheritable: []string{},
		Ambient:     []string{},
	}
}

func isolationNamespaces() []specs.LinuxNamespace {
	return []specs.LinuxNamespace{
		{Type: specs.PIDNamespace},
		{Type: specs.IPCNamespace},
		{Type: specs.UTSNamespace},
		{Type: specs.MountNamespace},
		{Type: specs.CgroupNamespace},
	}
}

func isolationDevices() []specs.LinuxDevice {
	mode := os.FileMode(0o666)
	uid, gid := uint32(0), uint32(0)
	return []specs.LinuxDevice{
		{Path: "/dev/null", Type: "c", Major: 1, Minor: 3, FileMode: &mode, UID: &uid, GID: &gid},
		{Path: "/dev/zero", Type: "c", Major: 1, Minor: 5, FileMode: &mode, UID: &uid, GID: &gid},
		{Path: "/dev/full", Type: "c", Major: 1, Minor: 7, FileMode: &mode, UID: &uid, GID: &gid},
		{Path: "/dev/random", Type: "c", Major: 1, Minor: 8, FileMode: &mode, UID: &uid, GID: &gid},
		{Path: "/dev/urandom", Type: "c", Major: 1, Minor: 9, FileMode: &mode, UID: &uid, GID: &gid},
		{Path: "/dev/tty", Type: "c", Major: 5, Minor: 0, FileMode: &mode, UID: &uid, GID: &gid},
	}
}

func isolationResources(limits WorkloadLimits) (*specs.LinuxResources, error) {
	// M3 deliberately leaves Resources.Pids absent; a PID limit remains a
	// known profile gap rather than an invented default.
	resources := &specs.LinuxResources{Devices: []specs.LinuxDeviceCgroup{{Allow: false, Access: "rwm"}}}
	for _, device := range isolationDevices() {
		major, minor := device.Major, device.Minor
		resources.Devices = append(resources.Devices, specs.LinuxDeviceCgroup{
			Allow: true, Type: device.Type, Major: &major, Minor: &minor, Access: "rwm",
		})
	}
	if limits.MemoryBytes > 0 {
		memory, swap := limits.MemoryBytes, limits.MemoryBytes
		resources.Memory = &specs.LinuxMemory{Limit: &memory, Swap: &swap}
	}
	if limits.CPUMillicores > 0 {
		quotaPerMillicore := int64(defaultCPUPeriodMicroseconds / 1000)
		if limits.CPUMillicores > math.MaxInt64/quotaPerMillicore {
			return nil, errors.New("CPU millicore limit overflows OCI quota")
		}
		quota := limits.CPUMillicores * quotaPerMillicore
		period := defaultCPUPeriodMicroseconds
		resources.CPU = &specs.LinuxCPU{Quota: &quota, Period: &period}
	}
	return resources, nil
}

func isolationMounts(input RuntimeSpecInput) ([]specs.Mount, error) {
	mounts := []specs.Mount{
		{Destination: "/proc", Type: "proc", Source: "proc", Options: []string{"nosuid", "noexec", "nodev"}},
		{Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
		{Destination: "/dev/pts", Type: "devpts", Source: "devpts", Options: []string{"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"}},
		{Destination: "/dev/shm", Type: "tmpfs", Source: "shm", Options: []string{"nosuid", "noexec", "nodev", "mode=1777", "size=65536k"}},
		{Destination: "/dev/mqueue", Type: "mqueue", Source: "mqueue", Options: []string{"nosuid", "noexec", "nodev"}},
		{Destination: "/sys", Type: "sysfs", Source: "sysfs", Options: []string{"nosuid", "noexec", "nodev", "ro"}},
		{Destination: "/sys/fs/cgroup", Type: "cgroup", Source: "cgroup", Options: []string{"nosuid", "noexec", "nodev", "relatime", "ro"}},
		{Destination: "/run", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
		readonlyBindMount(input.ResolverPath, "/etc/resolv.conf"),
		readonlyBindMount(input.HostsPath, "/etc/hosts"),
	}
	for _, volume := range input.Workload.ManagedVolumes {
		if volume.Kind == ManagedVolumeComputerDisk {
			mounts = append(mounts,
				specs.Mount{Destination: "/tmp", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "nodev", "mode=1777", fmt.Sprintf("size=%dk", computerTmpKilobytes)}},
				specs.Mount{Destination: "/var/tmp", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "nodev", "mode=1777", fmt.Sprintf("size=%dk", computerVarTmpKilobytes)}},
			)
			break
		}
	}
	for _, volume := range input.Workload.ManagedVolumes {
		var destination string
		switch volume.Kind {
		case ManagedVolumeHandoff:
			destination = "/wefty/handoff"
		case ManagedVolumeServiceData, ManagedVolumeComputerDisk:
			destination = "/wefty/service"
		case ManagedVolumeLogSegments:
			continue
		default:
			return nil, fmt.Errorf("managed volume kind %q is unsupported", volume.Kind)
		}
		mounts = append(mounts, bindMount(input.ManagedVolumeSources[volume.Kind], destination, volume.ReadOnly))
	}
	type translatedMount struct {
		mount  OperatorMount
		source string
	}
	operatorMounts := make([]translatedMount, len(input.Workload.OperatorMounts))
	for index, mount := range input.Workload.OperatorMounts {
		operatorMounts[index] = translatedMount{mount: mount, source: input.OperatorMountSources[index]}
	}
	sort.Slice(operatorMounts, func(left, right int) bool {
		leftDepth := strings.Count(operatorMounts[left].mount.ContainerPath, "/")
		rightDepth := strings.Count(operatorMounts[right].mount.ContainerPath, "/")
		if leftDepth != rightDepth {
			return leftDepth < rightDepth
		}
		return operatorMounts[left].mount.ContainerPath < operatorMounts[right].mount.ContainerPath
	})
	for _, translated := range operatorMounts {
		mounts = append(mounts, bindMount(translated.source, translated.mount.ContainerPath, translated.mount.ReadOnly))
	}
	return mounts, nil
}

func bindMount(source, destination string, readOnly bool) specs.Mount {
	options := []string{"rbind", "rw", "rprivate"}
	if readOnly {
		options = []string{"rbind", "rro", "rprivate"}
	}
	return specs.Mount{Destination: destination, Type: "bind", Source: source, Options: options}
}

func readonlyBindMount(source, destination string) specs.Mount {
	mount := bindMount(source, destination, true)
	mount.Options = append(mount.Options, "nosuid", "nodev", "noexec")
	return mount
}

func mergeRuntimeEnvironment(image []string, public, sensitive, reserved []EnvironmentVariable) ([]string, error) {
	merged := make([]EnvironmentVariable, 0, len(image)+len(public)+len(sensitive)+len(reserved))
	positions := make(map[string]int, cap(merged))
	put := func(variable EnvironmentVariable) error {
		if !envNamePattern.MatchString(variable.Name) || strings.IndexByte(variable.Value, 0) >= 0 {
			return fmt.Errorf("environment variable %q is invalid", variable.Name)
		}
		if position, exists := positions[variable.Name]; exists {
			merged[position] = variable
			return nil
		}
		positions[variable.Name] = len(merged)
		merged = append(merged, variable)
		return nil
	}
	for _, encoded := range image {
		name, value, ok := strings.Cut(encoded, "=")
		if !ok || !envNamePattern.MatchString(name) || strings.IndexByte(value, 0) >= 0 {
			return nil, fmt.Errorf("image environment entry %q is invalid", encoded)
		}
		if contract.IsOCIReservedEnvironmentName(name) {
			continue
		}
		if err := put(EnvironmentVariable{Name: name, Value: value}); err != nil {
			return nil, err
		}
	}
	for _, variable := range public {
		if contract.IsOCIReservedEnvironmentName(variable.Name) {
			continue
		}
		if err := put(variable); err != nil {
			return nil, err
		}
	}
	for _, variable := range sensitive {
		if contract.IsOCIReservedEnvironmentName(variable.Name) {
			continue
		}
		if err := put(variable); err != nil {
			return nil, err
		}
	}
	for _, variable := range reserved {
		if !contract.IsOCIReservedEnvironmentName(variable.Name) {
			return nil, fmt.Errorf("reserved environment variable %q is not helper-managed", variable.Name)
		}
		if err := put(variable); err != nil {
			return nil, err
		}
	}
	encoded := make([]string, 0, len(merged))
	for _, variable := range merged {
		encoded = append(encoded, variable.Name+"="+variable.Value)
	}
	return encoded, nil
}

func validateEffectiveArgv(values []string) error {
	if len(values) == 0 {
		return errors.New("argv is empty")
	}
	hasNonEmpty := false
	for _, value := range values {
		if strings.IndexByte(value, 0) >= 0 {
			return errors.New("argv contains NUL")
		}
		if value != "" {
			hasNonEmpty = true
		}
	}
	if !hasNonEmpty {
		return errors.New("argv contains no non-empty argument")
	}
	return nil
}

func validContainerPathAllowRoot(value string) bool {
	return strings.HasPrefix(value, "/") && path.Clean(value) == value
}

func isolationMaskedPaths() []string {
	return []string{
		"/proc/acpi", "/proc/asound", "/proc/kcore", "/proc/keys", "/proc/latency_stats",
		"/proc/timer_list", "/proc/timer_stats", "/proc/sched_debug", "/sys/firmware",
		"/sys/devices/virtual/powercap", "/proc/scsi",
	}
}

func isolationReadonlyPaths() []string {
	return []string{"/proc/bus", "/proc/fs", "/proc/irq", "/proc/sys", "/proc/sysrq-trigger"}
}
