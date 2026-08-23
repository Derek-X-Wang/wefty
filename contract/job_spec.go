package contract

import (
	"path"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

const ociImageReferencePattern = `^([a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*(:[0-9]+)?/)?[a-z0-9]+(([._]|__|-+)[a-z0-9]+)*(/[a-z0-9]+(([._]|__|-+)[a-z0-9]+)*)*(:[A-Za-z0-9_][A-Za-z0-9._-]{0,127})?$`

var ociImageReferenceRE = regexp.MustCompile(ociImageReferencePattern)

// ValidateJobSpec normalizes kind and runtime_handler in place, then applies
// the structural v1 JobSpec contract independently of caller-side JSON Schema
// validation. Kind and class remain open strings; the known process and OCI
// arms receive their kind-specific validation here.
func ValidateJobSpec(spec *JobSpec) error {
	if spec == nil {
		return invalidJobSpecf("job spec is required")
	}
	if spec.SchemaVersion != SchemaVersionV1 {
		return invalidJobSpecf("schema_version must be %d", SchemaVersionV1)
	}
	if strings.TrimSpace(spec.DispatchKey) == "" || strings.TrimSpace(spec.Kind) == "" || strings.TrimSpace(spec.Class) == "" {
		return invalidJobSpecf("dispatch_key, kind, and class are required")
	}
	if utf8.RuneCountInString(spec.DispatchKey) > 255 || utf8.RuneCountInString(spec.Kind) > 128 ||
		utf8.RuneCountInString(spec.Class) > 128 || utf8.RuneCountInString(spec.RuntimeHandler) > 128 {
		return invalidJobSpecf("job identifier fields exceed contract limits")
	}
	spec.Kind = strings.ToLower(strings.TrimSpace(spec.Kind))
	spec.RuntimeHandler = strings.ToLower(strings.TrimSpace(spec.RuntimeHandler))
	if strings.IndexFunc(spec.Kind, unicode.IsSpace) >= 0 || strings.IndexFunc(spec.RuntimeHandler, unicode.IsSpace) >= 0 {
		return invalidJobSpecf("kind and runtime_handler cannot contain whitespace")
	}
	if err := validateEnvironment(spec.Execution.Env); err != nil {
		return invalidJobSpecf("execution env: %v", err)
	}
	if err := validateEnvironment(spec.Execution.SensitiveEnv); err != nil {
		return invalidJobSpecf("execution sensitive_env: %v", err)
	}
	normalizedTags := make([]string, 0, len(spec.RoutingTags))
	seenTags := make(map[string]struct{}, len(spec.RoutingTags))
	for _, tag := range spec.RoutingTags {
		if !validRoutingTag(tag) {
			return invalidJobSpecf("routing tag %q is invalid", tag)
		}
		if _, duplicate := seenTags[tag]; duplicate {
			return invalidJobSpecf("routing tag %q is duplicated", tag)
		}
		seenTags[tag] = struct{}{}
		normalizedTags = append(normalizedTags, strings.ToLower(strings.TrimSpace(tag)))
	}

	switch spec.Kind {
	case JobKindProcess:
		if err := validateProcessExecution(spec); err != nil {
			return err
		}
	case JobKindOCI:
		if err := validateOCIExecution(spec); err != nil {
			return err
		}
	default:
		if spec.Execution.ociSet || spec.Execution.OCI != nil {
			return invalidJobSpecf("execution.oci is reserved for kind %q", JobKindOCI)
		}
	}

	if spec.Class == JobClassService {
		if spec.Restart != RestartAlways {
			return invalidJobSpecf("service restart must be %q", RestartAlways)
		}
		if spec.PublishedPort != nil && (*spec.PublishedPort < 1 || *spec.PublishedPort > 65535) {
			return invalidJobSpecf("published_port must be between 1 and 65535")
		}
		if spec.MaxRestartStreak != nil && *spec.MaxRestartStreak < 1 {
			return invalidJobSpecf("max_restart_streak must be at least 1")
		}
	}
	if RequiresPinnedPlacement(*spec) {
		message := "OCI jobs with mounts require exactly one stable-node routing tag"
		if spec.Execution.OCI.Computer != nil {
			message = "Pinned OCI jobs require exactly one stable-node routing tag"
		}
		if err := validatePinnedRouting(true, normalizedTags, message); err != nil {
			return err
		}
	}
	return nil
}

// ValidateImageProgram applies the same structural rules as an L1 OCI job to
// an L3 image snapshot. Class controls the digest requirement; routing is
// validated separately because saved Workflow versions have no placement.
func ValidateImageProgram(program ImageProgram, class string) error {
	oci := &OCIExecutionSpec{
		Image: OCIImageSpec{
			Reference:  program.Reference,
			Digest:     program.Digest,
			digestNull: program.digestNull,
		},
		Argv:             program.Argv,
		WorkingDirectory: program.WorkingDirectory,
		Mounts:           program.Mounts,
		Limits:           program.Limits,
		argvNull:         program.argvNull,
		workingDirNull:   program.workingDirNull,
		mountsNull:       program.mountsNull,
		limitsNull:       program.limitsNull,
	}
	spec := JobSpec{
		SchemaVersion:  SchemaVersionV1,
		DispatchKey:    "validate:image-program",
		Kind:           JobKindOCI,
		Class:          class,
		RuntimeHandler: program.RuntimeHandler,
		Execution:      ExecutionSpec{OCI: oci},
	}
	if class == JobClassService {
		spec.Restart = RestartAlways
	}
	if utf8.RuneCountInString(spec.RuntimeHandler) > 128 {
		return invalidJobSpecf("runtime_handler exceeds contract limits")
	}
	return validateOCIExecution(&spec)
}

// ValidatePinnedRouting is the ImageProgram-only placement validator. An
// ImageProgram deliberately cannot carry the Computer trait, so only its
// operator mounts can require exactly one stable node here.
func ValidatePinnedRouting(program ImageProgram, tags []string) error {
	return validatePinnedRouting(len(program.Mounts) > 0, tags, "OCI jobs with mounts require exactly one stable-node routing tag")
}

// RequiresPinnedPlacement reports whether an L1 JobSpec depends on Node-local
// state. Operator mounts and the Computer trait share this one placement
// predicate; neither introduces a scheduling axis.
func RequiresPinnedPlacement(spec JobSpec) bool {
	return spec.Kind == JobKindOCI && spec.Execution.OCI != nil &&
		(len(spec.Execution.OCI.Mounts) > 0 || spec.Execution.OCI.Computer != nil)
}

func validatePinnedRouting(required bool, tags []string, message string) error {
	if !required {
		return nil
	}
	nodeTags := 0
	for _, tag := range tags {
		tag = strings.ToLower(strings.TrimSpace(tag))
		if strings.HasPrefix(tag, StableNodeTagPrefix) && len(tag) > len(StableNodeTagPrefix) {
			nodeTags++
		}
	}
	if nodeTags != 1 {
		return invalidJobSpecf("%s %q", message, StableNodeTagPrefix+"<stable-node-id>")
	}
	return nil
}

func validateProcessExecution(spec *JobSpec) error {
	if spec.RuntimeHandler != "" {
		return unsupportedRuntimeHandlerf("runtime_handler is not supported for process jobs")
	}
	if spec.Execution.ociSet || spec.Execution.OCI != nil {
		return invalidJobSpecf("execution.oci is forbidden for process jobs")
	}
	if spec.Execution.WorkingDirectory == "" || len(spec.Execution.Argv) == 0 {
		return invalidJobSpecf("process execution argv and working_directory are required")
	}
	if spec.Class == JobClassOneShot && spec.Execution.HandoffDirectory == "" {
		return invalidJobSpecf("one-shot process execution handoff_directory is required")
	}
	if (spec.Execution.Executable.Path == "") == (spec.Execution.Executable.InlineBase64 == "") {
		return invalidJobSpecf("executable must contain exactly one of path or inline_base64")
	}
	if spec.Execution.Executable.InlineBase64 != "" && !validLowerSHA256(spec.Execution.Executable.SHA256) {
		return invalidJobSpecf("inline executable requires a lowercase SHA-256 digest")
	}
	if spec.Execution.Executable.Mode > 4095 {
		return invalidJobSpecf("executable mode exceeds 07777")
	}
	return nil
}

func validateOCIExecution(spec *JobSpec) error {
	if spec.Execution.OCI == nil {
		return invalidJobSpecf("execution.oci is required for OCI jobs")
	}
	if spec.Execution.executableSet || spec.Execution.argvSet || spec.Execution.workingDirSet || spec.Execution.handoffDirSet ||
		!zeroExecutable(spec.Execution.Executable) || spec.Execution.Argv != nil ||
		spec.Execution.WorkingDirectory != "" || spec.Execution.HandoffDirectory != "" {
		return invalidJobSpecf("OCI jobs forbid process execution fields")
	}
	oci := spec.Execution.OCI
	if oci.argvNull || oci.workingDirNull || oci.mountsNull || oci.limitsNull || oci.computerNull {
		return invalidJobSpecf("optional OCI execution fields cannot be null")
	}
	if utf8.RuneCountInString(oci.Image.Reference) > 2048 || !ociImageReferenceRE.MatchString(oci.Image.Reference) {
		return invalidJobSpecf("OCI image reference must be a lowercase distribution repository plus optional tag")
	}
	if oci.Image.digestNull {
		return invalidJobSpecf("OCI image digest cannot be null")
	}
	if oci.Image.Digest != nil && !validOCIDigest(*oci.Image.Digest) {
		return invalidJobSpecf("OCI image digest must be sha256 followed by 64 lowercase hexadecimal characters")
	}
	if spec.Class != JobClassOneShot && oci.Image.Digest == nil {
		return invalidJobSpecf("only one-shot OCI jobs may omit an image digest")
	}
	if oci.Argv != nil {
		if len(oci.Argv) == 0 || !containsNonEmpty(oci.Argv) {
			return invalidJobSpecf("OCI argv, when present, must contain a non-empty element")
		}
	}
	if oci.WorkingDirectory != nil && !validContainerPath(*oci.WorkingDirectory) {
		return invalidJobSpecf("OCI working_directory must be a normalized absolute container path")
	}
	for i, mount := range oci.Mounts {
		if mount.readOnlyNull {
			return invalidJobSpecf("OCI mount %d read_only cannot be null", i)
		}
		if !validNodePath(mount.NodePath) {
			return invalidJobSpecf("OCI mount %d node_path must be a normalized absolute path other than root", i)
		}
		if !validContainerPath(mount.ContainerPath) {
			return invalidJobSpecf("OCI mount %d container_path must be a normalized absolute container path", i)
		}
		if shadowsOCIReservedTarget(mount.ContainerPath) {
			return invalidJobSpecf("OCI mount %d container_path overlaps a reserved wefty target", i)
		}
	}
	if oci.Limits != nil {
		if oci.Limits.memoryNull || oci.Limits.cpuNull {
			return invalidJobSpecf("OCI limits cannot contain null values")
		}
		if oci.Limits.MemoryBytes == nil && oci.Limits.CPUMillicores == nil {
			return invalidJobSpecf("OCI limits must contain memory_bytes or cpu_millicores")
		}
		if oci.Limits.MemoryBytes != nil && *oci.Limits.MemoryBytes < 1 {
			return invalidJobSpecf("OCI memory_bytes must be positive")
		}
		if oci.Limits.CPUMillicores != nil && *oci.Limits.CPUMillicores < 1 {
			return invalidJobSpecf("OCI cpu_millicores must be positive")
		}
	}
	if oci.Computer != nil {
		if spec.Class != JobClassService {
			return invalidJobSpecf("OCI computer trait requires class %q", JobClassService)
		}
		if spec.PublishedPort != nil || spec.publishedPortSet {
			return invalidJobSpecf("published_port is forbidden for the OCI computer trait")
		}
		if oci.Computer.Display.Protocol != ComputerDisplayProtocolRFBWebSocketV1 {
			return invalidJobSpecf("OCI computer display protocol must be %q", ComputerDisplayProtocolRFBWebSocketV1)
		}
		if oci.Computer.DiskBytes < 1 {
			return invalidJobSpecf("OCI computer disk_bytes must be positive")
		}
		if oci.Limits == nil || oci.Limits.MemoryBytes == nil || *oci.Limits.MemoryBytes < 1 {
			return invalidJobSpecf("OCI computer trait requires positive explicit memory_bytes")
		}
	}
	return nil
}

func zeroExecutable(executable ExecutableSpec) bool {
	return executable.Path == "" && executable.InlineBase64 == "" && executable.SHA256 == "" &&
		executable.Interpreter == nil && executable.Mode == 0
}

func validateEnvironment(environment map[string]string) error {
	for name := range environment {
		if name == "" || !validEnvironmentStart(name[0]) {
			return invalidJobSpecf("environment name %q is invalid", name)
		}
		for i := 1; i < len(name); i++ {
			if !validEnvironmentContinue(name[i]) {
				return invalidJobSpecf("environment name %q is invalid", name)
			}
		}
	}
	return nil
}

func validEnvironmentStart(value byte) bool {
	return value == '_' || value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
}

func validEnvironmentContinue(value byte) bool {
	return validEnvironmentStart(value) || value >= '0' && value <= '9'
}

func containsNonEmpty(values []string) bool {
	for _, value := range values {
		if value != "" {
			return true
		}
	}
	return false
}

func validNodePath(value string) bool {
	// Symlink and allowed-root validation deliberately remains node-side where
	// the source filesystem is observable (M3 OCI spec section 2.3).
	return value != "/" && strings.HasPrefix(value, "/") && path.Clean(value) == value
}

func validContainerPath(value string) bool {
	return strings.HasPrefix(value, "/") && path.Clean(value) == value
}

func shadowsOCIReservedTarget(value string) bool {
	if value == "/" {
		return true
	}
	for _, reserved := range [...]string{OCIContainerHandoffDirectory, OCIContainerServiceDirectory} {
		if value == reserved || strings.HasPrefix(value, reserved+"/") || strings.HasPrefix(reserved, value+"/") {
			return true
		}
	}
	return false
}

func validOCIDigest(value string) bool {
	return strings.HasPrefix(value, "sha256:") && len(value) == len("sha256:")+64 && validLowerSHA256(value[len("sha256:"):])
}

func validLowerSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			if r < 'a' || r > 'f' {
				return false
			}
		}
	}
	return true
}

func validRoutingTag(tag string) bool {
	if tag == "" {
		return false
	}
	for i, r := range tag {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || i > 0 && strings.ContainsRune("._:-", r) {
			continue
		}
		return false
	}
	return true
}
