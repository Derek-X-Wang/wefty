package main

import (
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
)

type imageFlagSet struct {
	reference        string
	workingDirectory string
	runtimeHandler   string
	nodeID           string
	argv             stringListFlag
	mounts           mountListFlag
	memoryBytes      optionalInt64Flag
	cpuMillicores    optionalInt64Flag
}

func (f *imageFlagSet) nonRoutingOptionsSet() bool {
	return len(f.argv) > 0 || f.workingDirectory != "" || len(f.mounts) > 0 ||
		f.memoryBytes.set || f.cpuMillicores.set || strings.TrimSpace(f.runtimeHandler) != ""
}

func (f *imageFlagSet) bind(flags *flag.FlagSet) {
	flags.StringVar(&f.reference, "image", "", "OCI image reference with optional @sha256 digest")
	flags.Var(&f.argv, "argv", "image argv entry (repeatable; replaces the image vector)")
	flags.StringVar(&f.workingDirectory, "working-directory", "", "container working directory")
	flags.Var(&f.mounts, "mount", "operator mount NODE_PATH:CONTAINER_PATH[:ro] (repeatable)")
	flags.Var(&f.memoryBytes, "memory-bytes", "cgroup-v2 memory hard limit")
	flags.Var(&f.cpuMillicores, "cpu-millicores", "cgroup-v2 CPU hard limit")
	flags.StringVar(&f.runtimeHandler, "runtime-handler", "", "required OCI runtime handler")
	flags.StringVar(&f.nodeID, "node", "", "stable node ID used for Pinned routing")
}

func (f *imageFlagSet) programAndTags(tags []string, class string) (*contract.ImageProgram, []string, error) {
	if strings.TrimSpace(f.reference) == "" {
		return nil, tags, nil
	}
	reference, digest, err := splitImageReference(f.reference)
	if err != nil {
		return nil, nil, err
	}
	program := &contract.ImageProgram{
		Reference:      reference,
		Digest:         digest,
		Argv:           append([]string(nil), f.argv...),
		Mounts:         append([]contract.OCIMount(nil), f.mounts...),
		RuntimeHandler: strings.TrimSpace(f.runtimeHandler),
	}
	if f.workingDirectory != "" {
		workingDirectory := f.workingDirectory
		program.WorkingDirectory = &workingDirectory
	}
	if f.memoryBytes.set || f.cpuMillicores.set {
		program.Limits = &contract.OCILimits{}
		if f.memoryBytes.set {
			program.Limits.MemoryBytes = cloneCLIInt64(f.memoryBytes.value)
		}
		if f.cpuMillicores.set {
			program.Limits.CPUMillicores = cloneCLIInt64(f.cpuMillicores.value)
		}
	}
	tags, err = pinnedImageTags(tags, f.nodeID, len(program.Mounts) > 0)
	if err != nil {
		return nil, nil, err
	}
	if err := contract.ValidateImageProgram(*program, class, l1NormalizedTags(tags)); err != nil {
		return nil, nil, usageError(fmt.Sprintf("invalid image program: %v", err))
	}
	return program, tags, nil
}

func splitImageReference(value string) (string, *string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil, usageError("--image must not be empty")
	}
	at := strings.LastIndexByte(value, '@')
	if at < 0 {
		return value, nil, nil
	}
	if at == 0 || at == len(value)-1 || strings.Contains(value[:at], "@") {
		return "", nil, usageError("--image must use REFERENCE or REFERENCE@sha256:<digest>")
	}
	digest := value[at+1:]
	return value[:at], &digest, nil
}

func pinnedImageTags(tags []string, nodeID string, mounted bool) ([]string, error) {
	result := append([]string(nil), tags...)
	nodeID = strings.TrimSpace(nodeID)
	if nodeID != "" {
		result = append(result, contract.StableNodeTagPrefix+nodeID)
	}
	nodeTags := 0
	for _, tag := range l1NormalizedTags(result) {
		if strings.HasPrefix(tag, contract.StableNodeTagPrefix) && len(tag) > len(contract.StableNodeTagPrefix) {
			nodeTags++
		}
	}
	if mounted && nodeTags != 1 {
		return nil, usageError("--mount requires --node NODE_ID or exactly one wefty:node:* tag")
	}
	if nodeID != "" && nodeTags != 1 {
		return nil, usageError("--node conflicts with an existing stable-node routing tag")
	}
	return result, nil
}

func l1NormalizedTags(tags []string) []string {
	seen := make(map[string]struct{}, len(tags))
	result := make([]string, 0, len(tags))
	for _, tag := range tags {
		tag = strings.ToLower(strings.TrimSpace(tag))
		if _, ok := seen[tag]; ok {
			continue
		}
		seen[tag] = struct{}{}
		result = append(result, tag)
	}
	return result
}

type mountListFlag []contract.OCIMount

func (f *mountListFlag) Set(value string) error {
	parts := strings.Split(value, ":")
	if len(parts) < 2 || len(parts) > 3 || parts[0] == "" || parts[1] == "" {
		return errors.New("mount must use NODE_PATH:CONTAINER_PATH[:ro]")
	}
	readOnly := false
	if len(parts) == 3 {
		if parts[2] != "ro" {
			return errors.New("mount mode must be ro when present")
		}
		readOnly = true
	}
	*f = append(*f, contract.OCIMount{NodePath: parts[0], ContainerPath: parts[1], ReadOnly: readOnly})
	return nil
}

func (f *mountListFlag) String() string { return fmt.Sprintf("%v", []contract.OCIMount(*f)) }

type optionalInt64Flag struct {
	value int64
	set   bool
}

func (f *optionalInt64Flag) Set(value string) error {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return errors.New("must be a positive integer")
	}
	f.value = parsed
	f.set = true
	return nil
}

func (f *optionalInt64Flag) String() string {
	if !f.set {
		return ""
	}
	return strconv.FormatInt(f.value, 10)
}

func cloneCLIInt64(value int64) *int64 {
	return &value
}
