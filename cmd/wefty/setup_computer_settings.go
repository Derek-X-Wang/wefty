package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/l1"
	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

const setupComputerEnvironment = "WEFTY_SETUP_COMPUTER_SETTINGS"

// Only explicit settings are recorded. Each installed service retains its own
// settings, so changing a cluster value cannot restart the node agent.
type setupComputerOptions struct {
	Version         int     `json:"version,omitempty"`
	Image           *string `json:"image,omitempty"`
	SharedSeatLimit *int    `json:"shared_seat_limit,omitempty"`
	SeatMemoryBytes *int64  `json:"seat_memory_bytes,omitempty"`
	DedicatedCap    *int    `json:"dedicated_cap,omitempty"`
	SetupState      *string `json:"setup_state,omitempty"`
}

func bindSetupComputerFlags(flags *flag.FlagSet, values *setupComputerOptions) {
	flags.Func("computer-default-image", "cluster default Computer image reference", func(value string) error {
		if err := l1.ValidateComputerDefaultImage(value); err != nil {
			return err
		}
		values.Image = &value
		return nil
	})
	flags.Func("computer-shared-seat-limit", "new shared Computer Seat limit (default 2)", func(value string) error {
		n, err := strconv.Atoi(value)
		if err != nil || n < 2 {
			return errors.New("shared Computer Seat limit must be at least 2")
		}
		values.SharedSeatLimit = &n
		return nil
	})
	flags.Func("computer-seat-memory-bytes", "room per Seat in bytes (default 1073741824)", func(value string) error {
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n <= 0 {
			return errors.New("room per Seat must be positive bytes")
		}
		values.SeatMemoryBytes = &n
		return nil
	})
	flags.Func("computer-dedicated-cap", "running Seat-request-made dedicated Computers (default 1; 0 disables them)", func(value string) error {
		n, err := strconv.Atoi(value)
		if err != nil || n < 0 {
			return errors.New("dedicated Computer cap must be non-negative")
		}
		values.DedicatedCap = &n
		return nil
	})
	flags.Func("oci-setup-state", "configured host VM setup state, forwarded to the node agent without changing the VM", func(value string) error {
		if !filepath.IsAbs(value) {
			return errors.New("--oci-setup-state must be absolute")
		}
		values.SetupState = &value
		return nil
	})
}

func (values *setupComputerOptions) merge(next setupComputerOptions) {
	if next.Image != nil {
		values.Image = next.Image
	}
	if next.SharedSeatLimit != nil {
		values.SharedSeatLimit = next.SharedSeatLimit
	}
	if next.SeatMemoryBytes != nil {
		values.SeatMemoryBytes = next.SeatMemoryBytes
	}
	if next.DedicatedCap != nil {
		values.DedicatedCap = next.DedicatedCap
	}
	if next.SetupState != nil {
		values.SetupState = next.SetupState
	}
}

func (values setupComputerOptions) empty() bool {
	return values.Image == nil && values.SharedSeatLimit == nil && values.SeatMemoryBytes == nil && values.DedicatedCap == nil && values.SetupState == nil
}

func (values setupComputerOptions) validate() error {
	flags := flag.NewFlagSet("Computer settings", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var checked setupComputerOptions
	bindSetupComputerFlags(flags, &checked)
	return flags.Parse(values.arguments(true, true))
}

func (values setupComputerOptions) arguments(cluster, node bool) []string {
	var args []string
	if cluster {
		if values.Image != nil {
			args = append(args, "--computer-default-image="+*values.Image)
		}
		if values.SharedSeatLimit != nil {
			args = append(args, "--computer-shared-seat-limit="+strconv.Itoa(*values.SharedSeatLimit))
		}
	}
	if node {
		if values.SeatMemoryBytes != nil {
			args = append(args, "--computer-seat-memory-bytes="+strconv.FormatInt(*values.SeatMemoryBytes, 10))
		}
		if values.DedicatedCap != nil {
			args = append(args, "--computer-dedicated-cap="+strconv.Itoa(*values.DedicatedCap))
		}
		if values.SetupState != nil {
			args = append(args, "--oci-setup-state="+*values.SetupState)
		}
	}
	return args
}

func applySetupComputerSettings(paths setupPaths, profile connprofile.Profile, services []launchagent.Agent, changes ...setupComputerOptions) ([]launchagent.Agent, error) {
	var values setupComputerOptions
	hasL1 := slices.ContainsFunc(services, func(s launchagent.Agent) bool { return s.Label == setupL1Label })
	for _, label := range []string{setupL1Label, setupNodeAgentLabel} {
		if label == setupL1Label && !hasL1 {
			continue
		}
		stored, err := readSetupComputerSettings(paths.launchAgents, label)
		if err != nil {
			return nil, err
		}
		values.merge(stored)
	}
	for _, next := range changes {
		values.merge(next)
	}
	if err := values.validate(); err != nil {
		return nil, err
	}
	if (values.Image != nil || values.SharedSeatLimit != nil) && !hasL1 {
		return nil, errors.New("cluster Computer settings require the control-plane installation")
	}
	var room *l1.ComputerRoomConfig
	for _, service := range services {
		if service.Label != setupNodeAgentLabel {
			continue
		}
		statePath := ""
		for _, arg := range service.Arguments {
			if value, ok := strings.CutPrefix(arg, "--oci-setup-state="); ok {
				statePath = value
			}
		}
		if values.SetupState != nil {
			statePath = *values.SetupState
		}
		if statePath == "" {
			continue
		}
		state, err := ocicontrol.ReadSetupState(statePath)
		if err != nil {
			return nil, fmt.Errorf("read configured host VM room: %w", err)
		}
		capacity, reserve, err := limarunner.DefaultMacComputerCapacity(limarunner.Sizing{Memory: state.VMMemory, CPUs: state.VMCPUs, Disk: state.VMDisk})
		if err != nil {
			return nil, err
		}
		config := l1.DefaultComputerRoomConfig(capacity - reserve)
		if values.SeatMemoryBytes != nil {
			config.SeatMemoryBytes = *values.SeatMemoryBytes
		}
		if values.DedicatedCap != nil {
			config.DedicatedCap = *values.DedicatedCap
		}
		room = &config
	}
	for i := range services {
		service := &services[i]
		var scoped setupComputerOptions
		switch service.Label {
		case setupL1Label:
			scoped.Image, scoped.SharedSeatLimit = values.Image, values.SharedSeatLimit
			service.Arguments = append(service.Arguments, scoped.arguments(true, false)...)
			if room != nil && profile.Identities.Node != "" {
				policies := profile.L1().NodePolicies()
				nodeID := profile.Identities.Node
				policy, ok := policies[nodeID]
				if !ok {
					policy = l1.DefaultNodePolicy(contract.StableNodeTagPrefix + nodeID)
				}
				policy, err := policy.WithComputerRoom(*room)
				if err != nil {
					return nil, err
				}
				policies[nodeID] = policy
				var ids []string
				for id := range policies {
					ids = append(ids, id)
				}
				slices.Sort(ids)
				// Explicit slot flags replace the profile's policy map. Preserve
				// every node and its tags and one-shot Slots, not just this node.
				for _, id := range ids {
					p := policies[id]
					service.Arguments = append(service.Arguments, "--node-tags="+id+"="+strings.Join(p.Tags, ","),
						"--node-max-oneshot-slots="+id+"="+strconv.Itoa(p.MaxOneshotSlots), "--node-max-service-slots="+id+"="+strconv.Itoa(p.MaxServiceSlots))
				}
			}
		case setupNodeAgentLabel:
			scoped.SeatMemoryBytes, scoped.DedicatedCap, scoped.SetupState = values.SeatMemoryBytes, values.DedicatedCap, values.SetupState
			service.Arguments = append(service.Arguments, scoped.arguments(false, true)...)
			if room != nil {
				digest, err := connprofile.Digest(struct {
					Prior string
					Room  l1.ComputerRoomConfig
				}{service.InputDigest, *room})
				if err != nil {
					return nil, err
				}
				service.InputDigest = digest
			}
		default:
			continue
		}
		if scoped.empty() {
			continue
		}
		scoped.Version = 1
		payload, err := json.Marshal(scoped)
		if err != nil {
			return nil, err
		}
		if service.Environment == nil {
			service.Environment = map[string]string{}
		}
		service.Environment[setupComputerEnvironment] = base64.RawURLEncoding.EncodeToString(payload)
	}
	return services, nil
}

func readSetupComputerSettings(folder, label string) (setupComputerOptions, error) {
	var result setupComputerOptions
	for _, ext := range []string{".plist", ".service"} {
		payload, err := os.ReadFile(filepath.Join(folder, label+ext))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return result, err
		}
		encoded, err := recordedComputerEnvironment(payload, ext)
		if err != nil {
			return result, err
		}
		if encoded == "" {
			continue
		}
		if !result.empty() {
			return result, errors.New("ambiguous installed Computer settings")
		}
		data, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil {
			return result, errors.New("invalid installed Computer settings encoding")
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&result); err != nil {
			return result, err
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			return result, errors.New("trailing installed Computer settings data")
		}
		if result.Version != 1 {
			return result, errors.New("unsupported installed Computer settings version")
		}
		if label == setupNodeAgentLabel && (result.Image != nil || result.SharedSeatLimit != nil) ||
			label == setupL1Label && (result.SeatMemoryBytes != nil || result.DedicatedCap != nil || result.SetupState != nil) {
			return result, errors.New("installed Computer settings belong to another service")
		}
	}
	return result, nil
}

func recordedComputerEnvironment(payload []byte, ext string) (string, error) {
	if ext == ".service" {
		section, found := "", ""
		for _, line := range strings.Split(string(payload), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "[") {
				section = line
			}
			if section != "[Service]" {
				continue
			}
			value, ok := strings.CutPrefix(line, "Environment=")
			if !ok {
				continue
			}
			value, err := strconv.Unquote(value)
			if err != nil {
				return "", err
			}
			value, ok = strings.CutPrefix(value, setupComputerEnvironment+"=")
			if !ok {
				continue
			}
			if found != "" {
				return "", errors.New("duplicate installed Computer settings")
			}
			found = value
		}
		return found, nil
	}
	decoder := xml.NewDecoder(bytes.NewReader(payload))
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return "", nil
		}
		if err != nil {
			return "", err
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name.Local != "key" {
			continue
		}
		var key string
		if err := decoder.DecodeElement(&key, &start); err != nil {
			return "", err
		}
		if key != "EnvironmentVariables" {
			continue
		}
		for {
			token, err = decoder.Token()
			if err != nil {
				return "", err
			}
			if start, ok = token.(xml.StartElement); ok {
				break
			}
		}
		if start.Name.Local != "dict" {
			return "", errors.New("invalid installed service environment")
		}
		var env struct {
			Keys   []string `xml:"key"`
			Values []string `xml:"string"`
		}
		if err := decoder.DecodeElement(&env, &start); err != nil {
			return "", err
		}
		if len(env.Keys) != len(env.Values) {
			return "", errors.New("invalid installed service environment")
		}
		found := ""
		for i, key := range env.Keys {
			if key == setupComputerEnvironment {
				if found != "" {
					return "", errors.New("duplicate installed Computer settings")
				}
				found = env.Values[i]
			}
		}
		return found, nil
	}
}
