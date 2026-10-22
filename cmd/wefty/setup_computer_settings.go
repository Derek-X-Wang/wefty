package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/l1"
	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

// Only explicit settings are retained in the setup receipt, outside the
// installed service environment inherited by process jobs.
type setupComputerOptions struct {
	Image           *string `json:"image,omitempty"`
	SharedSeatLimit *int    `json:"shared_seat_limit,omitempty"`
	SeatMemoryBytes *int64  `json:"seat_memory_bytes,omitempty"`
	DedicatedCap    *int    `json:"dedicated_cap,omitempty"`
}

func bindSetupComputerFlags(flags *flag.FlagSet, values *setupComputerOptions) {
	flags.Func("computer-default-image", "digest-pinned cluster default Computer image reference", func(value string) error {
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
	flags.Func("computer-seat-memory-bytes", "room per Seat in bytes (minimum 268435456; default 1073741824)", func(value string) error {
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n < l1.MinimumSeatMemoryBytes {
			return errors.New("room per Seat must be at least 256 MiB")
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
}

func (values setupComputerOptions) empty() bool {
	return values.Image == nil && values.SharedSeatLimit == nil && values.SeatMemoryBytes == nil && values.DedicatedCap == nil
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
	}
	return args
}

func applySetupComputerSettings(paths setupPaths, profile connprofile.Profile, services []launchagent.Agent, changes ...setupComputerOptions) ([]launchagent.Agent, error) {
	values, err := resolveSetupComputerSettings(paths, changes...)
	if err != nil {
		return nil, err
	}
	hasL1 := slices.ContainsFunc(services, func(s launchagent.Agent) bool { return s.Label == setupL1Label })
	if (values.Image != nil || values.SharedSeatLimit != nil) && !hasL1 {
		return nil, errors.New("cluster Computer settings require the control-plane installation")
	}
	var room *l1.ComputerRoomConfig
	for _, service := range services {
		if service.Label == setupNodeAgentLabel {
			room, err = setupComputerRoom(service.Arguments, values)
			if err != nil {
				return nil, err
			}
		}
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
				if policies == nil {
					policies = map[string]l1.NodePolicy{}
				}
				nodeID := profile.Identities.Node
				policy, ok := policies[nodeID]
				if !ok {
					policy = l1.DefaultNodePolicy(contract.StableNodeTagPrefix + nodeID)
					policy, err = policy.WithComputerRoom(*room)
					if err != nil {
						return nil, err
					}
				} else if policy.MaxServiceSlots < int(room.MemoryBytes/room.SeatMemoryBytes) {
					log.Printf("event=computer_room_service_slots_owner_override node_id=%q service_slots=%d room_seats=%d", nodeID, policy.MaxServiceSlots, room.MemoryBytes/room.SeatMemoryBytes)
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
			scoped.SeatMemoryBytes, scoped.DedicatedCap = values.SeatMemoryBytes, values.DedicatedCap
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
	}
	return services, nil
}

// The receipt is the only persistence authority. No plist parsing is needed,
// including when launchd has converted a definition to a binary plist.
func resolveSetupComputerSettings(paths setupPaths, changes ...setupComputerOptions) (setupComputerOptions, error) {
	var values setupComputerOptions
	payload, err := os.ReadFile(filepath.Join(paths.config, setupManifestName))
	if err == nil {
		var receipt setupManifest
		if err := json.Unmarshal(payload, &receipt); err != nil {
			return values, err
		}
		if receipt.Version != 1 {
			return values, errors.New("unsupported setup receipt version")
		}
		if receipt.Computer != nil {
			values = *receipt.Computer
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return values, err
	}
	for _, next := range changes {
		values.merge(next)
	}
	return values, values.validate()
}

func (values setupComputerOptions) recorded() *setupComputerOptions {
	if values.empty() {
		return nil
	}
	return &values
}

// Resolve all settings and read any plan-owned setup state before enrolment.
// setup does not choose or record the per-user host VM state path (#1132).
func validateSetupComputerPlan(paths setupPaths, plan serviceplan.Plan, changes setupComputerOptions) (setupComputerOptions, error) {
	values, err := resolveSetupComputerSettings(paths, changes)
	if err != nil {
		return values, err
	}
	if plan.Role == serviceplan.Join && (values.Image != nil || values.SharedSeatLimit != nil) {
		return values, errors.New("cluster Computer settings require the control-plane installation")
	}
	for _, service := range plan.Services {
		if service.Name == "node-agent" {
			if _, err := setupComputerRoom(service.Arguments, values); err != nil {
				return values, err
			}
		}
	}
	return values, nil
}

func setupComputerRoom(args []string, values setupComputerOptions) (*l1.ComputerRoomConfig, error) {
	var statePath string
	for _, arg := range args {
		if value, ok := strings.CutPrefix(arg, "--oci-setup-state="); ok {
			statePath = value
		}
	}
	if statePath == "" {
		return nil, nil
	}
	state, err := ocicontrol.ReadSetupState(statePath)
	if err != nil {
		return nil, fmt.Errorf("read configured Computer room: %w", err)
	}
	capacity, reserve := state.MemoryCapacityBytes, state.MemoryReserveBytes
	for _, field := range []struct {
		name   string
		target *int64
	}{{"oci-memory-capacity-bytes", &capacity}, {"oci-memory-reserve-bytes", &reserve}} {
		for _, arg := range args {
			if value, ok := strings.CutPrefix(arg, "--"+field.name+"="); ok {
				parsed, err := strconv.ParseInt(value, 10, 64)
				if err != nil || parsed < 0 {
					return nil, fmt.Errorf("invalid --%s", field.name)
				}
				*field.target = parsed
			}
		}
	}
	if capacity == 0 {
		return nil, nil
	}
	available := max(int64(0), capacity-reserve)
	if state.VMType == "vz" {
		total, infra, err := limarunner.DefaultMacComputerCapacity(limarunner.Sizing{Memory: state.VMMemory, CPUs: state.VMCPUs, Disk: state.VMDisk})
		if err != nil {
			return nil, err
		}
		available = min(available, total-infra)
	}
	room := l1.DefaultComputerRoomConfig(available)
	if values.SeatMemoryBytes != nil {
		room.SeatMemoryBytes = *values.SeatMemoryBytes
	}
	if values.DedicatedCap != nil {
		room.DedicatedCap = *values.DedicatedCap
	}
	return &room, room.Validate()
}
