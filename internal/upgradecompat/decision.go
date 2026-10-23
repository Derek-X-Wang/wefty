// Package upgradecompat decides whether a verified upgrade inventory permits
// rolling activation. It performs no I/O and grants no activation authority.
// The target protocol is specified in docs/specs/2026-10-22-rolling-upgrade-spec.md;
// declaring it here does not advertise runtime support.
package upgradecompat

// ProtocolV1 is the first retirement protocol, not a binary or profile version.
const ProtocolV1 uint32 = 1

type Mode string

const (
	Rolling Mode = "rolling"
	Drain   Mode = "drain"
	Refuse  Mode = "refuse"
)

type Reason string

const (
	Compatible               Reason = "compatible"
	InvalidInventory         Reason = "invalid_inventory"
	UnsupportedInstallation  Reason = "unsupported_installation"
	UnsupportedWork          Reason = "unsupported_work"
	LegacyPrimary            Reason = "legacy_primary"
	CandidateUnsupported     Reason = "candidate_unsupported"
	IncompatibleProtocols    Reason = "incompatible_protocols"
	UnsafeDrainDuringOverlap Reason = "unsafe_drain_during_overlap"
)

type Installation string

const (
	PlainMac Installation = "plain_mac"
	FleetMac Installation = "fleet_mac"
	Linux    Installation = "linux"
)

type Role string

const (
	Primary  Role = "primary"
	Retiring Role = "retiring"
)

// Generation describes one live session. ID must be unique in the inventory.
// An empty Protocols set means a positively identified legacy agent, not a
// failed or missing observation. Versions are unordered supported-version sets.
type Generation struct {
	ID        string
	Role      Role
	Protocols []uint32
}

type WorkKind string

const (
	ProcessOneshot            WorkKind = "process_oneshot"
	RestartableProcessOneshot WorkKind = "restartable_process_oneshot"
	ProcessService            WorkKind = "process_service"
	OCIWork                   WorkKind = "oci"
)

// Input must contain every live generation and all resident work, including
// claims committed before admission closed. Callers must refuse unavailable or
// incomplete observations before calling Decide. Nil Work means verified idle.
type Input struct {
	Installation       Installation
	CandidateProtocols []uint32
	Live               []Generation
	Work               []WorkKind
}

type Decision struct {
	Mode   Mode
	Reason Reason
	// Protocol is the highest version supported by the entire overlap set.
	// It is zero for drain and refuse, which authorize no overlap protocol.
	Protocol uint32
}

// Decide is conservative: today's drain handles a single primary only.
// A scope fallback during existing overlap therefore refuses, preserving the
// installation and its work. Within rolling scope, version incompatibility
// refuses rather than selecting a destructive fallback.
func Decide(in Input) Decision {
	primary, retirees := -1, 0
	ids := make(map[string]bool, len(in.Live))
	if !validProtocols(in.CandidateProtocols) {
		return Decision{Mode: Refuse, Reason: InvalidInventory}
	}
	for i, generation := range in.Live {
		if generation.ID == "" || ids[generation.ID] || !validProtocols(generation.Protocols) {
			return Decision{Mode: Refuse, Reason: InvalidInventory}
		}
		ids[generation.ID] = true
		switch generation.Role {
		case Primary:
			if primary != -1 {
				return Decision{Mode: Refuse, Reason: InvalidInventory}
			}
			primary = i
		case Retiring:
			retirees++
		default:
			return Decision{Mode: Refuse, Reason: InvalidInventory}
		}
	}
	if primary == -1 {
		return Decision{Mode: Refuse, Reason: InvalidInventory}
	}
	if in.Installation != PlainMac && in.Installation != FleetMac && in.Installation != Linux {
		return Decision{Mode: Refuse, Reason: InvalidInventory}
	}
	oci := false
	for _, work := range in.Work {
		switch work {
		case ProcessOneshot, RestartableProcessOneshot, ProcessService:
		case OCIWork:
			oci = true
		default:
			return Decision{Mode: Refuse, Reason: InvalidInventory}
		}
	}
	fallback := func(reason Reason) Decision {
		if retirees > 0 {
			return Decision{Mode: Refuse, Reason: UnsafeDrainDuringOverlap}
		}
		return Decision{Mode: Drain, Reason: reason}
	}
	if in.Installation != PlainMac {
		return fallback(UnsupportedInstallation)
	}
	if oci {
		return fallback(UnsupportedWork)
	}
	if len(in.Live[primary].Protocols) == 0 {
		return fallback(LegacyPrimary)
	}
	if len(in.CandidateProtocols) == 0 {
		return Decision{Mode: Refuse, Reason: CandidateUnsupported}
	}
	var common uint32
	for _, version := range in.CandidateProtocols {
		if version <= common {
			continue
		}
		all := true
		for _, generation := range in.Live {
			if !supports(generation.Protocols, version) {
				all = false
				break
			}
		}
		if all {
			common = version
		}
	}
	if common == 0 {
		return Decision{Mode: Refuse, Reason: IncompatibleProtocols}
	}
	return Decision{Mode: Rolling, Reason: Compatible, Protocol: common}
}

func validProtocols(versions []uint32) bool {
	for _, version := range versions {
		if version == 0 {
			return false
		}
	}
	return true
}

func supports(versions []uint32, wanted uint32) bool {
	for _, version := range versions {
		if version == wanted {
			return true
		}
	}
	return false
}
