// Package connprofile reads and writes the connection profile: the one file
// that tells every wefty program on a machine where the others listen and who
// each of them is on the plain Fabric. `wefty setup` writes it.
//
// Separate plain-Fabric processes cannot find each other by logical name, and
// L1 and L3 refuse the built-in logical defaults in plain mode. Without this
// file every program needs the same endpoint and identity flags repeated in
// every shell and every login service. With it, each program reads the subset
// it needs, under one precedence: flags, then environment, then the profile,
// then built-in defaults.
package connprofile

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/durable"
	"github.com/Derek-X-Wang/wefty/internal/nodeload"
	"github.com/Derek-X-Wang/wefty/l1"
)

const (
	// Version is the only profile format this build reads and writes.
	Version = 1
	// EnvironmentVariable names a profile when no --profile flag does.
	EnvironmentVariable = "WEFTY_PROFILE"
	// FileName is the profile's name inside the wefty config folder.
	FileName = "profile.json"
)

// Profile is the whole file. Every field but Version is optional, so a
// profile can name only what differs from the built-in defaults.
type Profile struct {
	Version int `json:"version"`
	// OneshotLoadLimit is this node agent's local load setting. Nil uses the CPU-based default.
	OneshotLoadLimit *float64 `json:"oneshot_load_limit,omitempty"`
	// Fabric is the Fabric mode every program uses: "plain" in phase 1.
	Fabric string `json:"fabric,omitempty"`
	// PlainFabricID is the shared DEVELOPMENT ONLY plain-Fabric authority. It
	// stays the same across restarts so identities L1 recorded keep matching.
	PlainFabricID string `json:"plain_fabric_id,omitempty"`
	// L1Address is the loopback address L1 listens on and everyone else dials.
	L1Address string `json:"l1_address,omitempty"`
	// L3Address is the loopback address the run ledger listens on and
	// everyone else dials.
	L3Address  string     `json:"l3_address,omitempty"`
	Identities Identities `json:"identities,omitzero"`
	// Participants are independently enrolled identities. Absent on phase-1 profiles.
	Participants map[string]Participant `json:"participants,omitempty"`
	// AllowPlainPersonIdentities lets L1 accept the self-asserted plain person
	// identity on person routes. Plain mode on one machine needs it.
	AllowPlainPersonIdentities bool `json:"allow_plain_person_identities,omitempty"`
	// NodeAdmission is L1's node admission policy: the nodes it accepts for
	// claims, with their authoritative tags and slot limits.
	NodeAdmission []NodeAdmission `json:"node_admission,omitempty"`
	// AutomaticNodeAdmission treats authenticated agent enrolment as approval
	// for a previously unseen Node. It is off unless explicitly enabled.
	AutomaticNodeAdmission bool `json:"automatic_node_admission,omitempty"`
}

// Participant records only Wefty names, local state paths and opaque identity
// fields. Join credentials are never part of the connection profile.
type Participant struct {
	Name           string    `json:"name"`
	StateDirectory string    `json:"state_directory"`
	NodeID         string    `json:"node_id"`
	UserID         string    `json:"user_id,omitempty"`
	KeyExpiry      time.Time `json:"key_expiry,omitzero"`
}

const (
	ParticipantClient    = "client"
	ParticipantPerson    = "person"
	ParticipantNodeAgent = "node-agent"
	ParticipantL1        = "l1"
	ParticipantL3        = "l3"
)

// Identities are the plain Fabric identities the programs present.
type Identities struct {
	// CLI is the plain Fabric node ID the CLI presents as a client.
	CLI string `json:"cli,omitempty"`
	// PersonUserID and PersonDeviceID are the self-asserted plain person
	// identity the CLI presents for person commands.
	PersonUserID   string `json:"person_user_id,omitempty"`
	PersonDeviceID string `json:"person_device_id,omitempty"`
	// Node is this machine's stable node ID, which its node agent uses.
	Node string `json:"node,omitempty"`
}

// NodeAdmission is L1's authoritative policy for one stable node.
type NodeAdmission struct {
	NodeID          string   `json:"node_id"`
	Tags            []string `json:"tags,omitempty"`
	MaxOneshotSlots int      `json:"max_oneshot_slots"`
	MaxServiceSlots int      `json:"max_service_slots"`
}

// DefaultPath is the profile in the wefty config folder under home.
func DefaultPath(home string) (string, error) {
	if !filepath.IsAbs(home) {
		return "", errors.New("connection profile: home must be absolute")
	}
	return filepath.Join(home, ".config", "wefty", FileName), nil
}

// Validate rejects a profile that some program could not use as written.
func (p Profile) Validate() error {
	if p.OneshotLoadLimit != nil {
		if err := nodeload.ValidateLimit(*p.OneshotLoadLimit); err != nil {
			return err
		}
	}
	if p.Version != Version {
		return fmt.Errorf("connection profile: unsupported version %d, want %d", p.Version, Version)
	}
	if len(p.Participants) > 0 && p.Fabric == "" {
		return errors.New("connection profile: participant profiles require an explicit fabric mode")
	}
	switch p.Fabric {
	case "", "plain", "tsnet":
	default:
		return fmt.Errorf("connection profile: unsupported fabric %q", p.Fabric)
	}
	if p.PlainFabricID != "" && (!strings.HasPrefix(p.PlainFabricID, "plain-") || len(p.PlainFabricID) <= len("plain-") ||
		len(p.PlainFabricID) > 255 || strings.TrimSpace(p.PlainFabricID) != p.PlainFabricID) {
		return errors.New("connection profile: plain_fabric_id must use the bounded plain- prefix")
	}
	for name, value := range map[string]string{
		"l1_address": p.L1Address, "l3_address": p.L3Address,
		"identities.cli": p.Identities.CLI, "identities.person_user_id": p.Identities.PersonUserID,
		"identities.person_device_id": p.Identities.PersonDeviceID, "identities.node": p.Identities.Node,
	} {
		if strings.TrimSpace(value) != value || strings.ContainsAny(value, "\r\n\t") {
			return fmt.Errorf("connection profile: %s must not carry spaces or control characters", name)
		}
	}
	seen := make(map[string]bool, len(p.NodeAdmission))
	stateFolders := make(map[string]bool)
	for role, participant := range p.Participants {
		if err := ValidateParticipantName(role, participant.Name); err != nil {
			return err
		}
		if participant.Name == "" || participant.NodeID == "" || !filepath.IsAbs(participant.StateDirectory) ||
			filepath.Clean(participant.StateDirectory) != participant.StateDirectory {
			return fmt.Errorf("connection profile: participant %s needs a name, opaque node ID and absolute state directory", role)
		}
		for _, value := range []string{participant.Name, participant.NodeID, participant.UserID, participant.StateDirectory} {
			if strings.TrimSpace(value) != value || strings.ContainsAny(value, "\r\n\t") {
				return fmt.Errorf("connection profile: participant %s has invalid identity or path text", role)
			}
		}
		if stateFolders[participant.StateDirectory] {
			return fmt.Errorf("connection profile: participants must not share state directory %q", participant.StateDirectory)
		}
		stateFolders[participant.StateDirectory] = true
	}
	for _, admission := range p.NodeAdmission {
		if strings.TrimSpace(admission.NodeID) == "" || strings.TrimSpace(admission.NodeID) != admission.NodeID {
			return errors.New("connection profile: node_admission entries need a stable node_id")
		}
		if seen[admission.NodeID] {
			return fmt.Errorf("connection profile: node_admission names node %q twice", admission.NodeID)
		}
		seen[admission.NodeID] = true
		if admission.MaxOneshotSlots < 0 || admission.MaxServiceSlots < 0 {
			return fmt.Errorf("connection profile: node %q slot limits must be non-negative", admission.NodeID)
		}
		for _, tag := range l1.NormalizeTags(admission.Tags) {
			if !l1.ValidTag(tag) {
				return fmt.Errorf("connection profile: node %q tag %q is invalid", admission.NodeID, tag)
			}
		}
	}
	return nil
}

// Read reads one profile file. A missing file is an error satisfying
// errors.Is(err, os.ErrNotExist).
func Read(path string) (Profile, error) {
	if !filepath.IsAbs(path) {
		return Profile{}, fmt.Errorf("connection profile path %q must be absolute", path)
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return Profile{}, fmt.Errorf("read connection profile: %w", err)
	}
	var profile Profile
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&profile); err != nil {
		return Profile{}, fmt.Errorf("read connection profile %s: %w", path, err)
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return Profile{}, fmt.Errorf("read connection profile %s: trailing data", path)
	}
	if err := profile.Validate(); err != nil {
		return Profile{}, fmt.Errorf("read connection profile %s: %w", path, err)
	}
	return profile, nil
}

// Encode returns the canonical bytes Write stores, so a caller can tell
// whether a write would change anything.
func Encode(profile Profile) ([]byte, error) {
	if err := profile.Validate(); err != nil {
		return nil, err
	}
	payload, err := json.MarshalIndent(profile, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(payload, '\n'), nil
}

// Write durably replaces the profile at path. It is private to the owner.
func Write(path string, profile Profile) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("connection profile path %q must be absolute", path)
	}
	payload, err := Encode(profile)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create connection profile folder: %w", err)
	}
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer root.Close()
	return durable.WriteFile(root, filepath.Base(path), payload, 0o600)
}

// Resolve finds the profile a program uses. named is the --profile value,
// whose default is $WEFTY_PROFILE; a named profile must exist. When nothing is
// named, fallback is read if it exists, and an empty fallback means no
// profile. It reports the path it read, or "" when there is no profile.
func Resolve(named, fallback string) (Profile, string, error) {
	if named = strings.TrimSpace(named); named != "" {
		path, err := filepath.Abs(named)
		if err != nil {
			return Profile{}, "", err
		}
		profile, err := Read(path)
		if err != nil {
			return Profile{}, "", err
		}
		return profile, path, nil
	}
	if fallback == "" {
		return Profile{}, "", nil
	}
	profile, err := Read(fallback)
	if errors.Is(err, os.ErrNotExist) {
		return Profile{}, "", nil
	}
	if err != nil {
		return Profile{}, "", err
	}
	return profile, fallback, nil
}

// L1Settings is everything L1 reads from the profile.
type L1Settings struct {
	ParticipantProfile         bool                `json:"participant_profile,omitempty"`
	Participant                *ServiceParticipant `json:"participant,omitempty"`
	RunLedgerNodeID            string              `json:"run_ledger_node_id,omitempty"`
	Fabric                     string              `json:"fabric,omitempty"`
	PlainFabricID              string              `json:"plain_fabric_id,omitempty"`
	Listen                     string              `json:"listen,omitempty"`
	RunLedger                  string              `json:"run_ledger,omitempty"`
	AllowPlainPersonIdentities bool                `json:"allow_plain_person_identities,omitempty"`
	NodeAdmission              []NodeAdmission     `json:"node_admission,omitempty"`
	AutomaticNodeAdmission     bool                `json:"automatic_node_admission,omitempty"`
}

// L3Settings is everything the run ledger reads from the profile.
type L3Settings struct {
	ParticipantProfile bool                `json:"participant_profile,omitempty"`
	Participant        *ServiceParticipant `json:"participant,omitempty"`
	ControlPlaneNodeID string              `json:"control_plane_node_id,omitempty"`
	Fabric             string              `json:"fabric,omitempty"`
	PlainFabricID      string              `json:"plain_fabric_id,omitempty"`
	Listen             string              `json:"listen,omitempty"`
	ControlPlane       string              `json:"control_plane,omitempty"`
}

// NodeAgentSettings is everything the node agent reads from the profile.
type NodeAgentSettings struct {
	ParticipantProfile bool                `json:"participant_profile,omitempty"`
	Participant        *ServiceParticipant `json:"participant,omitempty"`
	OneshotLoadLimit   *float64            `json:"oneshot_load_limit,omitempty"`
	Fabric             string              `json:"fabric,omitempty"`
	PlainFabricID      string              `json:"plain_fabric_id,omitempty"`
	ControlPlane       string              `json:"control_plane,omitempty"`
	RunLedger          string              `json:"run_ledger,omitempty"`
	NodeID             string              `json:"node_id,omitempty"`
}

// CLISettings is everything the CLI reads from the profile.
type CLISettings struct {
	Fabric         string `json:"fabric,omitempty"`
	PlainFabricID  string `json:"plain_fabric_id,omitempty"`
	L1             string `json:"l1,omitempty"`
	L3             string `json:"l3,omitempty"`
	Identity       string `json:"identity,omitempty"`
	PersonUserID   string `json:"person_user_id,omitempty"`
	PersonDeviceID string `json:"person_device_id,omitempty"`
}

func (p Profile) L1() L1Settings {
	return L1Settings{ParticipantProfile: len(p.Participants) > 0,
		Participant: p.ServiceParticipant(ParticipantL1), RunLedgerNodeID: p.PeerNodeID(ParticipantL3),
		Fabric: p.Fabric, PlainFabricID: p.PlainFabricID, Listen: p.L1Address, RunLedger: p.L3Address,
		AllowPlainPersonIdentities: p.AllowPlainPersonIdentities,
		NodeAdmission:              slices.Clone(p.NodeAdmission),
		AutomaticNodeAdmission:     p.AutomaticNodeAdmission,
	}
}

func (p Profile) L3() L3Settings {
	return L3Settings{ParticipantProfile: len(p.Participants) > 0, Participant: p.ServiceParticipant(ParticipantL3), ControlPlaneNodeID: p.PeerNodeID(ParticipantL1), Fabric: p.Fabric, PlainFabricID: p.PlainFabricID, Listen: p.L3Address, ControlPlane: p.L1Address}
}

func (p Profile) NodeAgent() NodeAgentSettings {
	return NodeAgentSettings{ParticipantProfile: len(p.Participants) > 0,
		Fabric: p.Fabric, PlainFabricID: p.PlainFabricID, ControlPlane: p.L1Address, RunLedger: p.L3Address,
		NodeID: p.Identities.Node, OneshotLoadLimit: p.OneshotLoadLimit, Participant: p.ServiceParticipant(ParticipantNodeAgent),
	}
}

func (p Profile) CLI() CLISettings {
	return CLISettings{
		Fabric: p.Fabric, PlainFabricID: p.PlainFabricID, L1: p.L1Address, L3: p.L3Address,
		Identity: p.Identities.CLI, PersonUserID: p.Identities.PersonUserID, PersonDeviceID: p.Identities.PersonDeviceID,
	}
}

// NodePolicies converts the admission policy into L1's server configuration.
func (s L1Settings) NodePolicies() map[string]l1.NodePolicy {
	if len(s.NodeAdmission) == 0 {
		return nil
	}
	policies := make(map[string]l1.NodePolicy, len(s.NodeAdmission))
	for _, admission := range s.NodeAdmission {
		policies[admission.NodeID] = l1.NodePolicy{
			Tags:            slices.Clone(admission.Tags),
			MaxOneshotSlots: admission.MaxOneshotSlots,
			MaxServiceSlots: admission.MaxServiceSlots,
		}
	}
	return policies
}

// Digest is a stable fingerprint of a program's inputs. Setup records it with
// each login service, so a later run reloads only a service whose inputs
// changed.
func Digest(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// Layer applies profile values under the precedence every wefty program
// shares: an explicit flag, then a non-blank environment variable, then the
// profile, then the built-in default. A target already holds its flag's
// value, whose default reads the environment, so Layer decides only whether
// the profile may replace it.
type Layer struct {
	Explicit map[string]bool
	Getenv   func(string) string
}

// ExplicitFlags names the flags the caller actually passed.
func ExplicitFlags(flags *flag.FlagSet) map[string]bool {
	explicit := make(map[string]bool)
	flags.Visit(func(set *flag.Flag) { explicit[set.Name] = true })
	return explicit
}

// Fill replaces target with value when value is non-empty, the flag was not
// passed, and environment (when named) is blank.
func (l Layer) Fill(target *string, flagName, environment, value string) {
	if value == "" || l.Explicit[flagName] {
		return
	}
	if environment != "" {
		getenv := l.Getenv
		if getenv == nil {
			getenv = os.Getenv
		}
		if strings.TrimSpace(getenv(environment)) != "" {
			return
		}
	}
	*target = value
}

// Enable turns target on when the profile does and the flag was not passed.
func (l Layer) Enable(target *bool, flagName string, value bool) {
	if value && !l.Explicit[flagName] {
		*target = true
	}
}

// ValidateParticipantName checks both the logical grammar and the role's name.
func ValidateParticipantName(role, name string) error {
	prefix := ""
	switch role {
	case ParticipantClient:
		prefix = "wefty://client/"
	case ParticipantPerson:
		prefix = "wefty://person/"
	case ParticipantNodeAgent:
		prefix = "wefty://node/"
	case ParticipantL1:
		prefix = "wefty://control-plane"
	case ParticipantL3:
		prefix = "wefty://run-ledger"
	default:
		return fmt.Errorf("connection profile: unknown participant %q", role)
	}
	if err := fabric.ValidateLogicalName(name); err != nil {
		return fmt.Errorf("connection profile: participant %s: %w", role, err)
	}
	if !strings.HasPrefix(name, prefix) || (!strings.HasSuffix(prefix, "/") && name != prefix) {
		return fmt.Errorf("connection profile: participant %s needs logical name %s", role, prefix)
	}
	return nil
}
