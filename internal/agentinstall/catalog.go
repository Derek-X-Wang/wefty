// Package agentinstall owns the private, node-local installation inventory for
// rolling node agents. It supplies no L1 execution authority. Callers serialize
// installation operations with the existing installation lock; catalog reads
// and writes also take a short catalog lock, never a retiree-lifetime lock.
package agentinstall

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/durable"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
)

const Directory = "agent-generations"
const catalogName = "catalog.json"

var ErrStartupSuppressed = errors.New("node agent generation startup suppressed")

type Binding struct {
	ProcessStartID string    `json:"process_start_id"`
	PID            int       `json:"pid"`
	BootSessionID  string    `json:"boot_session_id"`
	SystemBootID   string    `json:"system_boot_id"`
	ReadyAt        time.Time `json:"ready_at"`
}

// ResidentAttempt is the immutable execution identity returned by L1. It is
// diagnostic installation inventory; it grants no completion/renewal authority.
type ResidentAttempt struct {
	JobID               string `json:"job_id"`
	AttemptID           string `json:"attempt_id"`
	BootSessionID       string `json:"boot_session_id"`
	AuthorityGeneration int64  `json:"authority_generation"`
	FencingToken        string `json:"fencing_token"`
}

type Generation struct {
	PreparationOperation   string            `json:"preparation_operation,omitempty"`
	AbortOperation         string            `json:"abort_operation,omitempty"`
	CandidateBootSessionID string            `json:"candidate_boot_session_id"`
	ResidentAttempts       []ResidentAttempt `json:"resident_attempts"`
	ID                     string            `json:"agent_generation_id"`
	Role                   string            `json:"role,omitempty"`
	Version                string            `json:"binary_version"`
	Digest                 string            `json:"binary_sha256"`
	Label                  string            `json:"label"`
	Executable             string            `json:"executable"`
	Plist                  string            `json:"plist"`
	PlistDigest            string            `json:"plist_sha256,omitempty"`
	CreatedAt              time.Time         `json:"created_at"`
	PromotedAt             time.Time         `json:"promoted_at,omitempty"`
	RetiringAt             time.Time         `json:"retiring_at,omitempty"`
	StartBootID            string            `json:"start_system_boot_id"`
	ProtocolVersions       []int             `json:"retirement_protocol_versions"`
	Protocol               int               `json:"retirement_protocol,omitempty"`
	Process                *Binding          `json:"process,omitempty"`
}

type Catalog struct {
	Version        int                   `json:"version"`
	NodeID         string                `json:"node_id"`
	Revision       uint64                `json:"revision"`
	Primary        string                `json:"primary_generation_id"`
	Generations    map[string]Generation `json:"generations"`
	PendingReceipt string                `json:"pending_receipt,omitempty"`
}

// Store paths must be supplied by the installation's home-confined resolver.
// Files are derived from random IDs, never from receipt paths.
type Store struct {
	InitializationStep        func(string) error
	initializationSyncDir     func(string, *os.Root) error
	afterPromotionWrite       func(string) error
	beforeGenerationRemoval   func(string) error
	afterRemovalIdentityCheck func(string) error
	afterCandidateWrite       func(string) error
	// beforePlistOpen is a test seam after validation and before the retained plist open.
	beforePlistOpen func() error
	// ProcessStartIdentity observes kernel process birth identity; nil uses the platform reader.
	ProcessStartIdentity func(pid int) (string, error)
	Data                 string
	LaunchAgents         string
	Manager              launchagent.Manager
}

func (s Store) Root() string { return filepath.Join(s.Data, Directory) }
func validID(id string) bool {
	b, err := hex.DecodeString(id)
	return err == nil && len(b) == 16 && hex.EncodeToString(b) == id
}
func newID() (string, error) {
	var b [16]byte
	_, err := rand.Read(b[:])
	return hex.EncodeToString(b[:]), err
}
func (s Store) label(id string) string      { return "dev.wefty.node-agent.g-" + id }
func (s Store) executable(id string) string { return filepath.Join(s.Root(), id, "wefty-agent") }
func (s Store) ReadyPath(id string) string  { return filepath.Join(s.Root(), id, "ready.json") }
func (s Store) validate() error {
	for _, p := range []string{s.Data, s.LaunchAgents} {
		if !filepath.IsAbs(p) || filepath.Clean(p) != p || strings.ContainsAny(p, "\x00\r\n") {
			return &VerificationRefusal{Message: "generation installation paths must be absolute and clean"}
		}
		resolved, err := filepath.EvalSymlinks(p)
		if err != nil {
			return err
		}
		if resolved != p {
			return &VerificationRefusal{Message: "generation installation paths must not traverse symbolic links"}
		}
		info, err := os.Lstat(p)
		if err != nil {
			return err
		}
		if !info.IsDir() || !owned(info) || info.Mode().Perm()&0022 != 0 {
			return &VerificationRefusal{Message: "generation installation directories must be owner-controlled"}
		}
	}
	return nil
}
func privateInfo(info os.FileInfo, mode os.FileMode) bool {
	return owned(info) && info.Mode().Perm() == mode && ((mode == 0700 && info.IsDir()) || (mode != 0700 && info.Mode().IsRegular()))
}
func privateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !privateInfo(info, 0700) {
		return &VerificationRefusal{Message: fmt.Sprintf("generation directory must be owner-only and unlinked: %s", path)}
	}
	return nil
}
func (s Store) open(create bool) (*os.Root, func(), error) {
	if err := s.validate(); err != nil {
		return nil, nil, err
	}
	if create {
		if err := durable.MkdirAll(s.Data, Directory, 0700); err != nil {
			return nil, nil, err
		}
	}
	if err := privateDirectory(s.Root()); err != nil {
		return nil, nil, err
	}
	unlock, err := lockCatalog(s.Root())
	if err != nil {
		return nil, nil, err
	}
	root, err := os.OpenRoot(s.Root())
	if err != nil {
		unlock()
		return nil, nil, err
	}
	return root, func() { root.Close(); unlock() }, nil
}
func readPrivate(root *os.Root, name string, target any) error {
	parent := root
	if folder := filepath.Dir(name); folder != "." {
		before, err := root.Lstat(folder)
		if err != nil {
			return err
		}
		if !privateInfo(before, 0700) {
			return &VerificationRefusal{Message: "generation record parent must be a private unlinked directory"}
		}
		parent, err = root.OpenRoot(folder)
		if err != nil {
			return err
		}
		defer parent.Close()
		directory, err := parent.Open(".")
		if err != nil {
			return err
		}
		held, statErr := directory.Stat()
		directory.Close()
		if statErr != nil {
			return statErr
		}
		if !privateInfo(held, 0700) || !os.SameFile(before, held) {
			return &VerificationRefusal{Message: "generation record parent changed during open"}
		}
	}
	payload, err := ReadPrivateFile(parent, filepath.Base(name))
	if err != nil {
		return err
	}
	if err := json.Unmarshal(payload, target); err != nil {
		return fmt.Errorf("invalid generation record %s: %w", name, err)
	}
	return nil
}
func writePrivate(root *os.Root, name string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return durable.WriteFileMode(root, name, append(payload, '\n'), 0600)
}
func (s Store) read(root *os.Root) (Catalog, error) {
	var c Catalog
	if err := readPrivate(root, catalogName, &c); err != nil {
		return c, err
	}
	if c.Version != 1 || c.NodeID == "" || c.Revision == 0 || !validID(c.Primary) || len(c.Generations) == 0 {
		return c, &VerificationRefusal{Message: "invalid generation catalog"}
	}
	if c.PendingReceipt != "" && !validID(c.PendingReceipt) {
		return c, &VerificationRefusal{Message: "invalid pending generation receipt"}
	}
	primary := 0
	for id, g := range c.Generations {
		digest, err := hex.DecodeString(g.Digest)
		if !validID(id) || !validID(g.CandidateBootSessionID) || g.ID != id || g.Label != s.label(id) || g.Executable != s.executable(id) || g.Plist != filepath.Join(s.LaunchAgents, g.Label+".plist") || g.Version == "" || err != nil || len(digest) != 32 || g.CreatedAt.IsZero() || !hostboot.ValidIdentity(g.StartBootID) {
			return c, &VerificationRefusal{Message: "invalid generation ownership record"}
		}
		if g.Process != nil && (g.Process.PID <= 0 || g.Process.ProcessStartID == "" || g.Process.BootSessionID == "" || !hostboot.ValidIdentity(g.Process.SystemBootID) || g.Process.ReadyAt.IsZero()) {
			return c, &VerificationRefusal{Message: "invalid generation process binding"}
		}
		if g.PlistDigest != "" {
			digest, err := hex.DecodeString(g.PlistDigest)
			if err != nil || len(digest) != 32 {
				return c, &VerificationRefusal{Message: "invalid generation plist digest"}
			}
		}
		switch g.Role {
		case "primary":
			primary++
			if c.Primary != id {
				return c, &VerificationRefusal{Message: "conflicting generation primary"}
			}
		case "retiring", "":
		default:
			return c, &VerificationRefusal{Message: "invalid generation role"}
		}
		if len(g.ProtocolVersions) == 0 {
			return c, &VerificationRefusal{Message: "missing generation protocols"}
		}
		for _, v := range g.ProtocolVersions {
			if v <= 0 {
				return c, &VerificationRefusal{Message: "invalid generation protocol"}
			}
		}
	}
	if primary != 1 {
		return c, &VerificationRefusal{Message: "generation catalog requires exactly one primary"}
	}
	return c, nil
}
func (s Store) Read() (Catalog, error) {
	root, close, err := s.open(false)
	if err != nil {
		return Catalog{}, err
	}
	defer close()
	return s.read(root)
}

// RefuseMaintenance is intentionally conservative until role-aware maintenance
// is wired: even an idle recorded cohort must not fall into legacy unload or
// recursive purge/stale-stage deletion. Missing catalog storage means legacy.
func RefuseMaintenance(data, command string) error {
	_, err := os.Lstat(filepath.Join(data, Directory))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("%s refuses retained node agent generations: retiring generations may still hold work; finish retirement and use generation-aware maintenance", command)
}

// Initialize records the first generation after the caller has drained the
// legacy install. It never adopts a legacy process or its work.
func (s Store) Initialize(nodeID, source, version, boot string, protocols []int, operation ...string) (Generation, error) {
	if len(operation) > 0 && operation[0] != "" {
		return s.initializeOperation(nodeID, source, version, boot, protocols, operation[0])
	}
	root, close, err := s.open(true)
	if err != nil {
		return Generation{}, err
	}
	defer close()
	if s.InitializationStep != nil {
		if err := s.InitializationStep("root_created"); err != nil {
			return Generation{}, err
		}
	}
	if _, err := root.Lstat(catalogName); err == nil {
		return Generation{}, &VerificationRefusal{Message: "generation catalog already exists"}
	} else if !errors.Is(err, os.ErrNotExist) {
		return Generation{}, err
	}
	if nodeID == "" {
		return Generation{}, &VerificationRefusal{Message: "generation Node ID required"}
	}
	g, err := s.prepare(root, source, version, boot, protocols)
	if err != nil {
		return g, err
	}
	if s.InitializationStep != nil {
		if err := s.InitializationStep("executable_created"); err != nil {
			return g, err
		}
	}
	if len(operation) > 0 {
		g.PreparationOperation = operation[0]
	}
	g.Role = "primary"
	g.PromotedAt = time.Now().UTC()
	c := Catalog{Version: 1, NodeID: nodeID, Revision: 1, Primary: g.ID, Generations: map[string]Generation{g.ID: g}}
	return g, writePrivate(root, catalogName, c)
}
func (s Store) prepare(root *os.Root, source, version, boot string, protocols []int) (Generation, error) {
	if version == "" || !hostboot.ValidIdentity(boot) || len(protocols) == 0 {
		return Generation{}, &VerificationRefusal{Message: "generation requires version, system boot and protocols"}
	}
	for _, v := range protocols {
		if v <= 0 {
			return Generation{}, &VerificationRefusal{Message: "invalid retirement protocol"}
		}
	}
	info, err := os.Lstat(source)
	if err != nil {
		return Generation{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return Generation{}, &VerificationRefusal{Message: "candidate executable must be a regular executable"}
	}
	sourceParent, err := os.OpenRoot(filepath.Dir(source))
	if err != nil {
		return Generation{}, err
	}
	defer sourceParent.Close()
	binary, err := readOwnedFile(sourceParent, filepath.Base(source), info.Mode().Perm())
	if err != nil {
		return Generation{}, err
	}
	digest := sha256.Sum256(binary)
	id, err := newID()
	if err != nil {
		return Generation{}, err
	}
	if err := root.Mkdir(id, 0700); err != nil {
		return Generation{}, err
	}
	if err := durable.SyncDir(root); err != nil {
		return Generation{}, err
	}
	dir, err := root.OpenRoot(id)
	if err != nil {
		return Generation{}, err
	}
	defer dir.Close()
	if err := durable.CreateFile(dir, "wefty-agent", binary, 0500); err != nil {
		return Generation{}, err
	}
	candidateBoot, err := newID()
	if err != nil {
		return Generation{}, err
	}
	g := Generation{CandidateBootSessionID: candidateBoot, ID: id, Version: version, Digest: hex.EncodeToString(digest[:]), Label: s.label(id), Executable: s.executable(id), CreatedAt: time.Now().UTC(), StartBootID: boot, ProtocolVersions: slices.Clone(protocols)}
	g.Plist = filepath.Join(s.LaunchAgents, g.Label+".plist")
	return g, nil
}
func (s Store) PrepareCandidate(source, version, boot string, protocols []int, operation ...string) (Generation, error) {
	root, close, err := s.open(false)
	if err != nil {
		return Generation{}, err
	}
	defer close()
	c, err := s.read(root)
	if err != nil {
		return Generation{}, err
	}
	if c.PendingReceipt != "" {
		return Generation{}, &VerificationRefusal{Message: "unresolved generation promotion"}
	}
	if len(operation) > 0 && operation[0] != "" {
		for _, g := range c.Generations {
			if g.PreparationOperation == operation[0] {
				if g.Version != version || !hostboot.SameBoot(g.StartBootID, boot) || !slices.Equal(g.ProtocolVersions, protocols) || g.AbortOperation != "" {
					return Generation{}, &VerificationRefusal{Message: "candidate preparation operation changed"}
				}
				if err := s.verifyExecutable(root, g); err != nil {
					return Generation{}, err
				}
				info, err := os.Lstat(source)
				if err != nil {
					return Generation{}, err
				}
				sourceParent, err := os.OpenRoot(filepath.Dir(source))
				if err != nil {
					return Generation{}, err
				}
				binary, err := readOwnedFile(sourceParent, filepath.Base(source), info.Mode().Perm())
				sourceParent.Close()
				if err != nil {
					return Generation{}, err
				}
				if digestBytes(binary) != g.Digest {
					return Generation{}, &VerificationRefusal{Message: "candidate preparation payload changed"}
				}
				return g, nil
			}
		}
	}
	g, err := s.prepare(root, source, version, boot, protocols)
	if err != nil {
		return g, err
	}
	if len(operation) > 0 {
		g.PreparationOperation = operation[0]
	}
	c.Generations[g.ID] = g
	c.Revision++
	return g, writePrivate(root, catalogName, c)
}
func (s Store) verifyExecutable(root *os.Root, g Generation) error {
	if err := privateDirectory(filepath.Join(s.Root(), g.ID)); err != nil {
		return err
	}
	dir, err := root.OpenRoot(g.ID)
	if err != nil {
		return err
	}
	defer dir.Close()
	binary, err := readOwnedFile(dir, "wefty-agent", 0500)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(binary)
	if hex.EncodeToString(digest[:]) != g.Digest {
		return &VerificationRefusal{Message: "retained executable digest changed"}
	}
	return nil
}
