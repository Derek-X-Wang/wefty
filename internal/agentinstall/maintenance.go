package agentinstall

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/durable"
	"github.com/Derek-X-Wang/wefty/l1"
)

const MaintenanceFile = "agent-maintenance.json"
const deEnrollmentFile = "de-enrollment.json"

// PreservationFile records a data-retention policy, not registration authority.
// It outlives removal journals so a later purge cannot reinterpret shared data.
const PreservationFile = "agent-generation-preservation.json"

type generationPreservation struct {
	Version   int    `json:"version"`
	NodeID    string `json:"node_id"`
	Primary   string `json:"primary"`
	Operation string `json:"operation"`
}

func (s Store) HasPreservedGenerationData() (bool, error) {
	root, err := os.OpenRoot(s.Data)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer root.Close()
	var record generationPreservation
	if err = readPrivate(root, PreservationFile, &record); errors.Is(err, os.ErrNotExist) {
		return s.hasArchivedGenerationData(root)
	}
	if err != nil {
		return false, err
	}
	if record.Version != 1 || record.NodeID == "" || !validID(record.Primary) || record.Operation == "" {
		return false, errors.New("invalid generation data preservation record")
	}
	return true, nil
}

// Earlier removals left a validated catalog snapshot but no policy marker.
// That durable provenance is sufficient to preserve data; it grants no authority.
func (s Store) hasArchivedGenerationData(parent *os.Root) (bool, error) {
	directory, err := parent.Open(".")
	if err != nil {
		return false, err
	}
	entries, err := directory.ReadDir(-1)
	directory.Close()
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		id, ok := strings.CutPrefix(entry.Name(), "agent-generation-history-")
		if !ok || !validID(id) {
			continue
		}
		preserved, err := s.archivedGenerationData(parent, entry.Name(), id)
		if err != nil || preserved {
			return preserved, err
		}
	}
	return false, nil
}
func (s Store) archivedGenerationData(parent *os.Root, name, id string) (bool, error) {
	held, err := holdGenerationDirectory(parent, name)
	if err != nil {
		return false, err
	}
	defer held.Close()
	catalog, err := s.read(held.root)
	if err != nil {
		return false, err
	}
	if catalog.Primary != id || quietCatalog(catalog) != nil {
		return false, errors.New("generation history snapshot differs from archive identity")
	}
	return true, nil
}

func (s Store) PreserveGenerationData(m *Maintenance) error {
	if preserved, err := s.HasPreservedGenerationData(); err != nil || preserved {
		return err
	}
	root, err := os.OpenRoot(s.Data)
	if err != nil {
		return err
	}
	defer root.Close()
	record := generationPreservation{Version: 1, NodeID: m.Catalog.NodeID, Primary: m.Catalog.Primary, Operation: m.Operation}
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return durable.CreateFile(root, PreservationFile, append(payload, '\n'), 0600)
}

type MaintenanceRefusal struct{ Message string }

func (e *MaintenanceRefusal) Error() string { return e.Message }

// Maintenance holds exact cleanup intent outside the catalog being retired.
// Archive is derived, never accepted as a path from a receipt.
type Maintenance struct {
	Version        int                               `json:"version"`
	Operation      string                            `json:"operation"`
	Kind           string                            `json:"kind"`
	Catalog        Catalog                           `json:"catalog"`
	Device         uint64                            `json:"device"`
	Inode          uint64                            `json:"inode"`
	Files          map[string][]byte                 `json:"files,omitempty"`
	Plist          []byte                            `json:"plist,omitempty"`
	Cleaning       bool                              `json:"cleaning,omitempty"`
	Retired        bool                              `json:"retired,omitempty"`
	Restoring      bool                              `json:"restoring,omitempty"`
	ServiceSettled bool                              `json:"service_settled,omitempty"`
	ServiceResumed bool                              `json:"service_resumed,omitempty"`
	Admission      json.RawMessage                   `json:"admission,omitempty"`
	Request        *l1.DeEnrollNodeGenerationRequest `json:"de_enrollment_request,omitempty"`
	Result         *l1.NodeGenerationInventory       `json:"de_enrollment_result,omitempty"`
}

func (m *Maintenance) Archive() string { return "agent-generation-history-" + m.Catalog.Primary }

// CheckMaintenance keeps the validated catalog and receipt observation under
// the same catalog lock. It makes no files and issues no manager calls.
func (s Store) CheckMaintenance(check func(Catalog) error) (*Catalog, error) {
	if _, err := os.Lstat(s.Root()); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	root, close, err := s.open(false)
	if err != nil {
		return nil, err
	}
	defer close()
	c, err := s.read(root)
	if err != nil {
		return nil, err
	}
	if err = quietCatalog(c); err != nil {
		return nil, err
	}
	primary := c.Generations[c.Primary]
	if primary.Process == nil {
		return nil, &MaintenanceRefusal{Message: "primary lacks a registered process binding; let rolling recovery complete with `wefty upgrade --rolling`"}
	}
	// Finished audit receipts are the positive closure evidence left after the
	// primary already checked fresh L1 closure and removed a retired generation.
	entries, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	names, err := entries.ReadDir(-1)
	entries.Close()
	if err != nil {
		return nil, err
	}
	for _, entry := range names {
		if !strings.HasPrefix(entry.Name(), "finished-") {
			continue
		}
		id := strings.TrimSuffix(strings.TrimPrefix(entry.Name(), "finished-"), ".json")
		if !validID(id) {
			return nil, &MaintenanceRefusal{Message: "generation closure audit identity is unverified; let rolling recovery complete with `wefty upgrade --rolling`"}
		}
		var r completionReceipt
		if err = readPrivate(root, entry.Name(), &r); err != nil {
			return nil, err
		}
		if r.Version != 1 || r.Generation.ID != id || r.Generation.Role != "retiring" || r.Generation.Process == nil || r.Proof.Binding != *r.Generation.Process || r.Proof.CatalogRevision == 0 || r.Proof.GenerationID != id || !r.Proof.PayloadsAbsent || !r.Proof.L1SessionClosed || !r.Proof.EvidenceSafe || r.Proof.EvidenceDigest == "" {
			return nil, &MaintenanceRefusal{Message: "closed generation " + id + " lacks positive closure evidence; let rolling recovery complete with `wefty upgrade --rolling`"}
		}
		var closed l1.NodeGeneration
		b := r.ControlFiles["retirement-closed.json"]
		if len(b) > 0 {
			if err = json.Unmarshal(b, &closed); err != nil {
				return nil, fmt.Errorf("decode closed generation %s: %w", id, err)
			}
		}
		// CleanupClosed binds the canonical L1 value, not the trailing newline
		// retained by writePrivate. Keep the original audit bytes untouched.
		canonical, err := json.Marshal(closed)
		if err != nil {
			return nil, err
		}
		if len(b) == 0 || closed.Role != l1.NodeGenerationClosed || closed.ClosedAt == nil || closed.PromotionOperationID == "" || !slices.Equal(asL1Residents(r.Generation.ResidentAttempts), closed.ResidentAttempts) || closed.AgentGenerationID != id || closed.BootSessionID != r.Proof.BootSessionID || digestBytes(canonical) != r.Proof.EvidenceDigest {
			return nil, &MaintenanceRefusal{Message: "closed generation " + id + " lacks exact L1 closure evidence; let rolling recovery complete with `wefty upgrade --rolling`"}
		}
	}
	if check != nil {
		if err = check(c); err != nil {
			return nil, err
		}
	}
	return &c, nil
}
func quietCatalog(c Catalog) error {
	retiring, staged := 0, 0
	for _, g := range c.Generations {
		if g.Role == "retiring" {
			retiring++
		}
		if g.Role == "" {
			staged++
		}
	}
	if retiring > 0 {
		return &MaintenanceRefusal{Message: fmt.Sprintf("wait for %d retiring generations to finish; then retry maintenance", retiring)}
	}
	if c.PendingReceipt != "" || staged > 0 {
		return &MaintenanceRefusal{Message: "pending or staged generation remains; let the rolling recovery complete with `wefty upgrade --rolling`"}
	}
	if len(c.Generations) != 1 {
		return &MaintenanceRefusal{Message: "maintenance requires exactly one primary; let the rolling recovery complete with `wefty upgrade --rolling`"}
	}
	return nil
}
func (s Store) ReadMaintenance() (*Maintenance, error) {
	root, err := os.OpenRoot(s.Data)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer root.Close()
	var m Maintenance
	err = readPrivate(root, MaintenanceFile, &m)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if m.Version != 1 || m.Operation == "" || !slices.Contains([]string{"remove", "convert"}, m.Kind) || !validID(m.Catalog.Primary) || m.Device == 0 || m.Inode == 0 || quietCatalog(m.Catalog) != nil {
		return nil, &MaintenanceRefusal{Message: "invalid maintenance intent; retain installation evidence and repair the receipt"}
	}
	// A retry validates the live or archived catalog under that same inode's
	// catalog lock. The outside receipt cannot substitute an invented catalog.
	name := Directory
	if _, e := root.Lstat(name); errors.Is(e, os.ErrNotExist) {
		name = m.Archive()
	} else if e != nil {
		return nil, e
	}
	info, e := root.Lstat(name)
	if e != nil {
		return nil, e
	}
	if !privateInfo(info, 0700) {
		return nil, &MaintenanceRefusal{Message: "maintenance catalog is no longer a private directory"}
	}
	unlock, e := lockCatalog(filepath.Join(s.Data, name))
	if e != nil {
		return nil, e
	}
	defer unlock()
	catalogRoot, e := root.OpenRoot(name)
	if e != nil {
		return nil, e
	}
	defer catalogRoot.Close()
	dev, ino, e := maintenanceIdentity(catalogRoot)
	if e != nil {
		return nil, e
	}
	if dev != m.Device || ino != m.Inode {
		return nil, &MaintenanceRefusal{Message: "maintenance catalog inode changed"}
	}
	current, e := s.read(catalogRoot)
	if e != nil {
		return nil, e
	}
	if e = quietCatalog(current); e != nil {
		return nil, e
	}
	if !reflect.DeepEqual(current, m.Catalog) {
		previous, next := m.Catalog.Generations[m.Catalog.Primary], current.Generations[current.Primary]
		canRefreshBinding := m.Kind == "convert"
		if m.Kind == "remove" && m.Result == nil {
			// An unresolved issued request retains its durable attestation.
			// Only definitive rejection removes it, so absence permits
			// precommit restoration rather than replaying a dead boot.
			var attestation DeEnrollmentRecord
			readErr := readPrivate(catalogRoot, deEnrollmentFile, &attestation)
			if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
				return nil, readErr
			}
			canRefreshBinding = errors.Is(readErr, os.ErrNotExist)
		}
		// Only the process incarnation and monotone catalog revision may change.
		expected := m.Catalog
		expected.Generations = map[string]Generation{previous.ID: previous}
		previous.Process = next.Process
		expected.Generations[previous.ID] = previous
		expected.Revision = current.Revision
		if !canRefreshBinding || m.Cleaning || current.Revision < m.Catalog.Revision || next.Process == nil || !reflect.DeepEqual(current, expected) {
			return nil, &MaintenanceRefusal{Message: "maintenance catalog differs from recorded intent"}
		}
		if m.Kind == "remove" {
			bounded, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, readyErr := s.ready(bounded, catalogRoot, current, next, *next.Process, next.CreatedAt)
			cancel()
			if readyErr != nil {
				return nil, readyErr
			}
			m.Request = nil
			m.Restoring = len(m.Admission) != 0
		}
		m.Catalog = current
	}
	g := m.Catalog.Generations[m.Catalog.Primary]
	if g.ID != m.Catalog.Primary || g.Executable != s.executable(g.ID) || g.Label != s.label(g.ID) || g.Plist != filepath.Join(s.LaunchAgents, g.Label+".plist") {
		return nil, &MaintenanceRefusal{Message: "maintenance generation ownership differs"}
	}
	if m.Request != nil {
		r := m.Request
		if m.Kind != "remove" || g.Process == nil || r.OperationID != m.Operation || r.PrimaryGenerationID != g.ID || r.PrimaryBootSessionID != g.Process.BootSessionID || r.IntentReason != "wefty maintenance "+m.Operation || r.AuthorityGeneration <= 0 {
			return nil, &MaintenanceRefusal{Message: "maintenance de-enrollment request changed"}
		}
		if m.Result != nil {
			if m.Result.NodeID != m.Catalog.NodeID || m.Result.CatalogRevision != r.CatalogRevision+1 {
				return nil, &MaintenanceRefusal{Message: "maintenance de-enrollment result changed"}
			}
			found := false
			for _, closed := range m.Result.Generations {
				if closed.Role != l1.NodeGenerationClosed || closed.ClosedAt == nil {
					return nil, &MaintenanceRefusal{Message: "maintenance result retains live generations"}
				}
				found = found || closed.AgentGenerationID == g.ID && closed.BootSessionID == g.Process.BootSessionID
			}
			if !found {
				return nil, &MaintenanceRefusal{Message: "maintenance result omits the recorded primary"}
			}
		}
	} else if m.Result != nil {
		return nil, &MaintenanceRefusal{Message: "maintenance result lacks its exact request"}
	}
	return &m, nil
}
func (s Store) SaveMaintenance(m *Maintenance) error {
	root, err := os.OpenRoot(s.Data)
	if err != nil {
		return err
	}
	defer root.Close()
	var previous Maintenance
	readErr := readPrivate(root, MaintenanceFile, &previous)
	if errors.Is(readErr, os.ErrNotExist) {
		payload, e := json.Marshal(m)
		if e != nil {
			return e
		}
		if e = durable.CreateFile(root, MaintenanceFile, append(payload, '\n'), 0600); e != nil {
			return e
		}
	} else {
		if readErr != nil {
			return readErr
		}
		if previous.Operation != m.Operation || previous.Kind != m.Kind || previous.Device != m.Device || previous.Inode != m.Inode || previous.Catalog.Primary != m.Catalog.Primary || previous.Result != nil && m.Result == nil || previous.Retired && !m.Retired || previous.Cleaning && !m.Cleaning {
			return &MaintenanceRefusal{Message: "maintenance intent changed; existing record preserved"}
		}
		publishingRequest := previous.Request == nil && m.Request != nil
		if publishingRequest && s.MaintenanceStep != nil {
			if err = s.MaintenanceStep("request_write:before"); err != nil {
				return err
			}
		}
		if err = writePrivate(root, MaintenanceFile, m); err != nil {
			return err
		}
		if publishingRequest && s.MaintenanceStep != nil {
			if err = s.MaintenanceStep("request_write:after"); err != nil {
				return err
			}
		}
	}
	if m.Retired {
		entry, err := root.Lstat(m.Archive())
		if err != nil {
			return err
		}
		if !privateInfo(entry, 0700) {
			return &MaintenanceRefusal{Message: "maintenance archive must be an unlinked private directory"}
		}
		archive, err := root.OpenRoot(m.Archive())
		if err != nil {
			return err
		}
		defer archive.Close()
		dev, ino, err := maintenanceIdentity(archive)
		if err != nil {
			return err
		}
		if dev != m.Device || ino != m.Inode {
			return &MaintenanceRefusal{Message: "maintenance archive identity changed"}
		}
		name := "maintenance-" + digestBytes([]byte(m.Operation)) + ".json"
		payload, e := json.Marshal(m)
		if e != nil {
			return e
		}
		payload = append(payload, '\n')
		if existing, e := ReadPrivateFile(archive, name); e == nil {
			if !slices.Equal(existing, payload) {
				return &MaintenanceRefusal{Message: "maintenance audit name already holds different evidence; file preserved"}
			}
			return nil
		} else if !errors.Is(e, os.ErrNotExist) {
			return e
		}
		return durable.CreateFile(archive, name, payload, 0600)
	}
	return nil
}
func (s Store) BeginMaintenance(kind, operation string, c Catalog) (*Maintenance, error) {
	if prior, err := s.ReadMaintenance(); err != nil || prior != nil {
		return prior, err
	}
	root, close, err := s.open(false)
	if err != nil {
		return nil, err
	}
	defer close()
	current, err := s.read(root)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(current, c) {
		return nil, &MaintenanceRefusal{Message: "generation catalog changed before maintenance"}
	}
	if err = quietCatalog(c); err != nil {
		return nil, err
	}
	device, inode, err := maintenanceIdentity(root)
	if err != nil {
		return nil, err
	}
	m := &Maintenance{Version: 1, Kind: kind, Operation: operation, Catalog: c, Device: device, Inode: inode}
	return m, s.SaveMaintenance(m)
}
func (s Store) FinishMaintenance() error {
	// Never unlink an unreadable, replaced or unrecognized journal merely
	// because its canonical name is known.
	current, err := s.ReadMaintenance()
	if err != nil {
		return err
	}
	if current == nil {
		return nil
	}
	parent, err := os.OpenRoot(s.Data)
	if err != nil {
		return err
	}
	defer parent.Close()
	if err = parent.Remove(MaintenanceFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return durable.SyncDir(parent)
}

// RetireMaintenance deletes only receipt-bound, descriptor-read installation
// files after confirmed unload and lifetime-lock release. Unknown files and
// audit evidence are retained in the derived history directory, never recursed.
func (s Store) RetireMaintenance(ctx context.Context, m *Maintenance) ([]string, error) {
	parent, err := os.OpenRoot(s.Data)
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	path := Directory
	if _, err = parent.Lstat(path); errors.Is(err, os.ErrNotExist) {
		path = m.Archive()
	} else if err != nil {
		return nil, err
	}
	entry, err := parent.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !privateInfo(entry, 0700) {
		return nil, &MaintenanceRefusal{Message: "maintenance catalog must remain an unlinked private directory"}
	}
	root, err := parent.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	dev, ino, err := maintenanceIdentity(root)
	if err != nil {
		return nil, err
	}
	if dev != m.Device || ino != m.Inode {
		return nil, &MaintenanceRefusal{Message: "maintenance catalog directory identity changed"}
	}
	if path == m.Archive() {
		if err = s.archiveInitialization(parent, root, m); err != nil {
			return nil, err
		}
		m.Retired = true
		return []string{filepath.Join(s.Data, path)}, s.SaveMaintenance(m)
	}
	unlock, err := lockCatalog(s.Root())
	if err != nil {
		return nil, err
	}
	defer unlock()
	current, err := s.read(root)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(current, m.Catalog) {
		return nil, &MaintenanceRefusal{Message: "catalog changed during maintenance cleanup"}
	}
	g := current.Generations[current.Primary]
	state, err := s.Manager.State(ctx, g.Label)
	if err != nil {
		return nil, err
	}
	if state.Loaded {
		return nil, &MaintenanceRefusal{Message: "recorded primary is still loaded; resume maintenance before cleanup"}
	}
	held, err := holdGenerationDirectory(root, g.ID)
	if err != nil {
		return nil, err
	}
	defer held.Close()
	folder, err := os.OpenRoot(s.LaunchAgents)
	if err != nil {
		return nil, err
	}
	defer folder.Close()
	files := []string{"wefty-agent", "ready.json", "candidate-start.json", "control-request.json", "control-result.json", "control-refusal.json", "retirement-ack.json", "retirement-closed.json"}
	if !m.Cleaning {
		m.Files = map[string][]byte{}
		for _, name := range files {
			mode := os.FileMode(0600)
			if name == "wefty-agent" {
				mode = 0500
			}
			b, e := readOwnedFile(held.root, name, mode)
			if errors.Is(e, os.ErrNotExist) {
				continue
			}
			if e != nil {
				return nil, e
			}
			if name == "wefty-agent" && digestBytes(b) != g.Digest {
				return nil, &MaintenanceRefusal{Message: "primary executable changed before cleanup"}
			}
			if name == "wefty-agent" {
				m.Files[name] = nil
			} else {
				m.Files[name] = b
			}
		}
		m.Plist, err = readRetainedPlist(folder, filepath.Base(g.Plist))
		if err != nil {
			return nil, err
		}
		if digestBytes(m.Plist) != g.PlistDigest {
			return nil, &MaintenanceRefusal{Message: "primary plist changed before cleanup"}
		}
		m.Cleaning = true
		if err = s.SaveMaintenance(m); err != nil {
			return nil, err
		}
	}
	for _, name := range files {
		mode := os.FileMode(0600)
		if name == "wefty-agent" {
			mode = 0500
		}
		b, e := readOwnedFile(held.root, name, mode)
		if errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil {
			return nil, e
		}
		expected, ok := m.Files[name]
		if !ok || name == "wefty-agent" && digestBytes(b) != g.Digest || name != "wefty-agent" && !slices.Equal(b, expected) {
			return nil, &MaintenanceRefusal{Message: "generation file changed after cleanup receipt: " + name}
		}
		if err = held.remove(root, g.ID, name); err != nil {
			return nil, err
		}
		if s.MaintenanceStep != nil {
			if err = s.MaintenanceStep("file_removed:" + name); err != nil {
				return nil, err
			}
		}
	}
	if err = durable.SyncDir(held.root); err != nil {
		return nil, err
	}
	if err = held.verify(root, g.ID); err != nil {
		return nil, err
	}
	if b, e := readRetainedPlist(folder, filepath.Base(g.Plist)); e == nil {
		if !slices.Equal(b, m.Plist) {
			return nil, &MaintenanceRefusal{Message: "primary plist changed after cleanup receipt"}
		}
		if err = folder.Remove(filepath.Base(g.Plist)); err != nil {
			return nil, err
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return nil, e
	}
	if err = durable.SyncDir(folder); err != nil {
		return nil, err
	}
	if err = durable.SyncDir(root); err != nil {
		return nil, err
	}
	if s.MaintenanceStep != nil {
		if err = s.MaintenanceStep("files_removed"); err != nil {
			return nil, err
		}
	}
	// Record the destination before the atomic, no-replace rename. Nothing in
	// the history directory can autostart; only LaunchAgents are definitions.
	if err = renameMaintenance(parent, Directory, m.Archive(), m.Device, m.Inode); err != nil {
		return nil, err
	}
	if err = durable.SyncDir(parent); err != nil {
		return nil, err
	}
	if s.MaintenanceStep != nil {
		if err = s.MaintenanceStep("catalog_retired"); err != nil {
			return nil, err
		}
	}
	if err = s.archiveInitialization(parent, root, m); err != nil {
		return nil, err
	}
	m.Retired = true
	return []string{filepath.Join(s.Data, m.Archive())}, s.SaveMaintenance(m)
}

type DeEnrollmentRecord struct {
	NodeID  string                           `json:"node_id"`
	Request l1.DeEnrollNodeGenerationRequest `json:"request"`
	Result  *l1.NodeGenerationInventory      `json:"result,omitempty"`
}

func (s Store) ReadDeEnrollment() (*DeEnrollmentRecord, error) {
	root, close, err := s.open(false)
	if err != nil {
		return nil, err
	}
	defer close()
	var r DeEnrollmentRecord
	err = readPrivate(root, deEnrollmentFile, &r)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	c, err := s.read(root)
	if err != nil {
		return nil, err
	}
	if r.NodeID != c.NodeID || r.Request.PrimaryGenerationID != c.Primary || r.Request.OperationID == "" || !r.Request.RuntimesReaped || !r.Request.EvidenceSettled {
		return nil, &MaintenanceRefusal{Message: "de-enrollment intent differs from catalog"}
	}
	if r.Result != nil {
		if r.Result.NodeID != r.NodeID || r.Result.CatalogRevision != r.Request.CatalogRevision+1 {
			return nil, &MaintenanceRefusal{Message: "de-enrollment result identity differs"}
		}
		found := false
		for _, closed := range r.Result.Generations {
			if closed.Role != l1.NodeGenerationClosed || closed.ClosedAt == nil {
				return nil, &MaintenanceRefusal{Message: "de-enrollment result retains a live generation"}
			}
			found = found || closed.AgentGenerationID == r.Request.PrimaryGenerationID && closed.BootSessionID == r.Request.PrimaryBootSessionID
		}
		if !found {
			return nil, &MaintenanceRefusal{Message: "de-enrollment result omits primary closure"}
		}
	}
	return &r, nil
}
func (s Store) PrepareDeEnrollment(node string, r l1.DeEnrollNodeGenerationRequest) error {
	root, close, err := s.open(false)
	if err != nil {
		return err
	}
	defer close()
	c, err := s.read(root)
	if err != nil {
		return err
	}
	if err = quietCatalog(c); err != nil {
		return err
	}
	if node != c.NodeID || r.PrimaryGenerationID != c.Primary || c.Generations[c.Primary].Process == nil || r.PrimaryBootSessionID != c.Generations[c.Primary].Process.BootSessionID || !r.RuntimesReaped || !r.EvidenceSettled {
		return &MaintenanceRefusal{Message: "de-enrollment local primary changed"}
	}
	var previous DeEnrollmentRecord
	if err = readPrivate(root, deEnrollmentFile, &previous); err == nil {
		if previous.NodeID != node || previous.Request != r {
			return &MaintenanceRefusal{Message: "de-enrollment operation payload changed"}
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return writePrivate(root, deEnrollmentFile, DeEnrollmentRecord{NodeID: node, Request: r})
}
func (s Store) CompleteDeEnrollment(in l1.NodeGenerationInventory) error {
	root, close, err := s.open(false)
	if err != nil {
		return err
	}
	defer close()
	var r DeEnrollmentRecord
	if err = readPrivate(root, deEnrollmentFile, &r); err != nil {
		return err
	}
	closed := false
	for _, g := range in.Generations {
		if g.Role != l1.NodeGenerationClosed || g.ClosedAt == nil {
			return &MaintenanceRefusal{Message: "de-enrollment result still has live generations"}
		}
		closed = closed || g.AgentGenerationID == r.Request.PrimaryGenerationID && g.BootSessionID == r.Request.PrimaryBootSessionID
	}
	if in.NodeID != r.NodeID || in.CatalogRevision != r.Request.CatalogRevision+1 || !closed {
		return &MaintenanceRefusal{Message: "de-enrollment result identity differs"}
	}
	if r.Result != nil && !reflect.DeepEqual(*r.Result, in) {
		return &MaintenanceRefusal{Message: "de-enrollment result changed"}
	}
	r.Result = &in
	return writePrivate(root, deEnrollmentFile, r)
}

// StopMaintenancePrimary uses legacy graceful bootout semantics for the exact
// recorded primary, with restart fenced before any unload attempt.
func (s Store) StopMaintenancePrimary(ctx context.Context, m *Maintenance) error {
	root, close, err := s.open(false)
	if err != nil {
		return err
	}
	defer close()
	c, err := s.read(root)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(c, m.Catalog) {
		old, current := m.Catalog.Generations[m.Catalog.Primary], c.Generations[c.Primary]
		if m.Kind != "convert" || c.Primary != m.Catalog.Primary || old.Digest != current.Digest || old.PlistDigest != current.PlistDigest {
			return &MaintenanceRefusal{Message: "primary changed before maintenance stop"}
		}
		m.Catalog = c
		if err = s.SaveMaintenance(m); err != nil {
			return err
		}
	}
	if err = quietCatalog(c); err != nil {
		return err
	}
	g := c.Generations[c.Primary]
	return s.Manager.UnloadMaintenancePrimary(ctx, g.Label)
}
func (s Store) RestoreMaintenancePrimary(ctx context.Context, m *Maintenance) error {
	root, close, err := s.open(false)
	if err != nil {
		return err
	}
	defer close()
	c, err := s.read(root)
	if err != nil {
		return err
	}
	if c.Primary != m.Catalog.Primary || c.PendingReceipt != "" {
		return &MaintenanceRefusal{Message: "primary changed before maintenance restoration"}
	}
	g := c.Generations[c.Primary]
	if err = s.verifyExecutable(root, g); err != nil {
		return err
	}
	folder, err := os.OpenRoot(s.LaunchAgents)
	if err != nil {
		return err
	}
	defer folder.Close()
	b, err := readRetainedPlist(folder, filepath.Base(g.Plist))
	if err != nil {
		return err
	}
	if digestBytes(b) != g.PlistDigest {
		return &MaintenanceRefusal{Message: "primary plist changed before restoration"}
	}
	return s.Manager.RestoreMaintenancePrimary(ctx, folder, g.Plist, g.Label, b)
}

func (s Store) archiveInitialization(parent, archive *os.Root, m *Maintenance) error {
	var in initializationIntent
	err := readPrivate(parent, initializationFile, &in)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if in.Version != 1 || in.NodeID != m.Catalog.NodeID || !validID(in.Generation.ID) {
		return &MaintenanceRefusal{Message: "initialization ownership differs; intent preserved"}
	}
	if in.Generation.ID != m.Catalog.Primary {
		var receipt completionReceipt
		if err = readPrivate(archive, "finished-"+in.Generation.ID+".json", &receipt); err != nil {
			return err
		}
		if receipt.Generation.ID != in.Generation.ID || !receipt.Proof.L1SessionClosed {
			return &MaintenanceRefusal{Message: "initialization generation lacks closure evidence"}
		}
	}
	// Both entries are receipt-derived and beneath held descriptors. Rename
	// preserves the entire intent and quarantine roster; it deletes no evidence.
	if err = moveMaintenanceRecord(parent, archive, initializationFile); err != nil {
		return err
	}
	return errors.Join(durable.SyncDir(archive), durable.SyncDir(parent))
}

// Rejections are immutable, exact-request outcomes. Publish one before removing
// the active attestation, so CLI recovery can distinguish rejection from a lost
// reply even if the primary restarts between the two durable steps.
func deEnrollmentRejectionFile(request l1.DeEnrollNodeGenerationRequest) string {
	payload, _ := json.Marshal(request)
	return "de-enrollment-rejected-" + strings.TrimPrefix(digestBytes(payload), "sha256:") + ".json"
}
func rejectedDeEnrollment(root *os.Root, request l1.DeEnrollNodeGenerationRequest) (bool, error) {
	var record DeEnrollmentRecord
	err := readPrivate(root, deEnrollmentRejectionFile(request), &record)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if record.NodeID == "" || record.Request != request || record.Result != nil {
		return false, &MaintenanceRefusal{Message: "de-enrollment rejection payload differs"}
	}
	return true, nil
}
func (s Store) DeEnrollmentRejected(request l1.DeEnrollNodeGenerationRequest) (bool, error) {
	root, close, err := s.open(false)
	if err != nil {
		return false, err
	}
	defer close()
	request.RuntimesReaped, request.EvidenceSettled = true, true
	rejected, err := rejectedDeEnrollment(root, request)
	if err != nil || !rejected {
		return rejected, err
	}
	c, err := s.read(root)
	if err != nil {
		return false, err
	}
	var record DeEnrollmentRecord
	if err = readPrivate(root, deEnrollmentRejectionFile(request), &record); err != nil {
		return false, err
	}
	if record.NodeID != c.NodeID || record.Request.PrimaryGenerationID != c.Primary {
		return false, &MaintenanceRefusal{Message: "de-enrollment rejection catalog differs"}
	}
	return true, nil
}

// AbortDeEnrollment accepts only an unambiguous L1 refusal supplied by the
// primary adapter. A successful or ambiguous operation is never undone.
func (s Store) AbortDeEnrollment(request l1.DeEnrollNodeGenerationRequest) error {
	request.RuntimesReaped, request.EvidenceSettled = true, true
	root, close, err := s.open(false)
	if err != nil {
		return err
	}
	defer close()
	var r DeEnrollmentRecord
	if err = readPrivate(root, deEnrollmentFile, &r); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if rejected, e := rejectedDeEnrollment(root, request); e != nil {
				return e
			} else if rejected {
				return nil
			}
		}
		return err
	}
	if r.Request != request || r.Result != nil {
		return &MaintenanceRefusal{Message: "cannot discard changed or committed de-enrollment"}
	}
	if rejected, e := rejectedDeEnrollment(root, request); e != nil {
		return e
	} else if !rejected {
		payload, e := json.Marshal(r)
		if e != nil {
			return e
		}
		if e = durable.CreateFile(root, deEnrollmentRejectionFile(request), append(payload, '\n'), 0600); e != nil {
			return e
		}
	}
	if err = root.Remove(deEnrollmentFile); err != nil {
		return err
	}
	return durable.SyncDir(root)
}
