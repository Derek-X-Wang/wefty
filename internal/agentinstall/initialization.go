package agentinstall

import (
	"encoding/json"
	"errors"
	"github.com/Derek-X-Wang/wefty/internal/durable"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
	"os"
	"path/filepath"
	"slices"
	"time"
)

const initializationFile = "agent-generation-initialization.json"

type initializationIntent struct {
	Version     int                        `json:"version"`
	NodeID      string                     `json:"node_id"`
	Operation   string                     `json:"operation_id"`
	Generation  Generation                 `json:"generation"`
	Quarantines []initializationQuarantine `json:"quarantines,omitempty"`
}

func (s Store) HasInitializationIntent(node, operation string) (bool, error) {
	parent, err := os.OpenRoot(s.Data)
	if err != nil {
		return false, err
	}
	defer parent.Close()
	var in initializationIntent
	err = readPrivate(parent, initializationFile, &in)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if in.Version != 1 || in.NodeID != node || in.Operation != operation || in.Generation.PreparationOperation != operation {
		return false, &VerificationRefusal{Message: "initialization operation ownership differs"}
	}
	return true, nil
}

// The intent is outside the generation root and durable before its first mkdir.
// It binds one exact ID and executable. Ambiguous scratch goes into recorded
// custody; unrelated retained generation storage refuses.
func (s Store) initializeOperation(node, source, version, boot string, protocols []int, operation string) (Generation, error) {
	if err := s.validate(); err != nil {
		return Generation{}, err
	}
	if node == "" || version == "" || !hostboot.ValidIdentity(boot) || len(protocols) == 0 {
		return Generation{}, &VerificationRefusal{Message: "invalid initialization identity"}
	}
	for _, v := range protocols {
		if v <= 0 {
			return Generation{}, &VerificationRefusal{Message: "invalid initialization protocol"}
		}
	}
	parent, err := os.OpenRoot(s.Data)
	if err != nil {
		return Generation{}, err
	}
	defer parent.Close()
	sourceParent, err := os.OpenRoot(filepath.Dir(source))
	if err != nil {
		return Generation{}, err
	}
	defer sourceParent.Close()
	info, err := sourceParent.Lstat(filepath.Base(source))
	if err != nil {
		return Generation{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return Generation{}, &VerificationRefusal{Message: "initialization requires an owned executable"}
	}
	binary, err := readOwnedFile(sourceParent, filepath.Base(source), info.Mode().Perm())
	if err != nil {
		return Generation{}, err
	}
	var in initializationIntent
	err = readPrivate(parent, initializationFile, &in)
	if errors.Is(err, os.ErrNotExist) {
		if _, err := parent.Lstat(Directory); !errors.Is(err, os.ErrNotExist) {
			if err != nil {
				return Generation{}, err
			}
			return Generation{}, &VerificationRefusal{Message: "existing generation storage lacks initialization intent"}
		}
		id, err := newID()
		if err != nil {
			return Generation{}, err
		}
		candidateBoot, err := newID()
		if err != nil {
			return Generation{}, err
		}
		now := time.Now().UTC()
		g := Generation{ID: id, CandidateBootSessionID: candidateBoot, Version: version, Digest: digestBytes(binary), Executable: s.executable(id), Label: s.label(id), Plist: filepath.Join(s.LaunchAgents, s.label(id)+".plist"), CreatedAt: now, PromotedAt: now, Role: "primary", StartBootID: boot, ProtocolVersions: slices.Clone(protocols), PreparationOperation: operation}
		in = initializationIntent{Version: 1, NodeID: node, Operation: operation, Generation: g}
		if err = persistInitializationIntent(parent, &in, true); err != nil {
			return Generation{}, err
		}
	} else if err != nil {
		return Generation{}, err
	}
	if s.InitializationStep != nil {
		if err = s.InitializationStep("intent_recorded"); err != nil {
			return in.Generation, err
		}
	}
	g := in.Generation
	if in.Version != 1 || in.NodeID != node || in.Operation != operation || !validID(g.ID) || !validID(g.CandidateBootSessionID) || g.PreparationOperation != operation || g.Role != "primary" || g.Version != version || g.Digest != digestBytes(binary) || !hostboot.SameBoot(g.StartBootID, boot) || !slices.Equal(g.ProtocolVersions, protocols) || g.Executable != s.executable(g.ID) || g.Label != s.label(g.ID) || g.Plist != filepath.Join(s.LaunchAgents, s.label(g.ID)+".plist") {
		return g, &VerificationRefusal{Message: "initialization intent payload changed"}
	}
	root, close, err := s.open(true)
	if err != nil {
		return g, err
	}
	defer close()
	// The initial read precedes the catalog lock. A concurrent exact retry
	// may have recorded custody meanwhile; never overwrite that newer history.
	var current initializationIntent
	if err = readPrivate(parent, initializationFile, &current); err != nil {
		return g, err
	}
	if current.Version != in.Version || current.NodeID != in.NodeID || current.Operation != in.Operation || !sameGeneration(current.Generation, in.Generation) {
		return g, &VerificationRefusal{Message: "initialization intent ownership changed under catalog lock"}
	}
	in = current
	publication := initializationPublication{store: s, parent: parent, intent: &in}
	if err = publication.validateQuarantines(); err != nil {
		return g, err
	}
	entries, err := root.Open(".")
	if err != nil {
		return g, err
	}
	names, err := entries.ReadDir(-1)
	entries.Close()
	if err != nil {
		return g, err
	}
	// The intent's exact generation ID binds both staging names before any
	// publication. No glob or arbitrary durable-helper leftovers are adopted.
	executableStage := "wefty-agent.init-" + g.ID + ".tmp"
	catalogStage := catalogName + ".init-" + g.ID + ".tmp"
	for _, entry := range names {
		if entry.Name() != g.ID && entry.Name() != catalogName && entry.Name() != catalogStage {
			return g, &VerificationRefusal{Message: "unowned storage in partial initialization"}
		}
	}
	var committed *Catalog
	if _, err := root.Lstat(catalogName); err == nil {
		c, err := s.read(root)
		if err != nil {
			return g, err
		}
		current := c.Generations[g.ID]
		if c.NodeID != node || c.Primary != g.ID || len(c.Generations) != 1 || current.PreparationOperation != operation || current.Digest != g.Digest {
			return g, &VerificationRefusal{Message: "initialization catalog ownership changed"}
		}
		committed = &c
	} else if !errors.Is(err, os.ErrNotExist) {
		return g, err
	}
	if s.InitializationStep != nil {
		if err = s.InitializationStep("root_created"); err != nil {
			return g, err
		}
	}
	if committed == nil {
		if err = root.Mkdir(g.ID, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return g, err
		}
		if err = durable.SyncDir(root); err != nil {
			return g, err
		}
		if s.InitializationStep != nil {
			if err = s.InitializationStep("generation_directory_created"); err != nil {
				return g, err
			}
		}
	}
	var dir *os.Root
	if committed != nil {
		dir, err = openInitializationDirectory(root, g.ID)
		if err != nil {
			return g, err
		}
		defer dir.Close()
	} else {
		held, err := holdGenerationDirectory(root, g.ID)
		if err != nil {
			return g, err
		}
		defer held.Close()
		dir = held.root
	}
	if committed == nil {
		f, err := dir.Open(".")
		if err != nil {
			return g, err
		}
		files, err := f.ReadDir(-1)
		f.Close()
		if err != nil {
			return g, err
		}
		for _, entry := range files {
			if entry.Name() != "wefty-agent" && entry.Name() != executableStage {
				return g, &VerificationRefusal{Message: "unowned partial generation evidence"}
			}
		}
	}
	payload, err := readOwnedFile(dir, "wefty-agent", 0500)
	if errors.Is(err, os.ErrNotExist) && committed == nil {
		err = publication.publish(dir, "wefty-agent", binary, 0500, "executable")
	} else if err == nil && digestBytes(payload) != g.Digest {
		err = &VerificationRefusal{Message: "partial initialization executable differs"}
	}
	if err != nil {
		return g, err
	}
	if _, err = publication.quarantine(dir, "executable"); err != nil {
		return g, err
	}
	// Acceptance of an existing entry does not prove its prior publication
	// reached the directory fsync. This fence always precedes catalog ownership.
	if err = s.syncInitializationDirectory("generation_executable", dir); err != nil {
		return g, err
	}
	if s.InitializationStep != nil {
		if err = s.InitializationStep("executable_directory_synced"); err != nil {
			return g, err
		}
	}
	if committed != nil {
		if _, err = publication.quarantine(root, "catalog"); err != nil {
			return g, err
		}
		if err = s.syncInitializationDirectory("catalog_existing", root); err != nil {
			return g, err
		}
		return committed.Generations[g.ID], nil
	}
	if s.InitializationStep != nil {
		if err = s.InitializationStep("executable_created"); err != nil {
			return g, err
		}
	}
	c := Catalog{Version: 1, NodeID: node, Revision: 1, Primary: g.ID, Generations: map[string]Generation{g.ID: g}}
	catalog, err := json.Marshal(c)
	if err != nil {
		return g, err
	}
	if err = publication.publish(root, catalogName, append(catalog, '\n'), 0600, "catalog"); err != nil {
		return g, err
	}
	if s.InitializationStep != nil {
		if err = s.InitializationStep("catalog_published"); err != nil {
			return g, err
		}
	}
	return g, nil
}

func (s Store) syncInitializationDirectory(kind string, dir *os.Root) error {
	if s.initializationSyncDir != nil {
		return s.initializationSyncDir(kind, dir)
	}
	return durable.SyncDir(dir)
}

// A committed primary may already hold its lifetime lock. Replay only verifies
// and syncs its executable and moves initialization scratch; it never replaces
// live executable or runtime entries and need not claim a second lifetime lock.
func openInitializationDirectory(parent *os.Root, id string) (*os.Root, error) {
	before, err := parent.Lstat(id)
	if err != nil {
		return nil, err
	}
	if !privateInfo(before, 0700) {
		return nil, &VerificationRefusal{Message: "initialization directory ownership changed"}
	}
	dir, err := parent.OpenRoot(id)
	if err != nil {
		return nil, err
	}
	f, err := dir.Open(".")
	if err != nil {
		dir.Close()
		return nil, err
	}
	held, err := f.Stat()
	f.Close()
	if err != nil {
		dir.Close()
		return nil, err
	}
	after, err := parent.Lstat(id)
	if err != nil {
		dir.Close()
		return nil, err
	}
	if !privateInfo(held, 0700) || !os.SameFile(before, held) || !os.SameFile(after, held) {
		dir.Close()
		return nil, &VerificationRefusal{Message: "initialization directory identity changed"}
	}
	return dir, nil
}
