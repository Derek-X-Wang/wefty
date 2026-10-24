package agentinstall

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"slices"

	"github.com/Derek-X-Wang/wefty/internal/durable"
)

// These records extend the original initialization intent. They record custody,
// never authority to delete: both unproved and freshly created stages are kept.
type initializationQuarantine struct {
	ID    string `json:"id"`
	Kind  string `json:"kind"`
	State string `json:"state"` // pending, retained, or positively observed absent
}
type initializationPublication struct {
	store  Store
	parent *os.Root
	intent *initializationIntent
}

const initializationQuarantinePrefix = "agent-initialization-quarantine-"

func initializationStage(generation, kind string) string {
	if kind == "catalog" {
		return catalogName + ".init-" + generation + ".tmp"
	}
	return "wefty-agent.init-" + generation + ".tmp"
}
func (p *initializationPublication) step(phase string) error {
	if p.store.InitializationStep != nil {
		return p.store.InitializationStep(phase)
	}
	return nil
}
func (p *initializationPublication) validateQuarantines() error {
	seen := map[string]bool{}
	for _, q := range p.intent.Quarantines {
		if !validID(q.ID) || seen[q.ID] || !slices.Contains([]string{"executable", "catalog"}, q.Kind) || !slices.Contains([]string{"pending", "retained", "absent"}, q.State) {
			return &VerificationRefusal{Message: "initialization quarantine identity changed"}
		}
		seen[q.ID] = true
	}
	return nil
}

// Never reuse, truncate or unlink a publication staging name. A leftover from a
// failed intent write remains in Data for the owner and cannot block a retry.
func persistInitializationIntent(parent *os.Root, in *initializationIntent, exclusive bool) error {
	payload, err := json.Marshal(in)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	id, err := newID()
	if err != nil {
		return err
	}
	name := initializationFile + ".publish-" + id
	f, err := parent.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = f.Chmod(0600); err != nil {
		return err
	}
	if _, err = f.Write(payload); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	held, err := f.Stat()
	if err != nil {
		return err
	}
	entry, err := parent.Lstat(name)
	if err != nil {
		return err
	}
	if !os.SameFile(held, entry) {
		return &VerificationRefusal{Message: "initialization intent publication identity changed"}
	}
	if exclusive {
		err = parent.Link(name, initializationFile)
	} else {
		err = parent.Rename(name, initializationFile)
	}
	if err != nil {
		return err
	}
	return durable.SyncDir(parent)
}
func (p *initializationPublication) persist() error {
	return persistInitializationIntent(p.parent, p.intent, false)
}

// First record the exact quarantine location, then rename the current source
// entry through held parent descriptors. A replacement is retained as well.
// Nothing at a staging name is ever unlinked or treated as operation-created.
func (p *initializationPublication) quarantine(dir *os.Root, kind string) (string, error) {
	for i := range p.intent.Quarantines {
		q := p.intent.Quarantines[i]
		if q.Kind == kind && q.State == "pending" {
			if err := p.finishQuarantine(dir, i); err != nil {
				return "", err
			}
		}
	}
	for collision := 0; collision < 4; collision++ {
		name := initializationStage(p.intent.Generation.ID, kind)
		if _, err := dir.Lstat(name); errors.Is(err, os.ErrNotExist) {
			return "", nil
		} else if err != nil {
			return "", err
		}
		id, err := newID()
		if err != nil {
			return "", err
		}
		p.intent.Quarantines = append(p.intent.Quarantines, initializationQuarantine{ID: id, Kind: kind, State: "pending"})
		if err = p.persist(); err != nil {
			return "", err
		}
		if err = p.step("quarantine_intent_recorded"); err != nil {
			return "", err
		}
		i := len(p.intent.Quarantines) - 1
		if err = p.finishQuarantine(dir, i); err != nil {
			return "", err
		}
		if p.intent.Quarantines[i].State != "retained" {
			return "", nil
		}
		if _, err := dir.Lstat(name); err == nil {
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		return id, nil
	}
	return "", &VerificationRefusal{Message: "initialization staging entry kept changing; all observed files retained"}

}
func (p *initializationPublication) finishQuarantine(dir *os.Root, index int) error {
	q := p.intent.Quarantines[index]
	name := initializationStage(p.intent.Generation.ID, q.Kind)
	destination := initializationQuarantinePrefix + q.ID
	if err := p.parent.Mkdir(destination, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	held, err := holdGenerationDirectory(p.parent, destination)
	if err != nil {
		return err
	}
	defer held.Close()
	if err = durable.SyncDir(p.parent); err != nil {
		return err
	}
	if err = p.step("quarantine_directory_created"); err != nil {
		return err
	}
	_, err = held.root.Lstat("artifact")
	if errors.Is(err, os.ErrNotExist) {
		if _, err = dir.Lstat(name); err == nil {
			if err = p.step("stage_validated_before_custody"); err != nil {
				return err
			}
			err = renameInitializationEntry(dir, name, held.root, "artifact")
		}
		if errors.Is(err, os.ErrExist) {
			// An unexpected target belongs to nobody we can prove. Preserve it
			// in this cell and move the source into a fresh recorded cell.
			q.State = "retained"
		} else if errors.Is(err, os.ErrNotExist) {
			q.State = "absent"
		} else if err != nil {
			return err
		} else {
			q.State = "retained"
		}
	} else if err != nil {
		return err
	} else {
		q.State = "retained"
	}
	if err = durable.SyncDir(held.root); err != nil {
		return err
	}
	if err = durable.SyncDir(dir); err != nil {
		return err
	}
	if err = p.step("quarantine_moved"); err != nil {
		return err
	}
	p.intent.Quarantines[index] = q
	if err = p.persist(); err != nil {
		return err
	}
	return p.step("quarantine_recorded")
}

func (p *initializationPublication) publish(dir *os.Root, name string, payload []byte, mode os.FileMode, kind string) error {
	if _, err := p.quarantine(dir, kind); err != nil {
		return err
	}
	staging := initializationStage(p.intent.Generation.ID, kind)
	f, err := dir.OpenFile(staging, os.O_CREATE|os.O_EXCL|os.O_RDWR, mode)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = f.Chmod(mode); err != nil {
		return err
	}
	if err = p.step(kind + "_staging_created"); err != nil {
		return err
	}
	if _, err = f.Write(payload); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = p.step(kind + "_staging_synced"); err != nil {
		return err
	}
	// The created descriptor remains open through custody verification and link.
	id, err := p.quarantine(dir, kind)
	if err != nil {
		return err
	}
	if id == "" {
		return &VerificationRefusal{Message: "initialization stage disappeared before publication"}
	}
	held, err := holdGenerationDirectory(p.parent, initializationQuarantinePrefix+id)
	if err != nil {
		return err
	}
	defer held.Close()
	original, err := f.Stat()
	if err != nil {
		return err
	}
	entry, err := held.root.Lstat("artifact")
	if err != nil {
		return err
	}
	if !os.SameFile(original, entry) {
		return &VerificationRefusal{Message: "initialization stage replaced before publication"}
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	current, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	if !bytes.Equal(current, payload) {
		return &VerificationRefusal{Message: "initialization stage payload changed"}
	}
	if err = linkInitializationEntry(held.root, "artifact", dir, name); err != nil {
		if errors.Is(err, os.ErrExist) {
			return &VerificationRefusal{Message: "initialization publication target appeared before commit"}
		}
		return err
	}
	if err = p.step(kind + "_published_before_sync"); err != nil {
		return err
	}
	if err = p.step(kind + "_staging_removed"); err != nil {
		return err
	}
	return p.store.syncInitializationDirectory("publication_"+kind, dir)
}
