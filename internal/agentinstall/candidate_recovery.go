package agentinstall

import (
	"context"
	"errors"
	"github.com/Derek-X-Wang/wefty/internal/durable"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"os"
	"path/filepath"
)

// RemoveCandidate is called only after a fresh L1 inventory proves the candidate
// was never staged or primary. Abort intent makes its waiting process exit and
// fences stale launchd starts before any file is removed. No sibling is changed.
func (s Store) RemoveCandidate(ctx context.Context, id, operation string) error {
	if !validID(id) || operation == "" {
		return &VerificationRefusal{Message: "invalid candidate removal operation"}
	}
	root, close, err := s.open(false)
	if err != nil {
		return err
	}
	defer close()
	c, err := s.read(root)
	if err != nil {
		return err
	}
	var receipt Generation
	name := "candidate-removed-" + id + ".json"
	receiptErr := readPrivate(root, name, &receipt)
	if receiptErr != nil && !errors.Is(receiptErr, os.ErrNotExist) {
		return receiptErr
	}
	g, present := c.Generations[id]
	if !present {
		if receiptErr == nil && receipt.ID == id && receipt.AbortOperation == operation {
			return nil
		}
		return &VerificationRefusal{Message: "candidate removal ownership missing"}
	}
	if g.Role != "" || c.Primary == id || c.PendingReceipt != "" || g.PreparationOperation != operation || (g.AbortOperation != "" && g.AbortOperation != operation) {
		return &VerificationRefusal{Message: "candidate removal would destroy generation authority"}
	}
	if g.AbortOperation == "" {
		g.AbortOperation = operation
		c.Generations[id] = g
		c.Revision++
		if err := writePrivate(root, catalogName, c); err != nil {
			return err
		}
	}
	if err := s.Manager.SuppressGenerationStart(ctx, g.Label); err != nil {
		return err
	}
	state, err := s.Manager.State(ctx, g.Label)
	if err != nil {
		return err
	}
	if state.Running {
		return &RecordPendingError{Cause: errors.New("candidate process has not exited")}
	}
	held, err := holdGenerationDirectory(root, id)
	if err == nil {
		defer held.Close()
		dir := held.root
		entries, err := dir.Open(".")
		if err != nil {
			return err
		}
		defer entries.Close()
		names, err := entries.ReadDir(-1)
		if err != nil {
			return err
		}
		for _, file := range names {
			switch file.Name() {
			case "wefty-agent", "ready.json", "candidate-start.json":
			default:
				return &VerificationRefusal{Message: "candidate has unknown evidence"}
			}
			mode := os.FileMode(0600)
			if file.Name() == "wefty-agent" {
				mode = 0500
			}
			payload, err := readOwnedFile(dir, file.Name(), mode)
			if err != nil {
				return err
			}
			if file.Name() == "wefty-agent" && digestBytes(payload) != g.Digest {
				return &VerificationRefusal{Message: "candidate executable changed"}
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) || receiptErr != nil {
		return err
	}
	folder, err := os.OpenRoot(s.LaunchAgents)
	if err != nil {
		return err
	}
	defer folder.Close()
	payload, err := readRetainedPlist(folder, filepath.Base(g.Plist))
	if err == nil && digestBytes(payload) != g.PlistDigest {
		return &VerificationRefusal{Message: "candidate plist changed"}
	}
	// The digest is publication intent, not proof the plist was published.
	// Exact stopped-candidate ownership and the held lifetime lock above fence this absence.
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if receiptErr == nil && !sameGeneration(g, receipt) {
		return &VerificationRefusal{Message: "candidate removal receipt changed"}
	}
	if receiptErr != nil {
		if err := writePrivate(root, name, g); err != nil {
			return err
		}
	}
	if err := s.Manager.UnloadFinishedGeneration(ctx, g.Label); err != nil {
		var running *launchagent.RunningProcessError
		if errors.As(err, &running) {
			return &VerificationRefusal{Message: running.Error()}
		}
		return err
	}
	if err := folder.Remove(filepath.Base(g.Plist)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := durable.SyncDir(folder); err != nil {
		return err
	}
	if s.beforeGenerationRemoval != nil {
		if err := s.beforeGenerationRemoval(id); err != nil {
			return err
		}
	}
	for _, file := range []string{"ready.json", "candidate-start.json", "wefty-agent"} {
		if held == nil {
			break
		}
		if err := held.remove(root, id, file); err != nil {
			return err
		}
	}
	if held != nil {
		if err := held.retainEmpty(root, id, s.afterRemovalIdentityCheck); err != nil {
			return err
		}
	}
	if err := durable.SyncDir(root); err != nil {
		return err
	}
	delete(c.Generations, id)
	c.Revision++
	return writePrivate(root, catalogName, c)
}

// BindCandidateOperation upgrades older rolling evidence only for an exact
// unpromoted candidate under the observed catalog revision. It grants no L1
// authority and cannot attach an unrelated candidate to a recovery operation.
func (s Store) BindCandidateOperation(expected Generation, primary, operation string, revision uint64) error {
	root, close, err := s.open(false)
	if err != nil {
		return err
	}
	defer close()
	c, err := s.read(root)
	if err != nil {
		return err
	}
	g, ok := c.Generations[expected.ID]
	if !ok || g.Role != "" || c.Primary != primary || c.PendingReceipt != "" || c.Revision != revision || g.Digest != expected.Digest || g.Version != expected.Version || g.CandidateBootSessionID != expected.CandidateBootSessionID || g.StartBootID != expected.StartBootID || operation == "" || (g.PreparationOperation != "" && g.PreparationOperation != operation) {
		return &VerificationRefusal{Message: "legacy candidate binding differs from exact rolling evidence"}
	}
	if g.PreparationOperation == operation {
		return nil
	}
	if err = s.verifyExecutable(root, g); err != nil {
		return err
	}
	g.PreparationOperation = operation
	c.Generations[g.ID] = g
	c.Revision++
	return writePrivate(root, catalogName, c)
}
