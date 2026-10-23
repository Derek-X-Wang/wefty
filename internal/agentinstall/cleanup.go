package agentinstall

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/Derek-X-Wang/wefty/internal/durable"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
)

// CompletionProof is supplied by authoritative retirement observation, not
// inferred from a timeout, an empty local map, a missing PID or custody alone.
// EvidenceDigest binds the caller's exact resident/finalization/custody evidence.
type CompletionProof struct {
	GenerationID    string `json:"agent_generation_id"`
	CatalogRevision uint64 `json:"catalog_revision"`
	Binding
	PayloadsAbsent  bool   `json:"payloads_absent"`
	L1SessionClosed bool   `json:"l1_session_closed"`
	EvidenceSafe    bool   `json:"evidence_safe"`
	EvidenceDigest  string `json:"evidence_sha256"`
}
type completionReceipt struct {
	Version     int             `json:"version"`
	Generation  Generation      `json:"generation"`
	Proof       CompletionProof `json:"proof"`
	PlistDigest string          `json:"plist_sha256"`
}

func digestBytes(b []byte) string { d := sha256.Sum256(b); return hex.EncodeToString(d[:]) }

// RemoveFinished removes only derived, verified owned files after both the
// generation process and payloads have ended. Audit receipts remain private.
// It can resume after any file deletion without recursive removal.
func (s Store) RemoveFinished(ctx context.Context, proof CompletionProof) error {
	if !validID(proof.GenerationID) {
		return errors.New("invalid completion generation")
	}
	evidence, decodeErr := hex.DecodeString(proof.EvidenceDigest)
	if !proof.PayloadsAbsent || !proof.L1SessionClosed || !proof.EvidenceSafe || decodeErr != nil || len(evidence) != 32 || proof.PID <= 0 || proof.BootSessionID == "" || !hostboot.ValidIdentity(proof.SystemBootID) || proof.ReadyAt.IsZero() {
		return errors.New("retiring generation still holds work or lacks exact completion evidence")
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
	if c.PendingReceipt != "" {
		return errors.New("cannot clean up an unresolved promotion")
	}
	id := proof.GenerationID
	name := "finished-" + id + ".json"
	var receipt completionReceipt
	receiptErr := readPrivate(root, name, &receipt)
	g, present := c.Generations[id]
	if present && (g.Role != "retiring" || c.Primary == id || g.Process == nil || proof.PID != g.Process.PID || proof.BootSessionID != g.Process.BootSessionID || !hostboot.SameBoot(proof.SystemBootID, g.Process.SystemBootID) || !proof.ReadyAt.Equal(g.Process.ReadyAt)) {
		return errors.New("completion proof does not match a retiring process")
	}

	if receiptErr == nil {
		if receipt.Generation.Role != "retiring" || receipt.Version != 1 || receipt.Generation.ID != id || receipt.Generation.Executable != s.executable(id) || receipt.Generation.Label != s.label(id) || receipt.Generation.Plist != filepath.Join(s.LaunchAgents, s.label(id)+".plist") || receipt.Proof != proof {
			return errors.New("completion receipt mismatch")
		}
		if !present {
			return nil
		}
		if !sameGeneration(g, receipt.Generation) {
			return errors.New("generation changed after completion receipt")
		}
	} else {
		if !errors.Is(receiptErr, os.ErrNotExist) {
			return receiptErr
		}
		if !present || proof.CatalogRevision != c.Revision {
			return errors.New("retiring generation still holds work or lacks exact completion evidence")
		}
		if err := s.verifyExecutable(root, g); err != nil {
			return err
		}
		info, err := os.Lstat(g.Plist)
		if err != nil {
			return err
		}
		if !privateInfo(info, 0644) {
			return errors.New("generation plist ownership changed")
		}
		b, err := os.ReadFile(g.Plist)
		if err != nil {
			return err
		}
		if g.PlistDigest == "" || digestBytes(b) != g.PlistDigest {
			return errors.New("generation plist differs from installation record")
		}
		receipt = completionReceipt{Version: 1, Generation: g, Proof: proof, PlistDigest: g.PlistDigest}
	}
	state, err := s.Manager.State(ctx, g.Label)
	if err != nil {
		return err
	}
	if state.Running {
		return errors.New("generation process still running")
	}
	if err := privateDirectory(filepath.Join(s.Root(), id)); err != nil {
		if !errors.Is(err, os.ErrNotExist) || receiptErr != nil {
			return err
		}
	} else {
		release, err := lockDirectory(filepath.Join(s.Root(), id))
		if err != nil {
			return errors.New("generation process lock still held")
		}
		defer release()
		// Unknown files are never recursively removed, even with a valid receipt.
		dir, err := root.OpenRoot(id)
		if err != nil {
			return err
		}
		defer dir.Close()
		entries, err := os.ReadDir(filepath.Join(s.Root(), id))
		if err != nil {
			return err
		}
		for _, entry := range entries {
			switch entry.Name() {
			case "wefty-agent", "ready.json", "candidate-start.json":
			default:
				return errors.New("generation directory contains unrecognized retained evidence")
			}
		}
		for _, entry := range entries {
			info, err := dir.Lstat(entry.Name())
			if err != nil {
				return err
			}
			mode := os.FileMode(0600)
			if entry.Name() == "wefty-agent" {
				mode = 0500
			}
			if !privateInfo(info, mode) {
				return errors.New("generation file ownership changed")
			}
			if entry.Name() == "wefty-agent" {
				b, err := dir.ReadFile(entry.Name())
				if err != nil {
					return err
				}
				if digestBytes(b) != g.Digest {
					return errors.New("retained executable changed")
				}
			}
		}
	}
	if b, err := os.ReadFile(g.Plist); err == nil {
		info, err := os.Lstat(g.Plist)
		if err != nil {
			return err
		}
		if !privateInfo(info, 0644) || digestBytes(b) != receipt.PlistDigest {
			return errors.New("generation plist changed after completion")
		}
	} else if !errors.Is(err, os.ErrNotExist) || receiptErr != nil {
		return err
	}
	if receiptErr != nil {
		if err := writePrivate(root, name, receipt); err != nil {
			return err
		}
	}
	if err := s.Manager.UnloadFinishedGeneration(ctx, g.Label); err != nil {
		return err
	}
	folder, err := os.OpenRoot(s.LaunchAgents)
	if err != nil {
		return err
	}
	defer folder.Close()
	if err := folder.Remove(filepath.Base(g.Plist)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := durable.SyncDir(folder); err != nil {
		return err
	}
	for _, file := range []string{"ready.json", "candidate-start.json", "wefty-agent"} {
		if err := root.Remove(filepath.Join(id, file)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := root.Remove(id); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := durable.SyncDir(root); err != nil {
		return err
	}
	delete(c.Generations, id)
	c.Revision++
	return writePrivate(root, catalogName, c)
}

func sameGeneration(a, b Generation) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return bytes.Equal(left, right)
}
