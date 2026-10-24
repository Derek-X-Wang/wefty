package agentinstall

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"slices"

	"github.com/Derek-X-Wang/wefty/l1"
)

// PublishClosedGeneration is called only after the retiree's positive reap and
// settled-evidence barrier and a successful L1 close. Publication must precede exit.
func (s Store) PublishClosedGeneration(g l1.NodeGeneration) error {
	if g.Role != l1.NodeGenerationClosed || g.PromotionOperationID == "" {
		return &VerificationRefusal{Message: "retirement closure acknowledgment is not positive"}
	}
	return s.control(g.AgentGenerationID, "retirement-closed.json", g, true)
}

// CleanupClosed requires fresh authoritative closure AND the exact runtime's
// positive completion record. Neither an activation nor a closure receipt alone
// proves that its process has ended. RemoveFinished verifies that independently.
func (s Store) CleanupClosed(ctx context.Context, in l1.NodeGenerationInventory) error {
	c, err := s.Read()
	if err != nil {
		return err
	}
	if in.NodeID != c.NodeID {
		return &VerificationRefusal{Message: "cleanup inventory belongs to another node"}
	}
	if c.PendingReceipt != "" {
		return nil
	}
	for _, remote := range in.Generations {
		g, present := c.Generations[remote.AgentGenerationID]
		if !present || remote.Role != l1.NodeGenerationClosed {
			continue
		}
		if g.Role != "retiring" || g.Process == nil || g.Process.BootSessionID != remote.BootSessionID || !slices.Equal(asL1Residents(g.ResidentAttempts), remote.ResidentAttempts) {
			return &VerificationRefusal{Message: "closed generation differs from retained ownership"}
		}
		// A crash can remove the live closure/control records before catalog
		// publication. Resume from the bound completion audit, but only after
		// the fresh L1 closed observation above, never from the audit alone.
		root, close, readErr := s.open(false)
		if readErr != nil {
			return readErr
		}
		var prior completionReceipt
		readErr = readPrivate(root, "finished-"+g.ID+".json", &prior)
		close()
		if readErr == nil {
			if !sameGeneration(prior.Generation, g) || prior.Proof.GenerationID != g.ID {
				return &VerificationRefusal{Message: "cleanup audit generation changed"}
			}
			if err = s.RemoveFinished(ctx, prior.Proof); err != nil {
				return err
			}
			c, err = s.Read()
			if err != nil {
				return err
			}
			continue
		} else if !errors.Is(readErr, os.ErrNotExist) {
			return readErr
		}
		var closed l1.NodeGeneration
		err = s.control(g.ID, "retirement-closed.json", &closed, false)
		var pending *RecordPendingError
		if errors.As(err, &pending) {
			continue
		}
		if err != nil {
			return err
		}
		if closed.Role != l1.NodeGenerationClosed || closed.AgentGenerationID != remote.AgentGenerationID || closed.BootSessionID != remote.BootSessionID || closed.PromotionOperationID != remote.PromotionOperationID || closed.AuthorityGeneration != remote.AuthorityGeneration || !slices.Equal(closed.ResidentAttempts, remote.ResidentAttempts) {
			return &VerificationRefusal{Message: "retirement closure receipt identity differs"}
		}
		state, err := s.Manager.State(ctx, g.Label)
		if err != nil {
			return err
		}
		if state.Running {
			continue
		}
		payload, err := json.Marshal(closed)
		if err != nil {
			return err
		}
		proof := CompletionProof{GenerationID: g.ID, CatalogRevision: c.Revision, Binding: *g.Process, PayloadsAbsent: true, L1SessionClosed: true, EvidenceSafe: true, EvidenceDigest: digestBytes(payload)}
		root, close, err = s.open(false)
		if err != nil {
			return err
		}
		var receipt completionReceipt
		err = readPrivate(root, "finished-"+g.ID+".json", &receipt)
		close()
		if err == nil {
			proof = receipt.Proof
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err = s.RemoveFinished(ctx, proof); err != nil {
			return err
		}
		c, err = s.Read()
		if err != nil {
			return err
		}
	}
	return nil
}
