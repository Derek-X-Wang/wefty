package agentinstall

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/durable"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
	"github.com/Derek-X-Wang/wefty/l1"
)

// PromotionEvidence comes from verified L1 inventory under closed admission.
type PromotionEvidence struct {
	// ClosedGenerations binds retained local ownership to authoritative L1 closed
	// history. These files remain for receipt-bound cleanup, outside the live cohort.
	ClosedGenerations           map[string]Binding           `json:"closed_generations,omitempty"`
	ExpectedAuthorityGeneration int64                        `json:"expected_authority_generation,omitempty"`
	IntentReason                string                       `json:"intent_reason,omitempty"`
	IntentRevision              int64                        `json:"intent_revision"`
	L1CatalogRevision           int64                        `json:"l1_catalog_revision"`
	Residents                   map[string][]ResidentAttempt `json:"resident_attempts"`
	ServiceRecoveryKeys         map[string]string            `json:"service_recovery_keys"`
}

// ActivationReceipt records installation intent, not L1 authority. The caller
// must reconcile its exact operation result. Pending intent fences startup.
type ActivationReceipt struct {
	Aborted              bool                             `json:"aborted,omitempty"`
	PromotionRequest     *l1.PromoteNodeGenerationRequest `json:"promotion_request,omitempty"`
	SuppressedPlists     map[string][]byte                `json:"suppressed_plists,omitempty"`
	OriginalPlistDigests map[string]string                `json:"original_plist_sha256,omitempty"`
	PromotionEvidence
	Processes           map[string]Binding `json:"processes"`
	Version             int                `json:"version"`
	ID                  string             `json:"id"`
	ExpectedPrimary     string             `json:"expected_primary_generation_id"`
	Candidate           string             `json:"candidate_generation_id"`
	Retiring            []string           `json:"retiring_generation_ids"`
	CatalogRevision     uint64             `json:"catalog_revision"`
	SystemBootID        string             `json:"system_boot_id"`
	Protocol            int                `json:"retirement_protocol"`
	Digests             map[string]string  `json:"binary_sha256"`
	L1OperationID       string             `json:"l1_promotion_operation_id"`
	L1PromotionRevision int64              `json:"l1_promotion_revision,omitempty"`
	Committed           bool               `json:"committed"`
}

func activationName(id string) string { return "activation-" + id + ".json" }
func (s Store) BeginPromotion(candidate, boot, operation string, expectedRevision uint64, protocol int, evidence PromotionEvidence) (ActivationReceipt, error) {
	root, close, err := s.open(false)
	if err != nil {
		return ActivationReceipt{}, err
	}
	defer close()
	c, err := s.read(root)
	if err != nil {
		return ActivationReceipt{}, err
	}
	if c.PendingReceipt != "" {
		var pending ActivationReceipt
		if err := readPrivate(root, activationName(c.PendingReceipt), &pending); err != nil {
			return pending, err
		}
		observed, _ := json.Marshal(canonicalPromotionEvidence(evidence))
		recorded, _ := json.Marshal(canonicalPromotionEvidence(pending.PromotionEvidence))
		if pending.ID != c.PendingReceipt || pending.L1OperationID != operation || pending.Candidate != candidate || pending.ExpectedPrimary != c.Primary || pending.CatalogRevision != expectedRevision || c.Revision != expectedRevision+1 || pending.Protocol != protocol || pending.Committed || pending.Aborted || len(pending.Digests)+len(pending.ClosedGenerations) != len(c.Generations) || !hostboot.SameBoot(pending.SystemBootID, boot) || !bytes.Equal(observed, recorded) {
			return pending, &VerificationRefusal{Message: "pending activation operation or evidence changed"}
		}
		for id, digest := range pending.Digests {
			g, ok := c.Generations[id]
			if !ok || g.Digest != digest || g.Process == nil || *g.Process != pending.Processes[id] {
				return pending, &VerificationRefusal{Message: "pending activation cohort changed"}
			}
		}
		return pending, nil
	}
	g, ok := c.Generations[candidate]
	if !ok || g.Role != "" || g.Process == nil || c.PendingReceipt != "" || c.Revision != expectedRevision || operation == "" || !hostboot.SameBoot(g.Process.SystemBootID, boot) || protocol != 1 {
		return ActivationReceipt{}, &VerificationRefusal{Message: "promotion requires a ready candidate, current catalog and protocol 1"}
	}
	if evidence.IntentRevision <= 0 || evidence.L1CatalogRevision < 0 || len(evidence.Residents)+len(evidence.ClosedGenerations) != len(c.Generations) {
		return ActivationReceipt{}, &VerificationRefusal{Message: "promotion requires complete verified resident inventory and intent revision"}
	}
	id := digestBytes([]byte(c.NodeID + "\x00" + candidate + "\x00" + operation))[:32]
	r := ActivationReceipt{PromotionEvidence: evidence, Processes: map[string]Binding{}, Version: 1, ID: id, ExpectedPrimary: c.Primary, Candidate: candidate, CatalogRevision: c.Revision, SystemBootID: boot, Protocol: protocol, Digests: map[string]string{}, L1OperationID: operation}
	r.ClosedGenerations = maps.Clone(evidence.ClosedGenerations)
	r.Residents = map[string][]ResidentAttempt{}
	r.ServiceRecoveryKeys = maps.Clone(evidence.ServiceRecoveryKeys)
	seenAttempts := map[string]bool{}
	for id, g := range c.Generations {
		if binding, closed := evidence.ClosedGenerations[id]; closed {
			if g.Role != "retiring" || id == c.Primary || g.Process == nil || *g.Process != binding {
				return r, &VerificationRefusal{Message: "closed generation history differs from retained ownership"}
			}
			if _, live := evidence.Residents[id]; live {
				return r, &VerificationRefusal{Message: "closed generation also included in live cohort"}
			}
			continue
		}
		if g.Role == "" && id != candidate {
			return r, &VerificationRefusal{Message: "another staged candidate requires reconciliation"}
		}
		if !slices.Contains(g.ProtocolVersions, protocol) {
			return r, &VerificationRefusal{Message: "protocol is not supported by the entire generation cohort"}
		}
		if g.Process == nil || !hostboot.SameBoot(g.Process.SystemBootID, boot) {
			return r, &VerificationRefusal{Message: "generation process binding missing"}
		}
		residents, observed := evidence.Residents[id]
		if !observed || residents == nil {
			return r, &VerificationRefusal{Message: "missing resident inventory; verified idle must be an empty array"}
		}
		residents = append([]ResidentAttempt{}, residents...)
		slices.SortFunc(residents, func(a, b ResidentAttempt) int { return strings.Compare(a.AttemptID, b.AttemptID) })
		if g.Role == "retiring" && !slices.Equal(g.ResidentAttempts, residents) {
			return r, &VerificationRefusal{Message: "retiring resident membership changed"}
		}
		for _, a := range residents {
			if a.JobID == "" || a.AttemptID == "" || a.FencingToken == "" || a.AuthorityGeneration <= 0 || a.BootSessionID != g.Process.BootSessionID || seenAttempts[a.AttemptID] {
				return r, &VerificationRefusal{Message: "invalid resident attempt identity"}
			}
			seenAttempts[a.AttemptID] = true
		}
		if id == candidate && len(residents) != 0 {
			return r, &VerificationRefusal{Message: "staged candidate has resident work"}
		}
		g.ResidentAttempts = append([]ResidentAttempt{}, residents...)
		r.Residents[id] = append([]ResidentAttempt{}, residents...)
		c.Generations[id] = g
		r.Processes[id] = *g.Process
		r.Digests[id] = g.Digest
		if g.Role == "retiring" {
			r.Retiring = append(r.Retiring, id)
		}
	}
	slices.Sort(r.Retiring)
	var previous ActivationReceipt
	if err := readPrivate(root, activationName(id), &previous); err == nil {
		before, _ := json.Marshal(previous)
		after, _ := json.Marshal(r)
		if !bytes.Equal(before, after) {
			return r, &VerificationRefusal{Message: "activation operation payload changed"}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return r, err
	}
	if err := writePrivate(root, activationName(id), r); err != nil {
		return r, err
	}
	if err := s.promotionWritten("activation_prepared"); err != nil {
		return r, err
	}
	c.PendingReceipt = id
	c.Revision++
	if err := writePrivate(root, catalogName, c); err != nil {
		return r, err
	}
	return r, s.promotionWritten("activation_pending")
}

// CommitPromotion is called only after #1179 has verified the exact L1 result.
// #1180 owns interrupted-cutover reconciliation. Any error leaves pending intent
// in place and startup closed. Retiring labels stay loaded and keep their PIDs.
func (s Store) CommitPromotion(ctx context.Context, receiptID, operation, boot string, l1Revision int64) error {
	return s.commitPromotion(ctx, receiptID, operation, boot, l1Revision, true)
}

func (s Store) commitPromotion(ctx context.Context, receiptID, operation, boot string, l1Revision int64, requireRunning bool) error {
	if !validID(receiptID) {
		return &VerificationRefusal{Message: "invalid activation receipt ID"}
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
	var r ActivationReceipt
	if err := readPrivate(root, activationName(receiptID), &r); err != nil {
		return err
	}
	if r.Aborted || r.ID != receiptID || r.Version != 1 || r.L1OperationID != operation || l1Revision <= 0 || l1Revision != r.L1CatalogRevision+1 || r.Protocol != 1 || !hostboot.SameBoot(r.SystemBootID, boot) {
		return &VerificationRefusal{Message: "activation receipt or promotion result mismatch"}
	}
	// Publication can stop after the catalog, after the receipt, or after clearing
	// pending intent. Finish only the exact already-published transition.
	if c.Primary == r.Candidate {
		g := c.Generations[r.Candidate]
		if g.Role != "primary" || g.Digest != r.Digests[r.Candidate] || g.Process == nil || (c.PendingReceipt != "" && *g.Process != r.Processes[r.Candidate]) || (c.PendingReceipt != "" && c.PendingReceipt != receiptID) || (r.Committed && r.L1PromotionRevision != l1Revision) {
			return &VerificationRefusal{Message: "published promotion ownership changed"}
		}
		if c.PendingReceipt == "" {
			if !r.Committed {
				return &VerificationRefusal{Message: "missing committed activation receipt"}
			}
			return nil
		}
		r.Committed, r.L1PromotionRevision = true, l1Revision
		if err := writePrivate(root, activationName(receiptID), r); err != nil {
			return err
		}
		if err := s.promotionWritten("receipt_committed"); err != nil {
			return err
		}
		c.PendingReceipt = ""
		c.Revision++
		if err := writePrivate(root, catalogName, c); err != nil {
			return err
		}
		return s.promotionWritten("pending_cleared")
	}
	if c.PendingReceipt != receiptID || r.Committed || c.Primary != r.ExpectedPrimary || c.Revision != r.CatalogRevision+1 || len(r.Digests)+len(r.ClosedGenerations) != len(c.Generations) {
		return &VerificationRefusal{Message: "activation catalog changed"}
	}
	retirees := []string{}
	for id, g := range c.Generations {
		if binding, closed := r.ClosedGenerations[id]; closed {
			if g.Role != "retiring" || id == c.Primary || g.Process == nil || *g.Process != binding {
				return &VerificationRefusal{Message: "closed generation ownership changed"}
			}
			continue
		}
		if r.Digests[id] != g.Digest || g.Process == nil || r.Processes[id] != *g.Process || !slices.Equal(r.Residents[id], g.ResidentAttempts) {
			return &VerificationRefusal{Message: "activation cohort changed"}
		}
		if g.Role == "retiring" {
			retirees = append(retirees, id)
		}
	}
	slices.Sort(retirees)
	if !slices.Equal(retirees, r.Retiring) {
		return &VerificationRefusal{Message: "activation retiree set changed"}
	}
	candidate, ok := c.Generations[r.Candidate]
	if !ok || candidate.Process == nil || candidate.Role != "" || r.Candidate == r.ExpectedPrimary {
		return &VerificationRefusal{Message: "candidate process binding missing"}
	}
	if requireRunning {
		if _, err := s.ready(ctx, root, c, candidate, *candidate.Process, candidate.Process.ReadyAt); err != nil {
			return err
		}
	}
	folder, err := os.OpenRoot(s.LaunchAgents)
	if err != nil {
		return err
	}
	defer folder.Close()
	now := time.Now().UTC()
	// Persist exact original and target bytes before suppressing any definition.
	if r.SuppressedPlists == nil {
		r.SuppressedPlists = map[string][]byte{}
		r.OriginalPlistDigests = map[string]string{}
		for id, g := range c.Generations {
			if _, closed := r.ClosedGenerations[id]; id == r.Candidate || closed {
				continue
			}
			old, err := readRetainedPlist(folder, filepath.Base(g.Plist))
			if err != nil {
				return err
			}
			if g.PlistDigest == "" || digestBytes(old) != g.PlistDigest {
				return &VerificationRefusal{Message: "retained generation plist changed"}
			}
			payload := bytes.Replace(old, []byte("  <key>RunAtLoad</key>\n  <true/>\n"), []byte("  <key>RunAtLoad</key>\n  <false/>\n"), 1)
			payload = bytes.Replace(payload, []byte("  <key>KeepAlive</key>\n  <dict>\n    <key>SuccessfulExit</key>\n    <false/>\n  </dict>\n"), []byte("  <key>KeepAlive</key>\n  <false/>\n"), 1)
			r.SuppressedPlists[id], r.OriginalPlistDigests[id] = payload, g.PlistDigest
		}
		if err := writePrivate(root, activationName(receiptID), r); err != nil {
			return err
		}
		if err := s.promotionWritten("suppression_prepared"); err != nil {
			return err
		}
	}
	for id, g := range c.Generations {
		if _, closed := r.ClosedGenerations[id]; id == r.Candidate || closed {
			continue
		}
		payload, ok := r.SuppressedPlists[id]
		if !ok || r.OriginalPlistDigests[id] != g.PlistDigest {
			return &VerificationRefusal{Message: "suppression receipt differs from cohort"}
		}
		old, err := readRetainedPlist(folder, filepath.Base(g.Plist))
		if err != nil {
			return err
		}
		if digestBytes(old) != g.PlistDigest && !bytes.Equal(old, payload) {
			return &VerificationRefusal{Message: "retained generation plist changed"}
		}
		if err := s.Manager.SuppressGenerationStart(ctx, g.Label); err != nil {
			return err
		}
		if !bytes.Equal(old, payload) {
			if err := durable.WriteFileMode(folder, filepath.Base(g.Plist), payload, 0644); err != nil {
				return err
			}
			if err := s.promotionWritten("retiree_plist:" + id); err != nil {
				return err
			}
		}
		g.Role = "retiring"
		if g.RetiringAt.IsZero() {
			g.RetiringAt = now
		}
		g.Protocol, g.PlistDigest = r.Protocol, digestBytes(payload)
		c.Generations[id] = g
	}
	candidate.Role = "primary"
	candidate.Protocol = r.Protocol
	candidate.PromotedAt = now
	c.Primary = candidate.ID
	c.Generations[candidate.ID] = candidate
	c.Revision++
	if err := writePrivate(root, catalogName, c); err != nil {
		return err
	}
	if err := s.promotionWritten("catalog_published"); err != nil {
		return err
	}
	r.Committed = true
	r.L1PromotionRevision = l1Revision
	if err := writePrivate(root, activationName(receiptID), r); err != nil {
		return err
	}
	if err := s.promotionWritten("receipt_committed"); err != nil {
		return err
	}
	c.PendingReceipt = ""
	c.Revision++
	if err := writePrivate(root, catalogName, c); err != nil {
		return err
	}
	return s.promotionWritten("pending_cleared")
}

// ReadActivationReceipt exposes a private record to installation recovery, not
// to public reports. It also reads committed audit receipts after cleanup.
func (s Store) ReadActivationReceipt(id string) (ActivationReceipt, error) {
	if !validID(id) {
		return ActivationReceipt{}, &VerificationRefusal{Message: "invalid activation receipt ID"}
	}
	root, close, err := s.open(false)
	if err != nil {
		return ActivationReceipt{}, err
	}
	defer close()
	var r ActivationReceipt
	if err := readPrivate(root, activationName(id), &r); err != nil {
		return r, err
	}
	if r.ID != id || r.Version != 1 || !validID(r.Candidate) || !validID(r.ExpectedPrimary) || r.Protocol != 1 || r.L1OperationID == "" || !hostboot.ValidIdentity(r.SystemBootID) {
		return r, &VerificationRefusal{Message: "invalid activation receipt"}
	}
	return r, nil
}

func (s Store) promotionWritten(phase string) error {
	if s.afterPromotionWrite != nil {
		return s.afterPromotionWrite(phase)
	}
	return nil
}

func canonicalPromotionEvidence(e PromotionEvidence) PromotionEvidence {
	e.Residents = maps.Clone(e.Residents)
	for id, residents := range e.Residents {
		if residents == nil {
			continue
		}
		copy := slices.Clone(residents)
		slices.SortFunc(copy, func(a, b ResidentAttempt) int { return strings.Compare(a.AttemptID, b.AttemptID) })
		e.Residents[id] = copy
	}
	return e
}
