package agentinstall

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/durable"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
)

// PromotionEvidence comes from verified L1 inventory under closed admission.
type PromotionEvidence struct {
	IntentRevision      int64                        `json:"intent_revision"`
	L1CatalogRevision   int64                        `json:"l1_catalog_revision"`
	Residents           map[string][]ResidentAttempt `json:"resident_attempts"`
	ServiceRecoveryKeys map[string]string            `json:"service_recovery_keys"`
}

// ActivationReceipt records installation intent, not L1 authority. The caller
// must reconcile its exact operation result. Pending intent fences startup.
type ActivationReceipt struct {
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
	g, ok := c.Generations[candidate]
	if !ok || g.Role != "" || g.Process == nil || c.PendingReceipt != "" || c.Revision != expectedRevision || operation == "" || !hostboot.SameBoot(g.Process.SystemBootID, boot) || protocol != 1 {
		return ActivationReceipt{}, errors.New("promotion requires a ready candidate, current catalog and protocol 1")
	}
	if evidence.IntentRevision <= 0 || evidence.L1CatalogRevision < 0 || len(evidence.Residents) != len(c.Generations) {
		return ActivationReceipt{}, errors.New("promotion requires complete verified resident inventory and intent revision")
	}
	id, err := newID()
	if err != nil {
		return ActivationReceipt{}, err
	}
	r := ActivationReceipt{PromotionEvidence: evidence, Processes: map[string]Binding{}, Version: 1, ID: id, ExpectedPrimary: c.Primary, Candidate: candidate, CatalogRevision: c.Revision, SystemBootID: boot, Protocol: protocol, Digests: map[string]string{}, L1OperationID: operation}
	r.Residents = map[string][]ResidentAttempt{}
	r.ServiceRecoveryKeys = maps.Clone(evidence.ServiceRecoveryKeys)
	seenAttempts := map[string]bool{}
	for id, g := range c.Generations {
		if g.Role == "" && id != candidate {
			return r, errors.New("another staged candidate requires reconciliation")
		}
		if !slices.Contains(g.ProtocolVersions, protocol) {
			return r, errors.New("protocol is not supported by the entire generation cohort")
		}
		if g.Process == nil || !hostboot.SameBoot(g.Process.SystemBootID, boot) {
			return r, errors.New("generation process binding missing")
		}
		residents, observed := evidence.Residents[id]
		if !observed || residents == nil {
			return r, errors.New("missing resident inventory; verified idle must be an empty array")
		}
		residents = append([]ResidentAttempt{}, residents...)
		slices.SortFunc(residents, func(a, b ResidentAttempt) int { return strings.Compare(a.AttemptID, b.AttemptID) })
		if g.Role == "retiring" && !slices.Equal(g.ResidentAttempts, residents) {
			return r, errors.New("retiring resident membership changed")
		}
		for _, a := range residents {
			if a.JobID == "" || a.AttemptID == "" || a.FencingToken == "" || a.AuthorityGeneration <= 0 || a.BootSessionID != g.Process.BootSessionID || seenAttempts[a.AttemptID] {
				return r, errors.New("invalid resident attempt identity")
			}
			seenAttempts[a.AttemptID] = true
		}
		if id == candidate && len(residents) != 0 {
			return r, errors.New("staged candidate has resident work")
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
	if err := writePrivate(root, activationName(id), r); err != nil {
		return r, err
	}
	c.PendingReceipt = id
	c.Revision++
	return r, writePrivate(root, catalogName, c)
}

// CommitPromotion is called only after #1179 has verified the exact L1 result.
// #1180 owns interrupted-cutover reconciliation. Any error leaves pending intent
// in place and startup closed. Retiring labels stay loaded and keep their PIDs.
func (s Store) CommitPromotion(ctx context.Context, receiptID, operation, boot string, l1Revision int64) error {
	if !validID(receiptID) {
		return errors.New("invalid activation receipt ID")
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
	if c.PendingReceipt != receiptID || r.ID != receiptID || r.Version != 1 || r.Committed || c.Primary != r.ExpectedPrimary || c.Revision != r.CatalogRevision+1 || r.L1OperationID != operation || l1Revision <= 0 || l1Revision != r.L1CatalogRevision+1 || r.Protocol != 1 || !hostboot.SameBoot(r.SystemBootID, boot) || len(r.Digests) != len(c.Generations) {
		return errors.New("activation receipt or promotion result mismatch")
	}
	retirees := []string{}
	for id, g := range c.Generations {
		if r.Digests[id] != g.Digest || g.Process == nil || r.Processes[id] != *g.Process || !slices.Equal(r.Residents[id], g.ResidentAttempts) {
			return errors.New("activation cohort changed")
		}
		if g.Role == "retiring" {
			retirees = append(retirees, id)
		}
	}
	slices.Sort(retirees)
	if !slices.Equal(retirees, r.Retiring) {
		return errors.New("activation retiree set changed")
	}
	candidate, ok := c.Generations[r.Candidate]
	if !ok || candidate.Process == nil || candidate.Role != "" || r.Candidate == r.ExpectedPrimary {
		return errors.New("candidate process binding missing")
	}
	if _, err := s.ready(ctx, root, c, candidate, *candidate.Process, candidate.Process.ReadyAt); err != nil {
		return err
	}
	folder, err := os.OpenRoot(s.LaunchAgents)
	if err != nil {
		return err
	}
	defer folder.Close()
	now := time.Now().UTC()
	for id, g := range c.Generations {
		if id == r.Candidate {
			continue
		}
		// No bootstrap, kickstart, reload or bootout here. The launchd override is
		// persistent, and stale loaded KeepAlive is fenced by the agent entry point.
		if err := s.Manager.SuppressGenerationStart(ctx, g.Label); err != nil {
			return err
		}
		old, err := folder.ReadFile(filepath.Base(g.Plist))
		if err != nil {
			return err
		}
		info, err := folder.Lstat(filepath.Base(g.Plist))
		if err != nil {
			return err
		}
		if !privateInfo(info, 0644) || g.PlistDigest == "" || digestBytes(old) != g.PlistDigest {
			return errors.New("retained generation plist changed")
		}
		g.Role = "retiring"
		if g.RetiringAt.IsZero() {
			g.RetiringAt = now
		}
		g.Protocol = r.Protocol
		payload := bytes.Replace(old, []byte("  <key>RunAtLoad</key>\n  <true/>\n"), []byte("  <key>RunAtLoad</key>\n  <false/>\n"), 1)
		payload = bytes.Replace(payload, []byte("  <key>KeepAlive</key>\n  <dict>\n    <key>SuccessfulExit</key>\n    <false/>\n  </dict>\n"), []byte("  <key>KeepAlive</key>\n  <false/>\n"), 1)
		g.PlistDigest = digestBytes(payload)
		if err := durable.WriteFileMode(folder, filepath.Base(g.Plist), payload, 0644); err != nil {
			return err
		}
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
	r.Committed = true
	r.L1PromotionRevision = l1Revision
	if err := writePrivate(root, activationName(receiptID), r); err != nil {
		return err
	}
	c.PendingReceipt = ""
	c.Revision++
	return writePrivate(root, catalogName, c)
}

// ReadActivationReceipt exposes a private record to installation recovery, not
// to public reports. It also reads committed audit receipts after cleanup.
func (s Store) ReadActivationReceipt(id string) (ActivationReceipt, error) {
	if !validID(id) {
		return ActivationReceipt{}, errors.New("invalid activation receipt ID")
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
		return r, errors.New("invalid activation receipt")
	}
	return r, nil
}
