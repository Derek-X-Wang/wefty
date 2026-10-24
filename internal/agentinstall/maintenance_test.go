package agentinstall

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

func TestMaintenanceReceiptCleanupRetries(t *testing.T) {
	for _, boundary := range []string{"file_removed:ready.json", "file_removed:wefty-agent", "files_removed", "catalog_retired"} {
		t.Run(boundary, func(t *testing.T) {
			f := newFixture(t)
			g := f.prepare(t, true)
			f.lifetimes[g.ID]()
			delete(f.lifetimes, g.ID)
			must(t, os.WriteFile(filepath.Join(f.s.Root(), "maintenance.json"), []byte("unknown root evidence"), 0600))
			c, err := f.s.Read()
			must(t, err)
			m, err := f.s.BeginMaintenance("convert", "operation", c)
			must(t, err)
			unknown := filepath.Join(f.s.Root(), g.ID, "keep-evidence")
			must(t, os.WriteFile(unknown, []byte("unknown evidence"), 0600))
			must(t, f.s.StopMaintenancePrimary(t.Context(), m))
			f.s.MaintenanceStep = func(step string) error {
				if step == boundary {
					return errors.New("injected death")
				}
				return nil
			}
			if _, err = f.s.RetireMaintenance(t.Context(), m); err == nil {
				t.Fatal("missing interruption")
			}
			f.s.MaintenanceStep = nil
			m, err = f.s.ReadMaintenance()
			must(t, err)
			_, err = f.s.RetireMaintenance(t.Context(), m)
			must(t, err)
			b, err := os.ReadFile(filepath.Join(f.s.Data, m.Archive(), g.ID, "keep-evidence"))
			must(t, err)
			if string(b) != "unknown evidence" {
				t.Fatal("unknown evidence changed")
			}
			b, err = os.ReadFile(filepath.Join(f.s.Data, m.Archive(), "maintenance.json"))
			must(t, err)
			if string(b) != "unknown root evidence" {
				t.Fatal("unknown root audit name was overwritten")
			}

			if _, err = os.Stat(f.s.Root()); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("catalog still active", err)
			}
			if _, err = os.Stat(filepath.Join(f.s.Data, m.Archive(), g.ID, "wefty-agent")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("owned executable remains", err)
			}
		})
	}
}

func TestMaintenanceQuietCatalogRejectsLiveCohort(t *testing.T) {
	f := newFixture(t)
	f.prepare(t, true)
	f.prepare(t, false)
	_, err := f.s.CheckMaintenance(nil)
	var refusal *MaintenanceRefusal
	if !errors.As(err, &refusal) || !strings.Contains(err.Error(), "staged") {
		t.Fatal("candidate accepted", err)
	}
}

// Use the production closure writer and cleanup path: retained JSON has a
// newline, while #1180 binds the canonical marshaled L1 closure as its proof.
func TestMaintenanceAcceptsCanonicalClosedGenerationAudit(t *testing.T) {
	f := newFixture(t)
	old := f.prepare(t, true)
	primary := f.prepare(t, false)
	f.promote(t, primary)
	c, err := f.s.Read()
	must(t, err)
	old = c.Generations[old.ID]
	closedAt := time.Now().UTC()
	closed := l1.NodeGeneration{AgentGenerationID: old.ID, BootSessionID: old.Process.BootSessionID, Role: l1.NodeGenerationClosed, PromotionOperationID: "promotion", ClosedAt: &closedAt, ResidentAttempts: []l1.ResidentAttempt{}}
	must(t, f.s.PublishClosedGeneration(closed))
	f.lifetimes[old.ID]()
	delete(f.lifetimes, old.ID)
	state := f.f.states[old.Label]
	state.Running, state.PID = false, 0
	f.f.states[old.Label] = state
	must(t, f.s.CleanupClosed(t.Context(), l1.NodeGenerationInventory{NodeID: "node", Generations: []l1.NodeGeneration{closed}}))
	quiet, err := f.s.CheckMaintenance(nil)
	if err != nil || quiet == nil || quiet.Primary != primary.ID {
		t.Fatalf("completed rolling cleanup is not accepted as quiet: %v", err)
	}
	root, close, err := f.s.open(false)
	must(t, err)
	var receipt completionReceipt
	must(t, readPrivate(root, "finished-"+old.ID+".json", &receipt))
	// A digest alone cannot invent positive closure. Retain the exact matching
	// canonical digest but remove the authoritative closure timestamp.
	closed.ClosedAt = nil
	payload, err := json.Marshal(closed)
	must(t, err)
	receipt.ControlFiles["retirement-closed.json"] = append(payload, '\n')
	receipt.Proof.EvidenceDigest = digestBytes(payload)
	must(t, writePrivate(root, "finished-"+old.ID+".json", receipt))
	close()
	_, err = f.s.CheckMaintenance(nil)
	var refusal *MaintenanceRefusal
	if !errors.As(err, &refusal) {
		t.Fatal("missing positive closure accepted", err)
	}
}

func TestMaintenanceAttestedBindingRemainsFenced(t *testing.T) {
	f := newFixture(t)
	g := f.prepare(t, true)
	c, err := f.s.Read()
	must(t, err)
	m, err := f.s.BeginMaintenance("remove", "operation", c)
	must(t, err)
	request := l1.DeEnrollNodeGenerationRequest{OperationID: m.Operation, PrimaryGenerationID: g.ID, PrimaryBootSessionID: g.Process.BootSessionID, AuthorityGeneration: 1, CatalogRevision: 1, IntentRevision: 1, IntentReason: "wefty maintenance " + m.Operation, RuntimesReaped: true, EvidenceSettled: true}
	m.Request = &request
	must(t, f.s.SaveMaintenance(m))
	must(t, f.s.PrepareDeEnrollment(c.NodeID, request))
	f.starts[os.Getpid()] = "replacement-process"
	must(t, f.s.PublishReady(g.ID, c.NodeID, g.Version, g.Executable, "replacement-boot", testBoot))
	_, err = f.s.ReadMaintenance()
	var refusal *MaintenanceRefusal
	if !errors.As(err, &refusal) {
		t.Fatal("attested operation accepted a different process binding", err)
	}
	saved, err := f.s.ReadDeEnrollment()
	must(t, err)
	if saved == nil || saved.Request != request || saved.Result != nil {
		t.Fatal("strict refusal changed the attestation")
	}
}

func TestMaintenancePreservationRejectsUnsafeRecords(t *testing.T) {
	for _, kind := range []string{"invalid-json", "wide-mode", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			path := filepath.Join(f.s.Data, PreservationFile)
			switch kind {
			case "invalid-json":
				must(t, os.WriteFile(path, []byte("{"), 0600))
			case "wide-mode":
				must(t, os.WriteFile(path, []byte("{}"), 0644))
			case "symlink":
				must(t, os.Symlink(f.source, path))
			}
			if preserved, err := f.s.HasPreservedGenerationData(); err == nil || preserved {
				t.Fatal("unsafe preservation record passed", preserved, err)
			}
		})
	}
}
