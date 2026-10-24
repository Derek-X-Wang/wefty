package agentinstall

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRollingCatalogValidationClassification(t *testing.T) {
	for _, name := range []string{"catalog", "pending-receipt", "ownership", "process", "plist-digest", "primary", "role", "protocols", "protocol", "primary-count", "directory-mode", "record-mode", "decode", "missing"} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			g := f.prepare(t, true)
			c, err := f.s.Read()
			must(t, err)
			switch name {
			case "catalog":
				c.Revision = 0
			case "pending-receipt":
				c.PendingReceipt = "bad"
			case "ownership":
				g.ID = "bad"
			case "process":
				g.Process.PID = 0
			case "plist-digest":
				g.PlistDigest = "bad"
			case "primary":
				c.Primary = strings.Repeat("0", 32)
			case "role":
				g.Role = "foreign"
			case "protocols":
				g.ProtocolVersions = nil
			case "protocol":
				g.ProtocolVersions = []int{0}
			case "primary-count":
				g.Role = "retiring"
			}
			c.Generations[strings.TrimPrefix(g.Label, "dev.wefty.node-agent.g-")] = g
			body, err := json.Marshal(c)
			must(t, err)
			path := filepath.Join(f.s.Root(), catalogName)
			must(t, os.WriteFile(path, body, 0600))
			switch name {
			case "directory-mode":
				must(t, os.Chmod(f.s.Root(), 0755))
			case "record-mode":
				must(t, os.Chmod(path, 0644))
			case "decode":
				must(t, os.WriteFile(path, []byte("{"), 0600))
			case "missing":
				must(t, os.Remove(path))
			}
			_, err = f.s.Read()
			var refusal *VerificationRefusal
			if name == "decode" {
				var syntax *json.SyntaxError
				if errors.As(err, &refusal) || !errors.As(err, &syntax) {
					t.Fatalf("decode failure reclassified: %v", err)
				}
			} else if name == "missing" {
				if errors.As(err, &refusal) || !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("read failure reclassified: %v", err)
				}
			} else if !errors.As(err, &refusal) {
				t.Fatalf("validation refusal not typed: %v", err)
			}
		})
	}
}

func TestRollingLoadedDefinitionReadFailureKeepsCause(t *testing.T) {
	f := newFixture(t)
	g := f.prepare(t, true)
	must(t, os.Remove(g.Plist))
	_, err := f.s.LoadCandidate(t.Context(), g.ID, f.base)
	var refusal *VerificationRefusal
	if errors.As(err, &refusal) || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("loaded definition read failure reclassified: %v", err)
	}
}

func TestRollingRecordPendingRequiresVerifiedStorage(t *testing.T) {
	f := newFixture(t)
	g := f.prepare(t, true)
	request := ControlRequest{ID: "operation", RequestID: "pending", GenerationID: g.ID, NodeID: "node", BootSessionID: g.Process.BootSessionID, Action: "generation_inventory"}
	_, err := f.s.ReadControlReply(request)
	var pending *RecordPendingError
	if !errors.As(err, &pending) || !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unpublished control reply is not pending", err)
	}
	_, err = f.s.ReadRetirement(g.ID)
	if !errors.As(err, &pending) {
		t.Fatal("unpublished retirement ack is not pending", err)
	}
	must(t, os.Remove(filepath.Join(f.s.Root(), g.ID, "ready.json")))
	_, err = f.s.BindReady(t.Context(), g.ID, *g.Process, g.CreatedAt)
	if !errors.As(err, &pending) {
		t.Fatal("unpublished ready record is not pending", err)
	}
	must(t, os.Remove(filepath.Join(f.s.Root(), catalogName)))
	for name, read := range map[string]func() error{
		"control":    func() error { _, err := f.s.ReadControlReply(request); return err },
		"retirement": func() error { _, err := f.s.ReadRetirement(g.ID); return err },
		"ready":      func() error { _, err := f.s.BindReady(t.Context(), g.ID, *g.Process, g.CreatedAt); return err },
	} {
		err := read()
		if errors.As(err, &pending) || !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s missing storage became pending publication: %v", name, err)
		}
	}
}
