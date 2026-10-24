package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/l1"
)

func TestUpgradeRollingRunUpgradeCatalogRefusalsKeepConflictExit(t *testing.T) {
	for _, stage := range []string{"before-plist", "after-installation"} {
		for _, kind := range []string{"primary", "ownership"} {
			t.Run(stage+"/"+kind, func(t *testing.T) {
				f := newRollingFixture(t)
				corrupted := false
				corrupt := func() {
					c, err := f.install.Read()
					mustRolling(t, err)
					if kind == "primary" {
						c.Primary = strings.Repeat("0", 32)
					} else {
						g := c.Generations[c.Primary]
						g.Label = "unowned-label"
						c.Generations[c.Primary] = g
					}
					body, err := json.Marshal(c)
					mustRolling(t, err)
					mustRolling(t, os.WriteFile(filepath.Join(f.install.Root(), "catalog.json"), body, 0600))
					corrupted = true
				}
				if stage == "before-plist" {
					corrupt()
				} else {
					f.operations.rollingStep = func(phase string) error {
						if phase == "installation_completing" {
							corrupt()
						}
						return nil
					}
				}
				_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
				if !corrupted {
					t.Fatal("did not reach the catalog-read boundary")
				}
				assertUpgradeControlCLIConflict(t, err)
			})
		}
	}
}

func TestUpgradeRollingSharedIntentRefusalKeepsGateReason(t *testing.T) {
	// Each phase precedes a distinct rolling caller of the shared drain guard.
	for _, phase := range []string{"admission_closed", "control_plane_renewed", "candidate_ready"} {
		t.Run(phase, func(t *testing.T) {
			f := newRollingFixture(t)
			changed := false
			f.operations.rollingStep = func(got string) error {
				if got == phase {
					node, err := f.h.store.GetNode(t.Context(), f.node.NodeID)
					if err != nil {
						return err
					}
					_, err = f.h.store.SetNodeClaimsByOperator(t.Context(), node.NodeID, "operator", l1.NodeIntentRequest{ClaimsEnabled: false, IntentRevision: node.IntentRevision, Reason: "operator changed intent"})
					if err != nil {
						return err
					}
					changed = true
				}
				return nil
			}
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			if !changed {
				t.Fatal("did not reach the rolling intent fence")
			}
			assertUpgradeControlCLIConflict(t, err)
		})
	}
}
