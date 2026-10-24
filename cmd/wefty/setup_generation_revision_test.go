package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestQuietCatalogPurgeRetryPreservesGenerationData(t *testing.T) {
	for _, archivedOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("archive_only=%t", archivedOnly), func(t *testing.T) { testQuietCatalogPurgeRetry(t, archivedOnly) })
	}
}
func testQuietCatalogPurgeRetry(t *testing.T, archivedOnly bool) {
	f := newQuietRemovalFixture(t)
	retained := []string{"l1.sqlite", "l1.sqlite-wal", "l3.sqlite", "node-agent/log-spool/shared.sqlite", "node-agent/custody/evidence"}
	for _, name := range retained {
		path := filepath.Join(f.paths.data, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("retained generation evidence"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	// Editing an installed file leaves the setup receipt for a later pass.
	if err := os.WriteFile(filepath.Join(f.paths.bin, "wefty"), []byte("owner edited installed executable"), 0700); err != nil {
		t.Fatal(err)
	}
	for pass := 1; pass <= 2; pass++ {
		if pass == 2 && archivedOnly {
			// Model an incomplete removal from before policy markers existed.
			if err := os.Remove(filepath.Join(f.paths.data, "agent-generation-preservation.json")); err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
		}
		report, err := runSetupRemove(t.Context(), f.host, true)
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if len(report.Warnings) == 0 {
			t.Fatalf("pass %d did not retain the edited installed file and receipt", pass)
		}
		for _, name := range retained {
			payload, err := os.ReadFile(filepath.Join(f.paths.data, name))
			if err != nil || string(payload) != "retained generation evidence" {
				t.Fatalf("pass %d deleted preserved %s: %q %v", pass, name, payload, err)
			}
		}
	}
}

func TestQuietCatalogRemovalReplacementBootBeforeAttestation(t *testing.T) {
	for _, boundary := range []string{"maintenance:admission_closed", "maintenance:de_enrollment_intent"} {
		t.Run(boundary, func(t *testing.T) {
			f := newQuietRemovalFixture(t)
			f.host.maintenanceStep = func(step string) error {
				if step == boundary {
					panic("process death")
				}
				return nil
			}
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("death boundary not reached")
					}
				}()
				_, _ = runSetupRemove(t.Context(), f.host, false)
			}()
			f.host.maintenanceStep = nil
			// Stop the primary's real private adapter and generation lifetime, then
			// register a replacement session at the real L1 and publish its binding.
			f.stopPrimaryControl()
			f.lifetimes[f.primary.ID].Close()
			delete(f.lifetimes, f.primary.ID)
			node, err := f.h.store.GetNode(t.Context(), f.node.NodeID)
			mustRolling(t, err)
			registration := node.NodeRegistration
			registration.BootSessionID = "replacement-boot"
			_, err = f.h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: node.NodeID}, registration, l1.DefaultNodePolicy(), true)
			mustRolling(t, err)
			originalLaunch := f.host.launchctl
			f.host.launchctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				output, err := originalLaunch(ctx, name, args...)
				if args[0] == "print" && err == nil && strings.HasSuffix(args[1], f.primary.Label) {
					return []byte(fmt.Sprintf("state = running\npid = %d\n", os.Getpid())), nil
				}
				return output, err
			}
			f.install.Manager = launchagent.Manager{Run: f.host.launchctl, UID: 501}
			lifetime, startErr := f.install.StartupWithLifetime(f.primary.ID, node.NodeID, f.primary.Executable, rollingTestBoot)
			mustRolling(t, startErr)
			f.lifetimes[f.primary.ID] = lifetime
			mustRolling(t, f.install.PublishReady(f.primary.ID, node.NodeID, f.primary.Version, f.primary.Executable, registration.BootSessionID, rollingTestBoot))
			exposeRollingL1(t, f)
			if _, err = runSetupRemove(t.Context(), f.host, false); err != nil {
				t.Fatal("replacement boot cannot resume removal:", err)
			}
			node, err = f.h.store.GetNode(t.Context(), f.node.NodeID)
			mustRolling(t, err)
			if node.AgentGenerationID != "" {
				t.Fatal("replacement primary remains enrolled")
			}
		})
	}
}

func TestQuietCatalogRemovalRequestPublicationFailure(t *testing.T) {
	for _, boundary := range []string{"request_write:before", "request_write:after"} {
		t.Run(boundary, func(t *testing.T) {
			f := newQuietRemovalFixture(t)
			fault := errors.New("injected request journal I/O failure")
			hit := false
			f.install.MaintenanceStep = func(step string) error {
				if step == boundary && !hit {
					hit = true
					return fault
				}
				return nil
			}
			control := f.operations.generationControl
			issued, resumed := 0, false
			f.operations.generationControl = func(ctx context.Context, store agentinstall.Store, r agentinstall.ControlRequest) (json.RawMessage, error) {
				if r.Action == "de_enroll_generation" {
					issued++
				}
				if r.Action == "resume_services" {
					resumed = true
				}
				return control(ctx, store, r)
			}
			_, err := runSetupRemove(t.Context(), f.host, false)
			if !hit || !errors.Is(err, fault) {
				t.Fatal("journal failure not returned:", err)
			}
			if issued != 0 {
				t.Fatal("issued de-enrollment after failed publication")
			}
			node, err := f.h.store.GetNode(t.Context(), f.node.NodeID)
			mustRolling(t, err)
			m, err := f.install.ReadMaintenance()
			mustRolling(t, err)
			if boundary == "request_write:before" {
				if !node.ClaimsEnabled || !resumed || m != nil {
					t.Fatalf("definite write failure skipped restoration: claims=%t resumed=%t journal=%+v", node.ClaimsEnabled, resumed, m)
				}
			} else {
				if node.ClaimsEnabled || resumed || m == nil || m.Request == nil {
					t.Fatal("uncertain publication discarded durable request or restored admission")
				}
				request := *m.Request
				f.install.MaintenanceStep = nil
				if _, err = runSetupRemove(t.Context(), f.host, false); err != nil {
					t.Fatal("published request retry:", err)
				}
				if issued != 1 {
					t.Fatal("retry did not issue exactly one request", issued)
				}
				if request.OperationID == "" {
					t.Fatal("missing operation identity")
				}
			}
		})
	}
}
