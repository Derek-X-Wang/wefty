package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/upgradecompat"
	"github.com/Derek-X-Wang/wefty/l1"
)

func classificationOperations(t *testing.T, f *rollingFixture) (upgradeHost, connprofile.Profile) {
	t.Helper()
	p, err := connprofile.Read(f.paths.profile)
	mustRolling(t, err)
	op := upgradeOperations(f.host)
	op.generations = nil
	host := f.host
	host.upgrade = &op
	return upgradeOperations(host), p
}

func TestUpgradeRollingInventoryStorageFailureKeepsCause(t *testing.T) {
	for _, name := range []string{"missing-catalog", "root-stat"} {
		t.Run(name, func(t *testing.T) {
			f := newRollingFixture(t)
			op, p := classificationOperations(t, f)
			mustRolling(t, os.Remove(filepath.Join(f.install.Root(), "catalog.json")))
			var cause error = os.ErrNotExist
			if name == "root-stat" {
				cause = &os.PathError{Op: "lstat", Path: f.install.Root(), Err: syscall.EIO}
				op.generationRootStat = func(string) (os.FileInfo, error) { return nil, cause }
			}
			// Use the production inventory caller, including its legacy-discovery branch.
			_, err := generationUpgradeControl(t.Context(), f.host, op, p, "generation_inventory", nil)
			assertUpgradeControlOperational(t, err)
			if !errors.Is(err, cause) {
				t.Fatalf("filesystem cause discarded: %v", err)
			}
		})
	}
}

func TestUpgradeRollingCatalogContradictionIsOperational(t *testing.T) {
	f := newRollingFixture(t)
	op, p := classificationOperations(t, f)
	c, err := f.install.Read()
	mustRolling(t, err)
	c.Primary = strings.Repeat("0", 32)
	body, err := json.Marshal(c)
	mustRolling(t, err)
	mustRolling(t, os.WriteFile(filepath.Join(f.install.Root(), "catalog.json"), body, 0600))
	_, err = op.generations(t.Context(), p)
	assertUpgradeControlOperational(t, err)
	if !strings.Contains(err.Error(), "conflicting generation primary") {
		t.Fatal("catalog contradiction lost", err)
	}
}

func TestUpgradeControlTypedBarrierKeepsCLIDiagnostics(t *testing.T) {
	f := newRollingFixture(t)
	refusal := &agent.ProtocolError{StatusCode: 409, APIError: contract.APIError{Code: contract.ErrorConflict, Message: "positive runtime reap is unavailable", Details: map[string]any{"reason": string(upgradecompat.InvalidInventory)}}}
	failure := &agent.UpgradeServiceQuiescenceError{JobID: "blocked-job", AttemptID: "blocked-attempt", Phase: "reap", Cause: refusal}
	r := agentinstall.ControlRequest{ID: "operation", RequestID: "typed-barrier", NodeID: f.node.NodeID, GenerationID: f.primary.ID, BootSessionID: f.node.BootSessionID, Action: "quiesce_services"}
	// Production reply encoder, actual private file exchange, production CLI reader.
	mustRolling(t, f.install.PublishControl(agent.NewUpgradeControlResult(r, nil, failure)))
	_, err := requestGenerationControl(context.Background(), f.install, r)
	assertUpgradeControlCLIConflict(t, err)
	for _, jsonOutput := range []bool{false, true} {
		var out bytes.Buffer
		decideCommandReport(err).writeError(&out, jsonOutput)
		for _, part := range []string{"blocked-job", "blocked-attempt", "phase reap", "pending=false", "positive runtime reap is unavailable"} {
			if !strings.Contains(out.String(), part) {
				t.Errorf("json=%t lost %q: %s", jsonOutput, part, &out)
			}
		}
	}
}

func TestUpgradeRollingClassificationSweepOperational(t *testing.T) {
	for _, name := range []string{"profile-response", "profile-command", "protocol-command", "protocol-decode", "inventory", "unsupported-inventory", "readiness-manager", "retirement-decode"} {
		t.Run(name, func(t *testing.T) {
			f := newRollingFixture(t)
			op, p := classificationOperations(t, f)
			cause := &os.PathError{Op: "read", Path: "rolling-classification", Err: syscall.EIO}
			var err error
			switch name {
			case "readiness-manager":
				store := f.install
				store.Manager.Run = func(context.Context, string, ...string) ([]byte, error) { return nil, cause }
				r := rollingUpgrade{op: op, store: store, receipt: upgradeRollingReceipt{SystemBootID: rollingTestBoot}}
				err = r.waitReady(t.Context(), f.primary, op.now())
			case "retirement-decode":
				mustRolling(t, os.WriteFile(filepath.Join(f.install.Root(), f.primary.ID, "retirement-ack.json"), []byte("{"), 0600))
				r := rollingUpgrade{op: op, store: f.install}
				err = r.waitRetirement(t.Context(), l1.NodeGenerationInventory{Generations: []l1.NodeGeneration{{Role: l1.NodeGenerationRetiring, AgentGenerationID: f.primary.ID}}})
			default:
				command := op.command
				op.command = func(ctx context.Context, dir string, env []string, path string, args ...string) ([]byte, error) {
					if name == "profile-response" && slices.Contains(args, "--profile-preflight") {
						return []byte("unexpected successful reply"), nil
					}
					if name == "profile-command" && slices.Contains(args, "--profile-preflight") {
						return nil, cause
					}
					if strings.HasPrefix(name, "protocol-") && slices.Contains(args, "--retirement-protocols") {
						if name == "protocol-decode" {
							return []byte("{"), nil
						}
						return nil, cause
					}
					return command(ctx, dir, env, path, args...)
				}
				if strings.Contains(name, "inventory") {
					op.generations = func(context.Context, connprofile.Profile) (l1.NodeGenerationInventory, error) {
						return l1.NodeGenerationInventory{}, cause
					}
				}
				if name == "unsupported-inventory" {
					err = refuseUnsupportedRolling(t.Context(), f.host, f.paths, op, p, upgradecompat.OCIMac)
				} else {
					_, _, err = prepareRollingUpgrade(t.Context(), f.host, op, f.paths, p, f.node, f.paths.bin, f.commit, f.loginPATH)
				}
			}
			assertUpgradeControlOperational(t, err)
			if name == "retirement-decode" || name == "protocol-decode" {
				var syntax *json.SyntaxError
				if !errors.As(err, &syntax) && !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Errorf("decode cause discarded: %v", err)
				}
			} else if name != "profile-response" && !errors.Is(err, cause) {
				t.Errorf("operational cause discarded: %v", err)
			}
		})
	}
}

func TestUpgradeControlActivationValidationKeepsConflictExit(t *testing.T) {
	c := newRollingControlFixture(t, nil)
	c.preparePromotion(t)
	catalog, err := c.install.Read()
	mustRolling(t, err)
	path := filepath.Join(c.install.Root(), "activation-"+catalog.PendingReceipt+".json")
	receipt, err := c.install.ReadActivationReceipt(catalog.PendingReceipt)
	mustRolling(t, err)
	receipt.Candidate = "invalid-owned-candidate"
	body, err := json.Marshal(receipt)
	mustRolling(t, err)
	mustRolling(t, os.WriteFile(path, body, 0600))
	_, err = c.op.promoteGeneration(t.Context(), c.profile, c.request)
	assertUpgradeControlCLIConflict(t, err)
}

func TestUpgradeRollingInstallerMixedCauseRemainsOperational(t *testing.T) {
	f := newRollingFixture(t)
	op, p := classificationOperations(t, f)
	cause := &os.PathError{Op: "read", Path: "catalog", Err: syscall.EIO}
	op.generations = func(context.Context, connprofile.Profile) (l1.NodeGenerationInventory, error) {
		return l1.NodeGenerationInventory{}, errors.Join(&agentinstall.VerificationRefusal{Message: "inventory mismatch"}, cause)
	}
	_, _, err := prepareRollingUpgrade(t.Context(), f.host, op, f.paths, p, f.node, f.paths.bin, f.commit, f.loginPATH)
	assertUpgradeControlOperational(t, err, "inventory mismatch", "input/output error")
	if !errors.Is(err, cause) {
		t.Fatal("joined operational cause lost", err)
	}
}

func TestUpgradeRollingPendingReadStorageLossKeepsCause(t *testing.T) {
	for _, name := range []string{"readiness", "retirement", "control-reply"} {
		t.Run(name, func(t *testing.T) {
			f := newRollingFixture(t)
			op, _ := classificationOperations(t, f)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			catalogPath := filepath.Join(f.install.Root(), "catalog.json")
			r := rollingUpgrade{op: op, store: f.install, receipt: upgradeRollingReceipt{SystemBootID: rollingTestBoot}}
			var err error
			if name == "control-reply" {
				done := make(chan error, 1)
				go func() {
					_, err := requestGenerationControl(ctx, f.install, agentinstall.ControlRequest{ID: "operation", RequestID: "storage-disappears", NodeID: f.node.NodeID, GenerationID: f.primary.ID, BootSessionID: f.node.BootSessionID, Action: "generation_inventory"})
					done <- err
				}()
				requestPath := filepath.Join(f.install.Root(), f.primary.ID, "control-request.json")
				for {
					if _, statErr := os.Stat(requestPath); statErr == nil {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal("private request was not written", ctx.Err())
					case <-time.After(10 * time.Millisecond):
					}
				}
				mustRolling(t, os.Remove(catalogPath))
				err = <-done
			} else {
				mustRolling(t, os.Remove(catalogPath))
				if name == "readiness" {
					err = r.waitReady(ctx, f.primary, op.now())
				} else {
					err = r.waitRetirement(ctx, l1.NodeGenerationInventory{Generations: []l1.NodeGeneration{{Role: l1.NodeGenerationRetiring, AgentGenerationID: f.primary.ID}}})
				}
			}
			assertUpgradeControlOperational(t, err)
			if !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("missing catalog became a pending-record timeout: %v", err)
			}
		})
	}
}
