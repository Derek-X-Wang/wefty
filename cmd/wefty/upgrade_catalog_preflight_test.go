package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

func installedCatalogFixture(t *testing.T, rolling bool) (*upgradeFixture, setupPaths, agentinstall.Store) {
	t.Helper()
	f := newUpgradeFixture(t)
	paths, err := resolveSetupPaths(f.home)
	mustRolling(t, err)
	// This is a complete installed plain profile, not a missing-profile guard fixture.
	profile, err := connprofile.Read(paths.profile)
	mustRolling(t, err)
	if profile.Identities.Node != f.node.NodeID || profile.Fabric != "plain" {
		t.Fatal("fixture is not an installed plain node")
	}
	store := agentinstall.Store{Data: paths.data, LaunchAgents: paths.launchAgents}
	_, err = store.Initialize(f.node.NodeID, filepath.Join(paths.bin, "wefty-agent"), "v1", rollingTestBoot, []int{1})
	mustRolling(t, err)
	_, err = store.Read()
	mustRolling(t, err)
	f.operations.requestRolling = rolling
	return f, paths, store
}

func breakUpgradeCatalog(t *testing.T, store agentinstall.Store, kind string) {
	t.Helper()
	path := filepath.Join(store.Root(), "catalog.json")
	switch kind {
	case "missing":
		mustRolling(t, os.Remove(path))
	case "malformed":
		mustRolling(t, os.WriteFile(path, []byte("{"), 0600))
	case "structural":
		mustRolling(t, os.WriteFile(path, []byte("{}"), 0600))
	case "unreadable":
		mustRolling(t, os.Chmod(path, 0000))
		t.Cleanup(func() { _ = os.Chmod(path, 0600) })
	default:
		t.Fatal(kind)
	}
}

func TestUpgradeCatalogPreflightPreservesInstalledState(t *testing.T) {
	for _, rolling := range []bool{false, true} {
		route := "plain"
		if rolling {
			route = "rolling"
		}
		for _, when := range []string{"entry", "under-lock"} {
			for _, kind := range []string{"missing", "malformed", "structural", "unreadable"} {
				t.Run(route+"/"+when+"/"+kind, func(t *testing.T) {
					f, paths, store := installedCatalogFixture(t, rolling)
					stage := filepath.Join(paths.data, ".upgrade-retained", "wefty-agent")
					mustRolling(t, os.MkdirAll(filepath.Dir(stage), 0700))
					mustRolling(t, os.WriteFile(stage, []byte("keep staged evidence"), 0700))
					f.node.ClaimsEnabled = false
					f.node.State = contract.NodeDraining
					f.node.IntentRevision = 1
					f.node.IntentReason = "wefty upgrade interrupted"
					receipt := &upgradeDrainReceipt{Version: 1, Home: f.home, NodeID: f.node.NodeID, Revision: 1, Reason: f.node.IntentReason, RestoreClaims: true, OldBoot: f.node.BootSessionID, Services: []l1.NodeActiveAttempt{{JobID: "service", AttemptID: "old-attempt", Class: contract.JobClassService}}}
					mustRolling(t, writeUpgradeDrain(paths.config, receipt))
					drainPath := filepath.Join(paths.config, upgradeDrainFile)
					beforeDrain, err := os.ReadFile(drainPath)
					mustRolling(t, err)
					beforeFiles := installedUpgradeFiles(t, f)
					beforeNode := f.node
					mutations := 0
					set := f.operations.setClaims
					f.operations.setClaims = func(ctx context.Context, p connprofile.Profile, r l1.NodeIntentRequest) (l1.Node, error) {
						mutations++
						return set(ctx, p, r)
					}
					f.operations.readJob = func(context.Context, connprofile.Profile, string) (l1.Job, error) {
						return l1.Job{JobID: "service", State: contract.JobFailed, CurrentAttemptID: "old-attempt", ServiceJob: &l1.ServiceJob{DesiredState: contract.ServiceDesiredRunning, BoundNodeID: f.node.NodeID}}, nil
					}
					f.operations.restartService = func(context.Context, connprofile.Profile, string, string) (l1.Job, error) {
						mutations++
						return l1.Job{}, nil
					}
					reached := when == "entry"
					if when == "entry" {
						breakUpgradeCatalog(t, store, kind)
					} else {
						calls := 0
						f.operations.generationStore = func(setupPaths) agentinstall.Store {
							calls++
							if calls == 2 {
								if !setupLockHeld(paths) {
									t.Fatal("revalidation is outside installation lock")
								}
								reached = true
								breakUpgradeCatalog(t, store, kind)
							}
							return store
						}
					}
					f.launchd.commands = nil
					_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
					if !reached {
						t.Fatal("did not reach installed catalog boundary", err)
					}
					if err == nil || commandExitCode(err) != exitFailure {
						t.Errorf("want operational catalog failure, exit=%d err=%v", commandExitCode(err), err)
					}
					if payload, e := os.ReadFile(stage); e != nil || string(payload) != "keep staged evidence" {
						t.Errorf("staging changed before validated enrollment: %s %v", payload, e)
					}
					if payload, e := os.ReadFile(drainPath); e != nil || !bytes.Equal(payload, beforeDrain) {
						t.Errorf("drain receipt changed before catalog validation: %v", e)
					}
					if mutations != 0 || !reflect.DeepEqual(beforeNode, f.node) {
						t.Errorf("admission/service recovery ran before validation: mutations=%d", mutations)
					}
					if len(f.launchd.mutating()) != 0 || !reflect.DeepEqual(beforeFiles, installedUpgradeFiles(t, f)) {
						t.Error("installed files/services changed before validation")
					}
				})
			}
		}
	}
}

func TestUpgradeCatalogReadFailuresAreOperational(t *testing.T) {
	for _, rolling := range []bool{false, true} {
		route := "plain"
		if rolling {
			route = "rolling"
		}
		for _, kind := range []string{"missing", "malformed", "structural", "unreadable"} {
			t.Run(route+"/"+kind, func(t *testing.T) {
				f, _, store := installedCatalogFixture(t, rolling)
				breakUpgradeCatalog(t, store, kind)
				_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
				if err == nil || commandExitCode(err) != exitFailure {
					t.Fatalf("want operational fault, exit=%d err=%v", commandExitCode(err), err)
				}
				var out bytes.Buffer
				decideCommandReport(err).writeError(&out, true)
				if !strings.Contains(out.String(), `"code": "internal"`) {
					t.Fatal("wrong JSON error classification", out.String())
				}
				switch kind {
				case "missing":
					if !errors.Is(err, os.ErrNotExist) {
						t.Fatal("lost missing-file cause", err)
					}
				case "malformed":
					var syntax *json.SyntaxError
					if !errors.As(err, &syntax) {
						t.Fatal("lost JSON cause", err)
					}
				case "structural":
					var refused *agentinstall.VerificationRefusal
					if !errors.As(err, &refused) {
						t.Fatal("lost catalog validation cause", err)
					}
				case "unreadable":
					if !errors.Is(err, os.ErrPermission) {
						t.Fatal("lost permission cause", err)
					}
				}
			})
		}
	}
}

// These paths fail before build, Fabric access, launchd or host-boot reads.
func TestUpgradeCatalogRealBinaryOperationalExit(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("plain installation is Mac-only")
	}
	binary := buildWefty(t)
	for _, rolling := range []bool{false, true} {
		route := "plain"
		if rolling {
			route = "rolling"
		}
		for _, kind := range []string{"missing", "malformed", "structural", "unreadable"} {
			t.Run(route+"/"+kind, func(t *testing.T) {
				f, paths, store := installedCatalogFixture(t, rolling)
				breakUpgradeCatalog(t, store, kind)
				args := []string{"--json", "upgrade"}
				if rolling {
					args = append(args, "--rolling")
				}
				code, output := runWeftyWithEnv(t, binary, 15*time.Second, []string{"HOME=" + f.home, "WEFTY_PROFILE=" + paths.profile}, args...)
				if code != exitFailure || !strings.Contains(output, `"code": "internal"`) {
					t.Fatalf("exit=%d output=%s", code, output)
				}
			})
		}
	}
}
