package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestSetupRemoveWithoutStateCreatesNothing(t *testing.T) {
	for _, purge := range []bool{false, true} {
		t.Run(map[bool]string{false: "remove", true: "purge"}[purge], func(t *testing.T) {
			f := newSetupFixture(t)
			if _, err := runSetupRemove(t.Context(), f.host, purge); err != nil {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(f.home)
			if err != nil || len(entries) != 0 {
				t.Fatalf("removal created folders in empty home: %v %v", entries, err)
			}
		})
	}
}

func TestUpgradeInvalidCloneNeedsReceiptOwnership(t *testing.T) {
	for _, receipt := range []string{"missing", "unowned", "invalid"} {
		t.Run(receipt, func(t *testing.T) {
			f := newUpgradeFixture(t)
			paths, _ := resolveSetupPaths(f.home)
			manifest := filepath.Join(paths.config, setupManifestName)
			if receipt == "missing" {
				if err := os.Remove(manifest); err != nil {
					t.Fatal(err)
				}
			} else if receipt == "invalid" {
				if err := os.WriteFile(manifest, []byte(`{"version":2,"upgrade_source":true,"files":{}}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			clone := filepath.Join(paths.data, upgradeSourceName)
			if err := os.Mkdir(clone, 0700); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(clone, "owner-data")
			if err := os.WriteFile(marker, []byte("keep"), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := runUpgrade(t.Context(), f.host, true, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "ownership") || !strings.Contains(err.Error(), "upgrade_source") {
				t.Fatalf("missing ownership refusal: %v", err)
			}
			if got, err := os.ReadFile(marker); err != nil || string(got) != "keep" {
				t.Fatalf("unowned clone deleted: %q %v", got, err)
			}
			if len(f.launchd.mutating()) != 0 {
				t.Fatal("ownership refusal restarted stack")
			}
		})
	}
}

func TestUpgradeValidCloneUsesCheapCheckAndRequiresFetch(t *testing.T) {
	for _, failedFetch := range []bool{false, true} {
		t.Run(map[bool]string{false: "valid", true: "fetch-failure"}[failedFetch], func(t *testing.T) {
			f := newUpgradeFixture(t)
			if _, err := runUpgrade(t.Context(), f.host, true, io.Discard); err != nil {
				t.Fatal(err)
			}
			f.launchd.commands = nil
			paths, _ := resolveSetupPaths(f.home)
			marker := filepath.Join(paths.data, upgradeSourceName, "keep-me")
			if err := os.WriteFile(marker, []byte("keep"), 0600); err != nil {
				t.Fatal(err)
			}
			original := f.operations.command
			f.operations.command = func(ctx context.Context, dir string, env []string, name string, args ...string) ([]byte, error) {
				if slices.Contains(args, "fsck") {
					t.Error("upgrade ran full fsck")
					return nil, errors.New("slow fsck")
				}
				if slices.Contains(args, "fetch") && failedFetch {
					return nil, errors.New("fetch failed")
				}
				return original(ctx, dir, env, name, args...)
			}
			_, err := runUpgrade(t.Context(), f.host, true, io.Discard)
			if failedFetch && (err == nil || !strings.Contains(err.Error(), "fetch failed")) {
				t.Fatalf("fetch failure ignored: %v", err)
			}
			if !failedFetch && err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(marker); err != nil {
				t.Fatalf("valid clone replaced: %v", err)
			}
			if len(f.launchd.mutating()) != 0 {
				t.Fatal("second preparation restarted stack")
			}
		})
	}
}

func TestUpgradeDownBeforeActivationHasStartInstruction(t *testing.T) {
	f := newUpgradeFixture(t)
	delete(f.launchd.loaded, setupL1Label)
	f.operations.observe = func(context.Context, connprofile.Profile) (l1.Node, error) {
		return l1.Node{}, errors.New("connection refused")
	}
	_, err := runUpgrade(t.Context(), f.host, true, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "L1 is down") || !strings.Contains(err.Error(), "wefty setup") || strings.Contains(err.Error(), "newly installed") || strings.Contains(err.Error(), "new version") {
		t.Fatalf("misleading recovery before activation: %v", err)
	}
}

func TestUpgradeInterruptedActivationRetainsReceipt(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "receipt", true: "legacy"}[legacy], func(t *testing.T) {
			f := newSetupFixture(t)
			f.setup(t, setupRequest{})
			paths, _ := resolveSetupPaths(f.home)
			path := filepath.Join(paths.config, setupManifestName)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if legacy {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
			for _, name := range setupBinaries {
				f.writeBinary(t, name, "v2")
			}
			_, err = runSetup(t.Context(), f.host, setupRequest{installedVersion: "new-commit", deferSkills: true, afterService: func(int, launchagent.Action) error { return errors.New("interrupted") }})
			if err == nil || !strings.Contains(err.Error(), "interrupted") {
				t.Fatalf("activation did not interrupt: %v", err)
			}
			after, readErr := os.ReadFile(path)
			if legacy {
				if !errors.Is(readErr, os.ErrNotExist) {
					t.Fatalf("partial receipt disabled legacy fallback: %q %v", after, readErr)
				}
			} else if readErr != nil || !bytes.Equal(before, after) {
				t.Fatalf("previous receipt lost: %q %v", after, readErr)
			}
		})
	}
}

func TestUpgradeLegacySkillWarningOnceAndSetupRefresh(t *testing.T) {
	f := newUpgradeFixture(t)
	paths, _ := resolveSetupPaths(f.home)
	if err := os.Remove(filepath.Join(paths.config, setupManifestName)); err != nil {
		t.Fatal(err)
	}
	before := map[string][]byte{}
	for _, target := range paths.skillTargets {
		name := filepath.Join(target, "SKILL.md")
		before[name], _ = os.ReadFile(filepath.Join(f.home, name))
	}
	report, err := runUpgrade(t.Context(), f.host, true, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	warnings := strings.Join(report.Setup.Notes, "\n")
	if strings.Count(warnings, "without ownership evidence") != 1 || !strings.Contains(warnings, "wefty setup") {
		t.Fatalf("missing one actionable legacy warning: %s", warnings)
	}
	for name, payload := range before {
		got, _ := os.ReadFile(filepath.Join(f.home, name))
		if !bytes.Equal(got, payload) {
			t.Fatalf("unowned skill overwritten: %s", name)
		}
	}
	if _, err := runSetup(t.Context(), f.host, setupRequest{from: paths.bin, skillSource: os.DirFS(localSkillSource(t) + "/skills")}); err != nil {
		t.Fatal(err)
	}
	for name := range before {
		got, _ := os.ReadFile(filepath.Join(f.home, name))
		if string(got) != "upgraded skill" {
			t.Fatalf("setup did not refresh %s: %q", name, got)
		}
	}
}

func TestUpgradeCanceledAttemptAcceptedOnPostRestartHeartbeatUnderYes(t *testing.T) {
	for _, outcome := range []string{"canceled", "", "read-failure", "stale-heartbeat"} {
		t.Run(outcome, func(t *testing.T) {
			f := newUpgradeFixture(t)
			deadline := f.now.Add(5 * time.Second)
			f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "canceling-job", AttemptID: "canceling-attempt", LeaseExpiresAt: deadline}}
			profile := f.profile(t)
			participant, closeFabric, err := fabricconfig.Open(t.Context(), fabricconfig.Config{Mode: "plain", PlainFabricID: profile.PlainFabricID, Identity: fabric.Identity{NodeID: "control"}})
			if err != nil {
				t.Fatal(err)
			}
			defer closeFabric()
			listener, err := participant.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			var reads atomic.Int32
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reads.Add(1)
				identity, err := participant.WhoIs(r.Context(), r.RemoteAddr)
				if err != nil || identity.NodeID != profile.Identities.CLI || !slices.Contains(identity.Tags, l1.DefaultClientPrincipalTag) {
					t.Errorf("wrong job-read identity: %+v %v", identity, err)
				}
				if r.Method != http.MethodGet || r.URL.Path != "/v1/jobs/canceling-job" {
					t.Errorf("wrong public job read: %s %s", r.Method, r.URL.Path)
				}
				w.Header().Set("Content-Type", "application/json")
				if outcome == "read-failure" {
					w.WriteHeader(http.StatusForbidden)
					_, _ = io.WriteString(w, `{"code":"forbidden","message":"denied"}`)
					return
				}
				_ = json.NewEncoder(w).Encode(l1.Job{JobID: "canceling-job", Outcome: contract.JobOutcome(outcome)})
			}))
			_ = server.Listener.Close()
			server.Listener = listener
			server.Start()
			defer server.Close()
			profile.L1Address = listener.Addr().String()
			paths, _ := resolveSetupPaths(f.home)
			if err := connprofile.Write(paths.profile, profile); err != nil {
				t.Fatal(err)
			}
			observe := f.operations.observe
			f.operations.observe = func(ctx context.Context, p connprofile.Profile) (l1.Node, error) {
				node, err := observe(ctx, p)
				node.ActiveAttempts[0].LeaseExpiresAt = deadline
				if outcome == "stale-heartbeat" {
					node.LastHeartbeatAt = f.node.LastHeartbeatAt
				}
				return node, err
			}
			_, err = runUpgrade(t.Context(), f.host, true, io.Discard)
			if outcome == "canceled" && (err != nil || reads.Load() == 0) {
				t.Fatalf("canceled attempt rejected or no job read: %v reads=%d", err, reads.Load())
			}
			if outcome != "canceled" && err == nil {
				t.Fatal("unproven renewal accepted")
			}
		})
	}
}
