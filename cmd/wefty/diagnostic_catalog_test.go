//go:build darwin || linux

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/l1"
)

func busyCatalogDoctorFixture(t *testing.T) (*setupFixture, setupPaths, agentinstall.Store, l1.NodeList) {
	t.Helper()
	nodes := round2Nodes(t, true)
	f, paths := round2DoctorFixture(t, &nodes)
	store := agentinstall.Store{Data: paths.data, LaunchAgents: paths.launchAgents}
	g, err := store.Initialize("test-mac", f.host.executable, "v1", rollingTestBoot, []int{1})
	if err != nil {
		t.Fatal(err)
	}
	nodes.Nodes[0].AgentGenerationID = g.ID
	nodes.Nodes[0].Generations[0].AgentGenerationID = g.ID
	legacy := filepath.Join(paths.launchAgents, setupNodeAgentLabel+".plist")
	plist, err := os.ReadFile(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(g.Plist, plist, 0644); err != nil {
		t.Fatal(err)
	}
	delete(f.launchd.loaded, setupNodeAgentLabel)
	f.launchd.loaded[g.Label] = true
	return f, paths, store, nodes
}

// Initialize's hook runs under the real catalog writer lock. Stop before it
// changes anything, retaining the healthy catalog for the diagnostic reader.
func holdDiagnosticCatalogWriter(t *testing.T, store agentinstall.Store) func() {
	t.Helper()
	held, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	stop := errors.New("test writer released")
	store.InitializationStep = func(string) error {
		close(held)
		<-release
		return stop
	}
	go func() {
		_, err := store.Initialize("", "", "", "", nil)
		done <- err
	}()
	select {
	case <-held:
	case err := <-done:
		t.Fatalf("writer did not acquire catalog lock: %v", err)
	}
	var once sync.Once
	unlock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() {
		unlock()
		if err := <-done; !errors.Is(err, stop) {
			t.Errorf("catalog writer: %v", err)
		}
	})
	return unlock
}

func TestDoctorCatalogBusyThenReleased(t *testing.T) {
	f, paths, store, _ := busyCatalogDoctorFixture(t)
	release := holdDiagnosticCatalogWriter(t, store)
	timer := time.AfterFunc(250*time.Millisecond, release)
	defer timer.Stop()
	report := runFixturePlainDoctor(t, f.host, globalOptions{nodeConfigPath: paths.nodeConfig, profilePath: paths.profile})
	for _, finding := range report.Findings {
		if finding.Check == "primary-generation" {
			t.Fatalf("released catalog retained primary-generation warning: %+v", finding)
		}
	}
	if !report.Healthy {
		t.Fatalf("released catalog should be healthy: %+v", report)
	}
}

func TestDoctorCatalogStillBusy(t *testing.T) {
	f, paths, store, _ := busyCatalogDoctorFixture(t)
	holdDiagnosticCatalogWriter(t, store)
	started := time.Now()
	report := runFixturePlainDoctor(t, f.host, globalOptions{nodeConfigPath: paths.nodeConfig, profilePath: paths.profile})
	if elapsed := time.Since(started); elapsed < 2*time.Second || elapsed > 15*time.Second {
		t.Fatalf("catalog retry must be short and bounded: %s", elapsed)
	}
	if report.Healthy {
		t.Fatal("busy catalog became healthy")
	}
	found := false
	for _, finding := range report.Findings {
		if strings.Contains(finding.Detail, "catalog was busy") && finding.Fix != "rerun doctor in a moment" {
			t.Errorf("busy catalog suggested repair: %+v", finding)
		}
		if finding.Check == "primary-generation" {
			found = true
			if finding.Severity != "warning" || !strings.Contains(finding.Detail, "catalog was busy") || !strings.Contains(finding.Detail, "upgrade or agent") || finding.Fix != "rerun doctor in a moment" {
				t.Fatalf("busy catalog advice: %+v", finding)
			}
		}
	}
	if !found {
		t.Fatal("busy primary-generation warning missing")
	}
}

func TestDoctorCatalogAccessError(t *testing.T) {
	f, paths, store, _ := busyCatalogDoctorFixture(t)
	if err := os.Chmod(store.Root(), 0755); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	report := runFixturePlainDoctor(t, f.host, globalOptions{nodeConfigPath: paths.nodeConfig, profilePath: paths.profile})
	if elapsed := time.Since(started); elapsed >= 2*time.Second {
		t.Fatalf("access error was delayed by contention retries: %s", elapsed)
	}
	if report.Healthy {
		t.Fatal("inaccessible catalog became healthy")
	}
	for _, finding := range report.Findings {
		if finding.Check == "primary-generation" {
			if finding.Severity != "warning" || !strings.Contains(finding.Detail, "generation directory must be owner-only and unlinked") || strings.Contains(finding.Detail, "catalog was busy") || finding.Fix != "restore catalog access and rerun doctor" {
				t.Fatalf("access error advice changed: %+v", finding)
			}
			return
		}
	}
	t.Fatal("access error warning missing")
}

func TestStatusCatalogBusyThenReleased(t *testing.T) {
	_, paths, store, nodes := busyCatalogDoctorFixture(t)
	h := newStatusHarness(t)
	h.nodes = nodes
	clients := h.clients()
	clients.nodeConfigPath = paths.nodeConfig
	release := holdDiagnosticCatalogWriter(t, store)
	timer := time.AfterFunc(250*time.Millisecond, release)
	defer timer.Stop()
	var out, stderr bytes.Buffer
	err := executeStatus(t.Context(), clients, true, nil, &out, &stderr)
	var status clusterStatus
	if decodeErr := json.Unmarshal(out.Bytes(), &status); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if err != nil || !status.Ready || len(status.EvidenceErrors) > 0 {
		t.Fatalf("released catalog prevented ready status: err=%v report=%s stderr=%s", err, &out, &stderr)
	}
}
