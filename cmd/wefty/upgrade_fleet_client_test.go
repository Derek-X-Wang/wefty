package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/local"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/l1"
)

type upgradeFixtureFabric struct {
	fabric.Fabric
	dial func(context.Context, string, string) (net.Conn, error)
}

func (f upgradeFixtureFabric) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	return f.dial(ctx, network, address)
}

func TestFleetUpgradeJoinDrainsThroughClientServiceAndRemoteL1Fake(t *testing.T) {
	f := newFleetUpgradeFixture(t, serviceplan.Join)
	p := f.profile(t)
	f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "restartable", AttemptID: "old-attempt", BootSessionID: f.node.BootSessionID, Restartable: true}}
	var interrupted atomic.Int32
	observe := f.operations.observe
	setClaims := f.operations.setClaims
	var disables, restores, checks, l1Dials, l3Dials atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var payload any
		switch {
		case strings.HasSuffix(r.URL.Path, "/restartable-attempts/old-attempt/interrupt"):
			var request l1.InterruptRestartableAttemptRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			if f.node.ClaimsEnabled || request.JobID != "restartable" || request.BootSessionID != f.node.BootSessionID || request.IntentRevision != f.node.IntentRevision || request.Reason != l1.InterruptionUpgrade || request.IdempotencyKey == "" {
				t.Errorf("wrong interruption: %+v", request)
			}
			interrupted.Add(1)
			payload = l1.Job{JobID: "restartable", State: contract.JobRunning}
		case r.URL.Path == "/v1/jobs/restartable":
			if interrupted.Load() == 0 {
				t.Error("read before interruption")
			}
			f.node.ActiveAttempts = nil
			payload = l1.Job{JobID: "restartable", State: contract.JobQueued, Attempts: []l1.Attempt{{AttemptID: "old-attempt", State: contract.AttemptInterrupted}}}
		case strings.HasSuffix(r.URL.Path, "/claims"):
			var request l1.NodeIntentRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			if request.ClaimsEnabled {
				restores.Add(1)
			} else {
				disables.Add(1)
			}
			node, err := setClaims(r.Context(), p, request)
			if err != nil {
				t.Error(err)
				w.WriteHeader(409)
				return
			}
			payload = node
		case r.URL.Path == "/v1/nodes/"+p.Identities.Node:
			node, err := observe(r.Context(), p)
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
			payload = node
		case r.URL.Path == "/v1/nodes":
			localNode, _ := observe(r.Context(), p)
			other := readyNode("other-machine")
			other.AgentVersion = "older-build"
			payload = l1.NodeList{Nodes: []l1.Node{localNode, other}}
		case r.URL.Path == "/v1/runs":
			payload = map[string]any{"runs": []any{}}
		default:
			t.Errorf("unexpected fleet API: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(404)
			return
		}
		_ = json.NewEncoder(w).Encode(payload)
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	backend := upgradeFixtureFabric{dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		switch address {
		case p.L1Address:
			l1Dials.Add(1)
		case p.L3Address:
			l3Dials.Add(1)
		default:
			return nil, fmt.Errorf("unexpected remote peer: %s", address)
		}
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, server.Listener.Addr().String())
	}}
	socket := local.DefaultSocketPath(f.home)
	var stop context.CancelFunc
	var done chan error
	start := func() {
		service, err := local.NewServer(socket, map[string]local.Participant{local.Client: {NodeID: p.Participants[connprofile.ParticipantClient].NodeID, Fabric: backend, Check: func(context.Context) error { checks.Add(1); return nil }}})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		stop = cancel
		done = make(chan error, 1)
		ready := make(chan struct{})
		go func() { done <- service.ServeReady(ctx, func() error { close(ready); return nil }) }()
		select {
		case <-ready:
		case err := <-done:
			t.Fatalf("client service failed: %v", err)
		case <-time.After(time.Second):
			t.Fatal("client service did not publish")
		}
	}
	start()
	t.Cleanup(func() {
		stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	runner := f.host.launchctl
	f.host.launchctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if args[0] == "bootout" && strings.HasSuffix(args[1], "/dev.wefty.client-service") {
			stop()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
		out, err := runner(ctx, name, args...)
		if err == nil && args[0] == "bootstrap" && strings.HasSuffix(args[2], "dev.wefty.client-service.plist") {
			start()
		}
		return out, err
	}
	// Default fleet operations must use setupFleetClients, which talks to the
	// running local service as the saved client and never opens a backend.
	f.operations.observe = nil
	f.operations.readNode = nil
	f.operations.setClaims = nil
	f.operations.readJob = nil
	report, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if disables.Load() != 1 || restores.Load() != 1 || checks.Load() == 0 || l1Dials.Load() == 0 || l3Dials.Load() == 0 {
		t.Fatalf("fleet drain or renewal bypassed client: disable=%d restore=%d checks=%d L1=%d L3=%d", disables.Load(), restores.Load(), checks.Load(), l1Dials.Load(), l3Dials.Load())
	}
	if interrupted.Load() != 1 || report.RestartPending != 1 {
		t.Fatalf("interruption bypassed saved client: calls=%d report=%+v", interrupted.Load(), report)
	}
	if len(report.VersionWarnings) == 0 || !strings.Contains(report.VersionWarnings[len(report.VersionWarnings)-1].Detail, "other-machine") {
		t.Fatalf("missing remaining machine: %+v", report.VersionWarnings)
	}
	for _, call := range f.launchd.commands {
		if strings.Contains(strings.Join(call, " "), "dev.wefty.l1") || strings.Contains(strings.Join(call, " "), "dev.wefty.l3") {
			t.Fatalf("join touched control-plane service: %v", call)
		}
	}
}

func TestFleetUpgradeProfilePreflightFromAllRealBinaries(t *testing.T) {
	binaries := buildParticipantServices(t)
	f := newFleetUpgradeFixture(t, serviceplan.ControlPlane)
	paths, _ := resolveSetupPaths(f.home)
	for _, name := range setupBinaries {
		t.Run(name, func(t *testing.T) {
			args := []string{"--profile-preflight", "--profile=" + paths.profile, "--role=control-plane"}
			command := exec.CommandContext(t.Context(), filepath.Join(binaries, name), args...)
			command.Env = []string{"PATH=/usr/bin:/bin"}
			out, err := command.CombinedOutput()
			if err != nil || strings.TrimSpace(string(out)) != "profile-readable control-plane" {
				t.Fatalf("real binary compatibility gate: %s %v", out, err)
			}
			// The active control-plane profile cannot be activated under a joining role.
			args[len(args)-1] = "--role=join"
			command = exec.CommandContext(t.Context(), filepath.Join(binaries, name), args...)
			command.Env = []string{"PATH=/usr/bin:/bin"}
			if out, err := command.CombinedOutput(); err == nil {
				t.Fatalf("real binary accepted incompatible role: %s", out)
			}
		})
	}
}
