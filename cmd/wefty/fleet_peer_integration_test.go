package main

import (
	"context"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/local"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

func TestFleetPeerProbeThroughRunningClientService(t *testing.T) {
	for _, mode := range []string{"healthy", "L1 down", "L3 down", "missing endpoint", "missing ping"} {
		t.Run(mode, func(t *testing.T) {
			f := healthyDoctorFixture(t, serviceplan.ControlPlane)
			p := f.profile(t)
			network := plain.NewNetwork()
			control := network.NewFabric(fabric.Identity{NodeID: p.L1Address})
			ledger := network.NewFabric(fabric.Identity{NodeID: p.L3Address, Tags: []string{l1.DefaultClientPrincipalTag}})
			client := network.NewFabric(fabric.Identity{NodeID: p.Participants[connprofile.ParticipantClient].NodeID, Tags: []string{l1.DefaultClientPrincipalTag}})
			ln1, err := control.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			ln3, err := ledger.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			one, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = one.Close() })
			three, err := l3.OpenStore(filepath.Join(t.TempDir(), "l3.sqlite"), l3.StoreOptions{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = three.Close() })
			policy := l1.DefaultNodePolicy(contract.StableNodeTagPrefix + p.Identities.Node)
			_, err = one.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, contract.NodeRegistration{NodeID: p.Identities.Node, BootSessionID: "boot-fixture", AgentVersion: "test", OS: "linux", Architecture: "amd64", Capabilities: map[string]bool{"kind:process": true}}, policy, true)
			if err != nil {
				t.Fatal(err)
			}
			server1, err := l1.NewServer(control, one, l1.ServerConfig{RunLedgerNodeID: p.L3Address, RunLedgerAddress: ln3.Addr().String(), NodePolicies: map[string]l1.NodePolicy{p.Identities.Node: policy}})
			if err != nil {
				t.Fatal(err)
			}
			server3, err := l3.NewServer(ledger, three, l3.ServerConfig{ControlPlaneNodeID: p.L1Address, ControlPlaneAddress: ln1.Addr().String()})
			if err != nil {
				t.Fatal(err)
			}
			var peerReads atomic.Int32
			wrap := func(h http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/v1/peer-health" {
						peerReads.Add(1)
						if mode == "missing endpoint" {
							http.NotFound(w, r)
							return
						}
					}
					if r.URL.Path == "/v1/peer/ping" && mode == "missing ping" {
						http.NotFound(w, r)
						return
					}
					h.ServeHTTP(w, r)
				})
			}
			http1, http3 := &http.Server{Handler: wrap(server1.Handler())}, &http.Server{Handler: wrap(server3.Handler())}
			done1, done3 := make(chan error, 1), make(chan error, 1)
			go func() { done1 <- http1.Serve(ln1) }()
			go func() { done3 <- http3.Serve(ln3) }()
			t.Cleanup(func() { _ = http1.Close(); _ = http3.Close(); <-done1; <-done3 })
			if mode == "L1 down" {
				_ = http1.Close()
			}
			if mode == "L3 down" {
				_ = http3.Close()
			}
			var checks atomic.Int32
			backend := upgradeFixtureFabric{dial: func(ctx context.Context, n, address string) (net.Conn, error) {
				target := ln1.Addr().String()
				if address == p.L3Address {
					target = ln3.Addr().String()
				} else if address != p.L1Address {
					t.Errorf("unexpected client target %s", address)
				}
				return client.Dial(ctx, n, target)
			}}
			localService, err := local.NewServer(local.DefaultSocketPath(f.home), map[string]local.Participant{local.Client: {NodeID: p.Participants[connprofile.ParticipantClient].NodeID, Fabric: backend, Check: func(ctx context.Context) error { checks.Add(1); return ctx.Err() }}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			ready := make(chan struct{})
			done := make(chan error, 1)
			go func() { done <- localService.ServeReady(ctx, func() error { close(ready); return nil }) }()
			t.Cleanup(func() {
				cancel()
				if err := <-done; err != nil {
					t.Error(err)
				}
			})
			select {
			case <-ready:
			case <-time.After(3 * time.Second):
				t.Fatal("client service not ready")
			}
			// The installation profile stays a fleet profile. Only its verified running
			// client's backend is the in-process plain adapter; no identity state opens.
			f.host.fleetClients = nil
			f.host.readinessBudget = 150 * time.Millisecond
			release := holdDoctorParticipantLocks(t, p)
			defer release()
			paths, _ := resolveSetupPaths(f.home)
			setup, setupErr := runSetup(t.Context(), f.host, setupRequest{skipSmokeRun: true})
			doctor, doctorErr := runFleetDoctor(t.Context(), f.host, globalOptions{}, paths, serviceplan.ControlPlane)
			expected := "VERIFIED"
			if strings.Contains(mode, "down") {
				expected = "FAILED"
			}
			if mode == "missing endpoint" || mode == "missing ping" {
				expected = "NOT-RUN"
			}
			assertFleetPeerChecks(t, setup.Readiness, expected)
			if strings.Contains(mode, "down") {
				unreadable := 3
				failed := 4
				if mode == "L3 down" {
					unreadable, failed = 4, 3
				}
				if doctor.PolicyEvidence[unreadable].Status != "NOT-RUN" || doctor.PolicyEvidence[unreadable].Detail != "peer observer could not be read; direction unverified" || doctor.PolicyEvidence[failed].Status != "FAILED" {
					t.Fatalf("down observer: %+v", doctor.PolicyEvidence)
				}
			} else {
				assertFleetPeerChecks(t, doctor.PolicyEvidence, expected)
			}
			if !strings.Contains(mode, "down") && (setupErr != nil || doctorErr != nil || !doctor.Healthy) {
				t.Fatalf("healthy fixture: setup=%v doctor=%v report=%+v", setupErr, doctorErr, doctor)
			}
			if strings.Contains(mode, "down") && (setupErr == nil || doctorErr == nil) {
				t.Fatalf("down peer did not fail: setup=%v doctor=%v", setupErr, doctorErr)
			}
			if checks.Load() == 0 || peerReads.Load() == 0 {
				t.Fatalf("running service was bypassed: identity=%d observations=%d", checks.Load(), peerReads.Load())
			}
		})
	}
}
