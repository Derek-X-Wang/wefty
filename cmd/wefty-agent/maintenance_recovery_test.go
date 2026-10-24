package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestDeEnrollmentStartupResolvesLostCommittedReply(t *testing.T) {
	testStartupDeEnrollment(t, true, false)
}
func TestDeEnrollmentStartupDefinitiveRefusalRestoresRegistration(t *testing.T) {
	testStartupDeEnrollment(t, false, false)
}
func TestDeEnrollmentStartupUncertainOutcomeRemainsFenced(t *testing.T) {
	testStartupDeEnrollment(t, false, true)
}
func TestDeEnrollmentStartupCommittedReplayAuthenticationFailure(t *testing.T) {
	for _, status := range []int{401, 403} {
		t.Run(fmt.Sprint(status), func(t *testing.T) { testStartupDeEnrollment(t, true, false, status) })
	}
}
func testStartupDeEnrollment(t *testing.T, commit, uncertain bool, replayFailure ...int) {
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data, plists := filepath.Join(home, "data"), filepath.Join(home, "plists")
	for _, p := range []string{data, plists} {
		if err = os.Mkdir(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	source := filepath.Join(home, "source")
	if err = os.WriteFile(source, []byte("fixture binary"), 0700); err != nil {
		t.Fatal(err)
	}
	store := agentinstall.Store{Data: data, LaunchAgents: plists}
	g, err := store.Initialize("node", source, "v1", "darwin:11111111-1111-1111-1111-111111111111", []int{1})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := store.Read()
	if err != nil {
		t.Fatal(err)
	}
	g.Process = &agentinstall.Binding{PID: 123, ProcessStartID: "old-start", BootSessionID: "old-boot", SystemBootID: g.StartBootID, ReadyAt: time.Now()}
	catalog.Generations[g.ID] = g
	b, _ := json.Marshal(catalog)
	if err = os.WriteFile(filepath.Join(store.Root(), "catalog.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	authority, err := l1.OpenStore(filepath.Join(home, "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer authority.Close()
	node, err := authority.RegisterNode(t.Context(), fabric.Identity{NodeID: "node"}, contract.NodeRegistration{NodeID: "node", AgentGenerationID: g.ID, BootSessionID: "old-boot", OS: "darwin", Architecture: "arm64", AgentVersion: "v1", Capabilities: map[string]bool{"kind:process": true, "upgrade:retirement:v1": true}}, l1.DefaultNodePolicy(), true)
	if err != nil {
		t.Fatal(err)
	}
	node, err = authority.SetNodeClaimsByOperator(t.Context(), "node", "operator", l1.NodeIntentRequest{IntentRevision: node.IntentRevision, ClaimsEnabled: false, Reason: "wefty maintenance operation"})
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := authority.NodeGenerations(t.Context(), "node", "node")
	if err != nil {
		t.Fatal(err)
	}
	r := l1.DeEnrollNodeGenerationRequest{OperationID: "operation", PrimaryGenerationID: g.ID, PrimaryBootSessionID: node.BootSessionID, AuthorityGeneration: node.AuthorityGeneration, CatalogRevision: inventory.CatalogRevision, IntentRevision: node.IntentRevision, IntentReason: node.IntentReason, RuntimesReaped: true, EvidenceSettled: true}
	if err = store.PrepareDeEnrollment("node", r); err != nil {
		t.Fatal(err)
	}
	var committed l1.NodeGenerationInventory
	if commit {
		if committed, err = authority.DeEnrollNodeGeneration(t.Context(), "node", "node", r); err != nil {
			t.Fatal(err)
		}
	} else if !uncertain {
		// Crash after attestation, before the L1 transaction; an operator then
		// changes admission. Recovery must observe the definitive 409.
		_, err = authority.SetNodeClaimsByOperator(t.Context(), "node", "operator", l1.NodeIntentRequest{IntentRevision: node.IntentRevision, ClaimsEnabled: false, Reason: "new operator intent"})
		if err != nil {
			t.Fatal(err)
		}
	}
	// Process death here loses the result but retains the exact attested intent.
	network, err := plain.NewNetworkWithID("plain-maintenance-recovery")
	if err != nil {
		t.Fatal(err)
	}
	control := network.NewFabric(fabric.Identity{NodeID: "control"})
	server, err := l1.NewServer(control, authority, l1.ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := control.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var denied atomic.Int64
	if len(replayFailure) > 0 {
		denied.Store(int64(replayFailure[0]))
	}
	handler := server.Handler()
	httpServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status := denied.Load(); status != 0 && strings.HasSuffix(r.URL.Path, "/generations/de-enroll") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(int(status))
			code := contract.ErrorUnauthorized
			if status == 403 {
				code = contract.ErrorPrincipalForbidden
			}
			_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: code, Message: "authentication unavailable"}})
			return
		}
		handler.ServeHTTP(w, r)
	})}
	done := make(chan error, 1)
	go func() { done <- httpServer.Serve(listener) }()
	defer func() {
		cancel()
		httpServer.Close()
		if err := <-done; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Error(err)
		}
	}()
	profilePath := filepath.Join(home, "profile.json")
	profile := connprofile.Profile{Version: 1, Fabric: "plain", PlainFabricID: "plain-maintenance-recovery", L1Address: listener.Addr().String(), L3Address: "127.0.0.1:48222", Identities: connprofile.Identities{Node: "node", CLI: "client"}}
	if err = connprofile.Write(profilePath, profile); err != nil {
		t.Fatal(err)
	}
	if len(replayFailure) > 0 {
		before, e := authority.GetNode(ctx, "node")
		if e != nil {
			t.Fatal(e)
		}
		for attempt := 0; attempt < 2; attempt++ {
			handled, e := reconcileInstalledDeEnrollment(ctx, store, profilePath)
			saved, readErr := store.ReadDeEnrollment()
			if readErr != nil || saved == nil || saved.Request != r || saved.Result != nil {
				t.Fatalf("HTTP %d erased unresolved committed attestation: %+v %v", replayFailure[0], saved, readErr)
			}
			if !handled || e == nil {
				t.Fatal("authentication failure was treated as definitive", handled, e)
			}
			rejected, rejectErr := store.DeEnrollmentRejected(r)
			if rejectErr != nil || rejected {
				t.Fatal("authentication failure recorded a precommit rejection", rejected, rejectErr)
			}
			after, readErr := authority.GetNode(ctx, "node")
			if readErr != nil {
				t.Fatal(readErr)
			}
			if after.ClaimsEnabled || after.IntentRevision != before.IntentRevision || after.IntentReason != before.IntentReason {
				t.Fatal("replay reopened or changed admission")
			}
		}
		denied.Store(0)
	}
	if uncertain {
		profile.L1Address = "127.0.0.1:1"
		if err = connprofile.Write(profilePath, profile); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2; i++ {
			handled, e := reconcileInstalledDeEnrollment(t.Context(), store, profilePath)
			if !handled || e == nil {
				t.Fatal("uncertain outcome allowed registration", handled, e)
			}
		}
		saved, e := store.ReadDeEnrollment()
		if e != nil || saved == nil || saved.Request != r || saved.Result != nil {
			t.Fatal("uncertain attestation changed", e)
		}
		return
	}
	for i := 0; i < 2; i++ {
		handled, e := reconcileInstalledDeEnrollment(t.Context(), store, profilePath)
		if handled != commit || e != nil {
			t.Fatalf("startup recovery handled=%t err=%v", handled, e)
		}
	}
	saved, err := store.ReadDeEnrollment()
	if !commit {
		if err != nil || saved != nil {
			t.Fatal("definitively refused attestation still fences startup", err)
		}
		entries, e := os.ReadDir(store.Root())
		if e != nil {
			t.Fatal(e)
		}
		rejectionRecorded := false
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Name(), "de-enrollment-rejected-") {
				continue
			}
			payload, e := os.ReadFile(filepath.Join(store.Root(), entry.Name()))
			if e != nil {
				t.Fatal(e)
			}
			var rejection agentinstall.DeEnrollmentRecord
			if e = json.Unmarshal(payload, &rejection); e != nil {
				t.Fatal(e)
			}
			if rejection.NodeID == "node" && rejection.Request == r && rejection.Result == nil {
				rejectionRecorded = true
			}
		}
		if !rejectionRecorded {
			t.Fatal("definitive rejection was not durably recorded")
		}
		// The resumed agent can register its replacement boot and serve its
		// private inventory adapter; no managed-root kernel reader is needed.
		runtime, e := agent.New(agent.Config{Fabric: network.NewFabric(fabric.Identity{NodeID: "node", Tags: []string{l1.DefaultAgentPrincipalTag}}), ControlPlaneAddress: profile.L1Address, NodeID: "node", AgentGenerationID: g.ID, BootSessionID: "replacement-boot", Version: "v1", OS: "darwin", Architecture: "arm64", LogSpoolDirectory: t.TempDir(), HandoffRoot: t.TempDir(), PrimaryGeneration: func(write func() error) error { return store.WithPrimary(g.ID, write) }})
		if e != nil {
			t.Fatal(e)
		}
		defer runtime.Close()
		registered, e := runtime.Register(t.Context())
		if e != nil {
			t.Fatal("refused startup cannot register:", e)
		}
		if registered.BootSessionID != "replacement-boot" {
			t.Fatal("replacement boot not registered")
		}
		if registered.ClaimsEnabled || registered.IntentReason != "new operator intent" {
			t.Fatal("startup overwrote newer operator admission intent")
		}

		if e = store.PublishReady(g.ID, "node", g.Version, g.Executable, registered.BootSessionID, g.StartBootID); e != nil {
			t.Fatal(e)
		}
		controlCtx, stop := context.WithCancel(t.Context())
		controlDone := make(chan error, 1)
		go func() { controlDone <- runtime.ServeUpgradeControl(controlCtx, store) }()
		defer func() {
			stop()
			if e := <-controlDone; e != nil {
				t.Error(e)
			}
		}()
		request := agentinstall.ControlRequest{ID: "inventory", NodeID: "node", GenerationID: g.ID, BootSessionID: registered.BootSessionID, Action: "generation_inventory"}
		if e = store.RequestControl(request); e != nil {
			t.Fatal(e)
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			reply, readErr := store.ReadControlResult(g.ID)
			if readErr == nil && reply.Request.ID == request.ID {
				if reply.Error != "" {
					t.Fatal(reply.Error)
				}
				break
			}
			e = readErr
			if time.Now().After(deadline) {
				t.Fatal("replacement adapter unavailable:", e)
			}
			time.Sleep(10 * time.Millisecond)
		}
		return
	}
	if err != nil || saved == nil || saved.Result == nil || !reflect.DeepEqual(*saved.Result, committed) {
		t.Fatal("original committed result was not persisted", err)
	}
	confirmed, e := authority.GetNode(t.Context(), "node")
	if e != nil {
		t.Fatal(e)
	}
	if confirmed.ClaimsEnabled || confirmed.IntentRevision != r.IntentRevision || confirmed.IntentReason != r.IntentReason {
		t.Fatal("committed recovery reopened admission")
	}
}
