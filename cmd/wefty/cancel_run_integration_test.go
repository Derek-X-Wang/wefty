package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

func TestCancelRunRealBinaryOverL1L3(t *testing.T) {
	binary := buildWefty(t)
	network, err := plain.NewNetworkWithID("plain-cancel-run-691")
	if err != nil {
		t.Fatal(err)
	}
	control := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	ledger := network.NewFabric(fabric.Identity{NodeID: "run-ledger", Tags: []string{l1.DefaultClientPrincipalTag}})
	l1Store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer l1Store.Close()
	l1Server, err := l1.NewServer(control, l1Store, l1.ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	l1Listener, err := control.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	client, err := l3.NewL1Client(ledger, l1Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	store, err := l3.OpenStore(filepath.Join(t.TempDir(), "l3.sqlite"), l3.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	reconciler, err := l3.NewReconciler(store, client, l3.ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	server, err := l3.NewServer(ledger, store, l3.ServerConfig{Jobs: client})
	if err != nil {
		t.Fatal(err)
	}
	l3Listener, err := ledger.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(t.Context())
	l1Done := serveTestServer(ctx, func() error { return l1Server.Serve(ctx, l1Listener) })
	l3Done := serveTestServer(ctx, func() error { return server.Serve(ctx, l3Listener) })
	defer func() {
		stop()
		if err := <-l3Done; err != nil {
			t.Error(err)
		}
		if err := <-l1Done; err != nil {
			t.Error(err)
		}
	}()
	global := []string{"--json", "--plain-fabric-id=plain-cancel-run-691", "--plain-identity=cancel-operator", "--l1=" + l1Listener.Addr().String(), "--l3=" + l3Listener.Addr().String()}
	content := "#!/bin/sh\nexit 0\n"
	digest := sha256.Sum256([]byte(content))
	source, _, err := store.CreateRun(ctx, l3.CreateRunInput{IdempotencyKey: "binary-cancel", Actor: "cancel-operator", Request: l3.CreateRunRequest{InlineScript: &l3.InlineScriptInput{Content: content, SHA256: hex.EncodeToString(digest[:]), Interpreter: []string{"/bin/sh"}}, Params: json.RawMessage(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	dispatched, err := store.GetRun(ctx, source.RunID)
	if err != nil || dispatched.L1JobID == "" {
		t.Fatalf("dispatch=%+v %v", dispatched, err)
	}
	for i := 0; i < 2; i++ {
		code, output := runWefty(t, binary, 30*time.Second, append(global, "cancel", source.RunID)...)
		if code != 0 {
			t.Fatalf("cancel exit=%d output=%s", code, output)
		}
		var record contract.RunRecord
		if err := json.Unmarshal([]byte(output), &record); err != nil {
			t.Fatal(err)
		}
		if record.RunID != source.RunID || record.Status != contract.RunFailed || record.FailureReason != "the L1 job was canceled" {
			t.Fatalf("cancel=%+v", record)
		}
	}
	job, err := l1Store.GetJob(ctx, dispatched.L1JobID)
	if err != nil || job.Outcome != contract.JobOutcomeCanceled {
		t.Fatalf("L1 outcome=%+v %v", job, err)
	}
}
