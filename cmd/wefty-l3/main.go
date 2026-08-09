package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

func main() {
	if err := run(); err != nil {
		log.Printf("wefty-l3: %v", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		fabricMode     = flag.String("fabric", "plain", "fabric implementation: plain or tsnet")
		listenAddress  = flag.String("listen", l3.DefaultL3Address, "run-ledger Fabric listen address")
		controlPlane   = flag.String("control-plane", l3.DefaultL1Address, "L1 control-plane Fabric address")
		databasePath   = flag.String("db", "wefty-l3.sqlite", "SQLite database path")
		reconcileEvery = flag.Duration("reconcile-interval", l3.DefaultReconcileInterval, "dispatch and run-state reconciliation interval")
		fabricName     = flag.String("fabric-name", "wefty-run-ledger", "tsnet logical node name")
		stateDirectory = flag.String("state-dir", "", "tsnet state directory")
		authKey        = flag.String("auth-key", os.Getenv("TS_AUTHKEY"), "tsnet auth key")
		controlURL     = flag.String("control-url", os.Getenv("TS_CONTROL_URL"), "optional tsnet coordination URL")
		ephemeral      = flag.Bool("ephemeral", false, "register an ephemeral tsnet node")
		readyFile      = flag.String("ready-file", "", "write listener metadata after the server is ready")
	)
	flag.Parse()
	if *reconcileEvery <= 0 {
		return fmt.Errorf("--reconcile-interval must be positive")
	}

	participant, closeFabric, err := fabricconfig.Open(fabricconfig.Config{
		Mode: *fabricMode,
		Identity: fabric.Identity{
			NodeID: "run-ledger",
			Tags:   []string{l1.DefaultClientPrincipalTag},
		},
		Name:           *fabricName,
		StateDirectory: *stateDirectory,
		AuthKey:        *authKey,
		ControlURL:     *controlURL,
		Ephemeral:      *ephemeral,
		Logf:           log.Printf,
	})
	if err != nil {
		return err
	}
	defer closeFabric()
	store, err := l3.OpenStore(*databasePath, l3.StoreOptions{})
	if err != nil {
		return err
	}
	defer store.Close()
	l1Client, err := l3.NewL1Client(participant, *controlPlane)
	if err != nil {
		return err
	}
	defer l1Client.CloseIdleConnections()
	reconciler, err := l3.NewReconciler(store, l1Client, l3.ReconcilerConfig{
		Interval: *reconcileEvery,
		OnError:  func(err error) { log.Printf("reconcile: %v", err) },
	})
	if err != nil {
		return err
	}
	server, err := l3.NewServer(participant, store, l3.ServerConfig{Reconciler: reconciler, Logs: l1Client})
	if err != nil {
		return err
	}
	listener, err := participant.Listen("tcp", *listenAddress)
	if err != nil {
		return err
	}
	if err := publishReadyFile(*readyFile, listener.Addr().String()); err != nil {
		return err
	}
	log.Printf("listening on %s (reconcile every %s)", listener.Addr(), reconcileEvery.String())
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return server.Serve(ctx, listener)
}

func publishReadyFile(path, address string) error {
	if path == "" {
		return nil
	}
	payload, err := json.Marshal(struct {
		Address string    `json:"address"`
		ReadyAt time.Time `json:"ready_at"`
	}{Address: address, ReadyAt: time.Now().UTC()})
	if err != nil {
		return err
	}
	temporaryPath := path + ".tmp"
	if err := os.WriteFile(temporaryPath, payload, 0o600); err != nil {
		return fmt.Errorf("write ready file: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish ready file: %w", err)
	}
	return nil
}
