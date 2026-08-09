package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/l1"
)

type nodeTagsFlag map[string][]string

func (values nodeTagsFlag) Set(value string) error {
	nodeID, tags, found := strings.Cut(value, "=")
	nodeID = strings.TrimSpace(nodeID)
	if !found || nodeID == "" {
		return errors.New("node tags must use node-id=tag,tag syntax")
	}
	var parsed []string
	if tags != "" {
		parsed = strings.Split(tags, ",")
	}
	values[nodeID] = parsed
	return nil
}

func (nodeTagsFlag) String() string { return "node-id=tag,tag" }

func main() {
	if err := run(); err != nil {
		log.Printf("wefty-l1: %v", err)
		os.Exit(1)
	}
}

func run() error {
	nodeTags := make(nodeTagsFlag)
	var (
		fabricMode     = flag.String("fabric", "plain", "fabric implementation: plain or tsnet")
		listenAddress  = flag.String("listen", "wefty://control-plane", "control-plane Fabric listen address")
		databasePath   = flag.String("db", "wefty-l1.sqlite", "SQLite database path")
		leaseDuration  = flag.Duration("lease-duration", l1.DefaultLeaseDuration, "attempt lease duration")
		fabricName     = flag.String("fabric-name", "wefty://control-plane", "tsnet logical service name")
		stateDirectory = flag.String("state-dir", "", "tsnet state directory")
		authKey        = flag.String("auth-key", os.Getenv("TS_AUTHKEY"), "tsnet auth key")
		controlURL     = flag.String("control-url", os.Getenv("TS_CONTROL_URL"), "optional tsnet coordination URL")
		ephemeral      = flag.Bool("ephemeral", false, "register an ephemeral tsnet node")
		readyFile      = flag.String("ready-file", "", "write listener metadata after the server is ready")
	)
	flag.Var(nodeTags, "node-tags", "authoritative routing tags as node-id=tag,tag (repeatable)")
	flag.Parse()

	participant, closeFabric, err := fabricconfig.Open(fabricconfig.Config{
		Mode:           *fabricMode,
		Identity:       fabric.Identity{NodeID: "control-plane"},
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
	store, err := l1.OpenStore(*databasePath, l1.StoreOptions{LeaseDuration: *leaseDuration})
	if err != nil {
		return err
	}
	defer store.Close()
	server, err := l1.NewServer(participant, store, l1.ServerConfig{AuthoritativeNodeTags: nodeTags})
	if err != nil {
		return err
	}
	listener, err := participant.Listen("tcp", *listenAddress)
	if err != nil {
		return err
	}
	if *readyFile != "" {
		metadata := struct {
			Address string `json:"address"`
		}{Address: listener.Addr().String()}
		payload, err := json.Marshal(metadata)
		if err != nil {
			return err
		}
		temporaryReadyFile := *readyFile + ".tmp"
		if err := os.WriteFile(temporaryReadyFile, payload, 0o600); err != nil {
			return fmt.Errorf("write ready file: %w", err)
		}
		if err := os.Rename(temporaryReadyFile, *readyFile); err != nil {
			return fmt.Errorf("publish ready file: %w", err)
		}
	}
	log.Printf("listening on %s", listener.Addr())
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return server.Serve(ctx, listener)
}
