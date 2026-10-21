package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric/local"
	"github.com/Derek-X-Wang/wefty/internal/serviceready"
)

func TestClientServicePublishesCurrentReadyFile(t *testing.T) {
	f := newServiceParticipantFixture(t, "wefty")
	f.write(t)
	ready := filepath.Join(f.home, "client-ready.json")
	options, _, err := parseGlobalOptions([]string{"--profile=" + f.path, "client-service"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- executeClientService(ctx, options, []string{"--socket=" + f.socket, "--ready-file=" + ready}, io.Discard, io.Discard)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("client service did not stop")
		}
	})
	deadline := time.Now().Add(3 * time.Second)
	for {
		select {
		case err := <-done:
			done <- err
			t.Fatalf("client service stopped before ready: %v", err)
		default:
		}
		payload, err := os.ReadFile(ready)
		if err == nil {
			var metadata serviceready.Metadata
			if json.Unmarshal(payload, &metadata) != nil || metadata.PID != os.Getpid() || metadata.Address != f.socket || metadata.NodeID != "opaque-client" || metadata.ReadyAt.IsZero() {
				t.Fatalf("ready point: %s", payload)
			}
			client, err := local.New(f.socket, local.Client, "opaque-client")
			if err != nil {
				t.Fatal(err)
			}
			if err := client.Check(t.Context()); err != nil {
				t.Fatal(err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("client service omitted ready file")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
