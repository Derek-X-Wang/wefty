package main

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

type unavailableIdentityFabric struct{ fabric.Fabric }

func (unavailableIdentityFabric) WhoIs(context.Context, string) (fabric.Identity, error) {
	return fabric.Identity{}, errors.New("identity backend offline")
}

func TestL1IdentityUnavailableExits13FromRealBinary(t *testing.T) {
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	server, err := l1.NewServer(unavailableIdentityFabric{}, store, l1.ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	address := startStubLedger(t, server.Handler().ServeHTTP)
	binary := buildWefty(t)
	code, output := runWefty(t, binary, 30*time.Second, "--json", "--l1", address, "nodes", "list")
	var response contract.ErrorResponse
	if err := json.Unmarshal([]byte(output), &response); err != nil {
		t.Fatalf("decode error: %v output=%s", err, output)
	}
	if code != 13 || response.Error.Code != contract.ErrorUnavailable || !response.Error.Retryable ||
		response.Error.Details["reason"] != "identity_unverifiable" || response.Error.RequestID == "" {
		t.Fatalf("exit=%d error=%+v, want 13 unavailable/identity_unverifiable retryable with request ID", code, response.Error)
	}
}
