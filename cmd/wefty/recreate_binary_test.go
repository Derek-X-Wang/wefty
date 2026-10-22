//go:build darwin

package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

func TestHostVMRecreateRefusalFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	root, err := os.MkdirTemp("/tmp", "wefty-recreate-cli-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	socket := filepath.Join(root, "control.sock")
	var refusal atomic.Pointer[lima.RecreateBlockedError]
	server, err := ocicontrol.NewServer(socket, ocicontrol.ServiceFuncs{SetupFunc: func(context.Context, ocicontrol.SetupRequest) (ocicontrol.SetupResponse, error) {
		return ocicontrol.SetupResponse{}, refusal.Load()
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	config := filepath.Join(root, "node.json")
	if err := ocicontrol.WriteInstalledConfig(config, ocicontrol.InstalledConfig{Version: ocicontrol.InstalledConfigVersion, ControlSocket: socket}); err != nil {
		t.Fatal(err)
	}
	// The file appears only once Serve has bound its listener.
	for deadline := time.Now().Add(5 * time.Second); ; {
		if _, err := os.Lstat(socket); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("control socket did not appear")
		}
		time.Sleep(time.Millisecond)
	}
	for _, test := range []struct {
		name    string
		failure *lima.RecreateBlockedError
		want    string
	}{
		{"retained", &lima.RecreateBlockedError{Blockers: []string{"Computer Storage generation stopped-generation", "Backup copy kept-copy"}}, "stopped-generation"},
		{"unknown", &lima.RecreateBlockedError{Reason: "host VM is stopped; durable inventory unavailable"}, "durable inventory unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			refusal.Store(test.failure)
			for _, jsonOutput := range []bool{true, false} {
				args := []string{"--node-config=" + config, "node", "setup-oci", "--recreate"}
				if jsonOutput {
					args = append(args, "--json")
				}
				code, output := runWefty(t, binary, 30*time.Second, args...)
				if code != 5 || !strings.Contains(output, test.want) {
					t.Fatalf("exit=%d output=%s", code, output)
				}
				if !jsonOutput {
					continue
				}
				var response contract.ErrorResponse
				if err := json.Unmarshal([]byte(output), &response); err != nil || response.Error.Code != "host_vm_recreate_blocked" || response.Error.Retryable {
					t.Fatalf("output=%s decode=%v", output, err)
				}
				if test.name == "retained" {
					blockers, ok := response.Error.Details["blockers"].([]any)
					if !ok || len(blockers) != 2 || !strings.Contains(response.Error.Message, "kept-copy") {
						t.Fatalf("lost blockers: %s", output)
					}
				} else if response.Error.Details["reason"] != test.failure.Reason {
					t.Fatalf("lost unknown-inventory reason: %s", output)
				}
			}
		})
	}
}
