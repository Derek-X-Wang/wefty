//go:build darwin || linux

package ocicontrol

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/lima"
)

func TestProcessJobCannotChangeHostVMThroughOperatorSocket(t *testing.T) {
	if socket := os.Getenv("WEFTY_PROCESS_JOB_CONTROL_TEST"); socket != "" {
		client, err := NewClient(socket)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if _, err := client.Start(ctx, 1); err == nil {
			t.Fatal("process job changed owner switch through operator socket")
		}
		return
	}
	root, err := os.MkdirTemp("/tmp", "wefty-switch-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	socket := filepath.Join(root, "s")
	called := false
	service := ServiceFuncs{IntentFunc: func(context.Context) (lima.OCIIntent, error) { return lima.OCIIntent{}, nil }, StartFunc: func(context.Context, IntentMutationRequest) (IntentResponse, error) {
		called = true
		return IntentResponse{}, nil
	}}
	server, err := NewNodeServer(socket, service)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	client, _ := NewClient(socket)
	defer client.Close()
	// Same-process test caller stands in for the operator. Readiness is not a
	// timing assertion; it waits only for the fixture's socket publication.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := client.Intent(t.Context()); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("socket not ready")
		}
		time.Sleep(time.Millisecond)
	}
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestProcessJobCannotChangeHostVMThroughOperatorSocket$", "-test.count=1")
	command.Env = append(os.Environ(), "WEFTY_PROCESS_JOB_CONTROL_TEST="+socket)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("process job: %v %s", err, output)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("process job reached controller")
	}
}
