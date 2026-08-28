package ocicontrol

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/lima"
)

const controlChildEnvironment = "WEFTY_OCI_CONTROL_CHILD"

func TestOperatorControlSocketUsesARealProcess(t *testing.T) {
	if os.Getenv(controlChildEnvironment) == "1" {
		runControlChild(t)
		return
	}
	root, err := os.MkdirTemp("/tmp", "wefty-oci-control-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	socket := filepath.Join(root, "control.sock")
	intentPath := filepath.Join(root, "intent.json")
	command := exec.Command(os.Args[0], "-test.run=^TestOperatorControlSocketUsesARealProcess$")
	var childOutput bytes.Buffer
	command.Stdout = &childOutput
	command.Stderr = &childOutput
	command.Env = append(os.Environ(), controlChildEnvironment+"=1", "WEFTY_CONTROL_TEST_SOCKET="+socket, "WEFTY_CONTROL_TEST_INTENT="+intentPath)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_, _ = command.Process.Wait()
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		if info, err := os.Stat(socket); err == nil && info.Mode()&os.ModeSocket != 0 {
			if info.Mode().Perm() != 0o600 {
				t.Fatalf("control socket mode=%#o", info.Mode().Perm())
			}
			parent, err := os.Stat(root)
			if err != nil {
				t.Fatal(err)
			}
			if parent.Mode().Perm() != 0o700 {
				t.Fatalf("control directory mode=%#o", parent.Mode().Perm())
			}
			break
		}
		if time.Now().After(deadline) {
			_ = command.Process.Kill()
			_, _ = command.Process.Wait()
			t.Fatalf("real control process did not publish its socket:\n%s", childOutput.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	client, err := NewClient(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	intent, err := client.Intent(t.Context())
	if err != nil || !intent.Enabled || intent.Revision != 1 {
		t.Fatalf("real-process intent=%+v err=%v", intent, err)
	}
	response, err := client.Stop(t.Context(), intent.Revision)
	if err != nil || response.Intent.Enabled || !response.RuntimeQuiesced {
		t.Fatalf("real-process stop=%+v err=%v", response, err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("real control process: %v\n%s", err, childOutput.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("real control process did not exit")
	}
}

func runControlChild(t *testing.T) {
	socket := os.Getenv("WEFTY_CONTROL_TEST_SOCKET")
	intentPath := os.Getenv("WEFTY_CONTROL_TEST_INTENT")
	if _, err := lima.InitializeOCIIntent(intentPath, time.Now()); err != nil {
		t.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	service := ServiceFuncs{
		IntentFunc: func(ctx context.Context) (lima.OCIIntent, error) {
			return (lima.FileIntentSource{Path: intentPath}).ReadIntent(ctx)
		},
		StopFunc: func(_ context.Context, request IntentMutationRequest) (IntentResponse, error) {
			intent, err := lima.SetOCIIntent(intentPath, request.ExpectedRevision, false, time.Now())
			if err == nil {
				go stop()
			}
			return IntentResponse{Intent: intent, RuntimeQuiesced: err == nil}, err
		},
	}
	server, err := NewServer(socket, service)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Serve(ctx); err != nil {
		t.Fatal(err)
	}
}
