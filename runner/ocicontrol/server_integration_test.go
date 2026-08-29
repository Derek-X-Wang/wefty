package ocicontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

const controlChildEnvironment = "WEFTY_OCI_CONTROL_CHILD"

func TestControlResponsePreservesSanitizedHelperMechanics(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeControlResponse(recorder, nil, &ocihelper.RPCError{
		Code:    ocihelper.CodeImageUnavailable,
		Message: "OCI image delivery failed",
		ImageFailure: &ocihelper.ImageFailureFact{
			Kind:           ocihelper.ImageFailureManifestRejected,
			TopLevelDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
	})

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d, want %d", recorder.Code, http.StatusInternalServerError)
	}
	var response contract.ErrorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error.Code != ErrorInternal || response.Error.Message != "node-local OCI control failed" {
		t.Fatalf("control error=%+v", response.Error)
	}
	if got := response.Error.Details["reason"]; got != string(ocihelper.CodeImageUnavailable) {
		t.Fatalf("helper reason=%v", got)
	}
	mechanics, ok := response.Error.Details["image_failure"].(map[string]any)
	if !ok || mechanics["kind"] != string(ocihelper.ImageFailureManifestRejected) || mechanics["top_level_digest"] == "" {
		t.Fatalf("image mechanics=%#v", response.Error.Details["image_failure"])
	}
}

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

func TestControlSocketRejectsUIDOutsideOperatorAllowlist(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "wefty-peer-auth-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	socket := filepath.Join(root, "control.sock")
	server, err := NewServer(socket, ServiceFuncs{IntentFunc: func(context.Context) (lima.OCIIntent, error) {
		return lima.OCIIntent{Version: 1, Revision: 1, Enabled: true, UpdatedAt: time.Now()}, nil
	}}, uint32(os.Geteuid()+1))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = server.Serve(ctx) }()
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(socket); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("control socket was not published")
		}
		time.Sleep(10 * time.Millisecond)
	}
	client, _ := NewClient(socket)
	defer client.Close()
	requestContext, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	if _, err := client.Intent(requestContext); err == nil {
		t.Fatal("control socket admitted a peer outside the operator UID allowlist")
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
			intent, err := lima.SetOCIIntent(context.Background(), intentPath, request.ExpectedRevision, false, time.Now())
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
