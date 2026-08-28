package ocicontrol

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type controlTestClock struct{ now time.Time }

func (clock *controlTestClock) Now() time.Time { return clock.now }

type controlTestRuntime struct {
	intentPath string
	mu         sync.Mutex
	recovered  int
	stopped    int
	stopErr    error
}

func (runtime *controlTestRuntime) RecoverOCIRuntimeCapabilities(context.Context) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	intent, err := (lima.FileIntentSource{Path: runtime.intentPath}).ReadIntent(context.Background())
	if err != nil || !intent.Enabled {
		return errors.New("runtime recovery preceded durable enable")
	}
	runtime.recovered++
	return nil
}

func (runtime *controlTestRuntime) StopOCIRuntime(context.Context) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	intent, err := (lima.FileIntentSource{Path: runtime.intentPath}).ReadIntent(context.Background())
	if err != nil || intent.Enabled {
		return errors.New("runtime stop preceded durable disable")
	}
	runtime.stopped++
	return runtime.stopErr
}

type controlTestImages struct {
	reference string
	archive   string
}

func (images *controlTestImages) LoadImage(_ context.Context, reference string, archive io.Reader) (ocihelper.EnsureImageResponse, error) {
	payload, err := io.ReadAll(archive)
	images.reference = reference
	images.archive = string(payload)
	return ocihelper.EnsureImageResponse{
		TopLevelDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		PlatformDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}, err
}

func TestControllerPersistsIntentBeforeRuntimeEffects(t *testing.T) {
	path := filepath.Join(t.TempDir(), "intent.json")
	clock := &controlTestClock{now: time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)}
	if _, err := lima.InitializeOCIIntent(path, clock.Now()); err != nil {
		t.Fatal(err)
	}
	runtime := &controlTestRuntime{intentPath: path}
	images := &controlTestImages{}
	controller, err := NewController(ControllerConfig{IntentPath: path, Runtime: runtime, Images: images, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	stopped, err := controller.Stop(t.Context(), IntentMutationRequest{ExpectedRevision: 1})
	if err != nil || stopped.Intent.Enabled || stopped.Intent.Revision != 2 || !stopped.RuntimeQuiesced || runtime.stopped != 1 {
		t.Fatalf("stop response=%+v runtime=%+v err=%v", stopped, runtime, err)
	}
	if _, err := controller.LoadImage(t.Context(), bytes.NewReader([]byte("archive"))); err == nil {
		t.Fatal("disabled intent admitted image loading")
	}
	clock.now = clock.now.Add(time.Minute)
	started, err := controller.Start(t.Context(), IntentMutationRequest{ExpectedRevision: 2})
	if err != nil || !started.Intent.Enabled || started.Intent.Revision != 3 || !started.CapabilityPublished || runtime.recovered != 1 {
		t.Fatalf("start response=%+v runtime=%+v err=%v", started, runtime, err)
	}
	loaded, err := controller.LoadImage(t.Context(), bytes.NewReader([]byte("verified-archive")))
	if err != nil || loaded.TopLevelDigest == "" || images.reference != "" || images.archive != "verified-archive" {
		t.Fatalf("load-image response=%+v reference=%q archive=%q err=%v", loaded, images.reference, images.archive, err)
	}
}

func TestControllerFailureNeverRollsBackPersistedDisable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "intent.json")
	clock := &controlTestClock{now: time.Date(2026, 8, 28, 13, 0, 0, 0, time.UTC)}
	if _, err := lima.InitializeOCIIntent(path, clock.Now()); err != nil {
		t.Fatal(err)
	}
	runtime := &controlTestRuntime{intentPath: path, stopErr: errors.New("positive runtime quiescence unavailable")}
	controller, _ := NewController(ControllerConfig{IntentPath: path, Runtime: runtime, Clock: clock})
	if _, err := controller.Stop(t.Context(), IntentMutationRequest{ExpectedRevision: 1}); err == nil {
		t.Fatal("failed runtime quiescence returned success")
	}
	intent, err := controller.Intent(t.Context())
	if err != nil || intent.Enabled || intent.Revision != 2 {
		t.Fatalf("persisted fail-closed intent=%+v err=%v", intent, err)
	}
	if _, err := controller.Start(t.Context(), IntentMutationRequest{ExpectedRevision: 1}); err == nil {
		t.Fatal("stale intent revision was accepted")
	}
}

func TestSetupPreservesExistingDisabledIntent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "intent.json")
	clock := &controlTestClock{now: time.Date(2026, 8, 28, 14, 0, 0, 0, time.UTC)}
	if _, err := lima.InitializeOCIIntent(path, clock.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := lima.SetOCIIntent(path, 1, false, clock.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	controller, _ := NewController(ControllerConfig{
		IntentPath: path, Clock: clock,
		Setup: func(_ context.Context, _ SetupRequest, intent lima.OCIIntent) (SetupResponse, error) {
			return SetupResponse{Configured: true, Intent: intent, Convergence: ConvergenceUnchanged}, nil
		},
	})
	response, err := controller.Setup(t.Context(), SetupRequest{VMMemory: "4GiB", VMCPUs: 4, VMDisk: "32GiB"})
	if err != nil || !response.Configured || response.Intent.Enabled || response.Intent.Revision != 2 {
		t.Fatalf("setup response=%+v err=%v", response, err)
	}
}

func TestSetupRejectsAdversarialSizingBeforeInitializingIntent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "intent.json")
	controller, _ := NewController(ControllerConfig{IntentPath: path, Clock: &controlTestClock{now: time.Now()}})
	if _, err := controller.Setup(t.Context(), SetupRequest{VMMemory: "-1", VMCPUs: 0, VMDisk: "32GiB"}); err == nil {
		t.Fatal("invalid setup sizing was accepted")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid setup initialized intent: %v", err)
	}
}

func TestInstalledConfigAndSocketPathFailClosed(t *testing.T) {
	root := t.TempDir()
	for _, test := range []struct {
		name    string
		payload string
	}{
		{name: "unknown version", payload: `{"version":2,"control_socket":"/tmp/wefty.sock"}`},
		{name: "relative socket", payload: `{"version":1,"control_socket":"wefty.sock"}`},
		{name: "root socket", payload: `{"version":1,"control_socket":"/"}`},
		{name: "malformed", payload: `{`},
		{name: "unknown field", payload: `{"version":1,"control_socket":"/tmp/wefty.sock","enabled":true}`},
		{name: "trailing value", payload: `{"version":1,"control_socket":"/tmp/wefty.sock"} {}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(root, test.name+".json")
			if err := os.WriteFile(path, []byte(test.payload), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadInstalledConfig(path); err == nil {
				t.Fatal("adversarial installed configuration was accepted")
			}
		})
	}
	nonSocket := filepath.Join(root, "control.sock")
	if err := os.WriteFile(nonSocket, []byte("do not replace"), 0o600); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(nonSocket, ServiceFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Serve(t.Context()); err == nil {
		t.Fatal("control server replaced a non-socket path")
	}
}
