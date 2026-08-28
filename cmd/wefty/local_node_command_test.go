package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func TestSingularNodeCommandsBypassFabricAndUseLiveAgent(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "wefty-node-cli-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	socket := filepath.Join(root, "control.sock")
	intentPath := filepath.Join(root, "intent.json")
	if _, err := lima.InitializeOCIIntent(intentPath, time.Now()); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var archiveBytes []byte
	service := ocicontrol.ServiceFuncs{
		DoctorFunc: func(ctx context.Context) (ocicontrol.DoctorResponse, error) {
			report := ocicontrol.BuildDoctor(ctx, ocicontrol.DoctorConfig{
				HostPlatform: ocicontrol.PlatformFacts{OS: "linux", Architecture: "amd64"},
				AgentUser:    "operator", LaunchUnit: "wefty-agent.service",
				CapabilitySnapshot: func() agent.CapabilitySnapshot {
					return agent.CapabilitySnapshot{CapabilityObservation: contract.CapabilityObservation{
						Revision: 3, ObservedAt: time.Now(), Capabilities: map[string]bool{"kind:process": true},
						MissingCapabilities: []string{"kind:oci"}, ReasonCode: contract.CapabilityReasonProbeFailed,
					}, LastProbeAt: time.Now()}
				},
				Intent: func(context.Context) (lima.OCIIntent, error) {
					return (lima.FileIntentSource{Path: intentPath}).ReadIntent(ctx)
				},
				Helper: func(context.Context) (ocicontrol.HelperDoctorSnapshot, error) {
					return ocicontrol.HelperDoctorSnapshot{
						ProtocolVersion: ocihelper.ProtocolVersion, Version: "test", Checksum: "sha256:test", InstanceID: "helper", SessionGeneration: 1,
						RuntimePlatformRecorded: true,
						Runtime:                 ocihelper.DoctorStatus{RuntimePlatform: ocihelper.OCIPlatform{OS: "linux", Architecture: "amd64"}, ContainerdVersion: "2.3.4", RuncVersion: "1.3.3", AllowedMountRoots: []string{}, Cache: ocihelper.ImageCacheStatus{CapBytes: 16 << 30}},
					}, nil
				},
			})
			return report, report.Validate()
		},
		IntentFunc: func(ctx context.Context) (lima.OCIIntent, error) {
			return (lima.FileIntentSource{Path: intentPath}).ReadIntent(ctx)
		},
		StopFunc: func(_ context.Context, request ocicontrol.IntentMutationRequest) (ocicontrol.IntentResponse, error) {
			intent, err := lima.SetOCIIntent(context.Background(), intentPath, request.ExpectedRevision, false, time.Now())
			return ocicontrol.IntentResponse{Intent: intent, RuntimeQuiesced: err == nil}, err
		},
		LoadImageFunc: func(_ context.Context, archive io.Reader) (ocicontrol.LoadImageResponse, error) {
			payload, err := io.ReadAll(archive)
			mu.Lock()
			archiveBytes = payload
			mu.Unlock()
			return ocicontrol.LoadImageResponse{
				TopLevelDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				PlatformDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			}, err
		},
	}
	server, err := ocicontrol.NewServer(socket, service)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(ctx) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if info, err := os.Stat(socket); err == nil && info.Mode()&os.ModeSocket != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("control server did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	configPath := filepath.Join(root, "node.json")
	configPayload, _ := json.Marshal(ocicontrol.InstalledConfig{Version: ocicontrol.InstalledConfigVersion, ControlSocket: socket})
	if err := os.WriteFile(configPath, configPayload, 0o600); err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(root, "image.oci.tar")
	if err := os.WriteFile(archivePath, []byte("verified archive bytes"), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if err := run(t.Context(), []string{
		"--fabric=invalid-must-not-open", "--node-config=" + configPath, "--json", "node", "doctor",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("doctor: %v stderr=%s", err, stderr.String())
	}
	var doctor ocicontrol.DoctorResponse
	if err := json.Unmarshal(stdout.Bytes(), &doctor); err != nil || doctor.Version != ocicontrol.DoctorVersion || doctor.Probe.Outcome != ocicontrol.DiagnosticFailed {
		t.Fatalf("doctor output=%q decoded=%+v err=%v", stdout.String(), doctor, err)
	}
	stdout.Reset()
	stderr.Reset()
	if err := run(t.Context(), []string{
		"--fabric=invalid-must-not-open", "--node-config=" + configPath,
		"node", "load-image", archivePath,
	}, &stdout, &stderr); err != nil {
		t.Fatalf("load-image: %v stderr=%s", err, stderr.String())
	}
	mu.Lock()
	gotArchive := string(archiveBytes)
	mu.Unlock()
	if gotArchive != "verified archive bytes" || !bytes.Contains(stdout.Bytes(), []byte("TOP-LEVEL DIGEST")) {
		t.Fatalf("load-image archive=%q stdout=%q", gotArchive, stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	if err := run(t.Context(), []string{
		"--fabric=invalid-must-not-open", "--node-config=" + configPath, "--json",
		"node", "oci", "stop",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("oci stop: %v stderr=%s", err, stderr.String())
	}
	var stopped ocicontrol.IntentResponse
	if err := json.Unmarshal(stdout.Bytes(), &stopped); err != nil || stopped.Intent.Enabled || !stopped.RuntimeQuiesced {
		t.Fatalf("stop output=%q decoded=%+v err=%v", stdout.String(), stopped, err)
	}

	cancel()
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("control server did not stop")
	}
}
