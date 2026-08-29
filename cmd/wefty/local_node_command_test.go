package main

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

const localNodeCLIChildEnvironment = "WEFTY_LOCAL_NODE_CLI_CHILD"

func TestNodeLoadImageCLIReportsHelperMechanics(t *testing.T) {
	if os.Getenv(localNodeCLIChildEnvironment) == "1" {
		args := []string{
			"--fabric=invalid-must-not-open",
			"--node-config=" + os.Getenv("WEFTY_LOCAL_NODE_CLI_CONFIG"),
			"--json", "node", "load-image", os.Getenv("WEFTY_LOCAL_NODE_CLI_ARCHIVE"),
		}
		if err := run(context.Background(), args, os.Stdout, os.Stderr); err != nil {
			writeCommandError(os.Stderr, err, true)
			os.Exit(commandExitCodeForArgs(err, args))
		}
		return
	}

	root, err := os.MkdirTemp("/tmp", "wefty-node-cli-mechanics-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	archivePath := filepath.Join(root, "gnu-shaped.oci.tar")
	var archive bytes.Buffer
	archiveWriter := tar.NewWriter(&archive)
	if err := archiveWriter.WriteHeader(&tar.Header{Name: "./", Mode: 0o700, Typeflag: tar.TypeDir}); err != nil {
		t.Fatal(err)
	}
	if err := archiveWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(archivePath, archive.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	socket := filepath.Join(root, "control.sock")
	service := ocicontrol.ServiceFuncs{LoadImageFunc: func(_ context.Context, archive io.Reader) (ocicontrol.LoadImageResponse, error) {
		header, err := tar.NewReader(archive).Next()
		if err != nil || header.Name != "./" || !header.FileInfo().IsDir() {
			return ocicontrol.LoadImageResponse{}, errors.New("CLI did not stream the GNU-shaped archive")
		}
		return ocicontrol.LoadImageResponse{}, &ocihelper.RPCError{
			Code:    ocihelper.CodeImageUnavailable,
			Message: "OCI image delivery failed",
			ImageFailure: &ocihelper.ImageFailureFact{
				Kind:   ocihelper.ImageFailureManifestRejected,
				Reason: `OCI archive path "." is unsafe`,
			},
		}
	}}
	server, err := ocicontrol.NewServer(socket, service)
	if err != nil {
		t.Fatal(err)
	}
	serverContext, cancelServer := context.WithCancel(t.Context())
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(serverContext) }()
	t.Cleanup(func() {
		cancelServer()
		if err := <-serverDone; err != nil {
			t.Errorf("stop node-local control server: %v", err)
		}
	})
	deadline := time.Now().Add(3 * time.Second)
	for {
		if info, statErr := os.Stat(socket); statErr == nil && info.Mode()&os.ModeSocket != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("control server did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	configPath := filepath.Join(root, "node.json")
	configPayload, err := json.Marshal(ocicontrol.InstalledConfig{Version: ocicontrol.InstalledConfigVersion, ControlSocket: socket})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, configPayload, 0o600); err != nil {
		t.Fatal(err)
	}

	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestNodeLoadImageCLIReportsHelperMechanics$")
	command.Env = append(os.Environ(),
		localNodeCLIChildEnvironment+"=1",
		"WEFTY_LOCAL_NODE_CLI_CONFIG="+configPath,
		"WEFTY_LOCAL_NODE_CLI_ARCHIVE="+archivePath,
	)
	output, err := command.CombinedOutput()
	exitErr, ok := err.(*exec.ExitError)
	if !ok || exitErr.ExitCode() != 1 {
		t.Fatalf("wefty node load-image exit=%v output=%s", err, output)
	}
	var response contract.ErrorResponse
	if err := json.Unmarshal(output, &response); err != nil {
		t.Fatalf("decode CLI error: %v output=%s", err, output)
	}
	if response.Error.Code != contract.ErrorInternal || response.Error.Retryable || response.Error.Details["reason"] != string(ocihelper.CodeImageUnavailable) {
		t.Fatalf("CLI error=%+v", response.Error)
	}
	mechanics, ok := response.Error.Details["image_failure"].(map[string]any)
	if !ok || mechanics["kind"] != string(ocihelper.ImageFailureManifestRejected) || mechanics["reason"] != `OCI archive path "." is unsafe` {
		t.Fatalf("CLI image mechanics=%#v", response.Error.Details["image_failure"])
	}
}

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
					probeAt := time.Now()
					return agent.CapabilitySnapshot{CapabilityObservation: contract.CapabilityObservation{
						Revision: 3, ObservedAt: time.Now(), Capabilities: map[string]bool{"kind:process": true},
						MissingCapabilities: []string{"kind:oci"}, ReasonCode: contract.CapabilityReasonProbeFailed,
					}, LastProbe: &contract.CapabilityObservation{Revision: 3, ObservedAt: probeAt, Capabilities: map[string]bool{"kind:process": true}, MissingCapabilities: []string{"kind:oci"}, ReasonCode: contract.CapabilityReasonProbeFailed}}
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
		SetupFunc: func(ctx context.Context, request ocicontrol.SetupRequest) (ocicontrol.SetupResponse, error) {
			intent, err := (lima.FileIntentSource{Path: intentPath}).ReadIntent(ctx)
			return ocicontrol.SetupResponse{Configured: true, Intent: intent, Convergence: ocicontrol.ConvergenceUnchanged, ProbePreloaded: true}, err
		},
		StartFunc: func(_ context.Context, request ocicontrol.IntentMutationRequest) (ocicontrol.IntentResponse, error) {
			intent, err := lima.SetOCIIntent(context.Background(), intentPath, request.ExpectedRevision, true, time.Now())
			return ocicontrol.IntentResponse{Intent: intent, CapabilityPublished: err == nil}, err
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
	setupClient, err := ocicontrol.NewClient(socket)
	if err != nil {
		t.Fatal(err)
	}
	if err := executeLocalSetupOCI(t.Context(), setupClient, true, nil, &stdout, &stderr); err != nil {
		t.Fatalf("setup-oci: %v stderr=%s", err, stderr.String())
	}
	setupClient.Close()
	var setup ocicontrol.SetupResponse
	if err := json.Unmarshal(stdout.Bytes(), &setup); err != nil || !setup.Configured || setup.Convergence != ocicontrol.ConvergenceUnchanged || !setup.ProbePreloaded {
		t.Fatalf("setup output=%q decoded=%+v err=%v", stdout.String(), setup, err)
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
	stdout.Reset()
	stderr.Reset()
	if err := run(t.Context(), []string{
		"--fabric=invalid-must-not-open", "--node-config=" + configPath, "--json",
		"node", "oci", "start",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("oci start: %v stderr=%s", err, stderr.String())
	}
	var started ocicontrol.IntentResponse
	if err := json.Unmarshal(stdout.Bytes(), &started); err != nil || !started.Intent.Enabled || !started.CapabilityPublished {
		t.Fatalf("start output=%q decoded=%+v err=%v", stdout.String(), started, err)
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

func TestOCINodeRunbookCommandsUseExercisedSurfaces(t *testing.T) {
	payload, err := os.ReadFile(filepath.Join("..", "..", ocicontrol.RunbookPath))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"bash scripts/install-oci-deps.sh --dry-run": false,
		"sudo bash scripts/install-oci-deps.sh":      false,
		"scripts/build-oci-install-manifest.sh ":     false,
		"sudo wefty node setup-oci":                  false,
		"wefty node setup-oci":                       false,
		"wefty node doctor":                          false,
		"wefty node oci start":                       false,
		"wefty node oci stop":                        false,
		"wefty node load-image FILE":                 false,
		"wefty --json node doctor":                   false,
	}
	inShellBlock := false
	for _, line := range strings.Split(string(payload), "\n") {
		switch line {
		case "```sh":
			inShellBlock = true
			continue
		case "```":
			inShellBlock = false
			continue
		}
		if !inShellBlock || strings.TrimSpace(line) == "" {
			continue
		}
		normalized := strings.Join(strings.Fields(line), " ")
		matched := ""
		for command := range want {
			if normalized == command || strings.HasSuffix(command, " ") && strings.HasPrefix(normalized, command) || command == "sudo wefty node setup-oci" && strings.HasPrefix(normalized, command+" ") {
				matched = command
				break
			}
		}
		if matched == "" {
			t.Fatalf("runbook shell command is not covered by the installer matrix or singular CLI acceptance: %q", line)
		}
		want[matched] = true
	}
	for command, seen := range want {
		if !seen {
			t.Fatalf("checked runbook command is missing: %q", command)
		}
	}
}
