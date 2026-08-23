//go:build service_acceptance_realtiming && (darwin || linux)

package ocihelper

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

const helperChildEnvironment = "WEFTY_OCI_HELPER_CHILD"

func TestServiceAcceptanceRealtimeRunsHelperChildWithFakeEngine(t *testing.T) {
	directory, err := os.MkdirTemp("", "wefty-oci-child-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)
	socketPath := filepath.Join(directory, "helper.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	unixListener := listener.(*net.UnixListener)
	unixListener.SetUnlinkOnClose(false)
	listenerFile, err := unixListener.File()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestRealtimeOCIHelperChildProcess$")
	command.Env = append(os.Environ(), helperChildEnvironment+"=1")
	command.ExtraFiles = []*os.File{listenerFile}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	_ = listenerFile.Close()
	_ = listener.Close()
	t.Cleanup(func() {
		_ = command.Process.Kill()
		_ = command.Wait()
	})

	client := NewUnixClient(socketPath, "realtime-fake-checksum")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	session, err := client.OpenSession(ctx, AcquireSessionRequest{
		NodeID: "realtime-node", BootSessionID: "realtime-boot",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if _, err := session.Sweep(ctx, SweepRequest{}); err != nil {
		t.Fatal(err)
	}
	if err := session.EnsureImage(ctx, EnsureImageRequest{Reference: "example.invalid/probe", Digest: "sha256:probe"}, nil); err != nil {
		t.Fatal(err)
	}
	authority := AttemptAuthority{
		NodeID: "realtime-node", JobID: "realtime-job", AttemptID: "realtime-attempt",
		FencingToken: "realtime-fence", BootSessionID: "realtime-boot", Class: "one-shot",
		RemovalGeneration: "realtime-removal",
	}
	if _, err := session.Run(ctx, testRunRequest(authority, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := session.Signal(ctx, SignalRequest{Authority: authority, Signal: "TERM"}); err != nil {
		t.Fatal(err)
	}
	if err := session.Watch(ctx, WatchRequest{Authority: authority}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Verify(ctx, VerifyRequest{Scope: VerifyAttempt, Authority: &authority}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Delete(ctx, DeleteRequest{Authority: authority}); err != nil {
		t.Fatal(err)
	}
}

func TestRealtimeOCIHelperChildProcess(t *testing.T) {
	if os.Getenv(helperChildEnvironment) != "1" {
		return
	}
	engine := newFakeEngine()
	if err := RunInvocation(context.Background(), []string{"wefty-agent", InvocationArg}, engine, ServerConfig{
		HelperVersion: "realtime-fake", HelperChecksum: "realtime-fake-checksum",
		HeartbeatTimeout: 10 * time.Second, MaximumAttemptDeadman: 10 * time.Second,
		AllowedUIDs: []uint32{uint32(os.Getuid())},
	}); err != nil {
		t.Fatal(err)
	}
}
