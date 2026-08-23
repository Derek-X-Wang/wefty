package lima

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEpochSocketDialerReconnectsAfterForwardReplacement(t *testing.T) {
	directory, err := os.MkdirTemp("", "wl-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "helper.sock")
	dialer := &EpochSocketDialer{Path: path, RetryInterval: time.Millisecond}

	first := listenUnix(t, path)
	connection, err := dialer.Dial(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := first.Accept()
	if err != nil {
		t.Fatal(err)
	}
	connection.Close()
	accepted.Close()
	first.Close()
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	second := listenUnix(t, path)
	defer second.Close()
	connection, err = dialer.Dial(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	accepted, err = second.Accept()
	if err != nil {
		t.Fatal(err)
	}
	connection.Close()
	accepted.Close()
}

func TestEpochSocketDialerBoundsMissingForwardByContext(t *testing.T) {
	dialer := &EpochSocketDialer{Path: filepath.Join(t.TempDir(), "missing.sock"), RetryInterval: time.Millisecond}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if _, err := dialer.Dial(ctx); err == nil {
		t.Fatal("missing forwarded socket did not honor the reconnect deadline")
	}
}

func TestEpochSocketDialerBoundsReconnectWithoutCallerDeadline(t *testing.T) {
	dialer := &EpochSocketDialer{
		Path: filepath.Join(t.TempDir(), "missing.sock"), RetryInterval: time.Millisecond, ReconnectTimeout: 10 * time.Millisecond,
	}
	started := time.Now()
	if _, err := dialer.Dial(context.Background()); err == nil {
		t.Fatal("missing forwarded socket was not independently bounded")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded reconnect took %v", elapsed)
	}
}

func listenUnix(t *testing.T, path string) net.Listener {
	t.Helper()
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	return listener
}
