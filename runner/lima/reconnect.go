package lima

import (
	"context"
	"errors"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

const defaultSocketRetry = 50 * time.Millisecond
const defaultReconnectTimeout = 2 * time.Second

// EpochSocketDialer follows Lima's forwarded socket across VM epochs. A
// socket inode replacement advances Epoch; each connection always dials the
// current path, and transient absence/refusal is retried only within the
// caller's deadline.
type EpochSocketDialer struct {
	Path             string
	RetryInterval    time.Duration
	ReconnectTimeout time.Duration
}

func (dialer *EpochSocketDialer) Dial(ctx context.Context) (net.Conn, error) {
	timeout := dialer.ReconnectTimeout
	if timeout <= 0 {
		timeout = defaultReconnectTimeout
	}
	reconnectContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	interval := dialer.RetryInterval
	if interval <= 0 {
		interval = defaultSocketRetry
	}
	connector := &net.Dialer{}
	for {
		info, statErr := os.Stat(dialer.Path)
		if statErr == nil && info.Mode()&os.ModeSocket == 0 {
			return nil, errors.New("Lima helper forward is not a Unix socket")
		}
		connection, err := connector.DialContext(reconnectContext, "unix", dialer.Path)
		if err == nil {
			return connection, nil
		}
		if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, syscall.ECONNREFUSED) {
			return nil, err
		}
		timer := time.NewTimer(interval)
		select {
		case <-reconnectContext.Done():
			timer.Stop()
			return nil, reconnectContext.Err()
		case <-timer.C:
		}
	}
}

func NewHelperClient(socketPath, expectedChecksum string) (*ocihelper.Client, *EpochSocketDialer) {
	dialer := &EpochSocketDialer{Path: socketPath}
	return &ocihelper.Client{
		Dial: dialer.Dial, Version: ocihelper.ProtocolVersion, ExpectedChecksum: expectedChecksum,
	}, dialer
}
