package ocihelper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
)

const helperListenerFD = 3

func IsInvocation(arguments []string) bool {
	return len(arguments) > 1 && arguments[1] == InvocationArg
}

// RunInvocation serves the inherited listener used by the private helper
// mode. Socket activation and installed units are intentionally left to the
// packaging ticket.
func RunInvocation(ctx context.Context, arguments []string, engine Engine, config ServerConfig) error {
	if len(arguments) != 2 || arguments[1] != InvocationArg {
		return errors.New("invalid OCI helper invocation")
	}
	if config.HelperChecksum == "" {
		checksum, err := ownExecutableChecksum()
		if err != nil {
			return err
		}
		config.HelperChecksum = checksum
	}
	listenerFile := os.NewFile(helperListenerFD, "wefty-oci-helper-listener")
	if listenerFile == nil {
		return errors.New("OCI helper listener is unavailable")
	}
	defer listenerFile.Close()
	listener, err := net.FileListener(listenerFile)
	if err != nil {
		return fmt.Errorf("open OCI helper listener: %w", err)
	}
	defer listener.Close()
	server, err := NewServer(engine, config)
	if err != nil {
		return err
	}
	return server.Serve(ctx, listener)
}

func ownExecutableChecksum() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("locate OCI helper executable: %w", err)
	}
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open OCI helper executable: %w", err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("checksum OCI helper executable: %w", err)
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}
