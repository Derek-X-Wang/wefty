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
	"strconv"
	"strings"
)

const helperListenerFD = 3

const AllowedUIDsEnvironment = "WEFTY_OCI_HELPER_ALLOWED_UIDS"

func IsInvocation(arguments []string) bool {
	return len(arguments) > 1 && arguments[1] == InvocationArg
}

// AllowedPeerUIDs parses the root-helper installation's comma-separated UID
// allowlist. An absent value retains the single-user development behavior.
func AllowedPeerUIDs(value string, fallback uint32) ([]uint32, error) {
	if strings.TrimSpace(value) == "" {
		return []uint32{fallback}, nil
	}
	parts := strings.Split(value, ",")
	result := make([]uint32, 0, len(parts))
	seen := make(map[uint32]struct{}, len(parts))
	for _, part := range parts {
		parsed, err := strconv.ParseUint(strings.TrimSpace(part), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("parse allowed OCI helper UID %q: %w", part, err)
		}
		uid := uint32(parsed)
		if _, exists := seen[uid]; exists {
			continue
		}
		seen[uid] = struct{}{}
		result = append(result, uid)
	}
	return result, nil
}

// RunInvocation serves the inherited listener used by the private helper
// mode. Mac socket activation is installed by the narrow Lima bootstrap;
// Linux unit installation is rendered by runner/linuxunit.
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
