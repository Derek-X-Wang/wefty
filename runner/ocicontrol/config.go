package ocicontrol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func DefaultInstalledConfigPath(home string) (string, error) {
	if !filepath.IsAbs(home) {
		return "", errors.New("operator home must be absolute")
	}
	return filepath.Join(home, ".config", "wefty", "node.json"), nil
}

func ReadInstalledConfig(path string) (InstalledConfig, error) {
	if !filepath.IsAbs(path) {
		return InstalledConfig{}, errors.New("installed node configuration path must be absolute")
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return InstalledConfig{}, fmt.Errorf("read installed node configuration: %w", err)
	}
	var config InstalledConfig
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return InstalledConfig{}, errors.New("read installed node configuration: invalid JSON")
	}
	if err := config.validate(); err != nil {
		return InstalledConfig{}, fmt.Errorf("read installed node configuration: %w", err)
	}
	return config, nil
}

// validate accepts two shapes. An OCI installation names its absolute control
// socket, and an older one may omit the node ID. A plain-process node has no
// OCI control socket, so it must name the stable node ID instead: a file with
// neither identifies nothing.
func (config InstalledConfig) validate() error {
	if config.Version != InstalledConfigVersion {
		return errors.New("invalid version")
	}
	if config.ControlSocket == "" {
		if strings.TrimSpace(config.NodeID) == "" {
			return errors.New("a configuration without an OCI control socket must record the stable node ID")
		}
		return nil
	}
	if !filepath.IsAbs(config.ControlSocket) || filepath.Clean(config.ControlSocket) == string(filepath.Separator) {
		return errors.New("invalid control socket")
	}
	return nil
}

func WriteInstalledConfig(path string, config InstalledConfig) error {
	if !filepath.IsAbs(path) {
		return errors.New("installed node configuration requires an absolute path")
	}
	payload, err := EncodeInstalledConfig(config)
	if err != nil {
		return err
	}
	return writeAtomic(path, payload, 0o600)
}

// EncodeInstalledConfig returns the exact bytes WriteInstalledConfig stores,
// so an installer that writes through its own confined folder handle stores
// the same document.
func EncodeInstalledConfig(config InstalledConfig) ([]byte, error) {
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("installed node configuration: %w", err)
	}
	payload, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	return append(payload, '\n'), nil
}

func writeAtomic(path string, payload []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create installed node configuration directory: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".wefty-node-config-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(mode); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(payload); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	err = directory.Sync()
	return errors.Join(err, directory.Close())
}
