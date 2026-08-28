package ocicontrol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

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
	if config.Version != InstalledConfigVersion || !filepath.IsAbs(config.ControlSocket) || filepath.Clean(config.ControlSocket) == string(filepath.Separator) {
		return InstalledConfig{}, errors.New("read installed node configuration: invalid version or control socket")
	}
	return config, nil
}

func WriteInstalledConfig(path string, config InstalledConfig) error {
	if !filepath.IsAbs(path) || config.Version != InstalledConfigVersion || !filepath.IsAbs(config.ControlSocket) || filepath.Clean(config.ControlSocket) == string(filepath.Separator) {
		return errors.New("installed node configuration requires absolute paths and the current version")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create installed node configuration directory: %w", err)
	}
	payload, err := json.Marshal(config)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	temporary, err := os.CreateTemp(filepath.Dir(path), ".wefty-node-config-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
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
