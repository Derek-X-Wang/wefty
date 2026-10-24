package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

func setupUserOCIFiles(host setupHost, paths setupPaths, request setupRequest, nodeID string, services []launchagent.Agent) ([]setupFile, error) {
	configPath := filepath.Join(paths.config, "oci-user.json")
	var c limarunner.UserInstallConfig
	err := limarunner.ReadUserInstallJSON(configPath, &c)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if request.ociConfig == "" && errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if request.ociConfig != "" {
		if err := limarunner.ReadUserInstallJSON(request.ociConfig, &c.Assets); err != nil {
			return nil, err
		}
	}
	if c.Instance == "" {
		sizing, err := limarunner.HostPerUserDefaultSizing()
		if err != nil {
			return nil, err
		}
		c.Instance = limarunner.DefaultInstanceName
		if host.proof {
			c.Instance = "wefty-proof"
			c.ProofDialog = true
		}
		c.LimaHome = filepath.Join(paths.data, "lima")
		c.MountRoot = paths.nodeData
		c.IntentFile = filepath.Join(paths.data, "oci-intent.json")
		c.SetupState = filepath.Join(paths.data, "oci-setup.json")
		c.TemplatePath = filepath.Join(paths.config, "host-vm.yaml")
		c.Sizing = sizing
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("per-user OCI install: %w", err)
	}
	if c.LimaHome != filepath.Join(paths.data, "lima") || c.MountRoot != paths.nodeData || c.IntentFile != filepath.Join(paths.data, "oci-intent.json") || c.SetupState != filepath.Join(paths.data, "oci-setup.json") || c.TemplatePath != filepath.Join(paths.config, "host-vm.yaml") {
		return nil, errors.New("per-user OCI install paths do not match this install")
	}
	if host.proof && c.Instance != "wefty-proof" || !host.proof && c.Instance != limarunner.DefaultInstanceName {
		return nil, errors.New("per-user OCI install instance does not match this install")
	}
	payload, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return nil, err
	}
	template, err := limarunner.RenderTemplate(limarunner.TemplateConfig{Sizing: c.Sizing, HostAllowedMountRoot: c.MountRoot})
	if err != nil {
		return nil, err
	}
	files := []setupFile{{folder: paths.configRel, name: "oci-user.json", payload: append(payload, '\n'), mode: 0600, change: "configured per-user host VM"}, {folder: paths.configRel, name: "host-vm.yaml", payload: template, mode: 0600, change: "wrote host VM template"}}

	if info, err := os.Lstat(c.IntentFile); err == nil {
		if !info.Mode().IsRegular() {
			return nil, errors.New("OCI intent must be a regular file")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if _, err := (limarunner.FileIntentSource{Path: c.IntentFile}).ReadIntent(context.Background()); err != nil {
		return nil, err
	}
	controlSocket := filepath.Join(paths.data, "oci-control.sock")
	node, err := ocicontrol.EncodeInstalledConfig(ocicontrol.InstalledConfig{Version: ocicontrol.InstalledConfigVersion, NodeID: nodeID, ControlSocket: controlSocket})
	if err != nil {
		return nil, err
	}
	files = append(files, setupFile{folder: paths.configRel, name: "node.json", payload: node, mode: 0600})
	capacity, reserve, _ := limarunner.DefaultMacComputerCapacity(c.Sizing)
	socket, _ := limarunner.HelperSocketPath(c.LimaHome, c.Instance)
	for i := range services {
		s := &services[i]
		if s.Label != setupNodeAgentLabel {
			continue
		}
		s.Environment["LIMA_HOME"] = c.LimaHome
		s.Environment["USER"] = host.username
		s.Environment["LOGNAME"] = host.username
		s.Arguments = append(s.Arguments, "--oci-user-install="+configPath, "--oci-helper-socket="+socket, "--oci-helper-checksum="+c.Assets.HelperChecksum,
			"--oci-probe-image="+c.Assets.ProbeReference, "--oci-probe-digest="+c.Assets.ProbeDigest, "--oci-probe-archive="+c.Assets.ProbeArchive,
			"--oci-lima-instance="+c.Instance, "--oci-lima-host-mount-root="+c.MountRoot, "--oci-intent-file="+c.IntentFile,
			"--oci-control-socket="+controlSocket, "--oci-setup-state="+c.SetupState, "--oci-minimal-doctor-facts="+filepath.Join(paths.data, "oci-facts.json"),
			"--oci-memory-capacity-bytes="+strconv.FormatInt(capacity, 10), "--oci-memory-reserve-bytes="+strconv.FormatInt(reserve, 10))
		digest, err := connprofile.Digest(struct {
			Previous string
			Config   limarunner.UserInstallConfig
		}{s.InputDigest, c})
		if err != nil {
			return nil, err
		}
		s.InputDigest = digest
	}
	return files, nil
}

func setupHasUserOCI(paths setupPaths) bool {
	var c limarunner.UserInstallConfig
	return limarunner.ReadUserInstallJSON(filepath.Join(paths.config, "oci-user.json"), &c) == nil && c.LimaHome == filepath.Join(paths.data, "lima")
}

// Use the existing exclusive initializer at the write boundary. In particular,
// never converge a stale snapshot over a concurrent operator intent change.
func initializeSetupOCIIntent(paths setupPaths) (bool, error) {
	return limarunner.InitializeOCIIntent(filepath.Join(paths.data, "oci-intent.json"), time.Now())
}
