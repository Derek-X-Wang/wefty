package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
)

const proofInstallRelative = ".local/share/wefty-proof"
const proofLabelPrefix = "dev.wefty-proof."

// Proof paths never reuse the daily profile, binaries, skills or Lima home.
func resolveProofSetupPaths(home string) (setupPaths, error) {
	p := setupPaths{home: home}
	// Check every component, including absent final paths, before any setup
	// writes. Canonicalizing only data/ misses an existing redirected Lima home.
	for _, suffix := range []string{"bin", "config/oci-user.json", "data/lima/wefty-proof/sock", "data/oci-intent.json", "data/oci-setup.json", "data/node-agent/log-spool", "data/node-agent/managed", "data/home", "data/tmp", "logs"} {
		for path := filepath.Join(home, proofInstallRelative, suffix); path != home; path = filepath.Dir(path) {
			info, err := os.Lstat(path)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return p, err
			}
			if err == nil && info.Mode()&os.ModeSymlink != 0 {
				return p, errors.New("proof install refuses symlinked paths")
			}
		}
	}
	var err error
	for _, f := range []struct {
		target *string
		suffix string
	}{
		{&p.binRel, "bin"}, {&p.configRel, "config"}, {&p.dataRel, "data"}, {&p.logsRel, "logs"},
	} {
		*f.target, err = canonicalHomeFolder(home, filepath.Join(proofInstallRelative, f.suffix))
		if err != nil {
			return p, err
		}
		if *f.target != filepath.Join(proofInstallRelative, f.suffix) {
			return p, errors.New("proof install refuses redirected paths")
		}
	}
	p.launchAgentsRel, err = canonicalHomeFolder(home, filepath.Join("Library", "LaunchAgents"))
	if err != nil {
		return p, err
	}
	p.bin = filepath.Join(home, p.binRel)
	p.config = filepath.Join(home, p.configRel)
	p.data = filepath.Join(home, p.dataRel)
	p.logs = filepath.Join(home, p.logsRel)
	p.launchAgents = filepath.Join(home, p.launchAgentsRel)
	p.profile = filepath.Join(p.config, connprofile.FileName)
	p.nodeConfig = filepath.Join(p.config, "node.json")
	p.nodeData = filepath.Join(p.data, "node-agent")
	for _, folder := range []string{"log-spool", "managed"} {
		p.nodeDataFolders = append(p.nodeDataFolders, filepath.Join(p.dataRel, "node-agent", folder))
	}
	return p, nil
}

func removeProofSetup(ctx context.Context, host setupHost, purge bool) (setupRemoveReport, error) {
	report := setupRemoveReport{Changes: []string{}, Warnings: []string{}}
	if host.goos != "darwin" || host.euid == 0 {
		return report, errors.New("proof removal needs the macOS login user, without sudo")
	}
	home, err := filepath.EvalSymlinks(host.home)
	if err != nil {
		return report, err
	}
	paths, err := resolveProofSetupPaths(home)
	if err != nil {
		return report, err
	}
	unlock, err := lockSetup(paths, false)
	if err != nil {
		return report, err
	}
	defer unlock()
	var manifest setupManifest
	if err := limarunner.ReadUserInstallJSON(filepath.Join(paths.config, setupManifestName), &manifest); err != nil {
		return report, fmt.Errorf("proof removal needs its setup receipt: %w", err)
	}
	if manifest.Version != 1 || manifest.ProofRoot != "" && manifest.ProofRoot != filepath.Join(home, proofInstallRelative) {
		return report, errors.New("invalid proof setup receipt")
	}
	root, err := os.OpenRoot(home)
	if err != nil {
		return report, err
	}
	defer root.Close()
	if host.proofRemoveOutput != nil {
		if _, err := fmt.Fprintf(host.proofRemoveOutput, "Proof removal root: %s (purge=%t)\n", filepath.Join(home, proofInstallRelative), purge); err != nil {
			return report, err
		}
	}
	manager := launchagent.Manager{Run: host.launchctl, UID: host.uid, UnloadBound: host.unloadBound}
	// Unload all three proof services before changing files or stopping the host VM.
	for _, name := range []string{"node-agent", "l3", "l1"} {
		label := proofLabelPrefix + name
		plist := filepath.Join(paths.launchAgentsRel, label+".plist")
		if payload, err := root.ReadFile(plist); err == nil && payloadDigest(payload) != manifest.Files[plist] {
			return report, errors.New("proof plist changed; restore it before removal")
		}
		if err := manager.Unload(ctx, label); err != nil {
			return report, err
		}
	}
	hasHostVM := false
	for _, path := range []string{filepath.Join(paths.config, "oci-user.json"), filepath.Join(paths.data, "lima")} {
		if _, err := os.Lstat(path); err == nil {
			hasHostVM = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return report, err
		}
	}
	if hasHostVM {
		stop := host.stopHostVM
		if stop == nil {
			stop = stopInstalledHost
		}
		if err := stop(ctx, filepath.Join(paths.data, "lima"), "wefty-proof"); err != nil {
			return report, err
		}
	}
	for _, name := range []string{"node-agent", "l3", "l1"} {
		label := proofLabelPrefix + name
		if err := root.Remove(filepath.Join(paths.launchAgentsRel, label+".plist")); err != nil && !errors.Is(err, os.ErrNotExist) {
			return report, err
		}
		report.Changes = append(report.Changes, label)
	}
	// This explicitly disposable proof has its own private root. Purge removes
	// only this root after its processes and host VM have stopped. Daily OCI
	// removal never authorizes deletion of a host VM disk.
	if purge {
		if err := root.RemoveAll(proofInstallRelative); err != nil {
			return report, err
		}
	} else {
		for _, folder := range []string{paths.binRel, paths.configRel} {
			if err := root.RemoveAll(folder); err != nil {
				return report, err
			}
		}
	}
	return report, nil
}

func isolateProofServices(services []launchagent.Agent) {
	for i := range services {
		s := &services[i]
		s.Label = proofLabelPrefix + strings.TrimPrefix(s.Label, "dev.wefty.")
		s.Environment["HOME"] = filepath.Join(s.WorkingDirectory, "home")
		s.Environment["TMPDIR"] = filepath.Join(s.WorkingDirectory, "tmp")
		if _, ok := s.Environment["WEFTY_LAUNCH_UNIT"]; ok {
			s.Environment["WEFTY_LAUNCH_UNIT"] = s.Label
		}
	}
}

func stopInstalledHost(ctx context.Context, limaHome, instance string) error {
	if instance != "wefty-proof" && instance != limarunner.DefaultInstanceName {
		return errors.New("unrecognized installed host VM")
	}
	run := func(args ...string) ([]byte, error) {
		bounded, cancel := context.WithTimeout(ctx, 4*time.Minute)
		defer cancel()
		command := exec.CommandContext(bounded, "limactl", args...)
		command.WaitDelay = time.Second
		command.Env = append(os.Environ(), "LIMA_HOME="+limaHome)
		return command.Output()
	}

	state, err := installedHostState(run, instance)
	if err != nil {
		return err
	}
	if state == "" || state == "Stopped" {
		return nil
	}
	if _, err := run("stop", instance); err != nil {
		return fmt.Errorf("graceful host VM stop: %w", err)
	}
	state, err = installedHostState(run, instance)
	if err != nil {
		return err
	}
	if state != "" && state != "Stopped" {
		return errors.New("host VM did not confirm stopped after graceful stop")
	}
	return nil
}

func installedHostState(run func(...string) ([]byte, error), instance string) (string, error) {
	payload, err := run("list", "--json")
	if err != nil {
		return "", fmt.Errorf("inspect installed host VM: %w", err)
	}
	type entry struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	}
	var instances []entry
	if len(bytes.TrimSpace(payload)) > 0 && bytes.TrimSpace(payload)[0] == '[' {
		err = json.Unmarshal(payload, &instances)
	} else {
		decoder := json.NewDecoder(bytes.NewReader(payload))
		for {
			var v entry
			e := decoder.Decode(&v)
			if e == io.EOF {
				break
			}
			if e != nil {
				err = e
				break
			}
			instances = append(instances, v)
		}
	}
	if err != nil {
		return "", err
	}
	found := ""
	for _, v := range instances {
		if v.Name == instance {
			if found != "" {
				return "", errors.New("duplicate host VM inventory entry")
			}
			if v.Status == "" {
				return "", errors.New("missing host VM status")
			}
			found = v.Status
		}
	}
	return found, nil
}
