package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

const setupManifestName = "setup-files.json"

type setupManifest struct {
	InstalledVersion string            `json:"installed_version,omitempty"`
	UpgradeSource    bool              `json:"upgrade_source,omitempty"`
	Version          int               `json:"version"`
	Files            map[string]string `json:"files"`
}
type setupRemoveReport struct {
	Changes  []string `json:"changes"`
	Warnings []string `json:"warnings"`
}

func payloadDigest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// Removal uses a fixed allowlist, never paths supplied by the manifest. The
// manifest records installed bytes; setup retains previously recorded skill
// files beneath the canonical skill roots when it rebuilds the receipt.
func runSetupRemove(ctx context.Context, host setupHost, purge bool) (setupRemoveReport, error) {
	report := setupRemoveReport{Changes: []string{}, Warnings: []string{}}
	if host.euid == 0 {
		return report, errors.New("wefty setup --remove must not run as root; run it as yourself without sudo")
	}
	if host.goos != "darwin" {
		return report, fmt.Errorf("wefty setup --remove needs macOS; this machine runs %s", host.goos)
	}
	if !filepath.IsAbs(host.home) {
		return report, errors.New("setup needs an absolute home folder")
	}
	home, err := filepath.EvalSymlinks(host.home)
	if err != nil {
		return report, err
	}
	paths, issues := resolveMaintenancePaths(home)
	for _, issue := range issues {
		if !slices.Contains(setupFolders.skills, issue.folder) {
			return report, issue.err
		}
		report.Warnings = append(report.Warnings, "left "+filepath.Join(home, issue.folder)+": "+issue.err.Error())
	}
	if purge && (paths.dataRel != setupFolders.data || paths.logsRel != setupFolders.logs) {
		return report, errors.New("purge requires standard data and log folders without redirected paths; move their symbolic links aside first")
	}
	unlock, err := lockSetup(paths, false)
	if err != nil {
		return report, err
	}
	defer unlock()
	root, err := os.OpenRoot(home)
	if err != nil {
		return report, err
	}
	defer root.Close()
	manifestPath := filepath.Join(paths.configRel, setupManifestName)
	manifest := setupManifest{Version: 1, Files: map[string]string{}}
	payload, err := root.ReadFile(manifestPath)
	setupEvidence := err == nil
	switch {
	case err == nil:
		if err := json.Unmarshal(payload, &manifest); err != nil || manifest.Version != 1 || manifest.Files == nil {
			return report, fmt.Errorf("invalid setup file digests at %s; restore it before removing the install", manifestPath)
		}
	case errors.Is(err, fs.ErrNotExist):
		// #894 installs predate the manifest. Prove ownership from setup's exact
		// rendered service definitions and their binary/profile input digests.
		manifest.Files = legacySetupFiles(host, paths, root)
	default:
		return report, err
	}
	skillFiles, err := setupSkillFiles(paths)
	if err != nil {
		return report, err
	}
	allowed := []string{filepath.Join(paths.configRel, connprofile.FileName), filepath.Join(paths.configRel, "node.json")}
	for _, name := range setupBinaries {
		allowed = append(allowed, filepath.Join(paths.binRel, name))
	}
	for _, file := range skillFiles {
		allowed = append(allowed, filepath.Join(file.folder, file.name))
	}
	labels := []string{setupNodeAgentLabel, setupL3Label, setupL1Label}
	plistNames := make([]string, 0, len(labels))
	for _, label := range labels {
		name := filepath.Join(paths.launchAgentsRel, label+".plist")
		plistNames = append(plistNames, name)
		allowed = append(allowed, name)
	}
	// Old embedded versions may have additional skill files. Accept only files
	// beneath the two canonical skill roots, never arbitrary manifest entries.
	for name := range manifest.Files {
		for _, target := range paths.skillTargets {
			rel, err := filepath.Rel(target, name)
			if err == nil && rel != "." && filepath.IsLocal(rel) && !slices.Contains(allowed, name) {
				allowed = append(allowed, name)
			}
		}
	}
	skillFolders := map[string]bool{}
	for _, target := range paths.skillTargets {
		skillFolders[target] = true
		for _, name := range allowed {
			rel, err := filepath.Rel(target, name)
			if err != nil || !filepath.IsLocal(rel) || rel == "." {
				continue
			}
			for folder := filepath.Dir(name); folder != target; folder = filepath.Dir(folder) {
				skillFolders[folder] = true
			}
		}
	}
	for _, target := range paths.skillTargets {
		if err := fs.WalkDir(root.FS(), target, func(name string, entry fs.DirEntry, err error) error {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			if err != nil {
				return err
			}
			if entry.IsDir() && !skillFolders[name] {
				report.Warnings = append(report.Warnings, "left "+filepath.Join(home, name)+": this skill folder was not installed by setup; inspect it before removing it yourself")
				return fs.SkipDir
			}
			if !entry.IsDir() && !slices.Contains(allowed, name) {
				allowed = append(allowed, name)
			}
			return nil
		}); err != nil {
			return report, err
		}
	}
	slices.Sort(allowed)
	owned := map[string]bool{}
	expected := map[string]string{}
	for _, name := range allowed {
		info, err := root.Lstat(name)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return report, err
		}
		if info.Mode().IsRegular() && slices.Contains(plistNames, name) {
			setupEvidence = true
		}
		if info.Mode().IsRegular() && setupMetadataPath(paths, name) {
			// These installation metadata files allow owner edits. Their path
			// and regular type establish removal, even without read access.
			owned[name] = true
		} else if info.Mode().IsRegular() {
			payload, err := root.ReadFile(name)
			if err != nil {
				return report, err
			}
			digest := manifest.Files[name]
			// Without a manifest, current embedded skill bytes are still known.
			if digest == "" {
				for _, file := range skillFiles {
					if name == filepath.Join(file.folder, file.name) {
						digest = payloadDigest(file.payload)
					}
				}
			}
			owned[name] = digest != "" && payloadDigest(payload) == digest
			expected[name] = digest
		}
		if !owned[name] {
			report.Warnings = append(report.Warnings, "left "+filepath.Join(home, name)+": content is not the file setup installed; inspect it and remove it yourself if appropriate")
		}
	}
	// Unload everything before deleting anything. A failed unload leaves the
	// files (and ownership evidence) available for another attempt.
	for _, label := range labels {
		name := filepath.Join(paths.launchAgentsRel, label+".plist")
		if !owned[name] {
			state, err := (launchagent.Manager{UID: host.uid, Run: host.launchctl}).State(ctx, label)
			if err != nil {
				return report, err
			}
			if state.Loaded {
				return report, fmt.Errorf("LaunchAgent %s is loaded but its setup-owned definition cannot be verified; run `wefty setup` again, then remove", label)
			}
			continue
		}
		bound := host.unloadBound
		if bound == 0 {
			bound = time.Duration(setupNodeAgentExitTimeout+15) * time.Second
		}
		if err := (launchagent.Manager{UID: host.uid, Run: host.launchctl, UnloadBound: bound}).Unload(ctx, label); err != nil {
			return report, err
		}
	}
	for _, name := range allowed {
		if !owned[name] {
			continue
		}
		// bootout may take time; recheck regular type and recorded bytes.
		// Regular installation metadata remains removable after owner edits.
		info, err := root.Lstat(name)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return report, err
		}
		matches := info.Mode().IsRegular()
		if matches && !setupMetadataPath(paths, name) {
			payload, err := root.ReadFile(name)
			matches = err == nil && payloadDigest(payload) == expected[name]
		}
		if !matches {
			report.Warnings = append(report.Warnings, "left "+filepath.Join(home, name)+": content is not the file setup installed; it changed during removal, so inspect it before deleting it yourself")
			continue
		}
		if err := root.Remove(name); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return report, err
		}
		report.Changes = append(report.Changes, filepath.Join(home, name))
	}
	if purge {
		if !setupEvidence {
			report.Warnings = append(report.Warnings, "left data and logs: no setup receipt or LaunchAgent plist proves setup installed this home; inspect them before removing them yourself")
		} else if err := purgeSetupEntries(root, paths, &report); err != nil {
			return report, err
		}
	}
	// Purge warnings also keep the receipt for another removal attempt.
	if len(report.Warnings) == 0 {
		if err := root.Remove(manifestPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return report, err
		}
	}
	// Remove empty private folders only. Shared bin and LaunchAgents folders
	// always remain; unrecognized skill files keep their folders in place.
	for _, target := range paths.skillTargets {
		pruneSetupEmpty(root, target, skillFolders)
	}
	_ = root.Remove(paths.configRel)
	// Unlink the locked state directory only as our final mutation. Another
	// installer may recreate it after this point without overlapping our writes.
	if purge && setupEvidence {
		if err := root.Remove(paths.dataRel); err == nil {
			report.Changes = append(report.Changes, paths.data)
		}
	}
	return report, nil
}
func setupMetadataPath(paths setupPaths, name string) bool {
	return name == filepath.Join(paths.configRel, connprofile.FileName) || name == filepath.Join(paths.configRel, "node.json")
}

func purgeSetupEntries(root *os.Root, paths setupPaths, report *setupRemoveReport) error {
	var manifest setupManifest
	if payload, err := root.ReadFile(filepath.Join(paths.configRel, setupManifestName)); err == nil {
		_ = json.Unmarshal(payload, &manifest)
	}
	for _, folder := range []string{paths.dataRel, paths.logsRel} {
		entries, err := fs.ReadDir(root.FS(), folder)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		for _, entry := range entries {
			name := entry.Name()
			if folder == paths.dataRel && name == "fabric" && entry.IsDir() {
				if err := purgeSetupParticipants(root, paths, report); err != nil {
					return err
				}
				continue
			}
			ownFile := folder == paths.dataRel && slices.Contains([]string{setupLockProbeFile, "l1.sqlite", "l1.sqlite-wal", "l1.sqlite-shm", "l1.sqlite-journal", "l3.sqlite", "l3.sqlite-wal", "l3.sqlite-shm", "l3.sqlite-journal"}, name) ||
				folder == paths.logsRel && slices.Contains([]string{"l1.log", "l3.log", "node-agent.log"}, name)
			ownFolder := folder == paths.dataRel && (name == "node-agent" || name == upgradeSourceName && manifest.Version == 1 && manifest.UpgradeSource) && entry.IsDir()
			rel := filepath.Join(folder, name)
			if !ownFolder && !(ownFile && entry.Type().IsRegular()) {
				report.Warnings = append(report.Warnings, "left "+filepath.Join(paths.home, rel)+": this entry was not installed by setup; inspect it before removing it yourself")
				continue
			}
			// The receipt never supplies data paths. Remove only known direct entries;
			// RemoveAll does not follow any links inside setup's node-agent folder.
			if err := root.RemoveAll(rel); err != nil {
				return err
			}
			report.Changes = append(report.Changes, filepath.Join(paths.home, rel))
		}
		// Remove only an empty container that existed when this pass began.
		if folder != paths.dataRel {
			if err := root.Remove(folder); err == nil {
				report.Changes = append(report.Changes, filepath.Join(paths.home, folder))
			}
		}
	}
	return nil
}

func purgeSetupParticipants(root *os.Root, paths setupPaths, report *setupRemoveReport) error {
	folder := filepath.Join(paths.dataRel, "fabric")
	entries, err := fs.ReadDir(root.FS(), folder)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		rel := filepath.Join(folder, entry.Name())
		_, _, known := setupParticipantPolicy(entry.Name())
		if !known || !entry.IsDir() {
			report.Warnings = append(report.Warnings, "left "+filepath.Join(paths.home, rel)+": this is not an enrolled participant folder; inspect it before removing it yourself")
			continue
		}
		if err := root.RemoveAll(rel); err != nil {
			return err
		}
		report.Changes = append(report.Changes, filepath.Join(paths.home, rel))
	}
	if err := root.Remove(folder); err == nil {
		report.Changes = append(report.Changes, filepath.Join(paths.home, folder))
	}
	return nil
}

func pruneSetupEmpty(root *os.Root, folder string, known map[string]bool) {
	entries, err := fs.ReadDir(root.FS(), folder)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() && known[filepath.Join(folder, entry.Name())] {
			pruneSetupEmpty(root, filepath.Join(folder, entry.Name()), known)
		}
	}
	_ = root.Remove(folder)
}

func legacySetupFiles(host setupHost, paths setupPaths, root *os.Root) map[string]string {
	result := map[string]string{}
	profile, err := connprofile.Read(paths.profile)
	if err != nil || profile.Fabric != "plain" {
		return result
	}
	config, err := ocicontrol.ReadInstalledConfig(paths.nodeConfig)
	if err != nil || config.ControlSocket != "" || config.NodeID != profile.Identities.Node {
		return result
	}
	nodePlist, err := root.ReadFile(filepath.Join(paths.launchAgentsRel, setupNodeAgentLabel+".plist"))
	if err != nil {
		return result
	}
	loginPATH, err := recordedNodePATH(nodePlist)
	if err != nil {
		return result
	}
	digests := map[string]string{}
	for _, name := range setupBinaries {
		payload, err := root.ReadFile(filepath.Join(paths.binRel, name))
		if err != nil {
			return result
		}
		digests[name] = payloadDigest(payload)
	}
	services, err := setupServices(paths, profile, digests, loginPATH)
	if err != nil {
		return result
	}
	for _, service := range services {
		want, err := launchagent.Render(service)
		if err != nil {
			return map[string]string{}
		}
		name := filepath.Join(paths.launchAgentsRel, service.Label+".plist")
		got, err := root.ReadFile(name)
		if err != nil || !bytes.Equal(got, want) {
			return map[string]string{}
		}
		result[name] = payloadDigest(want)
	}
	for _, name := range []string{"wefty-l1", "wefty-l3", "wefty-agent"} {
		result[filepath.Join(paths.binRel, name)] = digests[name]
	}
	// The CLI has no LaunchAgent input digest. Only the running version is
	// evidence for its bytes; an unrelated CLI in bin is left alone.
	if digest, err := fileDigest(host.executable); err == nil && digest == digests["wefty"] {
		result[filepath.Join(paths.binRel, "wefty")] = digest
	}
	for _, name := range []string{connprofile.FileName, "node.json"} {
		rel := filepath.Join(paths.configRel, name)
		if payload, err := root.ReadFile(rel); err == nil {
			result[rel] = payloadDigest(payload)
		}
	}
	return result
}
