package main

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
)

// Resolve existing ancestors before enforcing home confinement. This permits
// the login home spelling to differ from its canonical spelling (e.g. /home
// mounted through a link to /var/home), even when the state folder is new.
func canonicalLinuxStateHome(home, stateHome string) (string, error) {
	if !filepath.IsAbs(stateHome) || filepath.Clean(stateHome) != stateHome {
		return "", errors.New("XDG_STATE_HOME must be an absolute clean path inside the home folder")
	}
	existing := stateHome
	var missing []string
	for {
		_, err := os.Lstat(existing)
		if err == nil {
			break
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
		missing = append([]string{filepath.Base(existing)}, missing...)
		parent := filepath.Dir(existing)
		if parent == existing {
			return "", errors.New("cannot resolve state home")
		}
		existing = parent
	}
	resolved, err := filepath.EvalSymlinks(existing)
	if err != nil {
		return "", err
	}
	resolved = filepath.Join(append([]string{resolved}, missing...)...)
	rel, err := filepath.Rel(home, resolved)
	if err != nil || !filepath.IsLocal(rel) {
		return "", errors.New("XDG_STATE_HOME must resolve inside the home folder; setup writes only inside the home folder")
	}
	// Only the home alias itself may be linked. The state/log directories must
	// remain real directories so another account cannot replace append: targets.
	for _, path := range []string{stateHome, filepath.Join(stateHome, "wefty")} {
		info, err := os.Lstat(path)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", err
		}
		if path == stateHome && resolved == home {
			continue
		}
		if !info.IsDir() {
			return "", errors.New("Linux log folder and its parent must be unlinked directories")
		}
	}
	return rel, nil
}

// The log folder is managed by wefty. Its parent is not: never chmod a shared
// XDG state root. Refuse unsafe parents rather than taking over their policy.
func validateLinuxLogFolders(paths setupPaths, uid int) error {
	for _, path := range []string{filepath.Dir(paths.logs), paths.logs} {
		info, err := os.Lstat(path)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.IsDir() || !setupFolderOwnedBy(info, uid) {
			return errors.New("Linux log folder and its parent must be unlinked directories owned by the setup user; restore private owner folders before rerunning setup")
		}
		if path != paths.logs && info.Mode().Perm()&0022 != 0 {
			return errors.New("Linux log parent is group- or world-writable; choose a private XDG_STATE_HOME before first setup, or restore the recorded parent's owner permissions")
		}
	}
	return nil
}

// Linux pins its log root before the first key. Environment changes on an SSH
// rerun or in doctor must not silently produce a different service definition.
func resolveFleetMaintenancePaths(home string, host setupHost) (setupPaths, []setupPathIssue) {
	if host.goos != "linux" {
		return resolvePlatformMaintenancePaths(home, host.goos, host.xdgStateHome)
	}
	base, issues := resolvePlatformMaintenancePaths(home, "linux", "")
	record, err := readFleetSetupRecord(base)
	if err != nil {
		return base, append(issues, setupPathIssue{fleetSetupRecordName, err})
	}
	recorded := ""
	if record != nil {
		recorded = record.LinuxLogRoot
	}
	var receipt setupManifest
	root, err := os.OpenRoot(home)
	if err != nil {
		return base, append(issues, setupPathIssue{setupManifestName, err})
	}
	defer root.Close()
	payload, readErr := root.ReadFile(filepath.Join(base.configRel, setupManifestName))
	if readErr == nil {
		if json.Unmarshal(payload, &receipt) != nil {
			return base, append(issues, setupPathIssue{setupManifestName, errors.New("invalid fleet receipt; restore the original receipt")})
		}
		if receipt.LinuxLogRoot != "" {
			if recorded != "" && recorded != receipt.LinuxLogRoot {
				return base, append(issues, setupPathIssue{setupManifestName, errors.New("recorded Linux log roots conflict; restore the original setup record and receipt")})
			}
			recorded = receipt.LinuxLogRoot
		}
	} else if !errors.Is(readErr, fs.ErrNotExist) {
		return base, append(issues, setupPathIssue{setupManifestName, readErr})
	}
	if (record != nil || receipt.Role != serviceplan.Legacy) && recorded == "" {
		return base, append(issues, setupPathIssue{fleetSetupRecordName, errors.New("existing Linux fleet records have no log root; restore their original linux_log_root before rerunning setup")})
	}
	if recorded != "" {
		if !filepath.IsAbs(recorded) || filepath.Clean(recorded) != recorded || filepath.Base(recorded) != "wefty" {
			return base, append(issues, setupPathIssue{fleetSetupRecordName, errors.New("invalid recorded Linux log root; restore the original records")})
		}
		paths, pinnedIssues := resolvePlatformMaintenancePaths(home, "linux", filepath.Dir(recorded))
		if paths.logs != recorded {
			pinnedIssues = append(pinnedIssues, setupPathIssue{fleetSetupRecordName, errors.New("recorded Linux log root moved through a link; restore its original unlinked folder")})
		}
		return paths, pinnedIssues
	}
	return resolvePlatformMaintenancePaths(home, "linux", host.xdgStateHome)
}

var systemdVersionLine = regexp.MustCompile(`^systemd ([0-9]+)(?:[ \t].*)?$`)

func preflightLinuxFleet(ctx context.Context, host setupHost, paths setupPaths, digests map[string]string, loginPATH string, role serviceplan.Role) error {
	if err := validateLinuxLogFolders(paths, host.euid); err != nil {
		return err
	}
	// Render the full plan with the same descriptors used after enrolment. The
	// saved profile changes digests, not paths or arguments.
	services, err := setupRoleServices(paths, connprofile.Profile{}, digests, loginPATH, role)
	if err != nil {
		return err
	}
	manager := fleetServiceManager{host: host}
	backend, err := manager.linuxManager(paths)
	if err != nil {
		return err
	}
	previous := ""
	for _, service := range services {
		if _, err := manager.render(service, previous); err != nil {
			return err
		}
		if err := backend.Preflight(manager.name(service.Label)); err != nil {
			return err
		}
		previous = service.Label
	}
	if host.systemctl == nil {
		return errors.New("Linux fleet setup needs systemd 242 or newer; cannot inspect systemctl version")
	}
	probe, cancel := context.WithTimeout(ctx, statusProbeBudget)
	defer cancel()
	output, err := host.systemctl(probe, backend.ToolPath, "--version")
	first, _, _ := strings.Cut(string(output), "\n")
	match := systemdVersionLine.FindStringSubmatch(strings.TrimSuffix(first, "\r"))
	version := 0
	if len(match) == 2 {
		version, _ = strconv.Atoi(match[1])
	}
	if err != nil || version < 242 {
		return errors.New("Linux fleet setup requires systemd 242 or newer (append logs and literal ExecStart); version is old or unverified; install a supported systemd before rerunning setup")
	}
	return nil
}

const linuxPersonWarning = "Warning: --with-person is meant for the owner's laptop; it grants local programs access to the owner person identity."
