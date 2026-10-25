package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/internal/upgradestate"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

// Read-only selection precedes cleanup, locking and recovery. A partially
// published fleet profile must never fall through into legacy maintenance.
func selectUpgradeRole(paths setupPaths) (serviceplan.Role, error) {
	conversion, err := readPlainConversion(paths)
	if err != nil {
		return "", err
	}
	if conversion != nil {
		completed, err := plainConversionCompleted(paths, conversion)
		if err != nil {
			return "", err
		}
		if !completed {
			return "", refuseRecordedFleetRole(paths, "upgrade")
		}
	}
	record, err := readFleetSetupRecord(paths)
	if err != nil {
		return "", err
	}
	if record != nil && (!record.Committed || !record.ProfilePublished) {
		return "", errors.New("upgrade refuses staged fleet setup; resume setup before upgrade")
	}
	if record == nil {
		if _, err := os.Lstat(filepath.Join(paths.config, fleetSetupProfileName)); err == nil {
			return "", errors.New("upgrade refuses staged fleet profile without a committed record")
		} else if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
		// Preserve the legacy guard's exact errors, including malformed or
		// unreadable receipts. Only a committed fleet record takes the new path.
		if err := refuseRecordedFleetRole(paths, "upgrade"); err != nil {
			return "", err
		}
		return serviceplan.Legacy, nil
	}
	payload, err := os.ReadFile(filepath.Join(paths.config, setupManifestName))
	if err != nil {
		return "", err
	}
	var metadata map[string]json.RawMessage
	if json.Unmarshal(payload, &metadata) != nil {
		return "", errors.New("upgrade refuses invalid recorded fleet role receipt")
	}
	var role serviceplan.Role
	if raw, ok := metadata["role"]; ok {
		if json.Unmarshal(raw, &role) != nil || role.Validate() != nil {
			return "", errors.New("upgrade refuses invalid recorded fleet role metadata")
		}
	}
	if role == serviceplan.Legacy {
		return "", errors.New("upgrade refuses recorded fleet role without its matching receipt")
	}
	if record.Role != role {
		return "", errors.New("upgrade refuses recorded fleet role without a matching committed setup record")
	}
	if _, _, err := readFleetUpgradeInstall(paths, role); err != nil {
		return "", err
	}
	return role, nil
}

func readFleetUpgradeInstall(paths setupPaths, role serviceplan.Role) (connprofile.Profile, setupManifest, error) {
	var manifest setupManifest
	if err := checkFleetProfileFile(paths.profile); err != nil {
		return connprofile.Profile{}, manifest, err
	}
	payload, err := os.ReadFile(filepath.Join(paths.config, setupManifestName))
	if err != nil {
		return connprofile.Profile{}, manifest, err
	}
	if json.Unmarshal(payload, &manifest) != nil || manifest.Version != 1 || manifest.Role != role || manifest.Files == nil {
		return connprofile.Profile{}, manifest, errors.New("upgrade refuses invalid recorded fleet role receipt")
	}
	profilePayload, err := os.ReadFile(paths.profile)
	if err != nil {
		return connprofile.Profile{}, manifest, err
	}
	if digest := manifest.Files[filepath.Join(paths.configRel, connprofile.FileName)]; digest == "" || digest != payloadDigest(profilePayload) {
		return connprofile.Profile{}, manifest, errors.New("upgrade refuses fleet profile that disagrees with its receipt")
	}
	profile, err := connprofile.Read(paths.profile)
	if err != nil {
		return profile, manifest, err
	}
	if err := serviceplan.ValidateProfile(role, profile); err != nil {
		return profile, manifest, err
	}
	record, err := readFleetSetupRecord(paths)
	if err != nil {
		return profile, manifest, err
	}
	if record == nil || !record.Committed || !record.ProfilePublished || record.Role != role || record.NodeID != profile.Identities.Node {
		return profile, manifest, errors.New("upgrade refuses fleet role or Node ID that disagrees with its committed record")
	}
	if err := checkFleetConversionHistory(record, manifest.History); err != nil {
		return profile, manifest, err
	}
	if role == serviceplan.Join {
		request := setupRequest{joinCode: record.JoinCode, coordinatorURL: record.CoordinatorURL, withPerson: record.WithPerson}
		descriptor, err := selectJoinSetup(paths, &request, record)
		if err != nil {
			return profile, manifest, err
		}
		if descriptor.L1 != profile.L1Address || descriptor.L3 != profile.L3Address {
			return profile, manifest, errors.New("upgrade refuses fleet endpoints that disagree with the committed join code")
		}
	}
	for r, part := range profile.Participants {
		if part.StateDirectory != filepath.Join(paths.data, "fabric", r) || part.CoordinatorURL != record.CoordinatorURL {
			return profile, manifest, errors.New("upgrade refuses participant paths or coordinator that disagree with this installation")
		}
	}
	config, err := ocicontrol.ReadInstalledConfig(paths.nodeConfig)
	if err != nil {
		return profile, manifest, err
	}
	if config.ControlSocket != "" || config.NodeID != profile.Identities.Node {
		return profile, manifest, errors.New("fleet upgrade requires a matching plain-process node")
	}
	return profile, manifest, nil
}

func fleetUpgradeBinaries(plan serviceplan.Plan) []string {
	names := []string{"wefty"}
	for _, s := range plan.Services {
		if !slices.Contains(names, s.Binary) {
			names = append(names, s.Binary)
		}
	}
	return names
}

func fleetUpgradeOperations(host setupHost, paths setupPaths, p connprofile.Profile, plan serviceplan.Plan) upgradeHost {
	op := upgradeOperations(host)
	if host.upgrade == nil || host.upgrade.build == nil {
		op.build = func(ctx context.Context, source, dest, commit, loginPATH string) error {
			tool, err := findUpgradeTool(loginPATH, "go")
			if err != nil {
				return err
			}
			args := []string{"build", "-trimpath", "-ldflags=-X main.version=" + commit, "-o", dest + string(filepath.Separator)}
			for _, name := range fleetUpgradeBinaries(plan) {
				args = append(args, "./cmd/"+name)
			}
			_, err = op.command(ctx, source, upgradeBuildEnvironment(loginPATH), tool, args...)
			return err
		}
	}
	open := host.fleetClients
	if open == nil {
		open = setupFleetClients
	}
	with := func(ctx context.Context, action func(context.Context, *apiClients) error) error {
		bounded, cancel := context.WithTimeout(ctx, upgradeMutationBudget)
		defer cancel()
		clients, err := open(bounded, paths, p)
		if err != nil {
			return err
		}
		defer clients.close()
		return action(bounded, clients)
	}
	if host.upgrade == nil || host.upgrade.observe == nil {
		op.observe = func(ctx context.Context, _ connprofile.Profile) (node l1.Node, err error) {
			err = with(ctx, func(ctx context.Context, c *apiClients) error {
				var err error
				node, err = c.getNode(ctx, p.Identities.Node)
				if err != nil {
					return err
				}
				_, err = c.listRuns(ctx, "", 1)
				return err
			})
			return
		}
	}
	if host.upgrade == nil || host.upgrade.readNode == nil {
		op.readNode = func(ctx context.Context, _ connprofile.Profile) (node l1.Node, err error) {
			err = with(ctx, func(ctx context.Context, c *apiClients) error {
				var err error
				node, err = c.getNode(ctx, p.Identities.Node)
				return err
			})
			return
		}
	}
	if host.upgrade == nil || host.upgrade.setClaims == nil {
		op.setClaims = func(ctx context.Context, _ connprofile.Profile, r l1.NodeIntentRequest) (node l1.Node, err error) {
			err = with(ctx, func(ctx context.Context, c *apiClients) error {
				var err error
				node, err = c.setNodeClaims(ctx, p.Identities.Node, r)
				return err
			})
			return
		}
	}
	if host.upgrade == nil || host.upgrade.readJob == nil {
		op.readJob = func(ctx context.Context, _ connprofile.Profile, id string) (job l1.Job, err error) {
			err = with(ctx, func(ctx context.Context, c *apiClients) error {
				var err error
				err = c.l1.do(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(id), nil, nil, &job, http.StatusOK)
				return err
			})
			return
		}
	}
	if host.upgrade == nil || host.upgrade.restartService == nil {
		op.restartService = func(ctx context.Context, _ connprofile.Profile, id, key string) (job l1.Job, err error) {
			err = with(ctx, func(ctx context.Context, c *apiClients) error {
				var err error
				job, err = c.restartService(ctx, id, key)
				return err
			})
			return
		}
	}
	if host.upgrade == nil || host.upgrade.interruptAttempt == nil {
		op.interruptAttempt = func(ctx context.Context, _ connprofile.Profile, attempt string, request l1.InterruptRestartableAttemptRequest) (job l1.Job, err error) {
			err = with(ctx, func(ctx context.Context, c *apiClients) error {
				var err error
				job, err = c.interruptRestartableAttempt(ctx, p.Identities.Node, attempt, request)
				return err
			})
			return
		}
	}
	return op
}

// Require the maintenance response as well as exit zero. A stand-in which
// ignores unknown flags or merely prints its version is not compatibility proof.
func checkFleetUpgradeBinaries(ctx context.Context, op upgradeHost, paths setupPaths, role serviceplan.Role, directory, loginPATH string, names []string) error {
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for _, name := range names {
		output, err := op.command(bounded, directory, upgradeEnvironment(loginPATH), filepath.Join(directory, name), "--profile-preflight", "--profile="+paths.profile, "--role="+string(role))
		if err != nil || strings.TrimSpace(string(output)) != "profile-readable "+string(role) {
			return fmt.Errorf("%s cannot read the active fleet profile and role; activation or rollback refused: %v", name, err)
		}
	}
	return nil
}

func runFleetUpgrade(ctx context.Context, host setupHost, paths setupPaths, role serviceplan.Role, yes bool, warnings io.Writer, drainTimeout ...time.Duration) (report upgradeReport, resultErr error) {
	unlock, err := lockSetup(paths, true)
	if err != nil {
		return report, err
	}
	defer unlock()
	if err := refuseGenerationMaintenance(host, paths, "upgrade"); err != nil {
		return report, err
	}
	// Recheck under the same installation lock used by setup.
	if selected, err := selectUpgradeRole(paths); err != nil {
		return report, err
	} else if selected != role {
		return report, errors.New("upgrade installation role changed before lock")
	}
	if err := retireCompletedPlainConversion(paths); err != nil {
		return report, err
	}
	profile, manifest, err := readFleetUpgradeInstall(paths, role)
	if err != nil {
		return report, err
	}
	plan, err := setupRolePlan(paths, role)
	if err != nil {
		return report, err
	}
	// Preserve the Mac upgrade's existing stop budget and command order.
	managerHost := host
	managerHost.unloadBound = time.Duration(setupNodeAgentExitTimeout+15) * time.Second
	manager := fleetServiceManager{host: managerHost}
	op := fleetUpgradeOperations(host, paths, profile, plan)
	defer func() {
		resultErr = upgradeDrainRunHint(ctx, op, profile, resultErr)
	}()
	if err := recoverUpgradeDrain(ctx, op, paths, profile, warnings); err != nil {
		return report, err
	}
	if err := manager.preflightUpgrade(ctx, paths, role); err != nil {
		return report, err
	}
	plist, err := os.ReadFile(filepath.Join(paths.launchAgents, manager.file(setupNodeAgentLabel)))
	if err != nil {
		return report, err
	}
	loginPATH, err := manager.recordedPATH(plist)
	if err != nil {
		return report, err
	}
	for _, path := range []string{paths.data, filepath.Join(paths.data, upgradeSourceName)} {
		if info, err := os.Lstat(path); err == nil && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
			return report, fmt.Errorf("upgrade source path %s must be a real folder", path)
		} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return report, err
		}
	}
	entries, err := os.ReadDir(paths.data)
	if err != nil {
		return report, err
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".upgrade-") {
			if err := os.RemoveAll(filepath.Join(paths.data, entry.Name())); err != nil {
				return report, err
			}
		}
	}
	stage, err := os.MkdirTemp(paths.data, ".upgrade-")
	if err != nil {
		return report, err
	}
	defer func() { resultErr = errors.Join(resultErr, os.RemoveAll(stage)) }()
	source, dest := filepath.Join(stage, "source"), filepath.Join(stage, "binaries")
	for _, dir := range []string{source, dest} {
		if err := os.Mkdir(dir, 0700); err != nil {
			return report, err
		}
	}
	prep, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	commit, err := fetchUpgradeSource(prep, op, paths, source, loginPATH, warnings)
	if err != nil {
		return report, fmt.Errorf("upgrade fetch failed; installed stack unchanged: %w", err)
	}
	if err := op.build(prep, source, dest, commit, loginPATH); err != nil {
		return report, fmt.Errorf("upgrade build failed; installed stack unchanged: %w", err)
	}
	names := fleetUpgradeBinaries(plan)
	digests := map[string]string{}
	for _, name := range names {
		digest, err := executableDigest(filepath.Join(dest, name))
		if err != nil {
			return report, err
		}
		digests[name] = digest
		output, err := op.command(prep, stage, upgradeEnvironment(loginPATH), filepath.Join(dest, name), "--version")
		if err != nil || strings.TrimSpace(string(output)) != commit {
			return report, fmt.Errorf("upgrade version check failed for %s; installed stack unchanged", name)
		}
	}
	if err := checkFleetUpgradeBinaries(prep, op, paths, role, dest, loginPATH, names); err != nil {
		return report, err
	}
	report.Version = commit
	already := manifest.InstalledVersion == commit
	for _, name := range names {
		installed, err := executableDigest(filepath.Join(paths.bin, name))
		if err != nil || installed != digests[name] || installed != manifest.Files[filepath.Join(paths.binRel, name)] {
			already = false
		}
	}
	if already {
		report.AlreadyAt = true
		if _, err := fmt.Fprintln(warnings, "already at "+commit); err != nil {
			return report, err
		}
		err = finishFleetUpgradeWarnings(ctx, host, paths, profile, role, commit, warnings, &report)
		return report, err
	}
	var live, before []string
	if role == serviceplan.ControlPlane {
		for i, name := range []string{"wefty-l1", "wefty-l3"} {
			path := filepath.Join(paths.data, []string{"l1.sqlite", "l3.sqlite"}[i])
			temporary := filepath.Join(stage, fmt.Sprintf("preflight-%d.sqlite", i))
			if err := upgradestate.Snapshot(prep, path, temporary); err != nil {
				return report, fmt.Errorf("upgrade state snapshot failed; installed stack unchanged: %w", err)
			}
			if _, err := op.command(prep, stage, upgradeEnvironment(loginPATH), filepath.Join(dest, name), "--state-preflight", "--db="+temporary); err != nil {
				return report, fmt.Errorf("upgrade state preflight failed; installed stack unchanged: %w", err)
			}
			live = append(live, path)
		}
	}
	skillsRoot, err := os.OpenRoot(filepath.Join(source, "skills"))
	if err != nil {
		return report, err
	}
	defer skillsRoot.Close()
	skills, err := setupSkillFilesFrom(paths, skillsRoot.FS())
	if err != nil {
		return report, err
	}
	// Setup's renderer is shared; activation and receipt publication are upgrade's.
	services, err := setupRoleServices(paths, profile, digests, loginPATH, role)
	if err != nil {
		return report, err
	}
	root, err := os.OpenRoot(paths.home)
	if err != nil {
		return report, err
	}
	defer root.Close()
	var files []setupFile
	for _, name := range names {
		payload, err := os.ReadFile(filepath.Join(dest, name))
		if err != nil {
			return report, err
		}
		if payloadDigest(payload) != digests[name] {
			return report, errors.New("staged binary changed during upgrade")
		}
		files = append(files, setupFile{folder: paths.binRel, name: name, payload: payload, mode: 0755})
	}
	var plists [][]byte
	readyFolder := filepath.Join(paths.dataRel, "setup-ready")
	if info, err := root.Lstat(readyFolder); err == nil {
		if !info.IsDir() {
			return report, errors.New("fleet ready path must be an unlinked folder")
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return report, err
	}
	folders := []string{paths.binRel, paths.configRel, paths.launchAgentsRel, readyFolder}
	destinations := []string{filepath.Join(paths.configRel, setupManifestName)}
	for _, file := range append(slices.Clone(files), skills...) {
		folders = append(folders, file.folder)
		destinations = append(destinations, filepath.Join(file.folder, file.name))
	}
	previous := ""
	for _, service := range services {
		payload, err := manager.render(service, previous)
		previous = service.Label
		if err != nil {
			return report, err
		}
		plists = append(plists, payload)
		destinations = append(destinations, filepath.Join(paths.launchAgentsRel, manager.file(service.Label)))
	}
	if err := checkSetupDestinations(root, paths.home, folders, destinations); err != nil {
		return report, err
	}
	baseline, err := op.observe(ctx, profile)
	if err != nil {
		return report, upgradeObservationError(ctx, host, err, true)
	}
	report.RunningWork = slices.Clone(baseline.ActiveAttempts)
	bound := upgradeDrainTimeout
	if len(drainTimeout) > 0 {
		bound = drainTimeout[0]
	}
	var deadline time.Time
	baseline, deadline, err = waitUpgradeGap(ctx, op, profile, baseline, yes, bound, warnings, &report.RunningWork, &report.upgradeDrainCounts)
	if err != nil {
		return report, err
	}
	drain, err := beginUpgradeDrain(ctx, op, paths, profile, baseline, bound, deadline)
	if drain != nil {
		defer func() {
			restoreErr := finishUpgradeDrain(ctx, op, paths, profile, drain, false, warnings)
			resultErr = upgradeDrainResult(resultErr, restoreErr, !drain.claimsDisabled)
			report.RestartedServices = slices.Clone(drain.restarted)
			report.ClaimsDisabled = drain.claimsDisabled
			report.upgradeDrainCounts = drain.counts
		}()
	}
	if err != nil {
		return report, err
	}
	baseline, err = waitUpgradeDrain(ctx, op, profile, drain, yes, bound, warnings, &report.RunningWork)
	if err != nil {
		return report, err
	}
	for _, path := range live {
		fingerprint, err := upgradestate.Fingerprint(ctx, path)
		if err != nil {
			return report, err
		}
		before = append(before, fingerprint)
	}
	saved, err := saveFleetUpgradeInstall(paths, manager, files, services, skills)
	if err != nil {
		return report, err
	}
	// Recheck after a potentially long drain, directly before first activation.
	if _, _, err := readFleetUpgradeInstall(paths, role); err != nil {
		return report, err
	}
	if err := manager.preflightUpgrade(ctx, paths, role); err != nil {
		return report, err
	}
	if err := checkFleetUpgradeBinaries(ctx, op, paths, role, dest, loginPATH, names); err != nil {
		return report, err
	}
	if err := checkFleetUpgradeRollback(ctx, op, paths, role, saved, stage, loginPATH, names); err != nil {
		return report, fmt.Errorf("upgrade has no compatible rollback path: %w", err)
	}
	written := map[string]string{}
	nodeStage := false
	activated := false
	defer func() {
		if resultErr != nil && activated {
			recovery, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Duration(setupNodeAgentExitTimeout+90)*time.Second)
			defer cancel()
			recoveryErr := recoverFleetUpgrade(recovery, host, op, paths, role, plan, profile, live, before, saved, nodeStage, written, stage, loginPATH)
			resultErr = fmt.Errorf("upgrade activation failed: %w; %v", resultErr, recoveryErr)
		}
	}()
	write := func(file setupFile) error {
		changed, err := convergeHomeFile(root, file)
		if err == nil && changed {
			written[filepath.Join(file.folder, file.name)] = payloadDigest(file.payload)
		}
		return err
	}
	// Leave the node binary old until L1/L3 renewal has been proved.
	activated = true
	if err := root.MkdirAll(readyFolder, 0700); err != nil {
		return report, err
	}
	if err := root.Chmod(readyFolder, 0700); err != nil {
		return report, err
	}
	for _, file := range files {
		if file.name != "wefty-agent" {
			if err := write(file); err != nil {
				return report, err
			}
		}
	}
	folder, err := root.OpenRoot(paths.launchAgentsRel)
	if err != nil {
		return report, err
	}
	defer folder.Close()
	heartbeat := fleetUpgradeHeartbeatRequester(host, paths, op, warnings)
	nodeAction := launchagent.Unchanged
	started := op.now()
	controls, cancelControls := context.WithTimeout(ctx, upgradeRenewalWindow)
	defer cancelControls()
	for i, service := range services {
		if service.Label == setupNodeAgentLabel {
			current, err := op.observe(ctx, profile)
			if err != nil {
				return report, err
			}
			for _, attempt := range current.ActiveAttempts {
				recordUpgradeWork(&report.RunningWork, attempt)
			}
			if err := checkUpgradeReplacement(current, drain, yes); err != nil {
				return report, err
			}
			drain.OldBoot = current.BootSessionID
			for _, attempt := range current.ActiveAttempts {
				if attempt.Class == contract.JobClassService {
					drain.Services = append(drain.Services, attempt)
				} else if yes {
					if !slices.ContainsFunc(report.RunningWork, func(old l1.NodeActiveAttempt) bool { return old.AttemptID == attempt.AttemptID }) {
						report.RunningWork = append(report.RunningWork, attempt)
					}
					if _, err := fmt.Fprintf(warnings, "Ending one-shot job %s attempt %s (--yes).\n", attempt.JobID, attempt.AttemptID); err != nil {
						return report, err
					}
				}
			}
			if err := writeUpgradeDrain(paths.config, drain); err != nil {
				return report, err
			}
			nodeStage = true
			for _, file := range files {
				if file.name == "wefty-agent" {
					if err := write(file); err != nil {
						return report, err
					}
				}
			}
		}
		serviceCtx := ctx
		if service.Label == setupL1Label || service.Label == setupL3Label {
			serviceCtx = controls
		}
		action, err := manager.converge(serviceCtx, folder, paths, service, plists[i])
		if err != nil {
			return report, err
		}
		written[filepath.Join(paths.launchAgentsRel, manager.file(service.Label))] = payloadDigest(plists[i])
		if service.Label == setupNodeAgentLabel {
			nodeAction = action
		}
		report.Setup.Services = append(report.Setup.Services, manager.report(paths, service, action))
		if service.Label == setupL3Label {
			restarted := op.now()
			elapsed, err := waitFleetUpgradeRenewal(controls, op, profile, baseline, started, restarted, heartbeat)
			report.LeaseRenewal = elapsed
			if err != nil {
				return report, err
			}
			if _, err := fmt.Fprintf(warnings, "Successful node lease renewal after control-plane restart: %s (window %s).\n", elapsed, upgradeRenewalWindow); err != nil {
				return report, err
			}
		}
	}
	// Reconnect through the restarted client service before proving replacement.
	replacementStarted := op.now()
	if err := waitFleetUpgradeReplacement(ctx, host, op, paths, profile, baseline.BootSessionID, commit, replacementStarted, nodeAction, heartbeat); err != nil {
		return report, err
	}
	if role == serviceplan.Join {
		report.LeaseRenewal = op.now().Sub(replacementStarted)
		if _, err := fmt.Fprintf(warnings, "Successful node lease renewal after joining-machine restart: %s.\n", report.LeaseRenewal); err != nil {
			return report, err
		}
	}
	for _, file := range skills {
		relative := filepath.Join(file.folder, file.name)
		current, err := root.ReadFile(relative)
		owned := manifest.Files[relative]
		if err == nil && (owned == "" || payloadDigest(current) != owned) {
			report.Setup.Notes = append(report.Setup.Notes, "owner-edited or unrecorded skill copy preserved: "+relative)
			continue
		}
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return report, err
		}
		if err := root.MkdirAll(file.folder, 0755); err != nil {
			return report, err
		}
		if err := write(file); err != nil {
			return report, err
		}
	}
	for name, digest := range written {
		manifest.Files[name] = digest
	}
	for _, file := range files {
		manifest.Files[filepath.Join(file.folder, file.name)] = payloadDigest(file.payload)
	}
	manifest.InstalledVersion, manifest.UpgradeSource = commit, true
	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return report, err
	}
	if _, err := convergeHomeFile(root, setupFile{folder: paths.configRel, name: setupManifestName, payload: append(payload, '\n'), mode: 0600}); err != nil {
		return report, err
	}
	report.Setup.Role = role
	report.Setup.NodeID = profile.Identities.Node
	report.Setup.Profile, report.Setup.NodeConfig = paths.profile, paths.nodeConfig
	report.Setup.BinDir, report.Setup.DataDir, report.Setup.LogDir = paths.bin, paths.data, paths.logs
	report.Setup.L1Address, report.Setup.L3Address = profile.L1Address, profile.L3Address
	err = finishFleetUpgradeWarnings(ctx, host, paths, profile, role, commit, warnings, &report)
	return report, err
}

func finishFleetUpgradeWarnings(ctx context.Context, host setupHost, paths setupPaths, profile connprofile.Profile, role serviceplan.Role, commit string, warnings io.Writer, report *upgradeReport) error {
	if role == serviceplan.Join {
		list, err := fleetUpgradeVersionWarnings(ctx, host, paths, profile, commit)
		if err != nil {
			report.Setup.Notes = append(report.Setup.Notes, "Cannot verify other machines' versions; run wefty status.")
		} else {
			report.VersionWarnings = list
		}
	}
	for _, warning := range report.VersionWarnings {
		if _, err := fmt.Fprintln(warnings, "Warning:", warning.Detail); err != nil {
			return err
		}
	}
	for _, note := range report.Setup.Notes {
		if _, err := fmt.Fprintln(warnings, "Warning:", note); err != nil {
			return err
		}
	}
	return nil
}

func saveFleetUpgradeInstall(paths setupPaths, manager fleetServiceManager, files []setupFile, services []launchagent.Agent, skills []setupFile) ([]setupFile, error) {
	names := []string{filepath.Join(paths.configRel, setupManifestName)}
	for _, file := range append(slices.Clone(files), skills...) {
		names = append(names, filepath.Join(file.folder, file.name))
	}
	for _, service := range services {
		names = append(names, filepath.Join(paths.launchAgentsRel, manager.file(service.Label)))
	}
	root, err := os.OpenRoot(paths.home)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	var saved []setupFile
	for _, name := range names {
		file := setupFile{folder: filepath.Dir(name), name: filepath.Base(name)}
		info, err := root.Lstat(name)
		if errors.Is(err, fs.ErrNotExist) {
			saved = append(saved, file)
			continue
		}
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("upgrade refuses non-regular installed file %s", name)
		}
		file.payload, err = root.ReadFile(name)
		if err != nil {
			return nil, err
		}
		file.mode = info.Mode().Perm()
		saved = append(saved, file)
	}
	return saved, nil
}

func recoverFleetUpgrade(ctx context.Context, host setupHost, op upgradeHost, paths setupPaths, role serviceplan.Role, plan serviceplan.Plan, profile connprofile.Profile, live, before []string, saved []setupFile, nodeStage bool, written map[string]string, stage, loginPATH string) error {
	refuse := func(reason error) error {
		return fmt.Errorf("fleet rollback refused; new binaries retained; live databases were not restored: %w; receipt refresh: %v; run the newly installed ~/.local/bin/wefty setup to repair forward", reason, refreshPartialUpgradeReceipt(paths, written))
	}
	active, _, err := readFleetUpgradeInstall(paths, role)
	if err != nil {
		return refuse(err)
	}
	original, _ := connprofile.Encode(profile)
	current, _ := connprofile.Encode(active)
	if !bytes.Equal(original, current) {
		return refuse(errors.New("active fleet profile changed during activation"))
	}
	if err := checkFleetUpgradeRollback(ctx, op, paths, role, saved, stage, loginPATH, fleetUpgradeBinaries(plan)); err != nil {
		return refuse(err)
	}
	managerHost := host
	managerHost.unloadBound = time.Duration(setupNodeAgentExitTimeout+15) * time.Second
	manager := fleetServiceManager{host: managerHost}
	// The client is CLI-backed, so stop it before a possible CLI rollback. Never
	// touch a control service that the local role does not own.
	var stops []error
	for _, service := range slices.Backward(plan.Services) {
		if service.Name == "node-agent" && !nodeStage {
			continue
		}
		if err := manager.unload(ctx, paths, "dev.wefty."+service.Name); err != nil {
			stops = append(stops, err)
		}
	}
	if len(stops) > 0 {
		return refuse(errors.Join(stops...))
	}
	for i, path := range live {
		after, err := upgradestate.Fingerprint(ctx, path)
		if err != nil || after != before[i] {
			return refuse(fmt.Errorf("database format changed or could not be read: %v", err))
		}
	}
	root, err := os.OpenRoot(paths.home)
	if err != nil {
		return err
	}
	defer root.Close()
	for _, file := range saved {
		if file.mode == 0 {
			err = root.Remove(filepath.Join(file.folder, file.name))
			if errors.Is(err, fs.ErrNotExist) {
				err = nil
			}
		} else {
			_, err = convergeHomeFile(root, file)
		}
		if err != nil {
			return fmt.Errorf("fleet rollback failed; stack stopped: %w", err)
		}
	}
	if err := manager.reload(ctx, paths); err != nil {
		return fmt.Errorf("fleet rollback failed; stack stopped: %w", err)
	}
	folder, err := root.OpenRoot(paths.launchAgentsRel)
	if err != nil {
		return err
	}
	defer folder.Close()
	for _, service := range plan.Services {
		if service.Name == "node-agent" && !nodeStage {
			continue
		}
		label := "dev.wefty." + service.Name
		payload, err := folder.ReadFile(manager.file(label))
		if err != nil {
			return err
		}
		if _, err := manager.converge(ctx, folder, paths, launchagent.Agent{Label: label}, payload); err != nil {
			return err
		}
	}
	return errors.New("profile compatibility and schema fingerprints unchanged; old installation files restored and services reloaded; live databases preserved")
}

// A receipt proves that these exact bytes already operated this exact profile.
// Older recorded builds need not know the new preflight flag. Unrecorded bytes
// must prove compatibility in a subprocess, both before activation and rollback.
func checkFleetUpgradeRollback(ctx context.Context, op upgradeHost, paths setupPaths, role serviceplan.Role, saved []setupFile, stage, loginPATH string, names []string) error {
	var receipt setupManifest
	for _, file := range saved {
		if file.folder == paths.configRel && file.name == setupManifestName {
			if err := json.Unmarshal(file.payload, &receipt); err != nil {
				return err
			}
		}
	}
	active, err := os.ReadFile(paths.profile)
	if err != nil {
		return err
	}
	if receipt.Role != role || receipt.Files[filepath.Join(paths.configRel, connprofile.FileName)] != payloadDigest(active) {
		return errors.New("previous receipt does not match the active fleet profile and role")
	}
	previous := filepath.Join(stage, "previous-binaries")
	if err := os.MkdirAll(previous, 0700); err != nil {
		return err
	}
	for _, name := range names {
		var found *setupFile
		for i := range saved {
			if saved[i].folder == paths.binRel && saved[i].name == name && saved[i].mode != 0 {
				found = &saved[i]
				break
			}
		}
		if found == nil {
			return fmt.Errorf("previous binary %s is missing", name)
		}
		if digest := receipt.Files[filepath.Join(paths.binRel, name)]; digest != "" && digest == payloadDigest(found.payload) {
			continue
		}
		if err := os.WriteFile(filepath.Join(previous, name), found.payload, found.mode); err != nil {
			return err
		}
		if err := checkFleetUpgradeBinaries(ctx, op, paths, role, previous, loginPATH, []string{name}); err != nil {
			return err
		}
	}
	return nil
}
