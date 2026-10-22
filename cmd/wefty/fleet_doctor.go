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
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/local"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/internal/systemduser"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

// A diagnostic already wrote its evidence to stdout. Preserve its typed exit
// without writing a second JSON document to stderr.
type doctorResultError struct{ cause error }

func (e *doctorResultError) Error() string { return e.cause.Error() }
func (e *doctorResultError) Unwrap() error { return e.cause }

// The receipt selects a role even if the active profile is damaged. A durable
// stage is also explicit role evidence; participant shape never selects one.
func doctorRole(paths setupPaths) (serviceplan.Role, error) {
	root, err := os.OpenRoot(paths.home)
	if err != nil {
		return serviceplan.Legacy, err
	}
	defer root.Close()
	var receiptErr error
	receiptPath := filepath.Join(paths.configRel, setupManifestName)
	var payload []byte
	info, err := root.Lstat(receiptPath)
	if err == nil && info.Mode().Perm()&0400 == 0 {
		err = os.ErrPermission
	}
	if err == nil {
		payload, err = root.ReadFile(receiptPath)
	}
	if err == nil {
		var receipt setupManifest
		if json.Unmarshal(payload, &receipt) == nil {
			if receipt.Role.Validate() != nil {
				return serviceplan.Legacy, errors.New("installation role is invalid; restore the original role receipt")
			}
			if receipt.Role != serviceplan.Legacy {
				return receipt.Role, nil
			}
		} else {
			receiptErr = errors.New("installation role receipt is invalid")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		receiptErr = errors.New("cannot read installation role receipt")
	}
	stage, stageErr := readFleetSetupRecord(paths)
	if stage != nil {
		return stage.Role, receiptErr
	}
	if stageErr != nil {
		// No accessible explicit stage means no fleet selection. In particular,
		// a linked plain config folder must retain the full plain path findings.
		if receiptErr != nil {
			if _, err := root.Lstat(filepath.Join(paths.configRel, fleetSetupRecordName)); err != nil {
				return serviceplan.Legacy, nil
			}
		}
		return serviceplan.Legacy, errors.New("cannot read the fleet setup role record; restore the original record")
	}
	// An unreadable receipt alone cannot turn a legacy installation into fleet.
	return serviceplan.Legacy, nil
}

func writeDoctorReport(out io.Writer, report plainDoctorReport, jsonOutput bool) error {
	if jsonOutput {
		return writeJSON(out, report)
	}
	for _, f := range report.Findings {
		if _, err := fmt.Fprintf(out, "%s\t%s\t%s\n", f.Check, f.Severity, f.Detail); err != nil {
			return err
		}
		if f.Severity != "ok" && f.Fix != "" {
			if _, err := fmt.Fprintf(out, "  Fix: %s\n", f.Fix); err != nil {
				return err
			}
		}
	}
	for _, e := range report.PolicyEvidence {
		if _, err := fmt.Fprintf(out, "private network policy: %s\t%s\t%s\n", e.Check, e.Status, e.Detail); err != nil {
			return err
		}
	}
	return nil
}

func runFleetDoctor(ctx context.Context, host setupHost, options globalOptions, paths setupPaths, role serviceplan.Role) (plainDoctorReport, error) {
	report := plainDoctorReport{Version: 1, Install: "fleet-process", Role: role, Healthy: true, Findings: []plainDoctorFinding{}}
	add := func(check, severity, detail, fix string) {
		if severity == "ok" {
			fix = ""
		} else {
			report.Healthy = false
		}
		report.Findings = append(report.Findings, plainDoctorFinding{check, severity, detail, fix})
	}
	if host.goos == "linux" {
		pinned, issues := resolveFleetMaintenancePaths(paths.home, host)
		paths = pinned
		for _, issue := range issues {
			add("path:"+issue.folder, "error", "cannot verify the recorded Linux log root", issue.err.Error())
		}
		if err := validateLinuxLogFolders(paths, host.euid); err != nil {
			add("log-folder", "error", "unsafe Linux log folder or parent", err.Error())
		}
	}
	budget, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	plan, planErr := setupRolePlan(paths, role)
	if planErr != nil {
		add("service-plan", "error", "cannot validate the role's service paths", "restore private installation folders and rerun doctor")
	}
	// Service names are still useful when a path validation fails.
	if len(plan.Services) == 0 {
		if role == serviceplan.ControlPlane {
			plan.Services = append(plan.Services, serviceplan.Service{Name: "l1"}, serviceplan.Service{Name: "l3"})
		}
		plan.Services = append(plan.Services, serviceplan.Service{Name: "node-agent"}, serviceplan.Service{Name: "client-service"})
	}
	manager := fleetServiceManager{host: host}
	for _, service := range plan.Services {
		detail, severity := "loaded and running", "ok"
		probe, done := context.WithTimeout(budget, statusProbeBudget)
		if host.goos != "darwin" && host.goos != "linux" {
			detail, severity = "service-manager checks are unverified on this platform", "warning"
		} else {
			state, notes, err := manager.stateWithNotes(probe, "dev.wefty."+service.Name)
			if err == nil && len(notes) > 0 {
				add(manager.name("dev.wefty."+service.Name)+":system-drop-ins", "ok", "Informational: trusted root-owned system policy: "+strings.Join(notes, ", "), "")
			}
			switch {
			case err != nil:
				detail, severity = "cannot verify service state", "error"
			case !state.Loaded:
				detail, severity = "not loaded", "error"
			case !state.Running:
				detail, severity = "loaded but not running", "error"
			}
		}
		done()
		add(manager.name("dev.wefty."+service.Name), severity, detail, "run `wefty setup` with the recorded role; inspect this service's log if it stops again")
	}
	root, err := os.OpenRoot(paths.home)
	if err != nil {
		add("paths", "error", "cannot inspect installation folders", "restore access to the installation home")
		return report, &notReadyError{verdict: "fleet doctor found issues; follow the fix hints"}
	}
	defer root.Close()
	_, issues := resolveFleetMaintenancePaths(paths.home, host)
	for _, issue := range issues {
		add("path:"+issue.folder, "error", "folder is linked or inaccessible", "restore the original installation folder without links")
	}
	folders := append([]string{paths.binRel, paths.configRel, paths.dataRel, paths.logsRel, paths.launchAgentsRel}, paths.nodeDataFolders...)
	folders = append(folders, paths.skillTargets...)
	for _, folder := range folders {
		mode := os.FileMode(0700)
		if folder == paths.binRel || folder == paths.launchAgentsRel || slices.Contains(paths.skillTargets, folder) {
			mode = 0755
		}
		info, err := root.Lstat(folder)
		severity, detail := "ok", "exists with expected mode"
		if err != nil || !info.IsDir() || info.Mode().Perm() != mode {
			severity, detail = "error", fmt.Sprintf("folder is missing, linked or has the wrong mode (want %04o)", mode)
		}
		add("path:"+folder, severity, detail, "restore this folder and its owner permissions; rerun setup with the recorded role")
	}
	profilePath := options.profilePath
	if profilePath == "" {
		profilePath = paths.profile
	}
	var p connprofile.Profile
	info, profileErr := os.Lstat(profilePath)
	if profileErr == nil && (!info.Mode().IsRegular() || info.Mode().Perm() != 0600) {
		profileErr = errors.New("profile must be an unlinked private file")
	}
	if profileErr == nil {
		p, profileErr = connprofile.Read(profilePath)
	}
	if profileErr == nil {
		profileErr = serviceplan.ValidateProfile(role, p)
	}

	if profileErr != nil {
		add("connection-profile", "error", "profile is missing, invalid or incomplete for the recorded role", "restore the complete profile and mode 0600; resume setup with its recorded role")
	} else {
		add("connection-profile", "ok", "complete valid profile for "+string(role), "")
	}
	clientStateOK := false
	// Stat only: no state payload, authority database or backend is opened here.
	for _, partRole := range []string{connprofile.ParticipantL1, connprofile.ParticipantL3, connprofile.ParticipantNodeAgent, connprofile.ParticipantClient, connprofile.ParticipantPerson} {
		part, present := p.Participants[partRole]
		required := partRole == connprofile.ParticipantNodeAgent || partRole == connprofile.ParticipantClient || role == serviceplan.ControlPlane && (partRole == connprofile.ParticipantL1 || partRole == connprofile.ParticipantL3)
		if !required && !present {
			continue
		}
		expected := filepath.Join(paths.data, "fabric", partRole)
		rel, relErr := filepath.Rel(paths.home, expected)
		info, statErr := root.Lstat(rel)
		valid := present && relErr == nil && part.StateDirectory == expected && statErr == nil && info.IsDir() && info.Mode().Perm() == 0700
		if valid {
			canonical, err := canonicalHomeFolder(paths.home, rel)
			valid = err == nil && canonical == rel
		}
		if valid {
			valid = fabricconfig.SavedStateAvailable(p.Fabric, expected)
		}
		severity, detail := "ok", "private folder and saved state present; identity is checked by its running service"
		if !valid {
			severity, detail = "error", "participant record, private folder or saved state is missing or has the wrong mode"
		}
		add("participant:"+partRole, severity, detail, "restore this participant's original folder and saved state with private permissions; never copy another participant's identity")
		if partRole == connprofile.ParticipantClient {
			clientStateOK = valid
		}
	}
	configPath := options.nodeConfigPath
	if configPath == "" {
		configPath = paths.nodeConfig
	}
	config, configErr := ocicontrol.ReadInstalledConfig(configPath)
	configOK := configErr == nil && validSetupNodeID(config.NodeID) && config.ControlSocket == "" && config.NodeID == p.Identities.Node
	if !configOK {
		add("node-config", "error", "node configuration is invalid or differs from the profile's stable node ID", "restore this machine's stable node ID in node.json and the profile")
	} else {
		add("node-config", "ok", "node.json agrees with the profile's stable node ID", "")
	}
	plist, err := root.ReadFile(filepath.Join(paths.launchAgentsRel, manager.file(setupNodeAgentLabel)))
	loginPATH := ""
	if err == nil {
		loginPATH, err = manager.recordedPATH(plist)
	}
	if err != nil {
		add("node-agent-PATH", "error", "cannot read the node agent's recorded login-shell PATH", "rerun setup to capture the login-shell PATH")
	} else {
		add("node-agent-PATH", "ok", "recorded login-shell PATH is present", "")
		for _, tool := range []string{"go", "git", "node", "gh", "claude", "codex"} {
			severity, detail := "ok", "found on the node agent's recorded login-shell PATH"
			if !doctorFindTool(loginPATH, paths.data, tool) {
				severity, detail = "warning", "missing from the node agent's recorded login-shell PATH"
				if tool == "git" {
					severity = "error"
				}
			}
			add("tool:"+tool, severity, detail, "install "+tool+" or add it to the login-shell PATH, then rerun setup")
		}
	}
	skills, err := setupSkillFiles(paths)
	if err != nil {
		add("skills", "error", "cannot read embedded skill copies", "reinstall the CLI")
	} else {
		for _, file := range skills {
			name := filepath.Join(file.folder, file.name)
			payload, readErr := root.ReadFile(name)
			info, statErr := root.Lstat(name)
			severity, detail := "ok", "matches the embedded wefty skill"
			if readErr != nil || statErr != nil || !info.Mode().IsRegular() || !bytes.Equal(payload, file.payload) {
				severity, detail = "error", "missing or stale skill copy"
			}
			add("skill:"+name, severity, detail, "rerun setup from the installed version to refresh the skill copies")
		}
	}
	if host.goos == "darwin" && role == serviceplan.ControlPlane {
		doctorMacHost(budget, host, func(check, severity, detail, fix string) {
			healthy := report.Healthy
			add(check, severity, detail, fix)
			// Sleep, FileVault and unknown host output are advisory. Keep
			// every finding and fix hint without changing the local verdict.
			if severity == "warning" {
				report.Healthy = healthy
			}
		})
	}
	if host.goos == "linux" {
		doctorLinuxLingering(budget, host, func(check, severity, detail, fix string) {
			healthy := report.Healthy
			add(check, severity, detail, fix)
			// Logout survival is advisory, independent of current service health.
			report.Healthy = healthy
		})
	}
	// Policy evidence is independent of the local healthy verdict. Missing deny
	// proof is expected until the attended run; it is never a verified denial.
	report.PolicyEvidence = []setupReadinessCheck{
		{Check: "client → L1", Status: "NOT-RUN", Detail: "client identity has not been verified"},
		{Check: "client → L3", Status: "NOT-RUN", Detail: "client identity has not been verified"},
		{Check: "node agent → L1/L3", Status: "NOT-RUN", Detail: "requires cooperating participants in the attended run"},
		{Check: "L1 → L3 direction", Status: "NOT-RUN", Detail: "client identity has not been verified"},
		{Check: "L3 → L1 direction", Status: "NOT-RUN", Detail: "client identity has not been verified"},
		{Check: "owner → L1 (person commands)", Status: "NOT-RUN", Detail: "requires the owner's laptop in the attended run; run `wefty whoami` through its person identity"},
		{Check: "owner → published ports", Status: "NOT-RUN", Detail: "requires the owner's untagged device and Computer front-door/published-port checks in the attended run"},
		{Check: "machines → other machines' published ports denied", Status: "NOT-RUN", Detail: "a single machine cannot prove denial; check cooperating peers and overlapping policy grants in the attended run"},
		{Check: "agent-tag ownership restricted to trusted administrators", Status: "NOT-RUN", Detail: "the owner must inspect explicit tag owners and all privileged administrative roles; doctor never requests or stores an admin credential"},
	}
	var liveErr error
	keepErr := func(err error) {
		if err == nil {
			return
		}
		if errors.Is(ctx.Err(), context.Canceled) {
			liveErr = ctx.Err()
			return
		}
		classification := classifyCommandReport(err)
		if classification.exitCode != exitUnauthorized && classification.exitCode != exitUnavailable {
			err = &fabric.ParticipantUnreachableError{Cause: errors.New("running client service or remote service could not be confirmed")}
		}
		if liveErr == nil || classifyCommandReport(err).exitCode == exitUnauthorized {
			liveErr = err
		}
	}
	if profileErr != nil {
		add("client-service", "warning", "NOT-RUN: complete role profile is required", "repair the profile and rerun doctor")
	} else if !clientStateOK {
		keepErr(&fabric.ParticipantIdentityError{Reason: "state is missing; restore this participant's original saved state"})
		add("client-service", "error", "REFUSED: saved client state is missing or invalid", "restore the original client participant state")
	} else {
		open := host.fleetClients
		if open == nil {
			open = setupFleetClients
		}
		probe, done := context.WithTimeout(budget, statusProbeBudget)
		clients, err := open(probe, paths, p)
		done()
		if clients != nil {
			defer clients.close()
		}
		if err != nil || clients == nil {
			if err == nil {
				err = &fabric.ParticipantUnreachableError{Cause: errors.New("client service did not supply a transport")}
			}
			keepErr(err)
			detail := "NOT-RUN: running client service is unavailable or unverified"
			if classifyCommandReport(err).exitCode == exitUnauthorized {
				detail = "REFUSED: running client service refused this identity"
			}
			fix := "ask the owner to start or repair the client service with this profile, then rerun doctor"
			var fenced *local.FencedCallerError
			if errors.As(err, &fenced) {
				detail = "REFUSED: caller network or socket restrictions block the client service"
				fix = "ask the owner; keep the caller's network and socket restrictions"
			} else if classifyCommandReport(err).exitCode == exitUnauthorized {
				fix = "restore the recorded client identity or have the owner repair the service's user and identity; rerun doctor"
			}
			add("client-service", "error", detail, fix)
		} else {
			add("client-service", "ok", "running service verified the saved client identity", "")
			probe, done = context.WithTimeout(budget, statusProbeBudget)
			nodes, l1Err := clients.listNodes(probe)
			done()
			probe, done = context.WithTimeout(budget, statusProbeBudget)
			_, l3Err := clients.listRuns(probe, string(contract.RunQueued), 1)
			done()
			for i, err := range []error{l1Err, l3Err} {
				check := &report.PolicyEvidence[i]
				if err == nil {
					check.Status, check.Detail = "VERIFIED", "saved client read the service through the running client service"
					add(check.Check, "ok", check.Detail, "")
				} else {
					keepErr(err)
					check.Status, check.Detail = "NOT-RUN", "transport uncertainty; reachability is unverified"
					if classifyCommandReport(err).exitCode == exitUnauthorized {
						check.Status, check.Detail = "REFUSED", "the request was refused; this does not prove a network-policy denial"
					}
					add(check.Check, "error", check.Status+": "+check.Detail, "check the remote service, saved identity and private network policy; rerun doctor")
				}
			}
			for i, client := range []*apiClient{clients.l1, clients.l3} {
				check, err := readDoctorPeerHealth(budget, client, report.PolicyEvidence[3+i].Check)
				report.PolicyEvidence[3+i] = check
				if err != nil {
					keepErr(err)
				}
				switch check.Status {
				case "VERIFIED":
					add(check.Check, "ok", check.Detail, "")
				case "FAILED":
					add(check.Check, "error", "FAILED: "+check.Detail, "check both control services, their enrolled peer IDs and private network policy; rerun doctor")
				case "NOT-RUN", "REFUSED":
					if err != nil {
						fix := "restore saved client access to the observer service, then rerun doctor"
						if check.Detail == doctorPeerBudgetExpired {
							fix = "rerun doctor when service reads can finish within its time budget"
						}
						add(check.Check, "error", check.Status+": "+check.Detail, fix)
					}
				}
			}
			admitted := false
			if l1Err == nil && configOK {
				for _, node := range nodes.Nodes {
					if role == serviceplan.Join {
						admitted = admitted || setupJoinNodeAdmitted(p, node)
					} else {
						admitted = admitted || setupNodeAdmitted(p, node)
					}
				}
			}
			switch {
			case l1Err != nil:
				add("local-node", "warning", "NOT-RUN: L1 admission could not be read", "restore client access to L1 and rerun doctor")
			case !configOK:
				add("local-node", "warning", "NOT-RUN: node configuration is invalid", "repair node.json and rerun doctor")
			case admitted:
				add("local-node", "ok", "L1 confirms alive state, claims enabled, Pinned tag and class slots"+doctorAdmissionSuffix(role), "")
			default:
				add("local-node", "error", "node is not alive and admitted with its Pinned tag and class slots"+doctorAdmissionSuffix(role), "ask the control-plane owner to check admission and claims; a reused identity may need explicit node admission; joining doctor never enables claims")
			}
		}
	}
	if !hasDoctorFinding(report, "local-node") {
		add("local-node", "warning", "NOT-RUN: verified client access to L1 is required", "repair client access and rerun doctor")
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return report, ctx.Err()
	}
	if liveErr != nil {
		return report, liveErr
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return report, &fabric.ParticipantUnreachableError{Cause: context.DeadlineExceeded}
	}
	if !report.Healthy {
		return report, &notReadyError{verdict: "fleet doctor found issues; follow the fix hints"}
	}
	return report, nil
}

func hasDoctorFinding(report plainDoctorReport, check string) bool {
	for _, f := range report.Findings {
		if f.Check == check {
			return true
		}
	}
	return false
}
func doctorAdmissionSuffix(role serviceplan.Role) string {
	if role == serviceplan.Join {
		return "; automatically admitted mark required"
	}
	return ""
}

func doctorMacHost(ctx context.Context, host setupHost, add func(string, string, string, string)) {
	run := host.doctorCommand
	if run == nil {
		run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
			return exec.CommandContext(ctx, name, args...).CombinedOutput()
		}
	}
	probe, done := context.WithTimeout(ctx, statusProbeBudget)
	power, err := run(probe, "/usr/bin/pmset", "-g", "custom")
	done()
	severity, detail := "warning", "unverified: system sleep settings could not be read"
	if err == nil {
		enabled, verified := parseDoctorSystemSleep(string(power))
		if verified {
			severity, detail = "ok", "system sleep is disabled in all reported power profiles"
			if enabled {
				severity, detail = "warning", "system sleep is enabled; the control plane may stop answering while this Mac sleeps"
			}
		}
	}
	add("system-sleep", severity, detail, "review system sleep settings for this control-plane Mac; doctor never changes power settings")
	probe, done = context.WithTimeout(ctx, statusProbeBudget)
	vault, err := run(probe, "/usr/bin/fdesetup", "status")
	done()
	severity, detail = "warning", "unverified: FileVault status could not be confirmed"
	if err == nil {
		switch strings.TrimSpace(string(vault)) {
		case "FileVault is On.":
			severity, detail = "warning", "FileVault is on; after a cold boot the owner must unlock the Mac before per-user control-plane services can start"
		case "FileVault is Off.":
			severity, detail = "ok", "FileVault is off"
		}
	}
	add("FileVault", severity, detail, "plan an attended cold-boot unlock and login recovery check; doctor never changes FileVault")
}

// Every reported power profile must include a recognized system-sleep timer.
// A zero from one profile cannot stand in for a missing or unknown other one.
func parseDoctorSystemSleep(output string) (enabled, verified bool) {
	profiles, timers := 0, 0
	haveTimer := false
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasSuffix(line, ":") {
			switch line {
			case "AC Power:", "Battery Power:", "UPS Power:":
			default:
				return false, false
			}
			if profiles > 0 && !haveTimer {
				return false, false
			}
			profiles++
			haveTimer = false
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "sleep" {
			continue
		}
		if profiles == 0 || haveTimer || len(fields) < 2 {
			return false, false
		}
		value, err := strconv.Atoi(fields[1])
		if err != nil || value < 0 {
			return false, false
		}
		haveTimer = true
		timers++
		enabled = enabled || value > 0
	}
	return enabled, profiles > 0 && timers == profiles && haveTimer
}

// Only read logind state. Enabling lingering is always an explicit owner action.
func doctorLinuxLingering(ctx context.Context, host setupHost, add func(string, string, string, string)) {
	run := host.doctorCommand
	if run == nil {
		run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
			cmd := exec.CommandContext(ctx, name, args...)
			cmd.WaitDelay = time.Second
			return cmd.Output()
		}
	}
	user := host.username
	if user == "" {
		user = strconv.Itoa(host.uid)
	}
	quoted := "'" + strings.ReplaceAll(user, "'", "'\"'\"'") + "'"
	enable := "loginctl enable-linger " + quoted
	probe, done := context.WithTimeout(ctx, statusProbeBudget)
	tool, err := systemduser.ResolveTool("loginctl", host.linuxFiles)
	var output []byte
	if err == nil {
		output, err = run(probe, tool, "show-user", user, "-p", "Linger")
	}
	done()
	severity, detail := "warning", "unverified: cannot confirm whether user services survive logout"
	if tool == "" && err != nil {
		detail += "; " + err.Error()
	}
	fix := "inspect `loginctl show-user " + quoted + " -p Linger`; if Linger=no, run `" + enable + "`; doctor and setup never change lingering"
	if err == nil {
		switch strings.TrimSpace(string(output)) {
		case "Linger=yes":
			severity, detail, fix = "ok", "lingering is enabled; user services can survive logout", ""
		case "Linger=no":
			detail, fix = "lingering is disabled; user services do not survive the last logout", "to keep services after logout, run `"+enable+"`; doctor and setup never change lingering"
		}
	}
	add("lingering", severity, detail, fix)
}
