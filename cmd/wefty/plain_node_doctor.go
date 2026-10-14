package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

type plainDoctorFinding struct {
	Check    string `json:"check"`
	Severity string `json:"severity"`
	Detail   string `json:"detail"`
	Fix      string `json:"fix"`
}
type plainDoctorReport struct {
	Version  int                  `json:"version"`
	Install  string               `json:"install"`
	Healthy  bool                 `json:"healthy"`
	Findings []plainDoctorFinding `json:"findings"`
}

func executePlainDoctor(ctx context.Context, options globalOptions, args []string, stdout io.Writer) error {
	if len(args) != 0 {
		return usageError("wefty node doctor does not accept arguments")
	}
	host, err := realSetupHost()
	if err != nil {
		return err
	}
	report := runPlainDoctor(ctx, host, options)
	if options.jsonOutput {
		if err := writeJSON(stdout, report); err != nil {
			return err
		}
	} else {
		for _, finding := range report.Findings {
			if _, err := fmt.Fprintf(stdout, "%s\t%s\t%s\n", finding.Check, finding.Severity, finding.Detail); err != nil {
				return err
			}
			if finding.Severity != "ok" && finding.Fix != "" {
				if _, err := fmt.Fprintf(stdout, "  Fix: %s\n", finding.Fix); err != nil {
					return err
				}
			}
		}
	}
	if !report.Healthy {
		return &notReadyError{verdict: "plain-process node doctor found issues; follow the fix hints"}
	}
	return nil
}

// This diagnosis reads local files even if every service is down. Only node
// registration needs L1, and an unavailable L1 is explicitly unverified.
func runPlainDoctor(ctx context.Context, host setupHost, options globalOptions) plainDoctorReport {
	report := plainDoctorReport{Version: 1, Install: "plain-process", Healthy: true, Findings: []plainDoctorFinding{}}
	add := func(check, severity, detail, fix string) {
		if severity == "ok" {
			fix = ""
		}
		report.Findings = append(report.Findings, plainDoctorFinding{check, severity, detail, fix})
		if severity != "ok" {
			report.Healthy = false
		}
	}
	budget, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for _, label := range []string{setupL1Label, setupL3Label, setupNodeAgentLabel} {
		detail, severity := "loaded and running", "ok"
		if host.goos != "darwin" {
			detail, severity = "LaunchAgents require macOS", "error"
		} else {
			checkCtx, cancel := context.WithTimeout(budget, statusProbeBudget)
			state, err := (launchagent.Manager{UID: host.uid, Run: host.launchctl}).State(checkCtx, label)
			cancel()
			switch {
			case err != nil:
				detail, severity = err.Error(), "error"
			case !state.Loaded:
				detail, severity = "not loaded", "error"
			case !state.Running:
				detail, severity = "loaded but not running", "error"
			}
		}
		add(label, severity, detail, "run `wefty setup`; inspect the service log if it stops again")
	}
	home, err := filepath.EvalSymlinks(host.home)
	if err != nil {
		add("paths", "error", err.Error(), "restore access to your home folder")
		return report
	}
	paths, issues := resolveMaintenancePaths(home)
	blocked := map[string]error{}
	for _, issue := range issues {
		blocked[issue.folder] = issue.err
		add("path:"+issue.folder, "error", issue.err.Error(), "move the link aside or restore access, then run `wefty setup`")
	}
	profilePath := options.profilePath
	if profilePath == "" {
		profilePath = paths.profile
	}
	profilePath, err = filepath.Abs(profilePath)
	var profile connprofile.Profile
	if err == nil && options.profilePath == "" {
		err = blocked[setupFolders.config]
	}
	if err == nil {
		profile, err = connprofile.Read(profilePath)
	}
	profileOK := err == nil
	if err != nil {
		add("connection-profile", "error", err.Error(), "fix or move the broken profile aside, then run `wefty setup`")
	} else {
		add("connection-profile", "ok", "valid profile at "+profilePath, "run `wefty setup` to repair missing settings")
	}
	// A confined write probe checks actual access as well as permission bits.
	root, err := os.OpenRoot(home)
	if err != nil {
		add("paths", "error", err.Error(), "restore access to your home folder")
		return report
	}
	defer root.Close()
	folders := append([]string{paths.binRel, paths.configRel, paths.dataRel, paths.logsRel, paths.launchAgentsRel}, paths.nodeDataFolders...)
	folders = append(folders, paths.skillTargets...)
	for _, folder := range folders {
		if blocked[folder] != nil {
			continue
		}
		err := doctorWritableFolder(root, folder)
		severity, detail := "ok", "exists and writable"
		if err != nil {
			severity, detail = "error", err.Error()
		}
		add("path:"+folder, severity, detail, "restore owner write access and run `wefty setup`")
	}
	configPath := options.nodeConfigPath
	if configPath == "" {
		configPath = paths.nodeConfig
	}
	configPath, err = filepath.Abs(configPath)
	var config ocicontrol.InstalledConfig
	if err == nil && options.nodeConfigPath == "" {
		err = blocked[setupFolders.config]
	}
	if err == nil {
		config, err = ocicontrol.ReadInstalledConfig(configPath)
	}
	configOK := err == nil && strings.TrimSpace(config.NodeID) != "" && strings.TrimSpace(config.NodeID) == config.NodeID && !strings.ContainsAny(config.NodeID, "\r\n\t")
	switch {
	case err != nil:
		add("node-config", "error", err.Error(), "restore node.json with this machine's stable node ID, then run `wefty setup`")
	case !configOK:
		add("node-config", "error", "node.json has no valid stable node ID", "restore this machine's stable node ID in node.json")
	case profileOK && profile.Identities.Node != config.NodeID:
		add("node-config", "error", "node.json names "+config.NodeID+" but the connection profile names "+profile.Identities.Node, "restore this machine's stable node ID in node.json and the connection profile")
	default:
		add("node-config", "ok", "node.json names "+config.NodeID, "keep this stable node ID when repairing the install")
	}
	if configOK && profileOK {
		options.nodeConfigPath = configPath
		if err := applyConnectionProfile(&options); err == nil {
			checkCtx, cancel := context.WithTimeout(budget, statusProbeBudget)
			err = verifyDoctorLocalNode(checkCtx, options)
			cancel()
			if err == nil {
				add("local-node", "ok", "L1 confirms this machine's installed node and reserved routing tag", "keep this node's admission policy")
			} else {
				severity, detail := "error", err.Error()
				var responseErr *apiResponseError
				var localErr *localNodeError
				if errors.As(err, &responseErr) && errors.As(err, &localErr) && localErr.Code == localNodeUnavailable {
					detail = fmt.Sprintf("L1 refused this identity: HTTP %d (%s): %s", responseErr.StatusCode, responseErr.APIError.Code, responseErr.APIError.Message)
				} else if errors.As(err, &localErr) && localErr.Code == localNodeUnavailable {
					severity, detail = "warning", "NOT-RUN: L1 is unavailable; this machine's node could not be verified: "+err.Error()
				}
				add("local-node", severity, detail, "start L1 with `wefty setup`; check its endpoint and node admission policy")
			}
		} else {
			add("local-node", "warning", "NOT-RUN: "+err.Error(), "repair the connection profile and rerun doctor")
		}
	} else {
		add("local-node", "warning", "NOT-RUN: node configuration or connection profile is invalid", "repair these files and rerun doctor")
	}
	var plist []byte
	err = blocked[setupFolders.launchAgents]
	if err == nil {
		plist, err = root.ReadFile(filepath.Join(paths.launchAgentsRel, setupNodeAgentLabel+".plist"))
	}
	var loginPATH string
	if err == nil {
		loginPATH, err = recordedNodePATH(plist)
	}
	if err != nil {
		add("node-agent-PATH", "error", "cannot read the node agent's recorded login-shell PATH: "+err.Error(), "run `wefty setup` to capture your login-shell PATH")
	} else {
		for _, tool := range []string{"go", "git", "node", "gh", "claude", "codex"} {
			severity, detail := "ok", "found on the node agent's recorded login-shell PATH"
			if blocked[setupFolders.data] != nil || !doctorFindTool(loginPATH, paths.data, tool) {
				severity, detail = "warning", "missing from the node agent's recorded login-shell PATH"
				if tool == "git" {
					severity = "error"
				}
			}
			add("tool:"+tool, severity, detail, "install "+tool+" or add it to your login-shell PATH, then run `wefty setup`")
		}
	}
	skillFiles, err := setupSkillFiles(paths)
	if err != nil {
		add("skills", "error", err.Error(), "reinstall the wefty binary")
		return report
	}
	for _, file := range skillFiles {
		name := filepath.Join(file.folder, file.name)
		got, err := root.ReadFile(name)
		info, statErr := root.Lstat(name)
		severity, detail := "ok", "matches the embedded wefty skill"
		if err != nil || statErr != nil || !info.Mode().IsRegular() || !bytes.Equal(got, file.payload) {
			severity, detail = "error", "missing or stale skill copy at "+filepath.Join(home, name)
		}
		add("skill:"+name, severity, detail, "run `wefty setup` from the installed version to refresh the skill copies")
	}
	return report
}
func doctorWritableFolder(root *os.Root, folder string) error {
	info, err := root.Stat(folder)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm()&0o300 != 0o300 {
		return errors.New("folder needs owner write and search access")
	}
	dir, err := root.OpenRoot(folder)
	if err != nil {
		return err
	}
	defer dir.Close()
	name, err := randomHex(rand.Reader, 8)
	if err != nil {
		return err
	}
	name = ".wefty-doctor-" + name
	f, err := dir.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	closeErr := f.Close()
	return errors.Join(closeErr, dir.Remove(name))
}
func doctorFindTool(path, workingDirectory, tool string) bool {
	for _, folder := range filepath.SplitList(path) {
		if !filepath.IsAbs(folder) {
			folder = filepath.Join(workingDirectory, folder)
		}
		_, err := exec.LookPath(filepath.Join(folder, tool))
		if err == nil {
			return true
		}
	}
	return false
}
func verifyDoctorLocalNode(ctx context.Context, options globalOptions) error {
	// Daily setup uses plain Fabric. Opening a tsnet node can require enrollment
	// and write state; doctor must not do that while diagnosing a local install.
	if options.fabricMode != "plain" {
		return &localNodeError{Code: localNodeConfigInvalid, Detail: "plain-process doctor needs the plain Fabric profile"}
	}
	participant, closeFabric, err := fabricconfig.Open(fabricconfig.Config{Mode: "plain", Identity: fabric.Identity{NodeID: options.plainIdentity, Tags: []string{l1.DefaultClientPrincipalTag}}, PlainFabricID: options.plainFabricID})
	if err != nil {
		return &localNodeError{Code: localNodeConfigInvalid, Detail: err.Error(), Cause: err}
	}
	defer closeFabric()
	clients, err := newAPIClients(participant, options.l1Address, "")
	if err != nil {
		return &localNodeError{Code: localNodeConfigInvalid, Detail: err.Error(), Cause: err}
	}
	defer clients.close()
	clients.nodeConfigPath = options.nodeConfigPath
	_, err = resolveLocalNode(ctx, clients)
	return err
}

// Read only the EnvironmentVariables dict, never a PATH key elsewhere in
// the plist or the diagnostic process's own PATH.
func recordedNodePATH(payload []byte) (string, error) {
	decoder := xml.NewDecoder(bytes.NewReader(payload))
	for {
		token, err := decoder.Token()
		if err != nil {
			return "", fmt.Errorf("node agent plist has no PATH: %w", err)
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name.Local != "key" {
			continue
		}
		var key string
		if err := decoder.DecodeElement(&key, &start); err != nil {
			return "", err
		}
		if key != "EnvironmentVariables" {
			continue
		}
		for {
			token, err := decoder.Token()
			if err != nil {
				return "", err
			}
			if start, ok = token.(xml.StartElement); ok {
				break
			}
		}
		if start.Name.Local != "dict" {
			return "", errors.New("EnvironmentVariables is not a dict")
		}
		var environment struct {
			Keys   []string `xml:"key"`
			Values []string `xml:"string"`
		}
		if err := decoder.DecodeElement(&environment, &start); err != nil {
			return "", err
		}
		if len(environment.Keys) != len(environment.Values) {
			return "", errors.New("invalid node agent environment")
		}
		for index, key := range environment.Keys {
			if key == "PATH" && strings.TrimSpace(environment.Values[index]) != "" {
				return environment.Values[index], nil
			}
		}
		return "", errors.New("node agent environment has no PATH")
	}
}
