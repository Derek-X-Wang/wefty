package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/durable"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

const plainConversionName = "plain-conversion.json"
const plainHistoryFolder = "plain-history"

// This allowlist includes L1's authority-instance identity and L3's complete
// capture directory (including upload temporaries). Never open these as stores.
var plainHistoryEntries = []string{"l1.sqlite", "l1.sqlite-wal", "l1.sqlite-shm", "l1.sqlite-journal", "l1.sqlite.authority-instance", "l3.sqlite", "l3.sqlite-wal", "l3.sqlite-shm", "l3.sqlite-journal", "exec-captures"}

func isPlainHistoryEntry(name string) bool {
	if slices.Contains(plainHistoryEntries, name) {
		return true
	}
	// durable.CreateFile can leave this unique staging file after process death
	// while L1 creates its authority identity. Do not adopt arbitrary prefixes.
	suffix, ok := strings.CutPrefix(name, "l1.sqlite.authority-instance"+durable.StagingSuffix+"-")
	decoded, err := hex.DecodeString(suffix)
	return ok && err == nil && len(decoded) == 8 && suffix == strings.ToLower(suffix)
}

type plainHistoryFile struct {
	Name      string      `json:"name"`
	Size      int64       `json:"size_bytes"`
	SHA256    string      `json:"sha256,omitempty"`
	Mode      fs.FileMode `json:"original_mode"`
	Directory bool        `json:"directory,omitempty"`
}

type plainHistory struct {
	Directory    string             `json:"directory"`
	Size         int64              `json:"size_bytes"`
	ReadOnly     bool               `json:"read_only"`
	NeverRestore bool               `json:"never_restore"`
	Files        []plainHistoryFile `json:"files"`
}

type plainConversionService struct {
	Label string `json:"label"`
	Plist []byte `json:"plist"`
}

type plainConversion struct {
	Version    int                      `json:"version"`
	NodeID     string                   `json:"node_id"`
	Phase      string                   `json:"phase"`
	Profile    connprofile.Profile      `json:"plain_profile"`
	Services   []plainConversionService `json:"plain_services"`
	History    plainHistory             `json:"history"`
	JoinCode   string                   `json:"join_code"`
	WithPerson bool                     `json:"with_person,omitempty"`
}

func readPlainConversion(paths setupPaths) (*plainConversion, error) {
	root, err := os.OpenRoot(paths.home)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	name := filepath.Join(paths.configRel, plainConversionName)
	info, err := root.Lstat(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return nil, errors.New("plain conversion record must be an unlinked private file")
	}
	payload, err := root.ReadFile(name)
	if err != nil {
		return nil, err
	}
	var r plainConversion
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&r); err != nil {
		return nil, errors.New("invalid plain conversion record; restore its original record")
	}
	if decoder.Decode(new(any)) != io.EOF || r.Version != 1 || !validSetupNodeID(r.NodeID) || r.Profile.Fabric != "plain" || len(r.Profile.Participants) != 0 || r.Profile.Identities.Node != r.NodeID || r.Profile.Validate() != nil || r.History.Directory != filepath.Join(paths.data, plainHistoryFolder, r.NodeID) || !r.History.NeverRestore || len(r.Services) != 3 {
		return nil, errors.New("invalid plain conversion record; setup will not infer or restore authority")
	}
	if !slices.Contains([]string{"prepared", "drain", "stop", "moving", "restoring", "history", "joining"}, r.Phase) {
		return nil, errors.New("invalid plain conversion phase")
	}
	for i, label := range []string{setupL1Label, setupL3Label, setupNodeAgentLabel} {
		if r.Services[i].Label != label || len(r.Services[i].Plist) == 0 {
			return nil, errors.New("invalid plain conversion service record")
		}
	}
	seen := map[string]bool{}
	var size int64
	for _, f := range r.History.Files {
		if !filepath.IsLocal(f.Name) || filepath.Clean(f.Name) != f.Name || seen[f.Name] || !isPlainHistoryEntry(strings.Split(f.Name, string(filepath.Separator))[0]) || f.Size < 0 || f.Mode != f.Mode.Perm() {
			return nil, errors.New("invalid retained history inventory")
		}
		seen[f.Name] = true
		if f.Directory {
			if f.SHA256 != "" || f.Size != 0 {
				return nil, errors.New("invalid history directory")
			}
		} else {
			hash, err := hex.DecodeString(f.SHA256)
			if err != nil || len(hash) != sha256.Size {
				return nil, errors.New("invalid history hash")
			}
			size += f.Size
		}
	}
	if size != r.History.Size || (r.History.ReadOnly != conversionForward(&r)) {
		return nil, errors.New("invalid history size or read-only boundary")
	}
	return &r, nil
}

func conversionForward(r *plainConversion) bool { return r.Phase == "history" || r.Phase == "joining" }

func savePlainConversion(paths setupPaths, r *plainConversion) error {
	root, err := os.OpenRoot(paths.config)
	if err != nil {
		return err
	}
	defer root.Close()
	payload, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return durable.WriteFile(root, plainConversionName, append(payload, '\n'), 0600)
}

func runPlainConversion(ctx context.Context, host setupHost, request setupRequest, paths setupPaths, record *plainConversion) (report setupReport, resultErr error) {
	// Retained history must remain visible even when resume input validation
	// fails before the installation lock or forward join stage is reached.
	defer func() {
		if record == nil || !conversionForward(record) || report.History != nil {
			return
		}
		report.History = &record.History
		report.Role, report.NodeID = serviceplan.Join, record.NodeID
		report.JoinResult = "joined"
		if resultErr != nil {
			report.JoinResult = "incomplete"
		}
		report.Notes = append(report.Notes, "History is read-only and never restored (ADR-0003).", "wefty upgrade refuses fleet roles until role-aware upgrade (#901).")
	}()
	if record == nil || !conversionForward(record) {
		if !request.convertPlain || request.role != "join" {
			return report, errors.New("plain conversion is incomplete; explicitly resume with setup --role join --convert-plain --join-code CODE")
		}
	} else if request.role != "" && request.role != "join" {
		return report, errors.New("plain conversion can only continue as --role join")
	}
	request.role = "join"
	if request.abandonConversion {
		if record == nil || conversionForward(record) {
			return report, errors.New("--abandon-conversion requires an incomplete pre-history conversion; committed history can never be restored")
		}
		if !request.lockHeld {
			unlock, err := lockSetup(paths, false)
			if err != nil {
				return report, err
			}
			defer unlock()
			request.lockHeld = true
			current, err := readPlainConversion(paths)
			if err != nil {
				return report, err
			}
			return runPlainConversion(ctx, host, request, paths, current)
		}
		if err := restorePlainConversion(ctx, host, paths, record, upgradeOperations(host), io.Discard); err != nil {
			return report, err
		}
		return setupReport{NodeID: record.NodeID, Profile: paths.profile, NodeConfig: paths.nodeConfig, BinDir: paths.bin, DataDir: paths.data, LogDir: paths.logs, Notes: []string{"Conversion abandoned; the plain installation and maintenance commands are restored."}}, nil
	}
	// Validate the independently selected coordinator and code before draining.
	// Once the fleet stage exists its own exact-code recovery rules apply.
	stage, err := readFleetSetupRecord(paths)
	if err != nil {
		return report, err
	}
	if request.coordinatorURL != "" && request.fleetCoordinatorURL != "" && request.coordinatorURL != request.fleetCoordinatorURL {
		return report, errors.New("setup coordinator flags or environment conflict; use the recorded coordinator URL or unset the conflicting override")
	}
	if request.coordinatorURL == "" {
		request.coordinatorURL = request.fleetCoordinatorURL
	}
	if request.coordinatorURL == "" {
		request.coordinatorURL = os.Getenv("TS_CONTROL_URL")
	}
	if ambient := os.Getenv("TS_CONTROL_URL"); ambient != "" && ambient != request.coordinatorURL {
		return report, errors.New("setup coordinator conflicts with the environment; unset the conflicting TS_CONTROL_URL value")
	}
	if request.joinCode == "" {
		if stage != nil {
			request.joinCode = stage.JoinCode
		} else if record != nil {
			request.joinCode = record.JoinCode
		}
	}
	if _, err := selectJoinSetup(paths, &request, stage); err != nil {
		return report, err
	}
	if record != nil && conversionForward(record) && stage == nil && (record.JoinCode != request.joinCode || record.WithPerson != request.withPerson) {
		return report, errors.New("conversion join code or person choice differs from its record")
	}
	if !request.lockHeld {
		unlock, err := lockSetup(paths, false)
		if err != nil {
			return report, err
		}
		defer unlock()
		request.lockHeld = true
		current, err := readPlainConversion(paths)
		if err != nil {
			return report, err
		}
		return runPlainConversion(ctx, host, request, paths, current)
	}
	root, err := os.OpenRoot(paths.home)
	if err != nil {
		return report, err
	}
	defer root.Close()
	step := func(name string) error {
		if request.afterFleetStep != nil {
			return request.afterFleetStep(name)
		}
		return nil
	}
	op := upgradeOperations(host)
	out := host.enrolmentOutput
	if out == nil {
		out = io.Discard
	}
	if record == nil {
		if err := refuseConversionUpgradeStage(root, paths); err != nil {
			return report, err
		}
		if stage != nil {
			return report, errors.New("--convert-plain requires an existing plain installation, not a fleet stage")
		}
		if role, err := selectSetupRole(paths, ""); err != nil || role != serviceplan.Legacy {
			return report, errors.New("--convert-plain requires a plain installation")
		}
		if err := checkFleetProfileFile(paths.profile); err != nil {
			return report, err
		}
		profile, err := readSetupProfile(paths.profile)
		if err != nil {
			return report, err
		}
		node, err := ocicontrol.ReadInstalledConfig(paths.nodeConfig)
		if err != nil || node.ControlSocket != "" || !validSetupNodeID(node.NodeID) || profile.Fabric != "plain" || len(profile.Participants) != 0 || profile.Identities.Node != node.NodeID {
			return report, errors.New("--convert-plain requires the matching plain profile and node.json; run plain setup first")
		}
		if request.nodeID != "" && request.nodeID != node.NodeID {
			return report, errors.New("conversion keeps node.json's stable node ID; conflicting --node-id refused")
		}
		// Check every join input that does not need keys before changing admission.
		source, err := setupSourceDirectory(host, request)
		if err != nil {
			return report, err
		}
		for _, name := range []string{"wefty", "wefty-agent"} {
			if _, err := executableDigest(filepath.Join(source, name)); err != nil {
				return report, err
			}
		}
		if _, err := captureLoginPATH(ctx, host); err != nil {
			return report, err
		}
		if _, err := setupRolePlan(paths, serviceplan.Join); err != nil {
			return report, err
		}
		history := plainHistory{Directory: filepath.Join(paths.data, plainHistoryFolder, node.NodeID), NeverRestore: true}
		if _, err := root.Lstat(filepath.Join(paths.dataRel, plainHistoryFolder)); !errors.Is(err, fs.ErrNotExist) {
			return report, errors.New("unrecorded plain history folder exists or cannot be inspected; restore its conversion record")
		}
		record = &plainConversion{Version: 1, NodeID: node.NodeID, Phase: "prepared", Profile: profile, History: history, JoinCode: request.joinCode, WithPerson: request.withPerson}
		manager := launchagent.Manager{Run: host.launchctl, UID: host.uid}
		for _, label := range []string{setupL1Label, setupL3Label, setupNodeAgentLabel} {
			state, err := manager.State(ctx, label)
			if err != nil {
				return report, err
			}
			if !state.Running {
				return report, fmt.Errorf("plain LaunchAgent %s must be running for a verified drain; run plain setup first", label)
			}
			name := filepath.Join(paths.launchAgentsRel, label+".plist")
			info, err := root.Lstat(name)
			if err != nil {
				return report, err
			}
			if !info.Mode().IsRegular() || info.Mode().Perm() != 0644 {
				return report, errors.New("plain conversion requires regular setup LaunchAgents with mode 0644")
			}
			payload, err := root.ReadFile(name)
			if err != nil {
				return report, err
			}
			record.Services = append(record.Services, plainConversionService{Label: label, Plist: payload})
		}
		if err := savePlainConversion(paths, record); err != nil {
			return report, err
		}
		if err := step("convert:record"); err != nil {
			return report, errors.Join(err, restorePlainConversion(ctx, host, paths, record, op, out))
		}
	}
	if request.nodeID != "" && request.nodeID != record.NodeID {
		return report, errors.New("conversion keeps its recorded stable node ID")
	}
	// A hard kill leaves the durable phase and rename inventory. Before the
	// boundary, recover the original plain stack and its fenced claims first.
	if !conversionForward(record) && record.Phase != "prepared" {
		if err := restorePlainConversion(ctx, host, paths, record, op, out); err != nil {
			return report, err
		}
	}
	if !conversionForward(record) {
		record.JoinCode, record.WithPerson = request.joinCode, request.withPerson
	}
	defer func() {
		// A panic/process death intentionally leaves recovery to the durable record.
		if resultErr != nil && !conversionForward(record) {
			resultErr = errors.Join(resultErr, restorePlainConversion(ctx, host, paths, record, op, out))
		}
	}()
	if !conversionForward(record) {
		if err := refuseConversionUpgradeStage(root, paths); err != nil {
			return report, err
		}
		if _, err := root.Lstat(filepath.Join(paths.dataRel, plainHistoryFolder)); !errors.Is(err, fs.ErrNotExist) {
			return report, errors.New("unrecorded plain history folder exists or cannot be inspected; conversion refused")
		}
		if err := recoverUpgradeDrain(ctx, op, paths, record.Profile, out); err != nil {
			return report, err
		}
		record.Phase = "drain"
		if err := savePlainConversion(paths, record); err != nil {
			return report, err
		}
		node, err := op.observe(ctx, record.Profile)
		if err != nil {
			return report, err
		}
		if node.NodeID != record.NodeID {
			return report, errors.New("plain drain observed a different node")
		}
		receipt, err := beginUpgradeDrain(ctx, op, paths, record.Profile, node, upgradeDrainTimeout)
		if err != nil {
			return report, err
		}
		if err := step("convert:claims"); err != nil {
			return report, err
		}
		if err := conversionBlockers(ctx, host, record.Profile, node); err != nil {
			return report, err
		}
		var work []l1.NodeActiveAttempt
		drained, err := waitUpgradeDrain(ctx, op, record.Profile, receipt, false, upgradeDrainTimeout, out, &work)
		if err != nil {
			var refusal *apiResponseError
			if errors.As(err, &refusal) {
				problem := refusal.APIError
				problem.Message = "conversion refused; retry later after running work finishes, or resolve node intent: " + problem.Message
				return report, &apiResponseError{APIError: problem}
			}
			return report, fmt.Errorf("conversion refused; retry later after running one-shots finish: %w", err)
		}
		if err := conversionBlockers(ctx, host, record.Profile, drained); err != nil {
			return report, err
		}
		if err := step("convert:drain"); err != nil {
			return report, err
		}
		// Re-read immediately before stop: preserve revision fences and late claims.
		drained, err = op.observe(ctx, record.Profile)
		if err != nil {
			return report, err
		}
		if err := checkUpgradeReplacement(drained, receipt, false); err != nil {
			return report, err
		}
		if err := conversionBlockers(ctx, host, record.Profile, drained); err != nil {
			return report, err
		}
		if err := checkPlainConversionWork(ctx, host, record.Profile); err != nil {
			return report, err
		}
		if err := waitConversionOutbox(ctx, op, paths, record.NodeID, true); err != nil {
			return report, err
		}
		// Evidence delivery can take time. Re-read the admission fence and late
		// claims after that wait, immediately before stopping the agent.
		drained, err = op.observe(ctx, record.Profile)
		if err != nil {
			return report, err
		}
		if err := checkUpgradeReplacement(drained, receipt, false); err != nil {
			return report, err
		}
		if err := conversionBlockers(ctx, host, record.Profile, drained); err != nil {
			return report, err
		}
		record.Phase = "stop"
		if err := savePlainConversion(paths, record); err != nil {
			return report, err
		}
		if err := stopPlainConversion(ctx, host, record, func(name string) error {
			if err := step(name); err != nil {
				return err
			}
			if name == "convert:stopped:"+setupNodeAgentLabel {
				if err := conversionBlockers(ctx, host, record.Profile, drained); err != nil {
					return err
				}
				if err := checkPlainConversionWork(ctx, host, record.Profile); err != nil {
					return err
				}
				return waitConversionOutbox(ctx, op, paths, record.NodeID, false)
			}
			return nil
		}); err != nil {
			return report, err
		}
		if err := step("convert:stop"); err != nil {
			return report, err
		}
		// Remove restart definitions while all old processes are confirmed absent.
		for _, service := range record.Services {
			if err := root.Remove(filepath.Join(paths.launchAgentsRel, service.Label+".plist")); err != nil {
				return report, err
			}
		}
		if err := syncConversionFolder(root, paths.launchAgentsRel); err != nil {
			return report, err
		}
		closed := host.conversionFilesClosed
		if closed == nil {
			closed = checkConversionFilesClosed
		}
		var databaseFiles []string
		for _, entry := range plainHistoryEntries {
			if !strings.HasPrefix(entry, "l1.sqlite") && !strings.HasPrefix(entry, "l3.sqlite") {
				continue
			}
			if _, err := root.Lstat(filepath.Join(paths.dataRel, entry)); err == nil {
				databaseFiles = append(databaseFiles, filepath.Join(paths.data, entry))
			} else if !errors.Is(err, fs.ErrNotExist) {
				return report, err
			}
		}
		if err := closed(ctx, databaseFiles); err != nil {
			return report, err
		}
		if err := removeConversionIdentityStagingLink(root, paths.dataRel); err != nil {
			return report, err
		}
		record.History.Files, record.History.Size, err = inventoryPlainHistory(root, paths.dataRel)
		if err != nil {
			return report, err
		}
		record.Phase = "moving"
		if err := savePlainConversion(paths, record); err != nil {
			return report, err
		}
		if err := step("convert:inventory"); err != nil {
			return report, err
		}
		target := filepath.Join(paths.dataRel, plainHistoryFolder, record.NodeID)
		if err := root.MkdirAll(target, 0700); err != nil {
			return report, err
		}
		if err := syncConversionFolder(root, paths.dataRel); err != nil {
			return report, err
		}
		if err := syncConversionFolder(root, filepath.Dir(target)); err != nil {
			return report, err
		}
		for _, entry := range historyTopEntries(record.History.Files) {
			if err := renameConversion(root, filepath.Join(paths.dataRel, entry), filepath.Join(target, entry)); err != nil {
				return report, err
			}
			if err := step("convert:moved:" + entry); err != nil {
				return report, err
			}
		}
		if err := verifyPlainHistory(root, target, record.History, false); err != nil {
			return report, err
		}
		// Children first; read/search permission remains for inspection without stores.
		for i := len(record.History.Files) - 1; i >= 0; i-- {
			f := record.History.Files[i]
			mode := fs.FileMode(0400)
			if f.Directory {
				mode = 0500
			}
			if err := root.Chmod(filepath.Join(target, f.Name), mode); err != nil {
				return report, err
			}
			if err := syncConversionEntry(root, filepath.Join(target, f.Name)); err != nil {
				return report, err
			}
			if err := step("convert:readonly:" + f.Name); err != nil {
				return report, err
			}
		}
		if err := root.Chmod(target, 0500); err != nil {
			return report, err
		}
		if err := syncConversionFolder(root, target); err != nil {
			return report, err
		}
		if err := step("convert:readonly"); err != nil {
			return report, err
		}
		if err := verifyPlainHistory(root, target, record.History, true); err != nil {
			return report, err
		}
		record.History.ReadOnly, record.Phase = true, "history"
		if err := savePlainConversion(paths, record); err != nil {
			// An ambiguous durable publication may already hold the forward boundary.
			saved, readErr := readPlainConversion(paths)
			if readErr == nil && saved != nil && !conversionForward(saved) {
				record.Phase, record.History.ReadOnly = "moving", false
			}
			return report, err
		}
		if err := step("convert:history"); err != nil {
			return report, err
		}
	}
	// From here history is evidence only. No restart or store-open is permitted.
	// Reconfirm old authority absence on every resume, even after a reboot.
	if err := stopPlainConversion(ctx, host, record); err != nil {
		return report, err
	}
	if stage == nil || !stage.ProfilePublished && !stage.Committed {
		if err := verifyPlainHistory(root, filepath.Join(paths.dataRel, plainHistoryFolder, record.NodeID), record.History, true); err != nil {
			return report, err
		}
	}
	if record.Phase == "history" {
		for _, name := range []string{filepath.Join(paths.configRel, connprofile.FileName), filepath.Join(paths.configRel, setupManifestName)} {
			if err := root.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return report, err
			}
		}
		if err := syncConversionFolder(root, paths.configRel); err != nil {
			return report, err
		}
		// The old disable receipt refers only to historical authority. It must never
		// be replayed through the new fleet (or by legacy upgrade recovery).
		if err := removeUpgradeDrain(paths.config); err != nil {
			return report, err
		}
		record.Phase = "joining"
		if err := savePlainConversion(paths, record); err != nil {
			return report, err
		}
		if err := step("convert:joining"); err != nil {
			return report, err
		}
	}
	// Stop does not remove definitions after commit: the node now belongs to join.
	request.nodeID = record.NodeID
	request.convertPlain = false
	report, resultErr = runFleetSetup(ctx, host, request, paths, serviceplan.Join)
	return report, resultErr
}

func stopPlainConversion(ctx context.Context, host setupHost, r *plainConversion, steps ...func(string) error) error {
	manager := launchagent.Manager{Run: host.launchctl, UID: host.uid, UnloadBound: host.unloadBound}
	if manager.UnloadBound == 0 {
		manager.UnloadBound = time.Duration(setupNodeAgentExitTimeout+15) * time.Second
	}
	labels := []string{setupNodeAgentLabel, setupL3Label, setupL1Label}
	if conversionForward(r) {
		labels = []string{setupL3Label, setupL1Label}
	}
	for _, label := range labels {
		if err := manager.Unload(ctx, label); err != nil {
			return err
		}
		if len(steps) > 0 {
			if err := steps[0]("convert:stopped:" + label); err != nil {
				return err
			}
		}
	}
	return nil
}

func conversionBlockers(ctx context.Context, host setupHost, profile connprofile.Profile, node l1.Node) error {
	var blockers []string
	for _, a := range node.ActiveAttempts {
		if a.Class == contract.JobClassService {
			blockers = append(blockers, "service job "+a.JobID+" (attempt "+a.AttemptID+")")
		}
	}
	list := host.conversionComputers
	if list == nil {
		list = listPlainConversionComputers
	}
	computers, err := list(ctx, profile)
	if err != nil {
		return err
	}
	for _, c := range computers {
		// Removal intent is also 'removed' while cleanup is pending. Only
		// retired records can be ignored; their unfinished cleanup still blocks.
		if c.DesiredState == contract.ServiceDesiredRemoved && c.RemovalOutcome != "removal_pending" && c.CurrentJob.State != contract.JobRemovalPending {
			continue
		}
		if c.BoundNodeID == node.NodeID || c.PlacementNodeID == node.NodeID {
			blockers = append(blockers, "Computer "+c.ComputerID)
		}
	}
	if len(blockers) != 0 {
		return &apiResponseError{APIError: contract.APIError{Code: contract.ErrorConflict, Message: "conversion blocked by " + strings.Join(blockers, ", ") + "; stop or retire these through their normal lifecycle, then retry later"}}
	}
	return nil
}

func listPlainConversionComputers(ctx context.Context, profile connprofile.Profile) (result []l1.Computer, err error) {
	err = withUpgradeClients(ctx, profile, func(ctx context.Context, c *apiClients) error {
		seen := map[string]bool{}
		for cursor := ""; ; {
			page, err := c.listComputers(ctx, cursor, 100)
			if err != nil {
				return err
			}
			result = append(result, page.Computers...)
			if page.NextCursor == "" {
				return nil
			}
			if seen[page.NextCursor] {
				return errors.New("Computer inventory repeated a cursor; conversion refused")
			}
			seen[page.NextCursor], cursor = true, page.NextCursor
		}
	})
	return
}

func restorePlainConversion(ctx context.Context, host setupHost, paths setupPaths, r *plainConversion, op upgradeHost, out io.Writer) (resultErr error) {
	defer func() {
		if resultErr != nil {
			command := fmt.Sprintf("%q setup --role join --convert-plain --abandon-conversion", host.executable)
			cause := strings.ReplaceAll(resultErr.Error(), "run wefty upgrade to recover", "run "+command+" to finish restoring the plain installation")
			resultErr = fmt.Errorf("plain restoration is incomplete; repair plain L1/service reachability, then run %s; keep the conversion and drain records until it succeeds: %s", command, cause)
		}
	}()
	if conversionForward(r) {
		return errors.New("read-only authority history can never be restored")
	}
	recovery, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Duration(3*(setupNodeAgentExitTimeout+15))*time.Second+upgradeDrainRecoveryBudget)
	defer cancel()
	root, err := os.OpenRoot(paths.home)
	if err != nil {
		return err
	}
	defer root.Close()
	if r.Phase == "moving" {
		if err := stopPlainConversion(recovery, host, r); err != nil {
			return err
		}
		target := filepath.Join(paths.dataRel, plainHistoryFolder, r.NodeID)
		if _, err := root.Lstat(target); err == nil {
			if err := root.Chmod(target, 0700); err != nil {
				return err
			}
			// Restore permissions parent-first before renaming the closed dataset back.
			for _, f := range r.History.Files {
				name := filepath.Join(target, f.Name)
				if _, err := root.Lstat(name); errors.Is(err, fs.ErrNotExist) {
					continue
				} else if err != nil {
					return err
				}
				if err := root.Chmod(name, f.Mode); err != nil {
					return err
				}
				if err := syncConversionEntry(root, name); err != nil {
					return err
				}
			}
			for _, entry := range historyTopEntries(r.History.Files) {
				src, dst := filepath.Join(target, entry), filepath.Join(paths.dataRel, entry)
				if _, err := root.Lstat(src); errors.Is(err, fs.ErrNotExist) {
					continue
				} else if err != nil {
					return err
				}
				if _, err := root.Lstat(dst); !errors.Is(err, fs.ErrNotExist) {
					return errors.New("both active and staged history entries exist; conversion recovery refused")
				}
				if err := renameConversion(root, src, dst); err != nil {
					return err
				}
			}
			if err := root.Remove(target); err != nil {
				return err
			}
		} else if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		// A recovery crash can leave only this empty parent after removing target.
		// Remove (never RemoveAll) also refuses any unrecorded history contents.
		if err := root.Remove(filepath.Dir(target)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if err := syncConversionFolder(root, paths.dataRel); err != nil {
			return err
		}
		if err := verifyPlainHistory(root, paths.dataRel, r.History, false); err != nil {
			return err
		}
	}
	if r.Phase == "stop" || r.Phase == "moving" || r.Phase == "prepared" || r.Phase == "drain" {
		// Publish recovery of the dataset before any plain process can write it.
		r.Phase = "restoring"
		if err := savePlainConversion(paths, r); err != nil {
			return err
		}
	}
	if r.Phase == "restoring" {
		folder, err := root.OpenRoot(paths.launchAgentsRel)
		if err != nil {
			return err
		}
		defer folder.Close()
		manager := launchagent.Manager{Run: host.launchctl, UID: host.uid, UnloadBound: host.unloadBound}
		for _, s := range r.Services {
			if _, err := manager.Converge(recovery, folder, filepath.Join(paths.launchAgents, s.Label+".plist"), s.Label, s.Plist); err != nil {
				return fmt.Errorf("restore plain LaunchAgent: %w", err)
			}
			deadline := op.now().Add(upgradeMutationBudget)
			for {
				state, err := manager.State(recovery, s.Label)
				if err != nil {
					return err
				}
				if state.Running {
					break
				}
				if !op.now().Before(deadline) {
					return fmt.Errorf("restored plain service %s is not running", s.Label)
				}
				if err := op.wait(recovery, min(100*time.Millisecond, deadline.Sub(op.now()))); err != nil {
					return err
				}
			}
		}
	}
	if err := recoverUpgradeDrain(recovery, op, paths, r.Profile, out); err != nil {
		return err
	}
	r.Phase, r.History.Files, r.History.Size, r.History.ReadOnly = "prepared", nil, 0, false
	if err := root.Remove(filepath.Join(paths.configRel, plainConversionName)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return syncConversionFolder(root, paths.configRel)
}

func inventoryPlainHistory(root *os.Root, folder string) ([]plainHistoryFile, int64, error) {
	var files []plainHistoryFile
	var size int64
	for _, database := range []string{"l1.sqlite", "l3.sqlite"} {
		info, err := root.Lstat(filepath.Join(folder, database))
		if err != nil {
			return nil, 0, err
		}
		if !info.Mode().IsRegular() {
			return nil, 0, errors.New("plain databases must be regular files")
		}
	}
	entries, err := fs.ReadDir(root.FS(), folder)
	if err != nil {
		return nil, 0, err
	}
	for _, candidate := range entries {
		entry := candidate.Name()
		if !isPlainHistoryEntry(entry) {
			continue
		}
		if _, err := root.Lstat(filepath.Join(folder, entry)); errors.Is(err, fs.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, 0, err
		}
		err := fs.WalkDir(root.FS(), filepath.Join(folder, entry), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			info, err := root.Lstat(path)
			if err != nil {
				return err
			}
			if !info.IsDir() && (!info.Mode().IsRegular() || !conversionSingleLink(info)) {
				return errors.New("history contains a link or special file; conversion refused")
			}
			name, err := filepath.Rel(folder, path)
			if err != nil {
				return err
			}
			f := plainHistoryFile{Name: name, Mode: info.Mode().Perm(), Directory: info.IsDir()}
			if !info.IsDir() {
				f.Size, f.SHA256, err = hashConversionFile(root, path)
				if err != nil {
					return err
				}
				size += f.Size
			}
			files = append(files, f)
			return nil
		})
		if err != nil {
			return nil, 0, err
		}
	}
	return files, size, nil
}

func hashConversionFile(root *os.Root, name string) (int64, string, error) {
	f, err := root.Open(name)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return n, hex.EncodeToString(h.Sum(nil)), err
}

func verifyPlainHistory(root *os.Root, folder string, history plainHistory, readonly bool) error {
	expected := make(map[string]bool, len(history.Files))
	for _, f := range history.Files {
		expected[f.Name] = true
	}
	entries := historyTopEntries(history.Files)
	if readonly {
		// The retained folder is a closed dataset. Inspect the container too,
		// including siblings of the recorded top-level entries.
		entries = []string{"."}
	}
	for _, entry := range entries {
		if err := fs.WalkDir(root.FS(), filepath.Join(folder, entry), func(path string, _ fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			name, err := filepath.Rel(folder, path)
			if err != nil {
				return err
			}
			if readonly && name == "." {
				return nil
			}
			if !expected[name] {
				return errors.New("unrecorded file in the closed history dataset")
			}
			return nil
		}); err != nil {
			return err
		}
	}

	for _, f := range history.Files {
		name := filepath.Join(folder, f.Name)
		info, err := root.Lstat(name)
		if err != nil {
			return err
		}
		if info.IsDir() != f.Directory || !info.IsDir() && (!info.Mode().IsRegular() || !conversionSingleLink(info)) {
			return errors.New("history file type changed")
		}
		if readonly && info.Mode().Perm()&0222 != 0 {
			return errors.New("history is not read-only")
		}
		if !f.Directory {
			size, hash, err := hashConversionFile(root, name)
			if err != nil {
				return err
			}
			if size != f.Size || hash != f.SHA256 {
				return errors.New("history hash or size differs from the recorded closed dataset")
			}
		}
	}
	if readonly {
		info, err := root.Lstat(folder)
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode().Perm()&0222 != 0 {
			return errors.New("history folder is not read-only")
		}
	}
	return nil
}

func historyTopEntries(files []plainHistoryFile) []string {
	var result []string
	for _, f := range files {
		if !strings.ContainsRune(f.Name, filepath.Separator) {
			result = append(result, f.Name)
		}
	}
	return result
}

func syncConversionFolder(root *os.Root, name string) error {
	folder, err := root.OpenRoot(name)
	if err != nil {
		return err
	}
	defer folder.Close()
	return durable.SyncDir(folder)
}

func renameConversion(root *os.Root, from, to string) error {
	if err := root.Rename(from, to); err != nil {
		return err
	}
	return errors.Join(syncConversionFolder(root, filepath.Dir(from)), syncConversionFolder(root, filepath.Dir(to)))
}

func syncConversionEntry(root *os.Root, name string) error {
	file, err := root.Open(name)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

// durable.CreateFile publishes by linking a fully synced unique staging file.
// A crash can retain both names. Unlink only that exact two-link owned pair.
func removeConversionIdentityStagingLink(root *os.Root, folder string) error {
	const identity = "l1.sqlite.authority-instance"
	original, err := root.Lstat(filepath.Join(folder, identity))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !original.Mode().IsRegular() || !conversionDoubleLink(original) {
		return nil
	}
	entries, err := fs.ReadDir(root.FS(), folder)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, identity+durable.StagingSuffix+"-") || !isPlainHistoryEntry(name) {
			continue
		}
		other, err := root.Lstat(filepath.Join(folder, name))
		if err != nil {
			return err
		}
		if !other.Mode().IsRegular() || !conversionDoubleLink(other) || !os.SameFile(original, other) {
			continue
		}
		if err := root.Remove(filepath.Join(folder, name)); err != nil {
			return err
		}
		return syncConversionFolder(root, folder)
	}
	return nil // Inventory retains the ordinary fail-closed hard-link check.
}

func refuseConversionUpgradeStage(root *os.Root, paths setupPaths) error {
	entries, err := fs.ReadDir(root.FS(), paths.dataRel)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".upgrade-") {
			return errors.New("conversion refuses interrupted upgrade staging; run wefty upgrade on the plain installation first to recover and remove its preflight copies, then retry conversion")
		}
	}
	return nil
}
