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

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/internal/fleetjoin"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

const fleetSetupRecordName = "fleet-setup.json"
const fleetSetupProfileName = "fleet-setup-profile.json"

// Kept after commit to retain the explicit role, stable scheduling identity,
// and staged participant records across interruptions. The profile, never the
// informational markers, determines which participants have been saved.
type fleetSetupRecord struct {
	LinuxLogRoot            string           `json:"linux_log_root,omitempty"`
	ConversionHistoryDigest string           `json:"conversion_history_sha256,omitempty"`
	JoinCode                string           `json:"join_code,omitempty"`
	WithPerson              bool             `json:"with_person,omitempty"`
	PeerVerificationFailure string           `json:"peer_verification_failure,omitempty"`
	Version                 int              `json:"version"`
	Role                    serviceplan.Role `json:"role"`
	NodeID                  string           `json:"node_id"`
	StagedProfile           string           `json:"staged_profile"`
	CoordinatorURL          string           `json:"coordinator_url,omitempty"`
	Enrolled                []string         `json:"enrolled,omitempty"`
	ProfilePublished        bool             `json:"profile_published,omitempty"`
	Committed               bool             `json:"committed,omitempty"`
}

func readFleetSetupRecord(paths setupPaths) (*fleetSetupRecord, error) {
	root, err := os.OpenRoot(paths.home)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	path := filepath.Join(paths.configRel, fleetSetupRecordName)
	info, err := root.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return nil, errors.New("fleet setup record must be an unlinked private file; restore it before setup")
	}
	payload, err := root.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var record fleetSetupRecord
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return nil, errors.New("invalid fleet setup record; restore it before setup")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return nil, errors.New("invalid trailing fleet setup record data")
	}
	if record.Version != 1 || record.Role.Validate() != nil || record.Role == serviceplan.Legacy || !validSetupNodeID(record.NodeID) || record.StagedProfile != filepath.Join(paths.config, fleetSetupProfileName) || connprofile.ValidateCoordinatorURL(record.CoordinatorURL) != nil {
		return nil, errors.New("invalid fleet setup record; setup will not infer a role or replace saved identities")
	}
	if record.ConversionHistoryDigest != "" {
		digest, prefixed := strings.CutPrefix(record.ConversionHistoryDigest, "sha256:")
		decoded, err := hex.DecodeString(digest)
		if !prefixed || err != nil || len(decoded) != sha256.Size || record.ConversionHistoryDigest != strings.ToLower(record.ConversionHistoryDigest) || record.Role != serviceplan.Join || !record.Committed || !record.ProfilePublished {
			return nil, errors.New("invalid committed conversion history digest")
		}
	}
	if record.Role == serviceplan.Join {
		d, err := fleetjoin.Parse(record.JoinCode)
		if err != nil || d.CoordinatorURL != record.CoordinatorURL {
			return nil, errors.New("invalid recorded join code; restore the fleet setup record")
		}
		if !validJoinVerificationFailure(record.PeerVerificationFailure) {
			return nil, errors.New("invalid peer verification failure record")
		}
	} else if record.JoinCode != "" || record.WithPerson || record.PeerVerificationFailure != "" {
		return nil, errors.New("control-plane record contains joining metadata")
	}
	return &record, nil
}

// Selection is read-only and precedes installation locks or other writes.
// A recorded receipt continues its role; an uncommitted stage requires the
// explicit role flag, including the complete-profile/no-receipt crash window.
func selectSetupRole(paths setupPaths, requested string) (serviceplan.Role, error) {
	role := serviceplan.Role(requested)
	if err := role.Validate(); err != nil {
		return "", usageError(err.Error())
	}
	root, err := os.OpenRoot(paths.home)
	if err != nil {
		return "", err
	}
	defer root.Close()
	var metadata map[string]json.RawMessage
	if payload, err := root.ReadFile(filepath.Join(paths.configRel, setupManifestName)); err == nil {
		if json.Unmarshal(payload, &metadata) == nil {
			if raw, ok := metadata["role"]; ok {
				var saved serviceplan.Role
				if json.Unmarshal(raw, &saved) != nil || saved.Validate() != nil {
					return "", errors.New("setup refuses invalid recorded fleet role metadata")
				}
				if saved != "" {
					if role != "" && role != saved {
						return "", fmt.Errorf("setup refuses role %q over recorded fleet role %q; role conversion is not available", role, saved)
					}
					role = saved
				}
			}
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("setup cannot verify the installation role: %w", err)
	}
	record, err := readFleetSetupRecord(paths)
	if err != nil {
		return "", err
	}
	if record == nil {
		if _, err := root.Lstat(filepath.Join(paths.configRel, fleetSetupProfileName)); err == nil {
			return "", errors.New("staged fleet profile has no resume record; restore the original fleet setup resume record before setup")
		} else if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
	}
	if record != nil {
		if role == "" {
			return "", fmt.Errorf("incomplete fleet setup; resume with wefty setup --role %s", record.Role)
		}
		if role != record.Role {
			return "", fmt.Errorf("setup refuses role %q over staged fleet role %q; role conversion is not available", role, record.Role)
		}
	}
	if role != serviceplan.Legacy && record == nil {
		// A receipt without its durable setup record cannot reconstruct the
		// old participants. An existing plain install is never a conversion.
		for _, path := range []string{paths.profile, paths.nodeConfig, filepath.Join(paths.config, setupManifestName), filepath.Join(paths.data, "l1.sqlite"), filepath.Join(paths.data, "l3.sqlite")} {
			if _, err := os.Lstat(path); err == nil {
				return "", errors.New("setup refuses recorded fleet role without its resume record, or a fleet role over an existing plain installation; restore the fleet setup record, or use a fresh install (plain conversion is a later slice)")
			} else if !errors.Is(err, fs.ErrNotExist) {
				return "", err
			}
		}
	}
	return role, nil
}

func fleetSetupParticipants(plan serviceplan.Plan, nodeID string) ([]setupParticipant, error) {
	var participants []setupParticipant
	for _, service := range plan.Services {
		name, _, ok := setupParticipantPolicy(service.Participant)
		if !ok {
			return nil, errors.New("service plan selected an unknown participant")
		}
		if strings.HasSuffix(name, "/") {
			name += nodeID
		}
		participants = append(participants, setupParticipant{service.Participant, name})
	}
	return participants, nil
}

func runFleetSetup(ctx context.Context, host setupHost, request setupRequest, paths setupPaths, role serviceplan.Role) (report setupReport, resultErr error) {
	defer func() {
		if resultErr != nil && report.History != nil {
			report.JoinResult = "incomplete"
		}
	}()
	manager := fleetServiceManager{host: host}
	report = setupReport{Role: role, Profile: paths.profile, NodeConfig: paths.nodeConfig, BinDir: paths.bin, DataDir: paths.data, LogDir: paths.logs, Changes: []string{}, Participants: map[string]string{}}
	record, err := readFleetSetupRecord(paths)
	if err != nil {
		return report, err
	}
	if request.coordinatorURL != "" && request.fleetCoordinatorURL != "" && request.coordinatorURL != request.fleetCoordinatorURL {
		return report, errors.New("setup coordinator flags or environment conflict; use the recorded coordinator URL or unset the conflicting override")
	}
	if request.coordinatorURL == "" {
		request.coordinatorURL = request.fleetCoordinatorURL
	}

	var join fleetjoin.Descriptor
	if role == serviceplan.Join {
		if request.coordinatorURL == "" {
			request.coordinatorURL = os.Getenv("TS_CONTROL_URL")
		}
		join, err = selectJoinSetup(paths, &request, record)
		if err != nil {
			return report, err
		}
	} else if request.joinCode != "" || request.withPerson {
		return report, usageError("--join-code and --with-person require --role join")
	}
	if record != nil {
		if request.nodeID != "" && request.nodeID != record.NodeID {
			return report, errors.New("setup keeps its recorded stable node ID; a conflicting --node-id is refused")
		}
		if request.coordinatorURL != "" && request.coordinatorURL != record.CoordinatorURL {
			return report, errors.New("setup coordinator conflicts with its staged record; use the recorded coordinator URL or unset the conflicting override")
		}
		request.coordinatorURL = record.CoordinatorURL
	}
	if ambient := os.Getenv("TS_CONTROL_URL"); ambient != "" && ambient != request.coordinatorURL {
		return report, errors.New("setup coordinator conflicts with the environment; use the recorded coordinator URL or unset the conflicting TS_CONTROL_URL value")
	}
	if err := connprofile.ValidateCoordinatorURL(request.coordinatorURL); err != nil {
		return report, err
	}

	plan, err := setupRolePlan(paths, role)
	if err != nil {
		return report, err
	}
	_, receiptErr := os.Lstat(filepath.Join(paths.config, setupManifestName))
	if receiptErr != nil && !errors.Is(receiptErr, fs.ErrNotExist) {
		return report, receiptErr
	}
	if errors.Is(receiptErr, fs.ErrNotExist) {
		if record != nil && record.Committed {
			return report, errors.New("committed fleet role receipt is missing; restore its original receipt before setup")
		}
		if err := refuseUncommittedFleetServices(ctx, host, paths, plan); err != nil {
			return report, err
		}
	}

	source, err := setupSourceDirectory(host, request)
	if err != nil {
		return report, err
	}
	binaries := []string{"wefty"}
	for _, service := range plan.Services {
		if !slices.Contains(binaries, service.Binary) {
			binaries = append(binaries, service.Binary)
		}
	}
	digests := map[string]string{}
	for _, name := range binaries {
		digest, err := executableDigest(filepath.Join(source, name))
		if err != nil {
			if role == serviceplan.ControlPlane {
				return report, fmt.Errorf("build all four setup binaries into one folder first: %w", err)
			}
			return report, fmt.Errorf("build wefty and wefty-agent into one folder first: %w", err)
		}
		digests[name] = digest
	}
	loginPATH, err := captureLoginPATH(ctx, host)
	if err != nil {
		return report, err
	}
	if !slices.Contains(filepath.SplitList(loginPATH), paths.bin) {
		loginPATH += string(filepath.ListSeparator) + paths.bin
	}
	if host.goos == "linux" {
		if err := preflightLinuxFleet(ctx, host, paths, digests, loginPATH, role); err != nil {
			return report, err
		}
	}
	nodeID := request.nodeID
	if record != nil {
		nodeID = record.NodeID
	}
	if nodeID == "" {
		nodeID, err = newSetupNodeID(host.hostname, host.random)
		if err != nil {
			return report, err
		}
	}
	if !validSetupNodeID(nodeID) {
		return report, usageError("--node-id must be a valid stable lowercase node ID")
	}
	if record == nil {
		record = &fleetSetupRecord{Version: 1, Role: role, NodeID: nodeID, StagedProfile: filepath.Join(paths.config, fleetSetupProfileName), CoordinatorURL: request.coordinatorURL, JoinCode: request.joinCode, WithPerson: request.withPerson}
	}
	if host.goos == "linux" {
		record.LinuxLogRoot = paths.logs
		if record.WithPerson {
			report.Notes = append(report.Notes, linuxPersonWarning)
		}
	}
	if role == serviceplan.Join {
		if record.JoinCode != request.joinCode {
			record.PeerVerificationFailure = ""
		}
		record.JoinCode = request.joinCode
	}
	report.NodeID = nodeID
	participants, err := fleetSetupParticipants(plan, nodeID)
	if err != nil {
		return report, err
	}

	skills, err := setupSkillFilesFrom(paths, request.skillSource)
	if err != nil {
		return report, err
	}
	root, err := os.OpenRoot(paths.home)
	if err != nil {
		return report, err
	}
	defer root.Close()
	readyFolder := filepath.Join(paths.dataRel, "setup-ready")
	if info, err := root.Lstat(readyFolder); err == nil {
		if !info.IsDir() {
			return report, errors.New("fleet ready path must be an unlinked folder")
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return report, err
	}
	folders := append([]string{paths.binRel, paths.configRel, paths.dataRel, paths.logsRel, paths.launchAgentsRel, filepath.Join(paths.dataRel, "setup-ready")}, paths.nodeDataFolders...)
	for _, file := range skills {
		folders = append(folders, file.folder)
	}
	destinations := []string{filepath.Join(paths.configRel, connprofile.FileName), filepath.Join(paths.configRel, "node.json"), filepath.Join(paths.configRel, setupManifestName), filepath.Join(paths.configRel, fleetSetupRecordName), filepath.Join(paths.configRel, fleetSetupProfileName)}
	for _, name := range binaries {
		destinations = append(destinations, filepath.Join(paths.binRel, name))
	}
	for _, file := range skills {
		destinations = append(destinations, filepath.Join(file.folder, file.name))
	}
	for _, service := range plan.Services {
		destinations = append(destinations, filepath.Join(paths.launchAgentsRel, manager.file("dev.wefty."+service.Name)))
	}
	if err := checkSetupDestinations(root, paths.home, folders, destinations); err != nil {
		return report, err
	}
	if !request.lockHeld {
		unlock, err := lockSetup(paths, true)
		if err != nil {
			return report, err
		}
		defer unlock()
		request.lockHeld = true
		host.loginShell = func(context.Context, string, ...string) ([]byte, error) {
			return []byte(setupPATHBegin + "\n" + loginPATH + "\n" + setupPATHEnd + "\n"), nil
		}
		request.nodeID = nodeID
		return runSetup(ctx, host, request)
	}
	step := func(name string) error {
		if request.afterFleetStep != nil {
			return request.afterFleetStep(name)
		}
		return nil
	}
	write := func(file setupFile) error {
		changed, err := convergeHomeFile(root, file)
		if err != nil {
			return err
		}
		if changed {
			if file.change != "" {
				report.Changes = append(report.Changes, file.change)
			}
			if request.wroteFile != nil {
				request.wroteFile(filepath.Join(file.folder, file.name), file.payload)
			}
		}
		return nil
	}
	saveRecord := func() error {
		payload, err := json.MarshalIndent(record, "", "  ")
		if err != nil {
			return err
		}
		return write(setupFile{folder: paths.configRel, name: fleetSetupRecordName, payload: append(payload, '\n'), mode: 0600})
	}
	// Validate a completed installation without rewriting its active profile.
	var profile connprofile.Profile
	manifest := setupManifest{Version: 1, Role: role, Files: map[string]string{}}
	if host.goos == "linux" {
		manifest.LinuxLogRoot = paths.logs
	}
	conversion, err := readPlainConversion(paths)
	if err != nil {
		return report, err
	}
	if conversion != nil {
		if !conversionForward(conversion) {
			return report, errors.New("conversion history is incomplete")
		}
	}
	committed := false
	if payload, err := root.ReadFile(filepath.Join(paths.configRel, setupManifestName)); err == nil {
		if json.Unmarshal(payload, &manifest) != nil || manifest.Version != 1 || manifest.Role != role || manifest.Files == nil {
			return report, errors.New("invalid recorded fleet role receipt; restore it before setup")
		}
		if conversion != nil {
			if manifest.History == nil {
				return report, errors.New("converted role receipt or plain conversion record is missing history metadata; restore the original records")
			}
			recorded, err := json.Marshal(manifest.History)
			if err != nil {
				return report, err
			}
			expected, err := json.Marshal(conversion.History)
			if err != nil {
				return report, err
			}
			if !bytes.Equal(recorded, expected) {
				return report, errors.New("converted role receipt history differs from its conversion record; restore the original receipt")
			}
		}
		if err := checkFleetConversionHistory(record, manifest.History); err != nil {
			return report, err
		}
		if conversion == nil && manifest.History != nil && record.ConversionHistoryDigest == "" {
			return report, errors.New("converted role receipt is missing history metadata binding; restore the original records")
		}
		committed = true
		if err := checkFleetProfileFile(paths.profile); err != nil {
			return report, err
		}
		profilePath := filepath.Join(paths.configRel, connprofile.FileName)
		activePayload, err := root.ReadFile(profilePath)
		if err != nil {
			return report, err
		}
		if digest := manifest.Files[profilePath]; digest == "" || payloadDigest(activePayload) != digest {
			return report, errors.New("completed fleet profile differs from its receipt; restore the recorded profile")
		}
		profile, err = readSetupProfile(paths.profile)
		if err != nil {
			return report, err
		}
		if profile.Version == 0 {
			return report, errors.New("recorded fleet role profile is missing; restore the original profile before setup")
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return report, err
	}

	if !committed && conversion != nil {
		manifest.History = &conversion.History
	}

	if committed && role == serviceplan.Join && record.WithPerson {
		participants = append(participants, setupParticipant{connprofile.ParticipantPerson, "wefty://person/" + nodeID})
	}
	if !committed && record.Committed {
		return report, errors.New("committed fleet role receipt is missing; restore its original receipt before setup")
	}
	if committed && host.goos == "linux" {
		if err := validateLinuxLogFolders(paths, host.euid); err != nil {
			return report, err
		}
		if err := root.Chmod(paths.logsRel, 0700); err != nil {
			return report, err
		}
	}
	if !committed {
		for _, folder := range folders {
			mode := os.FileMode(0700)
			if folder == paths.binRel || folder == paths.launchAgentsRel || slices.Contains(paths.skillTargets, folder) {
				mode = 0755
			}
			if err := root.MkdirAll(folder, mode); err != nil {
				return report, err
			}
		}
		if host.goos == "linux" {
			if err := validateLinuxLogFolders(paths, host.euid); err != nil {
				return report, err
			}
			if err := root.Chmod(paths.logsRel, 0700); err != nil {
				return report, err
			}
		}
		if err := saveRecord(); err != nil {
			return report, err
		}
		if err := step("stage-record"); err != nil {
			return report, err
		}
		if err := checkFleetProfileFile(record.StagedProfile); err != nil {
			return report, err
		}
		profile, err = readSetupProfile(record.StagedProfile)
		if err != nil {
			return report, err
		}
		if profile.Version == 0 {
			if len(record.Enrolled) > 0 || record.ProfilePublished {
				return report, errors.New("staged participant profile is missing; restore it before setup")
			}
			profile = connprofile.Profile{Version: connprofile.Version, Fabric: "tsnet", Identities: connprofile.Identities{Node: nodeID}}
			payload, _ := connprofile.Encode(profile)
			if err := write(setupFile{folder: paths.configRel, name: fleetSetupProfileName, payload: payload, mode: 0600}); err != nil {
				return report, err
			}
		}
		if err := step("stage-profile"); err != nil {
			return report, err
		}
		for _, savedRole := range record.Enrolled {
			if _, ok := profile.Participants[savedRole]; !ok {
				return report, errors.New("recorded staged participant is missing; restore its saved profile before setup")
			}
		}

		if role == serviceplan.Join && record.WithPerson {
			if _, saved := profile.Participants[connprofile.ParticipantPerson]; !saved && !request.withPerson {
				// An unsaved optional person must not make machine setup mandatory sign-in.
				// The prior consistency gate already refused a missing recorded participant.
				if record.ProfilePublished {
					return report, errors.New("published person choice cannot be changed; restore its saved profile")
				}
				if _, err := root.Lstat(filepath.Join(paths.configRel, connprofile.FileName)); err == nil {
					return report, errors.New("published person choice cannot be changed; restore its saved profile")
				} else if !errors.Is(err, fs.ErrNotExist) {
					return report, err
				}
				record.WithPerson = false
				if err := saveRecord(); err != nil {
					return report, err
				}
			}
			if record.WithPerson {
				participants = append(participants, setupParticipant{connprofile.ParticipantPerson, "wefty://person/" + nodeID})
			}
		}
		if host.goos == "linux" && record.WithPerson && host.enrolmentOutput != nil {
			if _, err := fmt.Fprintln(host.enrolmentOutput, linuxPersonWarning); err != nil {
				return report, err
			}
		}
		if host.ownerLogin == nil {
			host.ownerLogin = openSetupOwnerLoginURL
			if host.goos == "linux" {
				host.ownerLogin = setupOwnerLoginBrowser("linux", host.browserCommand, host.linuxFiles)
			}
		}
		profile, err = enrolSetupParticipants(ctx, setupEnrolmentRequest{home: paths.home, data: paths.data, profilePath: record.StagedProfile, participants: participants, enroller: host.enroller, input: host.enrolmentInput, output: host.enrolmentOutput, controlURL: record.CoordinatorURL, openLoginURL: host.ownerLogin, deferNotices: true, afterSaved: func(role string) error {
			if !slices.Contains(record.Enrolled, role) {
				record.Enrolled = append(record.Enrolled, role)
			}
			if err := saveRecord(); err != nil {
				return err
			}
			return step("enrolled:" + role)
		}})
		if err != nil {
			_, nodeSaved := profile.Participants[connprofile.ParticipantNodeAgent]
			_, clientSaved := profile.Participants[connprofile.ParticipantClient]
			_, personSaved := profile.Participants[connprofile.ParticipantPerson]
			if role == serviceplan.Join && record.WithPerson && nodeSaved && clientSaved && !personSaved {
				return report, fmt.Errorf("%w; to finish machine setup without person sign-in, rerun setup without --with-person, keeping the join code and explicit coordinator selection", err)
			}
			return report, err
		}
		profile.Identities.Node = nodeID
		if role == serviceplan.ControlPlane {
			profile.AutomaticNodeAdmission = true
			profile.NodeAdmission = []connprofile.NodeAdmission{{NodeID: nodeID, Tags: []string{contract.StableNodeTagPrefix + nodeID}, MaxOneshotSlots: l1.DefaultMaxOneshotSlots, MaxServiceSlots: l1.DefaultMaxServiceSlots}}
		} else {
			profile.L1Address, profile.L3Address = join.L1, join.L3
		}
	}
	if role == serviceplan.Join {
		if profile.L1Address != join.L1 || profile.L3Address != join.L3 {
			return report, errors.New("joining profile differs from its recorded join code; restore the original profile")
		}
		if !committed {
			failure, verifyErr := verifySetupJoinPeers(ctx, host, profile)
			record.PeerVerificationFailure = failure
			if err := saveRecord(); err != nil {
				return report, err
			}
			if err := step("peer-verification"); err != nil {
				return report, err
			}
			if verifyErr != nil {
				return report, verifyErr
			}
		}
	}
	if profile.Identities.Node != record.NodeID {
		return report, errors.New("active profile differs from the recorded stable Node ID; restore it before setup")
	}
	if err := serviceplan.ValidateProfile(role, profile); err != nil {
		return report, err
	}
	for _, part := range participants {
		config, err := fabricconfig.SavedService(profile, part.role, fabricconfig.Config{Mode: profile.Fabric}, nil)
		if err != nil {
			return report, err
		}
		if config.Name != part.name || config.StateDirectory != filepath.Join(paths.data, "fabric", part.role) {
			return report, errors.New("saved participant differs from this installation; setup will not rebind it")
		}
		report.Participants[part.role] = profile.Participants[part.role].NetworkName
		if !committed {
			requestedName, _ := fabric.RequestedNetworkName(part.name)
			if granted := profile.Participants[part.role].NetworkName; granted != requestedName {
				report.Notes = append(report.Notes, fmt.Sprintf("The network gave %s because %s is taken; retire the stale device to free the name.", granted, requestedName))
			}
		}

	}
	services, err := setupRoleServices(paths, profile, digests, loginPATH, role)
	if err != nil {
		return report, err
	}
	plists := make([][]byte, len(services))
	for i, service := range services {
		previous := ""
		if i > 0 {
			previous = services[i-1].Label
		}
		plists[i], err = manager.render(service, previous)
		if err != nil {
			return report, err
		}
	}
	profilePayload, err := connprofile.Encode(profile)
	if err != nil {
		return report, err
	}
	report.L1Address, report.L3Address = profile.L1Address, profile.L3Address
	if role == serviceplan.ControlPlane {
		report.JoinCode, err = fleetjoin.Encode(fleetjoin.Descriptor{L1: profile.L1Address, L3: profile.L3Address, CoordinatorURL: record.CoordinatorURL})
		if err != nil {
			return report, err
		}
	}
	files := append([]setupFile{}, skills...)
	nodePayload, err := ocicontrol.EncodeInstalledConfig(ocicontrol.InstalledConfig{Version: ocicontrol.InstalledConfigVersion, NodeID: nodeID})
	if err != nil {
		return report, err
	}
	files = append(files, setupFile{folder: paths.configRel, name: "node.json", payload: nodePayload, mode: 0600, change: "recorded node " + nodeID})
	for _, name := range binaries {
		payload, err := os.ReadFile(filepath.Join(source, name))
		if err != nil {
			return report, err
		}
		if payloadDigest(payload) != digests[name] {
			return report, errors.New("source binary changed during setup; rerun setup")
		}
		files = append(files, setupFile{folder: paths.binRel, name: name, payload: payload, mode: 0755, change: "installed " + filepath.Join(paths.bin, name)})
	}
	if committed {
		// Repairing binaries or configuration under a running fleet belongs to
		// role-aware upgrade. A completed setup rerun only checks readiness.
		for _, file := range files {
			relative := filepath.Join(file.folder, file.name)
			info, statErr := root.Lstat(relative)
			if statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm() != file.mode.Perm() {
				return report, errors.New("completed fleet installation differs from setup inputs; restore the recorded files")
			}
			current, err := root.ReadFile(relative)
			if err != nil || !bytes.Equal(current, file.payload) {
				return report, errors.New("completed fleet installation differs from setup inputs; restore the recorded files")
			}
		}
	} else {
		if err := refuseUncommittedFleetServices(ctx, host, paths, plan); err != nil {
			return report, err
		}
		if request.beforeWrites != nil {
			if err := request.beforeWrites(); err != nil {
				return report, err
			}
		}
		for _, file := range files {
			if err := write(file); err != nil {
				return report, err
			}
			manifest.Files[filepath.Join(file.folder, file.name)] = payloadDigest(file.payload)
		}
		for i, service := range services {
			manifest.Files[filepath.Join(paths.launchAgentsRel, manager.file(service.Label))] = payloadDigest(plists[i])
		}
		manifest.Files[filepath.Join(paths.configRel, connprofile.FileName)] = payloadDigest(profilePayload)
		version, problem := reportedSetupBinaryVersion(ctx, host, paths.home, source, binaries)
		manifest.InstalledVersion = version
		if problem != "" {
			report.Notes = append(report.Notes, problem)
		}
		// Complete profile first. The receipt is the role's commit point. The
		// durable stage covers a crash between these two atomic replacements.
		if err := write(setupFile{folder: paths.configRel, name: fleetSetupProfileName, payload: profilePayload, mode: 0600}); err != nil {
			return report, err
		}
		if err := write(setupFile{folder: paths.configRel, name: connprofile.FileName, payload: profilePayload, mode: 0600, change: "published complete fleet profile"}); err != nil {
			return report, err
		}
		if err := step("profile"); err != nil {
			return report, err
		}
		record.ProfilePublished = true
		if err := saveRecord(); err != nil {
			return report, err
		}
		payload, err := json.MarshalIndent(manifest, "", "  ")
		if err != nil {
			return report, err
		}
		if err := write(setupFile{folder: paths.configRel, name: setupManifestName, payload: append(payload, '\n'), mode: 0600, change: "committed " + string(role) + " role"}); err != nil {
			return report, err
		}
		if err := step("receipt"); err != nil {
			return report, err
		}
	}
	if !record.Committed {
		record.ProfilePublished, record.Committed = true, true
		if err := saveRecord(); err != nil {
			return report, err
		}
	}
	if err := step("committed"); err != nil {
		return report, err
	}
	if conversion != nil {
		if err := retireCompletedPlainConversion(paths); err != nil {
			return report, err
		}
	}
	if manifest.History != nil {
		report.History, report.JoinResult = manifest.History, "joined"
		report.Notes = append(report.Notes, "History is read-only and never restored (ADR-0003).", "Completed conversion record retired to history; wefty upgrade is available (#901).")
	}
	// Runtime evidence can be deleted independently of the retained install.
	// Recreate its private folder before any service can publish readiness.
	if err := root.MkdirAll(readyFolder, 0700); err != nil {
		return report, err
	}
	if err := root.Chmod(readyFolder, 0700); err != nil {
		return report, err
	}
	folder, err := root.OpenRoot(paths.launchAgentsRel)
	if err != nil {
		return report, err
	}
	defer folder.Close()
	for i, service := range services {
		if request.beforeService != nil {
			if err := request.beforeService(i); err != nil {
				return report, err
			}
		}

		action, err := manager.converge(ctx, folder, paths, service, plists[i])
		if err != nil {
			report.Notes = append(report.Notes, fmt.Sprintf("%s %s activation failed: %v; installation stays published. Repair it and rerun setup.", manager.description(), manager.name(service.Label), err))
			break
		}
		report.Services = append(report.Services, manager.report(paths, service, action))
		if request.afterService != nil {
			if err := request.afterService(i, action); err != nil {
				return report, err
			}
		}
	}
	report.Notes = append(report.Notes, "Retire a machine: disable claims or drain it, then revoke its network identity.", "Make each agent join key single-use with a short expiry, immediately before use. Only the first Node per network identity is admitted automatically.")
	if role == serviceplan.ControlPlane {
		report.Notes = append(report.Notes, "On a fresh joining machine, use wefty setup --role join --join-code CODE with this report's non-secret join code; do not copy the full profile.")
		if record.CoordinatorURL != "" {
			report.Notes = append(report.Notes, "The code only cross-checks the coordinator. Explicitly select the same coordinator with --coordinator-url or TS_CONTROL_URL on the joining machine.")
		}
	} else if record.WithPerson {
		report.Notes = append(report.Notes, "Owner person sign-in is configured for this laptop only.")
	} else {
		report.Notes = append(report.Notes, "No person participant was enrolled. Select --with-person only on the owner's laptop during first setup.")
	}
	if role == serviceplan.ControlPlane {
		policy, err := fabricconfig.PrivateNetworkPolicy(profile.Fabric)
		if err != nil {
			return report, err
		}
		report.PrivateNetworkPolicy = &policy
	}
	if host.goos == "linux" {
		doctorLinuxLingering(ctx, host, func(_, _, detail, fix string) {
			note := "Linux logout survival: " + detail
			if fix != "" {
				note += "; " + fix
			}
			report.Notes = append(report.Notes, note)
		})
	}
	report.Readiness = checkFleetReadiness(ctx, host, paths, profile, services, request.skipSmokeRun, role)
	for _, check := range report.Readiness {
		if check.Status == "FAILED" {
			return report, &notReadyError{verdict: "fleet setup is published but not ready; fix the failed checks and rerun setup"}
		}
	}
	return report, nil
}

func checkFleetProfileFile(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return errors.New("fleet participant profile must be an unlinked private file; restore it before setup")
	}
	return nil
}

// Before commit no service or authority database can belong to this staged
// installation. Refuse old SQLite sidecars as well as databases: opening a new
// database next to old recovery material must never restore stale authority.
func refuseUncommittedFleetServices(ctx context.Context, host setupHost, paths setupPaths, plan serviceplan.Plan) error {
	for _, database := range []string{"l1.sqlite", "l3.sqlite"} {
		for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
			if _, err := os.Lstat(filepath.Join(paths.data, database+suffix)); err == nil {
				return errors.New("fresh control-plane setup refuses existing authority data; plain conversion is a later slice")
			} else if !errors.Is(err, fs.ErrNotExist) {
				return err
			}
		}
	}
	// A fresh join must also refuse leftover local control-plane services.
	// Inspect the complete fleet namespace without rendering or starting it.
	if plan.Role == serviceplan.Join {
		var err error
		plan, err = setupRolePlan(paths, serviceplan.ControlPlane)
		if err != nil {
			return err
		}
	}
	manager := fleetServiceManager{host: host}
	for _, service := range plan.Services {
		label := "dev.wefty." + service.Name
		if _, err := os.Lstat(filepath.Join(paths.launchAgents, manager.file(label))); err == nil {
			return fmt.Errorf("fresh fleet setup refuses existing %s definitions; plain conversion is a later slice", manager.description())
		} else if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		probe, cancel := context.WithTimeout(ctx, statusProbeBudget)
		state, err := manager.state(probe, label)
		cancel()
		if err != nil {
			return fmt.Errorf("cannot verify existing %s %s before fleet setup: %w", manager.description(), manager.name(label), err)
		}
		if state.Loaded {
			return fmt.Errorf("fresh fleet setup refuses existing loaded %ss; plain conversion is a later slice", manager.description())
		}
	}
	return nil
}
