package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/local"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/durable"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
	"github.com/Derek-X-Wang/wefty/skills"
)

// `wefty setup` installs the daily stack on this Mac: L1, the run ledger and
// a node agent as per-user LaunchAgents in plain network mode, the one
// connection profile every wefty program reads, the node configuration that
// records which node is this machine, the binaries in ~/.local/bin, and the
// wefty skill for Claude Code and Codex. It needs no sudo, and running it
// again changes only what is wrong.
//
// The binaries it installs are the ones next to the `wefty` that runs it (or
// in --from DIR): build all four into one folder, then run that folder's
// `wefty setup`. Upgrades own rebuilding; setup only installs what it is given.

const (
	setupLockProbeFile  = ".setup-lock"
	setupL1Label        = "dev.wefty.l1"
	setupL3Label        = "dev.wefty.l3"
	setupNodeAgentLabel = "dev.wefty.node-agent"
	// The installed stack's stable loopback ports. They sit below macOS's
	// ephemeral range (49152 and up), so an outgoing connection never holds
	// them, and away from the quickstart's 42101/42102.
	defaultSetupL1Address = "127.0.0.1:47101"
	defaultSetupL3Address = "127.0.0.1:47102"
	// L1 and L3 run no jobs, so they get launchd's ordinary minimal PATH.
	setupServicePATH = "/usr/bin:/bin:/usr/sbin:/sbin"
	// The installed node agent's shutdown. On launchd's SIGTERM it drains and
	// waits for resident attempts for setupNodeAgentShutdownBudget, then
	// cancels them in order, which agent.ForcedShutdownBound bounds.
	setupNodeAgentShutdownBudget = 15 * time.Second
	// setupNodeAgentExitSlack is for process exit and scheduling, never for
	// another shutdown phase.
	setupNodeAgentExitSlack = 10 * time.Second
	setupLoginShellBudget   = 20 * time.Second
	// setupVersionProbeBudget bounds asking the four binaries for their version,
	// and setupVersionOutputLimit bounds one answer: a version is one short word.
	setupVersionProbeBudget = 10 * time.Second
	setupVersionOutputLimit = 256
	setupPATHBegin          = "__WEFTY_LOGIN_PATH_BEGIN__"
	setupPATHEnd            = "__WEFTY_LOGIN_PATH_END__"
)

// setupNodeAgentExitTimeout is launchd's ExitTimeOut for the node agent, in
// seconds. It is derived from the agent's own shutdown phases, so a phase that
// grows moves it too, and launchd's SIGKILL never cuts the orderly path short.
var setupNodeAgentExitTimeout = int((setupNodeAgentShutdownBudget + agent.ForcedShutdownBound(agent.ShutdownTimeouts{}) +
	setupNodeAgentExitSlack + time.Second - 1) / time.Second)

// setupBinaries are installed together; the services run the last three.
var setupBinaries = []string{"wefty", "wefty-l1", "wefty-l3", "wefty-agent"}

// setupHost is everything setup reads from or does to the machine, so a test
// can run it against a temporary home with fake launchctl and a fake shell.
type setupHost struct {
	bootTime        func() (time.Time, error)
	peerLookup      func(context.Context, fabricconfig.Config) (fabric.PeerLookup, fabricconfig.CloseFunc, error)
	ownerLogin      func(context.Context, string) error
	enroller        fabric.Enroller
	enrolmentInput  io.Reader
	enrolmentOutput io.Writer
	fleetClients    func(context.Context, setupPaths, connprofile.Profile) (*apiClients, error)
	readinessBudget time.Duration
	smokeBudget     time.Duration
	goos            string
	euid            int
	uid             int
	home            string
	username        string
	hostname        string
	shell           string
	executable      string
	launchctl       launchagent.Runner
	loginShell      func(ctx context.Context, shell string, args ...string) ([]byte, error)
	random          io.Reader
	// l1Address and l3Address are the ports a new profile gets.
	l1Address, l3Address string
	unloadBound          time.Duration
	// versionProbeBudget shortens setupVersionProbeBudget in tests; zero uses it.
	versionProbeBudget time.Duration
	upgrade            *upgradeHost
}

type setupRequest struct {
	joinCode            string
	withPerson          bool
	role                string
	coordinatorURL      string
	fleetCoordinatorURL string
	skipSmokeRun        bool
	afterFleetStep      func(string) error
	from                string
	nodeID              string
	installedVersion    string
	skillSource         fs.FS
	beforeWrites        func() error
	beforeService       func(int) error
	serviceContext      func(int) context.Context
	afterService        func(int, launchagent.Action) error
	lockHeld            bool
	wroteFile           func(string, []byte)
	deferNodeBinary     bool
	deferSkills         bool
}

// setupPaths are the standard locations under one owner's home. Every
// folder is spelled without symbolic links (see canonicalHomeFolder), and
// each rel field is the same folder relative to home, for writes through
// the home folder's confined handle.
type setupPaths struct {
	home, bin, config, profile, nodeConfig, data, nodeData, logs, launchAgents string
	binRel, configRel, dataRel, logsRel, launchAgentsRel                       string
	nodeDataFolders                                                            []string
	skillTargets                                                               []string
}

// setupFolders are the folders setup writes into, relative to home.
var setupFolders = struct {
	bin, config, data, logs, launchAgents string
	skills                                []string
}{
	bin: filepath.Join(".local", "bin"), config: filepath.Join(".config", "wefty"),
	data: filepath.Join("Library", "Application Support", "wefty"), logs: filepath.Join("Library", "Logs", "wefty"),
	launchAgents: filepath.Join("Library", "LaunchAgents"),
	skills:       []string{filepath.Join(".claude", "skills", "wefty"), filepath.Join(".agents", "skills", "wefty")},
}

// resolveSetupPaths spells every destination folder from the home folder's
// real path. It is the first check of a run and writes nothing.
func resolveSetupPaths(home string) (setupPaths, error) {
	paths, issues := resolveMaintenancePaths(home)
	if len(issues) != 0 {
		return setupPaths{}, issues[0].err
	}
	return paths, nil
}

type setupPathIssue struct {
	folder string
	err    error
}

// Maintenance resolves each path independently. Unusable skill roots are
// excluded so neither diagnosis nor removal can follow a link into a checkout.
// Other unusable paths stay named for findings but must not be accessed.
func resolveMaintenancePaths(home string) (setupPaths, []setupPathIssue) {
	paths := setupPaths{home: home}
	var issues []setupPathIssue
	resolve := func(wanted string) string {
		rel, err := canonicalHomeFolder(home, wanted)
		if err != nil {
			issues = append(issues, setupPathIssue{wanted, err})
			return wanted
		}
		return rel
	}
	for _, folder := range []struct {
		rel    *string
		wanted string
	}{
		{&paths.binRel, setupFolders.bin}, {&paths.configRel, setupFolders.config}, {&paths.dataRel, setupFolders.data},
		{&paths.logsRel, setupFolders.logs}, {&paths.launchAgentsRel, setupFolders.launchAgents},
	} {
		*folder.rel = resolve(folder.wanted)
	}
	paths.bin, paths.config = filepath.Join(home, paths.binRel), filepath.Join(home, paths.configRel)
	paths.data, paths.logs = filepath.Join(home, paths.dataRel), filepath.Join(home, paths.logsRel)
	paths.launchAgents = filepath.Join(home, paths.launchAgentsRel)
	paths.profile = filepath.Join(paths.config, connprofile.FileName)
	paths.nodeConfig = filepath.Join(paths.config, "node.json")
	// The node agent refuses a state root reached through a link, so its
	// folders are resolved on their own rather than assumed to sit under data.
	for _, wanted := range []string{"log-spool", "managed"} {
		paths.nodeDataFolders = append(paths.nodeDataFolders, resolve(filepath.Join(setupFolders.data, "node-agent", wanted)))
	}
	paths.nodeData = filepath.Join(home, filepath.Dir(paths.nodeDataFolders[0]))
	for _, wanted := range setupFolders.skills {
		// A link here could point into a checkout, even one inside home, and
		// setup would then write the skill into that checkout.
		if info, err := os.Lstat(filepath.Join(home, wanted)); err == nil && info.Mode()&os.ModeSymlink != 0 {
			issues = append(issues, setupPathIssue{wanted, fmt.Errorf("%s is a symbolic link; setup installs a copy of the skill there, so remove the link and run setup again", filepath.Join(home, wanted))})
			continue
		}
		rel, err := canonicalHomeFolder(home, wanted)
		if err != nil {
			issues = append(issues, setupPathIssue{wanted, err})
			continue
		}
		paths.skillTargets = append(paths.skillTargets, rel)
	}
	return paths, issues
}

// canonicalHomeFolder spells wanted (relative to home) without symbolic
// links. A link that stays inside home is followed; one that leads outside
// is refused, so no write can leave the owner's home folder. Missing folders
// are kept as named: setup creates them.
func canonicalHomeFolder(home, wanted string) (string, error) {
	existing := filepath.Join(home, wanted)
	var missing []string
	for existing != home {
		_, err := os.Lstat(existing)
		if err == nil {
			break
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("inspect %s: %w", existing, err)
		}
		missing = append([]string{filepath.Base(existing)}, missing...)
		existing = filepath.Dir(existing)
	}
	resolved, err := filepath.EvalSymlinks(existing)
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", existing, err)
	}
	inside, err := filepath.Rel(home, filepath.Join(append([]string{resolved}, missing...)...))
	if err != nil || inside == "." || !filepath.IsLocal(inside) {
		return "", fmt.Errorf("%s leads outside the home folder %s through a symbolic link; setup writes only inside the home folder",
			filepath.Join(home, wanted), home)
	}
	return inside, nil
}

// setupFile is one file setup converges, relative to home.
type setupFile struct {
	folder, name string
	payload      []byte
	mode         os.FileMode
	change       string
}

type setupServiceReport struct {
	Label  string             `json:"label"`
	Plist  string             `json:"plist"`
	Log    string             `json:"log"`
	Action launchagent.Action `json:"action"`
}

type setupReport struct {
	Role         serviceplan.Role      `json:"role,omitempty"`
	Participants map[string]string     `json:"participants,omitempty"`
	Readiness    []setupReadinessCheck `json:"readiness,omitempty"`
	NodeID       string                `json:"node_id"`
	Profile      string                `json:"profile"`
	NodeConfig   string                `json:"node_config"`
	L1Address    string                `json:"l1_address"`
	L3Address    string                `json:"l3_address"`
	BinDir       string                `json:"bin_dir"`
	DataDir      string                `json:"data_dir"`
	LogDir       string                `json:"log_dir"`
	Changes      []string              `json:"changes"`
	Services     []setupServiceReport  `json:"services"`
	Notes        []string              `json:"notes,omitempty"`
	JoinCode     string                `json:"join_code,omitempty"`
}

func executeSetup(ctx context.Context, options globalOptions, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("setup", flag.ContinueOnError)
	flags.SetOutput(flagErrorOutput(stderr, options.jsonOutput))
	var request setupRequest
	var remove, purge bool
	flags.StringVar(&request.role, "role", "", "fleet role for a new install: control-plane or join")
	flags.StringVar(&request.joinCode, "join-code", "", "non-secret join code from the control plane setup report")
	flags.BoolVar(&request.withPerson, "with-person", false, "enrol the owner through browser sign-in on the owner laptop only")
	flags.BoolVar(&request.skipSmokeRun, "skip-smoke-run", false, "skip the bounded pinned fleet smoke run")
	flags.StringVar(&request.coordinatorURL, "coordinator-url", "", "non-default HTTPS enrolment coordinator (no credentials)")
	flags.BoolVar(&remove, "remove", false, "remove the per-user install, keeping data and logs")
	flags.BoolVar(&purge, "purge", false, "also delete data and logs (requires --remove)")
	flags.StringVar(&request.from, "from", "", "folder holding wefty, wefty-l1, wefty-l3 and wefty-agent (default: the folder of this wefty)")
	flags.StringVar(&request.nodeID, "node-id", "", "stable node ID for a first install (default: chosen once from the host name and kept)")
	if err := parseCommandFlags(flags, args, false); err != nil {
		return flagUsageError(err)
	}
	if flags.NArg() != 0 {
		return usageError("usage: wefty setup [--role control-plane] [--from DIR] [--node-id ID] | --role join --join-code CODE [--with-person] | --remove [--purge]")
	}
	var fleetFlag, joinFlag bool
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "join-code" || f.Name == "with-person" {
			joinFlag = true
		}
		if f.Name == "role" || f.Name == "coordinator-url" || f.Name == "skip-smoke-run" {
			fleetFlag = true
		}
	})
	if remove && joinFlag {
		return usageError("--remove cannot use --join-code or --with-person")
	}
	if remove && fleetFlag {
		return usageError("--remove cannot use --role, --coordinator-url or --skip-smoke-run")
	}
	if purge && !remove || remove && (request.from != "" || request.nodeID != "") {
		return usageError("--purge requires --remove; --remove cannot use --from or --node-id")
	}
	host, err := setupCommandHost()
	if err != nil {
		return err
	}
	if remove {
		report, err := runSetupRemove(ctx, host, purge)
		if err != nil {
			return err
		}
		if options.jsonOutput {
			if err := writeJSON(stdout, report); err != nil {
				return err
			}
		} else {
			for _, change := range report.Changes {
				if _, err := fmt.Fprintln(stdout, "Removed:", change); err != nil {
					return err
				}
			}
			for _, warning := range report.Warnings {
				if _, err := fmt.Fprintln(stdout, "Warning:", warning); err != nil {
					return err
				}
			}
		}
		if len(report.Warnings) != 0 {
			return &notReadyError{verdict: "setup removal left files behind; inspect the warnings"}
		}
		return nil
	}
	host.enrolmentOutput = stderr
	request.fleetCoordinatorURL = options.controlURL
	if request.fleetCoordinatorURL == "" {
		request.fleetCoordinatorURL = os.Getenv("TS_CONTROL_URL")
	}
	report, err := runSetup(ctx, host, request)
	if err != nil && len(report.Readiness) == 0 {
		return err
	}
	var writeErr error
	if options.jsonOutput {
		writeErr = writeJSON(stdout, report)
	} else {
		writeErr = writeSetupReport(stdout, report)
	}
	if writeErr != nil {
		return writeErr
	}
	return err
}

func realSetupHost() (setupHost, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return setupHost{}, fmt.Errorf("locate home folder: %w", err)
	}
	executable, err := os.Executable()
	if err != nil {
		return setupHost{}, fmt.Errorf("locate the running wefty: %w", err)
	}
	username := os.Getenv("USER")
	if username == "" {
		if current, err := user.Current(); err == nil {
			username = current.Username
		}
	}
	hostname, _ := os.Hostname()
	return setupHost{
		goos: runtime.GOOS, euid: os.Geteuid(), uid: os.Getuid(), home: home, username: username,
		hostname: hostname, shell: os.Getenv("SHELL"), executable: executable,
		launchctl: launchagent.ExecRunner, loginShell: runLoginShell, random: rand.Reader,
		l1Address: defaultSetupL1Address, l3Address: defaultSetupL3Address,
	}, nil
}

// runLoginShell returns the shell's standard output only: an interactive
// login shell may print anything on standard error.
func runLoginShell(ctx context.Context, shell string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, shell, args...)
	command.WaitDelay = time.Second
	return command.Output()
}

func runSetup(ctx context.Context, host setupHost, request setupRequest) (setupReport, error) {
	// Refuse root before anything is read or written: setup installs login
	// services for the account that runs it, and a root-owned copy of any of
	// its files would block every later run as the owner.
	if host.euid == 0 {
		return setupReport{}, errors.New("wefty setup must not run as root: it installs per-user login services for the account that runs it and never needs sudo; run it as yourself")
	}
	if host.goos != "darwin" {
		return setupReport{}, fmt.Errorf("wefty setup installs macOS login services; this machine runs %s", host.goos)
	}
	if !filepath.IsAbs(host.home) {
		return setupReport{}, errors.New("wefty setup needs an absolute home folder")
	}
	// The node agent refuses a state root reached through a symbolic link,
	// so every installed path is spelled from the home folder's real path.
	home, err := filepath.EvalSymlinks(host.home)
	if err != nil {
		return setupReport{}, fmt.Errorf("resolve home folder: %w", err)
	}
	paths, err := resolveSetupPaths(home)
	if err != nil {
		return setupReport{}, err
	}
	role, err := selectSetupRole(paths, request.role)
	if err != nil {
		return setupReport{}, err
	}
	if role != serviceplan.Legacy {
		return runFleetSetup(ctx, host, request, paths, role)
	}
	if request.joinCode != "" || request.withPerson {
		return setupReport{}, usageError("--join-code and --with-person require --role join")
	}
	if request.coordinatorURL != "" || request.skipSmokeRun {
		return setupReport{}, usageError("--coordinator-url and --skip-smoke-run require a fleet role")
	}
	if !request.lockHeld {
		if _, err := os.Stat(paths.data); err == nil {
			unlock, err := lockSetup(paths, true)
			if err != nil {
				return setupReport{}, err
			}
			defer unlock()
			request.lockHeld = true
		} else if !errors.Is(err, fs.ErrNotExist) {
			return setupReport{}, err
		}
	}
	report := setupReport{
		Profile: paths.profile, NodeConfig: paths.nodeConfig,
		BinDir: paths.bin, DataDir: paths.data, LogDir: paths.logs,
	}

	// Everything that can refuse is checked, and every output is prepared,
	// before the first write: a refusal changes no file and no service.
	sourceDirectory, err := setupSourceDirectory(host, request)
	if err != nil {
		return report, err
	}
	sourceDigests := make(map[string]string, len(setupBinaries))
	for _, name := range setupBinaries {
		digest, err := executableDigest(filepath.Join(sourceDirectory, name))
		if err != nil {
			return report, fmt.Errorf("setup installs the binaries next to this wefty (or in --from DIR); build all four into one folder first: %w", err)
		}
		sourceDigests[name] = digest
	}
	existingProfile, err := readSetupProfile(paths.profile)
	if err != nil {
		return report, err
	}
	nodeID, err := chooseSetupNodeID(host, request, paths, existingProfile)
	if err != nil {
		return report, err
	}
	profile, err := desiredSetupProfile(host, existingProfile, nodeID)
	if err != nil {
		return report, err
	}
	loginPATH, err := captureLoginPATH(ctx, host)
	if err != nil {
		return report, err
	}
	nodeAgentPATH := loginPATH
	if !slices.Contains(filepath.SplitList(loginPATH), paths.bin) {
		nodeAgentPATH = loginPATH + string(filepath.ListSeparator) + paths.bin
		report.Notes = append(report.Notes, paths.bin+" is not on your login-shell PATH; add it so a new shell finds `wefty` (the node agent already gets it)")
	}
	report.NodeID, report.L1Address, report.L3Address = nodeID, profile.L1Address, profile.L3Address

	profilePayload, err := connprofile.Encode(profile)
	if err != nil {
		return report, err
	}
	nodeConfigPayload, err := ocicontrol.EncodeInstalledConfig(ocicontrol.InstalledConfig{Version: ocicontrol.InstalledConfigVersion, NodeID: nodeID})
	if err != nil {
		return report, err
	}
	files := []setupFile{
		{folder: paths.configRel, name: connprofile.FileName, payload: profilePayload, mode: 0o600, change: "wrote connection profile " + paths.profile},
		{folder: paths.configRel, name: "node.json", payload: nodeConfigPayload, mode: 0o600, change: "recorded node " + nodeID + " in " + paths.nodeConfig},
	}
	skillFiles, err := setupSkillFilesFrom(paths, request.skillSource)
	if err != nil {
		return report, err
	}
	files = append(files, skillFiles...)
	services, err := setupServices(paths, profile, sourceDigests, nodeAgentPATH)
	if err != nil {
		return report, err
	}
	plists := make([][]byte, len(services))
	for index, service := range services {
		if plists[index], err = launchagent.Render(service); err != nil {
			return report, err
		}
	}
	homeRoot, err := os.OpenRoot(home)
	if err != nil {
		return report, err
	}
	defer homeRoot.Close()
	folders := append([]string{paths.binRel, paths.configRel, paths.dataRel, paths.logsRel, paths.launchAgentsRel}, paths.nodeDataFolders...)
	for _, file := range files {
		folders = append(folders, file.folder)
	}
	destinations := []string{filepath.Join(paths.configRel, setupManifestName)}
	for _, name := range setupBinaries {
		destinations = append(destinations, filepath.Join(paths.binRel, name))
	}
	for _, file := range files {
		destinations = append(destinations, filepath.Join(file.folder, file.name))
	}
	for _, service := range services {
		destinations = append(destinations, filepath.Join(paths.launchAgentsRel, service.Label+".plist"))
	}
	if err := checkSetupDestinations(homeRoot, home, folders, destinations); err != nil {
		return report, err
	}

	// A first install validates without creating state folders. Once validation
	// succeeds, acquire the state lock and re-read under it: another first install
	// may have completed while this read-only preparation was running.
	if !request.lockHeld {
		unlock, err := lockSetup(paths, true)
		if err != nil {
			return report, err
		}
		defer unlock()
		request.lockHeld = true
		// Reuse the validated PATH; opening an interactive login shell twice
		// can repeat owner shell side effects. Identity and files are re-read.
		host.loginShell = func(context.Context, string, ...string) ([]byte, error) {
			return []byte(setupPATHBegin + "\n" + loginPATH + "\n" + setupPATHEnd + "\n"), nil
		}
		return runSetup(ctx, host, request)
	}
	// Upgrade passes the commit it already checked every staged binary for.
	// Setup asks the binaries it was given, so the receipt holds only what
	// they report.
	reportedVersion, versionProblem := "", ""
	if request.installedVersion == "" {
		reportedVersion, versionProblem = reportedSetupVersion(ctx, host, home, sourceDirectory)
	}
	if request.beforeWrites != nil {
		if err := request.beforeWrites(); err != nil {
			return report, err
		}
	}
	// Writes start here, all through the confined home handle.
	for _, folder := range folders {
		mode := os.FileMode(0o700)
		if folder == paths.binRel || folder == paths.launchAgentsRel || slices.Contains(paths.skillTargets, folder) {
			mode = 0o755
		}
		if err := homeRoot.MkdirAll(folder, mode); err != nil {
			return report, fmt.Errorf("create %s: %w", filepath.Join(home, folder), err)
		}
	}
	for _, name := range setupBinaries {
		payload, err := os.ReadFile(filepath.Join(sourceDirectory, name))
		if err != nil {
			return report, err
		}
		if digest := sha256.Sum256(payload); "sha256:"+hex.EncodeToString(digest[:]) != sourceDigests[name] {
			return report, fmt.Errorf("%s changed while setup was reading it; run setup again", filepath.Join(sourceDirectory, name))
		}
		// The rename gives a running service's old binary its own inode, so
		// the service keeps running until setup reloads it below.
		files = append([]setupFile{{folder: paths.binRel, name: name, payload: payload, mode: 0o755,
			change: "installed " + filepath.Join(paths.bin, name)}}, files...)
	}
	previous := setupManifest{Version: 1, Files: map[string]string{}}
	manifest := setupManifest{Version: 1, InstalledVersion: request.installedVersion, UpgradeSource: request.installedVersion != "", Files: map[string]string{}}
	if payload, err := homeRoot.ReadFile(filepath.Join(paths.configRel, setupManifestName)); err == nil {
		if json.Unmarshal(payload, &previous) == nil && previous.Version == 1 {
			manifest.Role = previous.Role
			if request.installedVersion == "" {
				manifest.UpgradeSource = previous.UpgradeSource
				unchanged := true
				for _, name := range setupBinaries {
					if sourceDigests[name] != previous.Files[filepath.Join(paths.binRel, name)] {
						unchanged = false
					}
				}
				if unchanged {
					manifest.InstalledVersion = previous.InstalledVersion
				}
			}
			for name, digest := range previous.Files {
				for _, target := range paths.skillTargets {
					rel, err := filepath.Rel(target, name)
					if err == nil && rel != "." && filepath.IsLocal(rel) {
						manifest.Files[name] = digest
					}
				}
			}
		}
	}
	// What the binaries report wins. Without a report, the previous receipt's
	// version stands only for byte-identical binaries.
	if reportedVersion != "" {
		manifest.InstalledVersion = reportedVersion
	} else if versionProblem != "" && manifest.InstalledVersion == "" {
		report.Notes = append(report.Notes, versionProblem)
	}
	if request.deferSkills {
		unownedWarning := false
		files = slices.DeleteFunc(files, func(file setupFile) bool {
			name := filepath.Join(file.folder, file.name)
			if !setupSkillPath(paths, name) {
				return false
			}
			payload, err := homeRoot.ReadFile(name)
			// Never claim or overwrite an owner edit, including a previously unowned copy.
			if err == nil && payloadDigest(payload) != previous.Files[name] {
				if previous.Files[name] == "" {
					if !unownedWarning {
						report.Notes = append(report.Notes, "left existing skill copies without ownership evidence; run wefty setup to refresh the skill copies")
						unownedWarning = true
					}
				} else {
					report.Notes = append(report.Notes, "left owner-edited skill copy "+filepath.Join(home, name))
				}
				return true
			}
			return false
		})
	}
	for _, file := range files {
		manifest.Files[filepath.Join(file.folder, file.name)] = payloadDigest(file.payload)
	}
	for index, service := range services {
		manifest.Files[filepath.Join(paths.launchAgentsRel, service.Label+".plist")] = payloadDigest(plists[index])
	}
	manifestPayload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return report, err
	}
	files = append(files, setupFile{folder: paths.configRel, name: setupManifestName, payload: append(manifestPayload, '\n'), mode: 0o600, change: ""})
	// Keep the previous receipt until activation and deferred publication finish.
	// A partial receipt can lose ownership entries and disable legacy removal.
	var completedReceipt *setupFile
	if request.installedVersion != "" {
		file := files[len(files)-1]
		completedReceipt = &file
		files = files[:len(files)-1]
	}
	var deferredNodeFile *setupFile
	var deferredSkills []setupFile
	for _, file := range files {
		if request.deferSkills {
			skill := false
			for _, target := range paths.skillTargets {
				rel, err := filepath.Rel(target, filepath.Join(file.folder, file.name))
				if err == nil && rel != "." && filepath.IsLocal(rel) {
					skill = true
				}
			}
			if skill {
				deferredSkills = append(deferredSkills, file)
				continue
			}
		}
		if request.deferNodeBinary && file.folder == paths.binRel && file.name == "wefty-agent" {
			copy := file
			deferredNodeFile = &copy
			continue
		}
		changed, err := convergeHomeFile(homeRoot, file)
		if err != nil {
			return report, fmt.Errorf("write %s: %w", filepath.Join(home, file.folder, file.name), err)
		}
		if changed && request.wroteFile != nil {
			request.wroteFile(filepath.Join(file.folder, file.name), file.payload)
		}
		if changed && file.change != "" && !slices.Contains(report.Changes, file.change) {
			report.Changes = append(report.Changes, file.change)
		}
	}
	launchAgents, err := homeRoot.OpenRoot(paths.launchAgentsRel)
	if err != nil {
		return report, err
	}
	defer launchAgents.Close()
	for index, service := range services {
		if request.beforeService != nil {
			if err := request.beforeService(index); err != nil {
				return report, err
			}
		}
		serviceCtx := ctx
		if request.serviceContext != nil {
			serviceCtx = request.serviceContext(index)
		}
		if index == 2 && deferredNodeFile != nil {
			changed, err := convergeHomeFile(homeRoot, *deferredNodeFile)
			if err != nil {
				return report, err
			}
			if changed && request.wroteFile != nil {
				request.wroteFile(filepath.Join(deferredNodeFile.folder, deferredNodeFile.name), deferredNodeFile.payload)
			}
		}
		manager := launchagent.Manager{Run: host.launchctl, UID: host.uid, UnloadBound: host.unloadBound}
		if manager.UnloadBound == 0 && service.ExitTimeOut > 0 {
			manager.UnloadBound = time.Duration(service.ExitTimeOut+15) * time.Second
		}
		plist := filepath.Join(paths.launchAgents, service.Label+".plist")
		action, err := manager.Converge(serviceCtx, launchAgents, plist, service.Label, plists[index])
		if request.wroteFile != nil && action != launchagent.Unchanged {
			// Converge can publish its plist before bootstrap fails. Record only bytes
			// that are actually present, never an unrelated owner modification.
			if payload, readErr := launchAgents.ReadFile(service.Label + ".plist"); readErr == nil && bytes.Equal(payload, plists[index]) {
				request.wroteFile(filepath.Join(paths.launchAgentsRel, service.Label+".plist"), payload)
			}
		}
		if err != nil {
			return report, err
		}
		if request.afterService != nil {
			if err := request.afterService(index, action); err != nil {
				return report, err
			}
		}
		report.Services = append(report.Services, setupServiceReport{
			Label: service.Label, Plist: plist, Log: service.LogPath, Action: action,
		})
	}
	for _, file := range deferredSkills {
		changed, err := convergeHomeFile(homeRoot, file)
		if err != nil {
			return report, err
		}
		if changed && request.wroteFile != nil {
			request.wroteFile(filepath.Join(file.folder, file.name), file.payload)
		}
		if changed && !slices.Contains(report.Changes, file.change) {
			report.Changes = append(report.Changes, file.change)
		}
	}
	if completedReceipt != nil {
		if _, err := convergeHomeFile(homeRoot, *completedReceipt); err != nil {
			return report, err
		}
	}
	if report.Changes == nil {
		report.Changes = []string{}
	}
	return report, nil
}

// checkSetupDestinations refuses, before any write, a destination folder that
// is not a folder or a destination file that is a folder or another
// non-file, and lets the confined handle refuse any path that would leave
// home. A file that is a link is replaced, never followed.
func setupSkillPath(paths setupPaths, name string) bool {
	for _, target := range paths.skillTargets {
		rel, err := filepath.Rel(target, name)
		if err == nil && rel != "." && filepath.IsLocal(rel) {
			return true
		}
	}
	return false
}

func checkSetupDestinations(homeRoot *os.Root, home string, folders, files []string) error {
	for _, folder := range folders {
		for ancestor := folder; ancestor != "."; ancestor = filepath.Dir(ancestor) {
			info, err := homeRoot.Stat(ancestor)
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return fmt.Errorf("inspect %s: %w", filepath.Join(home, ancestor), err)
			}
			if !info.IsDir() {
				return fmt.Errorf("%s is not a folder; setup needs a folder there, so move it aside and run setup again", filepath.Join(home, ancestor))
			}
		}
	}
	for _, file := range files {
		info, err := homeRoot.Lstat(file)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect %s: %w", filepath.Join(home, file), err)
		}
		if !info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 {
			return fmt.Errorf("%s is not a file; setup needs to write a file there, so move it aside and run setup again", filepath.Join(home, file))
		}
	}
	return nil
}

// convergeHomeFile publishes a file unless a regular file with exactly its
// bytes and permission bits is already there, so a lost executable bit or a
// link standing in for the file is repaired.
func convergeHomeFile(homeRoot *os.Root, file setupFile) (bool, error) {
	folder, err := homeRoot.OpenRoot(file.folder)
	if err != nil {
		return false, err
	}
	defer folder.Close()
	if durable.Holds(folder, file.name, file.payload, file.mode) {
		return false, nil
	}
	return true, durable.WriteFileMode(folder, file.name, file.payload, file.mode)
}

func setupSourceDirectory(host setupHost, request setupRequest) (string, error) {
	if request.from != "" {
		directory, err := filepath.Abs(request.from)
		if err != nil {
			return "", err
		}
		return directory, nil
	}
	executable, err := filepath.EvalSymlinks(host.executable)
	if err != nil {
		return "", fmt.Errorf("locate the running wefty: %w", err)
	}
	return filepath.Dir(executable), nil
}

func executableDigest(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o100 == 0 {
		return "", fmt.Errorf("%s is not an executable file", path)
	}
	return fileDigest(path)
}

func fileDigest(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func readSetupProfile(path string) (connprofile.Profile, error) {
	profile, err := connprofile.Read(path)
	if errors.Is(err, os.ErrNotExist) {
		return connprofile.Profile{}, nil
	}
	if err != nil {
		return connprofile.Profile{}, fmt.Errorf("%w; fix it, or move it aside to let setup write a new one", err)
	}
	return profile, nil
}

// chooseSetupNodeID keeps the node ID this machine already has: the node
// configuration records it, and the profile repeats it. A new one is chosen
// only for a first install.
func chooseSetupNodeID(host setupHost, request setupRequest, paths setupPaths, profile connprofile.Profile) (string, error) {
	existing := ""
	config, err := ocicontrol.ReadInstalledConfig(paths.nodeConfig)
	switch {
	case err == nil:
		if config.ControlSocket != "" {
			return "", fmt.Errorf("%s belongs to an OCI node installation (control socket %s); wefty setup installs a plain-process node and does not replace it",
				paths.nodeConfig, config.ControlSocket)
		}
		existing = config.NodeID
	case errors.Is(err, os.ErrNotExist):
		existing = profile.Identities.Node
	default:
		return "", fmt.Errorf("%w; fix it, or move it aside to let setup record this machine's node again", err)
	}
	if request.nodeID != "" {
		if !validSetupNodeID(request.nodeID) {
			return "", usageError("--node-id must be 1-63 lowercase letters, digits, '.', '_' or '-', starting with a letter or digit")
		}
		if existing != "" && existing != request.nodeID {
			return "", fmt.Errorf("this machine is already node %q; setup keeps a node ID once chosen, so --node-id %q is refused", existing, request.nodeID)
		}
		return request.nodeID, nil
	}
	if existing != "" {
		return existing, nil
	}
	return newSetupNodeID(host.hostname, host.random)
}

func validSetupNodeID(nodeID string) bool {
	return len(nodeID) <= 63 && strings.ToLower(nodeID) == nodeID && l1.ValidTag(nodeID)
}

// newSetupNodeID names the node after the host, readable in `wefty status`,
// with a random suffix so two machines with one host name never collide. It
// is chosen once: renaming the host later does not change it.
func newSetupNodeID(hostname string, random io.Reader) (string, error) {
	base := strings.ToLower(strings.SplitN(hostname, ".", 2)[0])
	var builder strings.Builder
	for _, value := range base {
		if builder.Len() >= 32 {
			break
		}
		switch {
		case value >= 'a' && value <= 'z' || value >= '0' && value <= '9':
			builder.WriteRune(value)
		case builder.Len() > 0 && !strings.HasSuffix(builder.String(), "-"):
			builder.WriteByte('-')
		}
	}
	name := strings.Trim(builder.String(), "-")
	if name == "" {
		name = "mac"
	}
	suffix, err := randomHex(random, 4)
	if err != nil {
		return "", err
	}
	return name + "-" + suffix, nil
}

func randomHex(random io.Reader, size int) (string, error) {
	value := make([]byte, size)
	if _, err := io.ReadFull(random, value); err != nil {
		return "", fmt.Errorf("choose a random identifier: %w", err)
	}
	return hex.EncodeToString(value), nil
}

// desiredSetupProfile keeps every value the profile already has and fills
// only what is missing, so ports, the plain Fabric ID and the owner's own
// edits survive a rerun. The node identity always follows the node
// configuration, and L1 always admits this machine's node.
func desiredSetupProfile(host setupHost, existing connprofile.Profile, nodeID string) (connprofile.Profile, error) {
	profile := existing
	profile.NodeAdmission = slices.Clone(existing.NodeAdmission)
	profile.Version = connprofile.Version
	if profile.Fabric == "" {
		profile.Fabric = "plain"
	}
	if profile.Fabric != "plain" {
		return connprofile.Profile{}, fmt.Errorf("the connection profile uses the %s fabric; wefty setup installs plain network mode only", profile.Fabric)
	}
	if profile.PlainFabricID == "" {
		suffix, err := randomHex(host.random, 8)
		if err != nil {
			return connprofile.Profile{}, err
		}
		profile.PlainFabricID = "plain-wefty-" + suffix
	}
	if profile.L1Address == "" {
		profile.L1Address = host.l1Address
	}
	if profile.L3Address == "" {
		profile.L3Address = host.l3Address
	}
	if profile.Identities.CLI == "" {
		profile.Identities.CLI = "wefty-cli"
	}
	if profile.Identities.PersonUserID == "" {
		profile.Identities.PersonUserID = host.username
		if profile.Identities.PersonUserID == "" {
			profile.Identities.PersonUserID = "owner"
		}
	}
	if profile.Identities.PersonDeviceID == "" {
		profile.Identities.PersonDeviceID = nodeID
	}
	profile.Identities.Node = nodeID
	profile.AllowPlainPersonIdentities = true
	pinnedTag := contract.StableNodeTagPrefix + nodeID
	index := slices.IndexFunc(profile.NodeAdmission, func(admission connprofile.NodeAdmission) bool {
		return admission.NodeID == nodeID
	})
	if index < 0 {
		profile.NodeAdmission = append(profile.NodeAdmission, connprofile.NodeAdmission{
			NodeID: nodeID, Tags: []string{pinnedTag},
			MaxOneshotSlots: l1.DefaultMaxOneshotSlots, MaxServiceSlots: l1.DefaultMaxServiceSlots,
		})
	} else if admission := &profile.NodeAdmission[index]; !slices.Contains(l1.NormalizeTags(admission.Tags), pinnedTag) {
		admission.Tags = append(slices.Clone(admission.Tags), pinnedTag)
	}
	return profile, profile.Validate()
}

// setupServices keeps the legacy plan until fleet setup explicitly publishes
// role metadata. Participant presence never selects an installation role.
func setupServices(paths setupPaths, profile connprofile.Profile, binaries map[string]string, nodeAgentPATH string) ([]launchagent.Agent, error) {
	return setupRoleServices(paths, profile, binaries, nodeAgentPATH, serviceplan.Legacy)
}

// setupRoleServices is the Mac adapter for the platform-neutral named plan.
func setupRoleServices(paths setupPaths, profile connprofile.Profile, binaries map[string]string, nodeAgentPATH string, role serviceplan.Role) ([]launchagent.Agent, error) {
	plan, err := setupRolePlan(paths, role)
	if err != nil {
		return nil, err
	}
	var agents []launchagent.Agent
	for _, entry := range plan.Services {
		inputDigest, err := connprofile.Digest(struct {
			Binary   string `json:"binary"`
			Settings any    `json:"settings"`
		}{binaries[entry.Binary], entry.Settings(profile)})
		if err != nil {
			return nil, err
		}
		service := launchagent.Agent{Label: "dev.wefty." + entry.Name, Program: filepath.Join(paths.bin, entry.Binary), Arguments: entry.Arguments,
			Environment: map[string]string{"HOME": paths.home, "PATH": setupServicePATH}, WorkingDirectory: paths.data, LogPath: entry.LogPath, InputDigest: inputDigest}
		if entry.Name == "node-agent" {
			service.Environment["PATH"] = nodeAgentPATH
			service.Environment["WEFTY_LAUNCH_UNIT"] = setupNodeAgentLabel
			service.ExitTimeOut = setupNodeAgentExitTimeout
			service.ProcessType = "Interactive"
		}
		agents = append(agents, service)
	}
	return agents, nil
}

// captureLoginPATH asks the owner's login shell for its PATH. A LaunchAgent
// otherwise gets a minimal PATH, and jobs would not find the owner's tools.
// The shell is interactive as well, because many setups extend PATH in the
// interactive startup file, and the markers keep startup noise out.
func captureLoginPATH(ctx context.Context, host setupHost) (string, error) {
	shell := host.shell
	if shell == "" {
		shell = "/bin/zsh"
	}
	if !filepath.IsAbs(shell) {
		return "", fmt.Errorf("SHELL=%s is not an absolute path; setup reads the login-shell PATH from it", shell)
	}
	budget, cancel := context.WithTimeout(ctx, setupLoginShellBudget)
	defer cancel()
	output, err := host.loginShell(budget, shell, "-l", "-i", "-c",
		"echo "+setupPATHBegin+"; /usr/bin/printenv PATH; echo "+setupPATHEnd)
	if err != nil {
		return "", fmt.Errorf("read the login-shell PATH from %s: %w", shell, err)
	}
	var lines []string
	inside := false
	scanner := bufio.NewScanner(bytes.NewReader(output))
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		switch {
		case line == setupPATHBegin:
			inside, lines = true, nil
		case line == setupPATHEnd:
			inside = false
		case inside && line != "":
			lines = append(lines, line)
		}
	}
	if len(lines) != 1 || !strings.Contains(lines[0], "/") {
		return "", fmt.Errorf("read the login-shell PATH from %s: the shell did not print one PATH", shell)
	}
	return lines[0], nil
}

// reportedSetupVersion asks each binary setup installs for its version, the
// `--version` check upgrade makes of its own build. It returns a version only
// when all four print the same one, so the receipt never holds a guessed
// commit; otherwise it returns why there is none. A development build reports
// `dev`, which is recorded as is: upgrade compares it with a full commit, so it
// never counts as current. A binary that prints too much or does not answer in
// time is stopped and gives no version; setup goes on without one.
func reportedSetupVersion(ctx context.Context, host setupHost, home, directory string) (string, string) {
	return reportedSetupBinaryVersion(ctx, host, home, directory, setupBinaries)
}

func reportedSetupBinaryVersion(ctx context.Context, host setupHost, home, directory string, binaries []string) (string, string) {
	budget := host.versionProbeBudget
	if budget <= 0 {
		budget = setupVersionProbeBudget
	}
	probe, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	environment := []string{"HOME=" + home, "PATH=" + setupServicePATH}
	reports := make([]string, 0, len(binaries))
	agreed := true
	for _, name := range binaries {
		path := filepath.Join(directory, name)
		output, err := runVersionProbe(probe, directory, environment, path)
		switch {
		case errors.Is(err, errVersionOutputLimit):
			return "", fmt.Sprintf("%s --version printed more than %d bytes, not a version; setup recorded no installed version", path, setupVersionOutputLimit)
		case err != nil && probe.Err() != nil:
			return "", fmt.Sprintf("%s --version did not answer within %s; setup recorded no installed version", path, budget)
		case err != nil:
			return "", fmt.Sprintf("could not read the version of %s (%v); setup recorded no installed version", path, err)
		}
		value := strings.TrimSpace(string(output))
		if !validReportedVersion(value) {
			if len(value) > 64 {
				value = value[:64] + "..."
			}
			return "", fmt.Sprintf("%s --version printed %q, not a version; setup recorded no installed version", path, value)
		}
		agreed = agreed && (len(reports) == 0 || value == reports[0])
		reports = append(reports, value)
	}
	if !agreed {
		described := make([]string, len(binaries))
		for index, name := range binaries {
			described[index] = name + " " + reports[index]
		}
		return "", "the binaries report different versions (" + strings.Join(described, ", ") + "); setup recorded no installed version"
	}
	return reports[0], ""
}

var errVersionOutputLimit = errors.New("version output limit exceeded")

// versionOutput keeps at most limit bytes of an answer. A longer answer stops
// the process at once, so a binary that prints without end cannot fill memory
// and is not read to the end.
type versionOutput struct {
	buffer   bytes.Buffer
	limit    int
	exceeded bool
	stop     context.CancelFunc
}

func (output *versionOutput) Write(payload []byte) (int, error) {
	if output.exceeded || output.buffer.Len()+len(payload) > output.limit {
		output.exceeded = true
		output.stop()
		return 0, errVersionOutputLimit
	}
	return output.buffer.Write(payload)
}

// runVersionProbe runs path --version and returns its standard output, at
// most setupVersionOutputLimit bytes. Standard error is discarded.
func runVersionProbe(ctx context.Context, directory string, environment []string, path string) ([]byte, error) {
	run, stop := context.WithCancel(ctx)
	defer stop()
	output := &versionOutput{limit: setupVersionOutputLimit, stop: stop}
	command := exec.CommandContext(run, path, "--version")
	command.Dir, command.Env, command.Stdout, command.WaitDelay = directory, environment, output, time.Second
	err := command.Run()
	// Run has waited for the copy into output, so exceeded is final here.
	if output.exceeded {
		return nil, errVersionOutputLimit
	}
	if err != nil {
		return nil, err
	}
	return output.buffer.Bytes(), nil
}

// validReportedVersion accepts one printable word, as `--version` prints.
func validReportedVersion(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if character <= ' ' || character > '~' {
			return false
		}
	}
	return true
}

// setupSkillFiles prepares the embedded skill's files for both skill
// folders: Claude Code reads ~/.claude/skills, Codex ~/.agents/skills.
func setupSkillFiles(paths setupPaths) ([]setupFile, error) {
	return setupSkillFilesFrom(paths, nil)
}

func setupSkillFilesFrom(paths setupPaths, source fs.FS) ([]setupFile, error) {
	if source == nil {
		source = skills.Wefty
	}
	var files []setupFile
	for _, target := range paths.skillTargets {
		err := fs.WalkDir(source, "wefty", func(name string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			payload, err := fs.ReadFile(source, name)
			if err != nil {
				return err
			}
			destination := filepath.Join(target, filepath.FromSlash(strings.TrimPrefix(name, "wefty/")))
			files = append(files, setupFile{
				folder: filepath.Dir(destination), name: filepath.Base(destination), payload: payload, mode: 0o644,
				change: "installed the wefty skill in " + filepath.Join(paths.home, target),
			})
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("read the embedded wefty skill: %w", err)
		}
	}
	return files, nil
}

func writeSetupReport(writer io.Writer, report setupReport) error {
	var output strings.Builder
	if report.Role != serviceplan.Legacy {
		fmt.Fprintf(&output, "Installation role: %s\n", report.Role)
		if report.JoinCode != "" {
			fmt.Fprintf(&output, "Join code (non-secret): %s\n", report.JoinCode)
		}
		for _, role := range []string{"l1", "l3", "node-agent", "client", "person"} {
			if name := report.Participants[role]; name != "" {
				fmt.Fprintf(&output, "%s network name: %s\n", role, name)
			}
		}
		for _, check := range report.Readiness {
			fmt.Fprintf(&output, "%s: %s (%s)\n", check.Check, check.Status, check.Detail)
		}
	}
	fmt.Fprintf(&output, "This machine is node %s.\n", report.NodeID)
	fmt.Fprintf(&output, "Connection profile: %s (L1 %s, L3 %s)\n", report.Profile, report.L1Address, report.L3Address)
	if len(report.Changes) == 0 {
		output.WriteString("Files: nothing to change\n")
	}
	for _, change := range report.Changes {
		fmt.Fprintf(&output, "Changed: %s\n", change)
	}
	for _, service := range report.Services {
		fmt.Fprintf(&output, "%-21s %s (log %s)\n", service.Label, service.Action, service.Log)
	}
	for _, note := range report.Notes {
		fmt.Fprintf(&output, "Note: %s\n", note)
	}
	output.WriteString("Next: run `wefty status` in a new shell.\n")
	_, err := io.WriteString(writer, output.String())
	return err
}

func setupRolePlan(paths setupPaths, role serviceplan.Role) (serviceplan.Plan, error) {
	return serviceplan.Build(role, serviceplan.Paths{
		Profile: paths.profile, Data: paths.data, Logs: paths.logs,
		Spool: filepath.Join(paths.home, paths.nodeDataFolders[0]), Managed: filepath.Join(paths.home, paths.nodeDataFolders[1]),
		ClientSocket: local.DefaultSocketPath(paths.home), NodeShutdownBudget: setupNodeAgentShutdownBudget,
	})
}
