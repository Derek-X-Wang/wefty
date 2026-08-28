// Package linuxunit renders the Linux boot topology for one unprivileged
// wefty-agent and its socket-activated privileged OCI helper.
package linuxunit

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

const (
	AgentUnit         = "wefty-agent.service"
	HelperSocketUnit  = "wefty-oci-helper.socket"
	HelperServiceUnit = "wefty-oci-helper.service"
	HelperSocketPath  = "/run/wefty-oci/oci-helper.sock"
	HelperGroup       = "wefty-oci"
)

var userPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]{0,63}$`)

type Config struct {
	AgentPath           string
	AgentArguments      []string
	OperatorUser        string
	OperatorGroup       string
	OperatorUID         int
	OperatorGID         int
	WorkingDirectory    string
	AllowedMountRoots   []string
	ContainerdAddress   string
	ContainerdStateRoot string
	RuntimeRoot         string
	RuncExecutable      string
	MemoryCapacityBytes int64
	MemoryReserveBytes  int64
}

type Units struct {
	Agent         []byte
	HelperSocket  []byte
	HelperService []byte
}

func Render(config Config) (Units, error) {
	if !filepath.IsAbs(config.AgentPath) || !filepath.IsAbs(config.WorkingDirectory) || config.OperatorUID == 0 || config.OperatorGID == 0 ||
		strings.ContainsAny(config.AgentPath+config.WorkingDirectory, "\r\n") ||
		!userPattern.MatchString(config.OperatorUser) || !userPattern.MatchString(config.OperatorGroup) {
		return Units{}, errors.New("Linux units require an absolute agent/working path and a non-root operator identity")
	}
	for _, path := range append([]string{config.ContainerdAddress, config.ContainerdStateRoot, config.RuntimeRoot}, config.AllowedMountRoots...) {
		if !filepath.IsAbs(path) || filepath.Clean(path) == string(filepath.Separator) || strings.ContainsAny(path, "\r\n") {
			return Units{}, errors.New("Linux OCI unit paths must be absolute, non-root, and single-line")
		}
	}
	if config.RuncExecutable != "" && (!filepath.IsAbs(config.RuncExecutable) || strings.ContainsAny(config.RuncExecutable, "\r\n")) {
		return Units{}, errors.New("Linux runc executable must be an absolute single-line path")
	}
	if config.MemoryCapacityBytes < 0 || config.MemoryReserveBytes < 0 {
		return Units{}, errors.New("Linux OCI memory capacity configuration must not be negative")
	}
	for _, argument := range config.AgentArguments {
		lower := strings.ToLower(argument)
		if strings.ContainsAny(argument, "\r\n") || strings.Contains(lower, "--auth-key") || strings.Contains(lower, "ts_authkey") {
			return Units{}, errors.New("Linux agent unit arguments must be secret-free and single-line")
		}
	}
	agentArguments := append([]string{config.AgentPath}, config.AgentArguments...)
	helperArguments := []string{
		config.AgentPath, ocihelper.InvocationArg,
		"--oci-containerd-address=" + filepath.Clean(config.ContainerdAddress),
		"--oci-containerd-state-root=" + filepath.Clean(config.ContainerdStateRoot),
		"--oci-runtime-root=" + filepath.Clean(config.RuntimeRoot),
		"--oci-memory-capacity-bytes=" + strconv.FormatInt(config.MemoryCapacityBytes, 10),
		"--oci-memory-reserve-bytes=" + strconv.FormatInt(config.MemoryReserveBytes, 10),
	}
	if config.RuncExecutable != "" {
		helperArguments = append(helperArguments, "--oci-runc-executable="+filepath.Clean(config.RuncExecutable))
	}
	for _, root := range config.AllowedMountRoots {
		helperArguments = append(helperArguments, "--oci-allowed-mount-root="+filepath.Clean(root))
	}
	return Units{
		Agent: []byte(`[Unit]
Description=Wefty node agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=` + config.OperatorUser + `
Group=` + config.OperatorGroup + `
SupplementaryGroups=` + HelperGroup + `
WorkingDirectory=` + quote(config.WorkingDirectory) + `
RuntimeDirectory=wefty-agent
RuntimeDirectoryMode=0700
RuntimeDirectoryPreserve=restart
Environment=WEFTY_LAUNCH_UNIT=` + AgentUnit + `
ExecStart=` + quoteArguments(agentArguments) + `
Restart=on-failure
RestartSec=5s
KillSignal=SIGTERM
TimeoutStopSec=45s
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
`),
		HelperSocket: []byte(`[Unit]
Description=Wefty OCI helper socket

[Socket]
ListenStream=` + HelperSocketPath + `
RuntimeDirectory=wefty-oci
RuntimeDirectoryMode=0755
RuntimeDirectoryPreserve=restart
SocketUser=root
SocketGroup=` + HelperGroup + `
SocketMode=0660
DirectoryMode=0755
RemoveOnStop=true

[Install]
WantedBy=sockets.target
`),
		HelperService: []byte(`[Unit]
Description=Wefty privileged OCI helper
After=containerd.service
Requires=containerd.service

[Service]
Type=simple
User=root
Group=root
		Environment=` + ocihelper.AllowedUIDsEnvironment + `=` + strconv.Itoa(config.OperatorUID) + `
ExecStart=` + quoteArguments(helperArguments) + `
StandardOutput=journal
StandardError=journal
NoNewPrivileges=false
PrivateTmp=true
ProtectHome=true
`),
	}, nil
}

// ReconcileCommands is the exact post-write systemd sequence. The agent is
// restarted whenever setup newly adds its supplementary group; an existing
// member rerun does not manufacture another restart.
func ReconcileCommands(groupAdded bool) [][]string {
	commands := [][]string{
		{"systemctl", "daemon-reload"},
		{"systemctl", "enable", "--now", HelperSocketUnit},
		{"systemctl", "enable", AgentUnit},
	}
	if groupAdded {
		commands = append(commands, []string{"systemctl", "restart", AgentUnit})
	} else {
		commands = append(commands, []string{"systemctl", "start", AgentUnit})
	}
	return commands
}

func quoteArguments(arguments []string) string {
	quoted := make([]string, len(arguments))
	for index, argument := range arguments {
		quoted[index] = quote(argument)
	}
	return strings.Join(quoted, " ")
}

func quote(value string) string {
	value = strings.ReplaceAll(value, `%`, `%%`)
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return fmt.Sprintf(`"%s"`, value)
}
