package lima

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const (
	LaunchDaemonLabel          = "dev.wefty.agent"
	LaunchDaemonPath           = "/Library/LaunchDaemons/dev.wefty.agent.plist"
	GuestHelperPath            = "/usr/local/libexec/wefty-agent"
	GuestHelperSocketUnit      = "dev.wefty.oci-helper.socket"
	GuestHelperServiceUnit     = "dev.wefty.oci-helper.service"
	GuestHelperSocketUnitPath  = "/etc/systemd/system/" + GuestHelperSocketUnit
	GuestHelperServiceUnitPath = "/etc/systemd/system/" + GuestHelperServiceUnit
	DefaultLaunchPATH          = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
	BootstrapInvocationArg     = "__wefty_mac_bootstrap"
)

var (
	bootstrapUserPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]{0,63}$`)
	bootstrapSHA256      = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

// LaunchDaemonConfig is the complete, secret-free system boot contract for
// the operator-user agent. Arguments may contain paths but never credentials.
type LaunchDaemonConfig struct {
	AgentPath         string
	Arguments         []string
	OperatorUser      string
	Home              string
	LimaHome          string
	PATH              string
	WorkingDirectory  string
	StandardOutPath   string
	StandardErrorPath string
}

type LaunchDaemonRemovalEvidence struct {
	Label       string `json:"label"`
	Unloaded    bool   `json:"unloaded"`
	PlistAbsent bool   `json:"plist_absent"`
}

func (config LaunchDaemonConfig) validate() error {
	for name, path := range map[string]string{
		"agent": config.AgentPath, "HOME": config.Home, "LIMA_HOME": config.LimaHome,
		"working directory": config.WorkingDirectory, "stdout": config.StandardOutPath, "stderr": config.StandardErrorPath,
	} {
		if !filepath.IsAbs(path) || strings.ContainsAny(path, "\r\n") {
			return fmt.Errorf("LaunchDaemon %s path must be absolute", name)
		}
	}
	if !bootstrapUserPattern.MatchString(config.OperatorUser) {
		return errors.New("LaunchDaemon operator user is invalid")
	}
	if strings.TrimSpace(config.PATH) == "" || strings.ContainsAny(config.PATH, "\r\n") {
		return errors.New("LaunchDaemon PATH must be explicit and single-line")
	}
	for _, argument := range config.Arguments {
		lower := strings.ToLower(argument)
		if strings.Contains(lower, "--auth-key") || strings.Contains(lower, "ts_authkey") || strings.ContainsAny(argument, "\r\n") {
			return errors.New("LaunchDaemon arguments must not contain credentials or newlines")
		}
	}
	return nil
}

// RenderLaunchDaemon returns the exact operator-user boot unit installed by
// setup. It intentionally contains no Lima autostart unit or secret source.
func RenderLaunchDaemon(config LaunchDaemonConfig) ([]byte, error) {
	if config.PATH == "" {
		config.PATH = DefaultLaunchPATH
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	var output bytes.Buffer
	output.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	output.WriteString("<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n")
	output.WriteString("<plist version=\"1.0\">\n<dict>\n")
	writePlistString(&output, "Label", LaunchDaemonLabel)
	output.WriteString("  <key>ProgramArguments</key>\n  <array>\n")
	writePlistArrayString(&output, config.AgentPath)
	for _, argument := range config.Arguments {
		writePlistArrayString(&output, argument)
	}
	output.WriteString("  </array>\n")
	writePlistString(&output, "UserName", config.OperatorUser)
	writePlistString(&output, "WorkingDirectory", config.WorkingDirectory)
	output.WriteString("  <key>EnvironmentVariables</key>\n  <dict>\n")
	for _, entry := range []struct{ name, value string }{
		{"HOME", config.Home}, {"LIMA_HOME", config.LimaHome}, {"USER", config.OperatorUser},
		{"LOGNAME", config.OperatorUser}, {"PATH", config.PATH}, {"WEFTY_LAUNCH_UNIT", LaunchDaemonLabel},
	} {
		writePlistString(&output, entry.name, entry.value)
	}
	output.WriteString("  </dict>\n")
	writePlistString(&output, "StandardOutPath", config.StandardOutPath)
	writePlistString(&output, "StandardErrorPath", config.StandardErrorPath)
	output.WriteString("  <key>RunAtLoad</key>\n  <true/>\n")
	output.WriteString("  <key>KeepAlive</key>\n  <dict>\n")
	output.WriteString("    <key>SuccessfulExit</key>\n    <false/>\n")
	output.WriteString("  </dict>\n")
	output.WriteString("  <key>ThrottleInterval</key>\n  <integer>10</integer>\n")
	output.WriteString("</dict>\n</plist>\n")
	return output.Bytes(), nil
}

func writePlistString(output *bytes.Buffer, name, value string) {
	output.WriteString("  <key>")
	_ = xml.EscapeText(output, []byte(name))
	output.WriteString("</key>\n  <string>")
	_ = xml.EscapeText(output, []byte(value))
	output.WriteString("</string>\n")
}

func writePlistArrayString(output *bytes.Buffer, value string) {
	output.WriteString("    <string>")
	_ = xml.EscapeText(output, []byte(value))
	output.WriteString("</string>\n")
}

// ValidateLaunchDaemonInstall rejects missing local prerequisites before any
// privileged plist or launchd mutation occurs.
func ValidateLaunchDaemonInstall(config LaunchDaemonConfig) error {
	if _, err := RenderLaunchDaemon(config); err != nil {
		return err
	}
	agentInfo, err := os.Stat(config.AgentPath)
	if err != nil {
		return fmt.Errorf("inspect installed macOS agent: %w", err)
	}
	if !agentInfo.Mode().IsRegular() || agentInfo.Mode().Perm()&0o111 == 0 {
		return errors.New("installed macOS agent must be an executable regular file")
	}
	for name, path := range map[string]string{
		"HOME": config.Home, "LIMA_HOME": config.LimaHome, "working directory": config.WorkingDirectory,
		"stdout directory": filepath.Dir(config.StandardOutPath), "stderr directory": filepath.Dir(config.StandardErrorPath),
	} {
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("inspect LaunchDaemon %s: %w", name, err)
		}
		if !info.IsDir() {
			return fmt.Errorf("LaunchDaemon %s must be a directory", name)
		}
	}
	return nil
}

type GuestHelperInstallConfig struct {
	Instance            string
	Limactl             string
	GuestUser           string
	GuestUID            uint32
	HelperBinary        string
	ExpectedVersion     string
	ExpectedChecksum    string
	HostMountRoot       string
	HelperSocket        string
	ProbeArchive        string
	ProbeReference      string
	ProbeDigest         string
	NodeID              string
	BootSessionID       string
	MemoryCapacityBytes int64
	MemoryReserveBytes  int64
}

type GuestHelperRemovalConfig struct {
	Instance string
	Limactl  string
}

type GuestHelperRemovalEvidence struct {
	SocketStopped  bool `json:"socket_stopped"`
	ServiceStopped bool `json:"service_stopped"`
	FilesAbsent    bool `json:"files_absent"`
}

func (config GuestHelperInstallConfig) validate() error {
	if !instanceNamePattern.MatchString(config.Instance) || !bootstrapUserPattern.MatchString(config.GuestUser) {
		return errors.New("guest helper install requires valid Lima instance and guest user")
	}
	for name, path := range map[string]string{
		"helper binary": config.HelperBinary, "host mount root": config.HostMountRoot,
		"helper socket": config.HelperSocket, "probe archive": config.ProbeArchive,
	} {
		if !filepath.IsAbs(path) || strings.ContainsAny(path, "\r\n") {
			return fmt.Errorf("guest helper %s path must be absolute", name)
		}
	}
	if config.ExpectedVersion == "" || config.ExpectedChecksum == "" || config.ProbeReference == "" || config.ProbeDigest == "" || config.NodeID == "" || config.BootSessionID == "" {
		return errors.New("guest helper install requires version, checksum, probe identity, and boot identity")
	}
	if !bootstrapSHA256.MatchString(config.ExpectedChecksum) || !bootstrapSHA256.MatchString(config.ProbeDigest) {
		return errors.New("guest helper and probe checksums must be immutable sha256 digests")
	}
	if filepath.Clean(config.HostMountRoot) == string(filepath.Separator) {
		return errors.New("guest helper host mount root must not be filesystem root")
	}
	if config.MemoryCapacityBytes < 0 || config.MemoryReserveBytes < 0 {
		return errors.New("guest helper memory capacity configuration must not be negative")
	}
	if config.Limactl == "" {
		config.Limactl = "limactl"
	}
	return nil
}

// ValidateGuestHelperInstall performs every host-side prerequisite check used
// by the private bootstrap before it starts or mutates Lima.
func ValidateGuestHelperInstall(config GuestHelperInstallConfig) error {
	if err := config.validate(); err != nil {
		return err
	}
	checksum, err := checksumFile(config.HelperBinary)
	if err != nil {
		return err
	}
	if checksum != config.ExpectedChecksum {
		return errors.New("guest helper source checksum does not match configured checksum")
	}
	info, err := os.Stat(config.ProbeArchive)
	if err != nil {
		return fmt.Errorf("inspect probe image archive: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("probe image archive must be a regular file")
	}
	return nil
}

func renderGuestSocketUnit() []byte {
	return []byte(`[Unit]
Description=Wefty OCI helper socket

[Socket]
ListenStream=/run/wefty/oci-helper.sock
SocketUser=root
SocketGroup=wefty-oci
SocketMode=0660
DirectoryMode=0755
RemoveOnStop=true

[Install]
WantedBy=sockets.target
`)
}

func renderGuestServiceUnit(config GuestHelperInstallConfig) []byte {
	arguments := []string{
		GuestHelperPath, ocihelper.InvocationArg,
		"--oci-allowed-mount-root=" + GuestAllowedMountRoot,
		"--oci-lima-host-mount-root=" + filepath.Clean(config.HostMountRoot),
		"--oci-lima-guest-mount-root=" + GuestAllowedMountRoot,
		"--oci-memory-capacity-bytes=" + strconv.FormatInt(config.MemoryCapacityBytes, 10),
		"--oci-memory-reserve-bytes=" + strconv.FormatInt(config.MemoryReserveBytes, 10),
	}
	for index := range arguments {
		arguments[index] = systemdQuote(arguments[index])
	}
	return []byte(`[Unit]
Description=Wefty privileged OCI helper
After=containerd.service

[Service]
Type=simple
User=root
Group=root
Environment=` + ocihelper.AllowedUIDsEnvironment + `=` + strconv.FormatUint(uint64(config.GuestUID), 10) + `
ExecStart=` + strings.Join(arguments, " ") + `
StandardOutput=journal
StandardError=journal
NoNewPrivileges=false
`)
}

func systemdQuote(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return `"` + value + `"`
}

func checksumFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open guest helper binary: %w", err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("checksum guest helper binary: %w", err)
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}
