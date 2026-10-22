// Package systemduser renders and manages unprivileged fleet services. It never
// uses system units, changes accounts or groups, or invokes a privilege helper.
package systemduser

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// Unit is the service-manager projection of one serviceplan entry. Inputs must
// contain saved-identity references only, never enrolment credentials.
type Unit struct {
	Name, Program, WorkingDirectory, LogPath, InputDigest string
	Arguments                                             []string
	Environment                                           map[string]string
	After                                                 []string
	TimeoutStopSeconds                                    int
}

const DefaultStopTimeoutSeconds = 20

var unitName = regexp.MustCompile(`^dev\.wefty\.[a-z0-9-]+\.service$`)
var environmentName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func (u Unit) validate() error {
	if !unitName.MatchString(u.Name) {
		return errors.New("invalid fleet user unit name")
	}
	for _, path := range []string{u.Program, u.WorkingDirectory, u.LogPath} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return errors.New("user unit paths must be absolute and clean")
		}
	}
	// systemd's config_parse_exec rejects these even inside a quoted word.
	for _, c := range u.Program {
		if c < 32 || c == 127 || strings.ContainsRune(`'"\`, c) {
			return errors.New("systemd executable paths must not contain quotes, backslashes or control characters; use a safe home and binary path before enrolment")
		}
	}
	values := append(slices.Clone(u.Arguments), u.Program, u.WorkingDirectory, u.LogPath, u.InputDigest)
	for _, path := range []string{u.WorkingDirectory, u.LogPath} {
		if strings.TrimSpace(path) != path || strings.HasSuffix(path, `\`) {
			return errors.New("user unit scalar paths must not end in whitespace or a backslash")
		}
	}
	for k, v := range u.Environment {
		if !environmentName.MatchString(k) {
			return errors.New("invalid user unit environment name")
		}
		if strings.Contains(strings.ToLower(k), "authkey") || strings.Contains(strings.ToLower(k), "auth_key") {
			return errors.New("user units must not contain enrolment credentials")
		}
		values = append(values, v)
	}
	for _, v := range values {
		if strings.ContainsAny(v, "\x00\r\n") {
			return errors.New("user unit values must be single-line without NUL")
		}
	}
	for _, arg := range u.Arguments {
		if strings.Contains(strings.ToLower(arg), "--auth-key") || strings.Contains(strings.ToLower(arg), "ts_authkey") {
			return errors.New("user units must not contain enrolment credentials")
		}
	}
	for _, dependency := range u.After {
		if !unitName.MatchString(dependency) || dependency == u.Name {
			return errors.New("invalid user unit dependency")
		}
	}
	if u.TimeoutStopSeconds < 0 {
		return errors.New("invalid user unit stop timeout")
	}
	return nil
}

// quote protects word-list syntax and percent specifiers. ExecStart uses the
// colon prefix to disable all environment expansion, including in argv[0].
// Environment= never expands dollar variables (systemd.exec).
func quote(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	var escaped strings.Builder
	for _, c := range value {
		if c < 32 || c == 127 {
			fmt.Fprintf(&escaped, `\x%02x`, c)
		} else {
			escaped.WriteRune(c)
		}
	}
	return `"` + strings.ReplaceAll(escaped.String(), `%`, `%%`) + `"`
}

// These scalar directives are not word lists: systemd v252's
// config_parse_working_directory/config_parse_exec_output call unit_path_printf
// directly. Quotes and backslashes are literal, only percent specifiers expand.
func scalarPath(value string) string { return strings.ReplaceAll(value, "%", "%%") }

// Render starts at user-manager startup and restarts only unsuccessful exits.
// After orders boot activation without causing another unit to restart when
// this one's definition changes. Setup starts entries in serviceplan order.
func Render(u Unit) ([]byte, error) {
	if err := u.validate(); err != nil {
		return nil, err
	}
	var b bytes.Buffer
	b.WriteString("# Written by wefty setup. Rerun setup instead of editing.\n")
	if u.InputDigest != "" {
		b.WriteString("# wefty-input-digest: " + u.InputDigest + "\n")
	}
	b.WriteString("[Unit]\nDescription=Wefty " + u.Name + "\n")
	if len(u.After) > 0 {
		b.WriteString("After=" + strings.Join(u.After, " ") + "\n")
	}
	b.WriteString("\n[Service]\nType=simple\nWorkingDirectory=" + scalarPath(u.WorkingDirectory) + "\n")
	names := make([]string, 0, len(u.Environment))
	for name := range u.Environment {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		b.WriteString("Environment=" + quote(name+"="+u.Environment[name]) + "\n")
	}
	b.WriteString("ExecStart=:" + quote(u.Program))
	for _, arg := range u.Arguments {
		b.WriteByte(' ')
		b.WriteString(quote(arg))
	}
	b.WriteString("\nStandardOutput=" + "append:" + scalarPath(u.LogPath) + "\nStandardError=" + "append:" + scalarPath(u.LogPath) + "\nRestart=on-failure\nRestartSec=10s\nKillMode=mixed\n")
	timeout := u.TimeoutStopSeconds
	if timeout == 0 {
		timeout = DefaultStopTimeoutSeconds
	}
	fmt.Fprintf(&b, "TimeoutStopSec=%ds\n", timeout)
	b.WriteString("\n[Install]\nWantedBy=default.target\n")
	return b.Bytes(), nil
}

// RecordedPATH reads the literal captured login PATH from our generated unit,
// never from the diagnostic process. Unsupported syntax or overrides fail closed.
func RecordedPATH(payload []byte) (string, error) {
	found := ""
	section := ""
	for _, line := range strings.Split(string(payload), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "[") {
			section = line
			continue
		}
		if section != "[Service]" {
			continue
		}
		value, ok := strings.CutPrefix(line, "Environment=")
		if !ok {
			continue
		}
		value, err := strconv.Unquote(value)
		if err != nil {
			return "", errors.New("cannot read user unit environment")
		}
		value = strings.ReplaceAll(value, "%%", "%")
		if value, ok = strings.CutPrefix(value, "PATH="); ok {
			if found != "" || value == "" {
				return "", errors.New("invalid recorded PATH")
			}
			found = value
		}
	}
	if found == "" {
		return "", errors.New("user unit has no recorded PATH")
	}
	return found, nil
}
