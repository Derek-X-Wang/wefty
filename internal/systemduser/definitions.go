package systemduser

import (
	"errors"
	"io/fs"
	"path/filepath"
	"strings"
)

var foreignDefinition = errors.New("foreign user unit fragment or unsafe drop-ins; remove unit-specific/user overrides and restore the original unit in ~/.config/systemd/user before rerunning setup")

func inside(path, root string) bool {
	if !filepath.IsAbs(root) {
		return false
	}
	rel, err := filepath.Rel(root, path)
	return err == nil && filepath.IsLocal(rel)
}
func (m Manager) checkDropIns(name string, paths []string) error {
	for _, path := range paths {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsAny(path, "\x00\r\n") {
			return foreignDefinition
		}
		if inside(path, m.Home) || inside(path, m.RuntimeDir) {
			return foreignDefinition
		}
		directory := filepath.Dir(path)
		if filepath.Base(directory) == name+".d" {
			return foreignDefinition
		}
		// Only shared type/prefix policy is trusted. The complete path,
		// including symlink targets and parents, must be root controlled.
		shared := filepath.Base(directory)
		if shared != "service.d" && !(strings.HasPrefix(shared, "dev.wefty") && strings.HasSuffix(shared, "-.service.d")) {
			return foreignDefinition
		}

		resolved, err := m.Files.resolve(path)
		if err != nil || inside(resolved, m.Home) || inside(resolved, m.RuntimeDir) || !m.Files.protectedFile(path) {
			return foreignDefinition
		}
	}
	return nil
}

// Preflight also discovers unloaded units' overrides. A not-found unit's
// DropInPaths is empty, so show alone cannot expose an override before keys.
func (m Manager) Preflight(name string) error {
	if !unitName.MatchString(name) {
		return foreignDefinition
	}
	directories := []string{name + ".d", "service.d", "dev.wefty-.service.d"}
	stem := strings.TrimSuffix(name, ".service")
	for i, c := range stem {
		if c == '-' {
			directories = append(directories, stem[:i+1]+".service.d")
		}
	}
	roots := []string{m.UnitDirectory, filepath.Join(m.Home, ".config", "systemd", "user"), filepath.Join(m.Home, ".config", "systemd", "user.control"), filepath.Join(m.Home, ".local", "share", "systemd", "user"), "/etc/systemd/user.control", "/run/systemd/user.control", "/etc/systemd/user", "/usr/lib/systemd/user", "/usr/local/lib/systemd/user", "/run/systemd/user", "/lib/systemd/user",
		// The XDG configuration and data user-unit folders are on systemd's search
		// path too; an override there must also be caught before any key is used.
		"/etc/xdg/systemd/user", "/usr/local/share/systemd/user", "/usr/share/systemd/user"}
	if m.RuntimeDir != "" {
		roots = append(roots, filepath.Join(m.RuntimeDir, "systemd", "user"), filepath.Join(m.RuntimeDir, "systemd", "user.control"), filepath.Join(m.RuntimeDir, "systemd", "transient"))
	}
	for _, root := range roots {
		if !filepath.IsAbs(root) {
			continue
		}
		for _, directory := range directories {
			folder := filepath.Join(root, directory)
			entries, err := m.Files.readDir(folder)
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return errors.New("cannot inspect user unit drop-ins before enrollment; restore access to systemd user configuration")
			}
			for _, entry := range entries {
				// Match systemd dirent_is_file_with_suffix: ignore hidden
				// entries and non-files, including directories named *.conf.
				if strings.HasPrefix(entry.Name(), ".") || !strings.HasSuffix(entry.Name(), ".conf") || (!entry.Type().IsRegular() && entry.Type()&fs.ModeSymlink == 0) {
					continue
				}
				if err := m.checkDropIns(name, []string{filepath.Join(folder, entry.Name())}); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// systemctl show prints string arrays using shell_maybe_quote. Parse literal
// words only; no expansion, shell execution or unknown escape syntax is used.
func dropInWords(value string) ([]string, error) {
	var words []string
	var word strings.Builder
	var quote byte
	active := false
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c < 32 && c != '\t' || c == 127 {
			return nil, foreignDefinition
		}
		if quote == 0 && (c == ' ' || c == '\t') {
			if active {
				words = append(words, word.String())
				word.Reset()
				active = false
			}
			continue
		}
		if c == '\'' && quote != '"' || c == '"' && quote != '\'' {
			if quote == 0 {
				quote = c
			} else {
				quote = 0
			}
			active = true
			continue
		}
		if c == '\\' && quote != '\'' {
			i++
			if i >= len(value) {
				return nil, foreignDefinition
			}
			c = value[i]
		}
		word.WriteByte(c)
		active = true
	}
	if quote != 0 {
		return nil, foreignDefinition
	}
	if active {
		words = append(words, word.String())
	}
	return words, nil
}
