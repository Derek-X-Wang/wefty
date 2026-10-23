package serviceplan

import (
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
)

// NodeGeneration selects one installed incarnation without changing the named
// node-agent service or the legacy plan. The installation catalog owns these
// paths; selection is not deletion or activation authority.
type NodeGeneration struct {
	ID, Executable, Root, LaunchAgents, ReadyFile string
}

func GenerationArguments(arguments []string, g NodeGeneration) ([]string, error) {
	id, err := hex.DecodeString(g.ID)
	if err != nil || len(id) != 16 || hex.EncodeToString(id) != g.ID {
		return nil, errors.New("invalid node agent generation ID")
	}
	for _, path := range []string{g.Executable, g.Root, g.LaunchAgents, g.ReadyFile} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsAny(path, "\x00\r\n") {
			return nil, errors.New("generation plan paths must be absolute and clean")
		}
	}
	if g.Executable != filepath.Join(g.Root, g.ID, "wefty-agent") || g.ReadyFile != filepath.Join(g.Root, g.ID, "ready.json") {
		return nil, errors.New("generation plan paths differ from owned layout")
	}
	out := []string{}
	for _, arg := range arguments {
		if !strings.HasPrefix(arg, "--ready-file=") && !strings.HasPrefix(arg, "--agent-generation-") {
			out = append(out, arg)
		}
	}
	return append(out, "--agent-generation-id="+g.ID, "--agent-generation-root="+g.Root, "--agent-generation-launch-agents="+g.LaunchAgents, "--ready-file="+g.ReadyFile), nil
}
