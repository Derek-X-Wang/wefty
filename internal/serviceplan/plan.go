// Package serviceplan selects the installation's processes independently of
// its service manager. Renderers consume this one named plan on every platform.
package serviceplan

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric/local"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
)

type Role string

const (
	// Legacy is absence of fleet role metadata, never inferred from enrolment.
	Legacy       Role = ""
	ControlPlane Role = "control-plane"
	Join         Role = "join"
)

func (r Role) Validate() error {
	switch r {
	case Legacy, ControlPlane, Join:
		return nil
	default:
		return fmt.Errorf("unknown installation role %q", r)
	}
}

type Paths struct {
	Profile, Data, Logs, Spool, Managed, ClientSocket string
	NodeShutdownBudget                                time.Duration
}

// Removal identifies service-manager and installed-file targets. It excludes
// databases and participant state, whose retention belongs to removal policy.
type Removal struct {
	Service            string
	Binary             string
	RuntimeFiles       []string
	RuntimeDirectories []string
}

type Service struct {
	Name        string
	Binary      string
	Participant string
	Arguments   []string
	LogPath     string
	Removal     Removal
}

type Plan struct {
	Name     string
	Role     Role
	Services []Service
}

func Build(role Role, paths Paths) (Plan, error) {
	if err := role.Validate(); err != nil {
		return Plan{}, err
	}
	if err := paths.validate(role); err != nil {
		return Plan{}, err
	}
	plan := Plan{Name: string(role), Role: role}
	if role == Legacy {
		plan.Name = "legacy-plain"
	}
	add := func(name, binary, participant string, args ...string) {
		if role == Legacy {
			participant = ""
		}
		plan.Services = append(plan.Services, Service{Name: name, Binary: binary, Participant: participant, Arguments: append([]string{"--profile=" + paths.Profile}, args...), LogPath: filepath.Join(paths.Logs, name+".log"), Removal: Removal{Service: name, Binary: binary}})
	}
	addReady := func(name string) {
		path := filepath.Join(paths.Data, "setup-ready", name+".json")
		service := &plan.Services[len(plan.Services)-1]
		service.Arguments = append(service.Arguments, "--ready-file="+path)
		service.Removal.RuntimeFiles = append(service.Removal.RuntimeFiles, path)
		service.Removal.RuntimeDirectories = append(service.Removal.RuntimeDirectories, filepath.Dir(path))
	}
	if role != Join {
		add("l1", "wefty-l1", connprofile.ParticipantL1, "--db="+filepath.Join(paths.Data, "l1.sqlite"))
		if role != Legacy {
			addReady("l1")
		}
		add("l3", "wefty-l3", connprofile.ParticipantL3, "--db="+filepath.Join(paths.Data, "l3.sqlite"))
		if role != Legacy {
			addReady("l3")
		}
	}
	add("node-agent", "wefty-agent", connprofile.ParticipantNodeAgent, "--log-spool-dir="+paths.Spool, "--managed-root="+paths.Managed, "--shutdown-budget="+paths.NodeShutdownBudget.String())
	if role != Legacy {
		addReady("node-agent")
	}
	if role != Legacy {
		add("client-service", "wefty", connprofile.ParticipantClient, "client-service", "--socket="+paths.ClientSocket)
		plan.Services[len(plan.Services)-1].Removal.RuntimeFiles = []string{paths.ClientSocket, paths.ClientSocket + ".lock"}
		plan.Services[len(plan.Services)-1].Removal.RuntimeDirectories = []string{filepath.Dir(paths.ClientSocket)}
	}
	return plan, nil
}

// Settings are precisely the inputs the entry's binary reads from a profile.
func (s Service) Settings(p connprofile.Profile) any {
	switch s.Name {
	case "l1":
		return p.L1()
	case "l3":
		return p.L3()
	case "node-agent":
		return p.NodeAgent()
	case "client-service":
		return p.ClientService()
	default:
		panic("unknown service plan entry")
	}
}

// validate checks only the paths consumed by this plan. The socket's folder
// must be private; an absent folder can be created privately at installation.
func (p Paths) validate(role Role) error {
	paths := map[string]string{"profile": p.Profile, "data": p.Data, "logs": p.Logs, "spool": p.Spool, "managed": p.Managed}
	if role != Legacy {
		paths["socket"] = p.ClientSocket
	}
	for name, path := range paths {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsAny(path, "\x00\r\n") {
			return fmt.Errorf("service plan %s path must be absolute and clean", name)
		}
	}
	if role == Legacy {
		return nil
	}
	if err := local.ValidateSocketPath(p.ClientSocket); err != nil {
		return err
	}
	folder := filepath.Dir(p.ClientSocket)
	switch folder {
	case "/", "/tmp", "/private/tmp", "/var/tmp", "/private/var/tmp", "/run", "/var/run", "/private/var/run":
		return fmt.Errorf("client-service socket requires a private folder")
	}
	info, err := os.Lstat(folder)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect client-service private folder: %w", err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		return fmt.Errorf("client-service socket requires a private folder with mode 0700")
	}
	return nil
}
