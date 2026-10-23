package agentinstall

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/durable"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
)

// Agent preserves today's service arguments and keys, replacing only identity,
// executable and the private ready path. Nothing secret is put in the plist.
func (s Store) Agent(base launchagent.Agent, g Generation) (launchagent.Agent, error) {
	args, err := serviceplan.GenerationArguments(base.Arguments, serviceplan.NodeGeneration{ID: g.ID, Executable: g.Executable, Root: s.Root(), LaunchAgents: s.LaunchAgents, ReadyFile: s.ReadyPath(g.ID)})
	if err != nil {
		return launchagent.Agent{}, err
	}
	base.Label = g.Label
	base.Program = g.Executable
	base.Arguments = args
	base.InputDigest = g.Digest
	base.Environment = maps.Clone(base.Environment)
	if base.Environment != nil {
		if _, ok := base.Environment["WEFTY_LAUNCH_UNIT"]; ok {
			base.Environment["WEFTY_LAUNCH_UNIT"] = g.Label
		}
	}
	base.SuppressAutostart = g.Role == "retiring"
	return base, nil
}

// LoadCandidate loads only an absent label. A loaded definition is never
// replaced, and a stopped generation is never implicitly restarted here.
func (s Store) LoadCandidate(ctx context.Context, id string, base launchagent.Agent) (launchagent.Action, error) {
	root, close, err := s.open(false)
	if err != nil {
		return "", err
	}
	defer close()
	c, err := s.read(root)
	if err != nil {
		return "", err
	}
	g, ok := c.Generations[id]
	if !ok || g.Role == "retiring" || c.PendingReceipt != "" {
		return "", errors.New("generation is not eligible to start")
	}
	if err := s.verifyExecutable(root, g); err != nil {
		return "", err
	}
	selected, err := s.Agent(base, g)
	if err != nil {
		return "", err
	}
	payload, err := launchagent.Render(selected)
	if err != nil {
		return "", err
	}
	folder, err := os.OpenRoot(s.LaunchAgents)
	if err != nil {
		return "", err
	}
	defer folder.Close()
	state, err := s.Manager.State(ctx, g.Label)
	if err != nil {
		return "", err
	}
	if state.Loaded {
		if state.Running && durable.Holds(folder, filepath.Base(g.Plist), payload, 0644) && g.PlistDigest == digestBytes(payload) {
			return launchagent.Unchanged, nil
		}
		return "", errors.New("retained generation definition cannot be replaced or restarted")
	}
	g.PlistDigest = digestBytes(payload)
	c.Generations[id] = g
	c.Revision++
	if err := writePrivate(root, catalogName, c); err != nil {
		return "", err
	}
	return s.Manager.LoadGeneration(ctx, folder, g.Plist, g.Label, payload)
}

// Startup runs before profiles, participants, managed roots or recovery open.
// It holds a generation-specific process lock for the entire agent lifetime.
// Candidates get a single start on the preparation boot; retirees never start.
// Primary restarts are allowed, including after reboot, unless a promotion is
// unresolved. A clean suppressed exit defeats stale loaded KeepAlive policy.
func (s Store) Startup(id, nodeID, executable, boot string) (func(), error) {
	root, close, err := s.open(false)
	if err != nil {
		return nil, err
	}
	defer close()
	c, err := s.read(root)
	if err != nil {
		return nil, err
	}
	g, ok := c.Generations[id]
	if !ok || c.NodeID != nodeID || executable != g.Executable {
		return nil, errors.New("generation startup identity mismatch")
	}
	if c.PendingReceipt != "" {
		return nil, errors.New("generation promotion unresolved; startup requires reconciliation")
	}
	if g.Role == "retiring" || (g.Role == "" && !hostboot.SameBoot(g.StartBootID, boot)) {
		return nil, ErrStartupSuppressed
	}
	if !hostboot.ValidIdentity(boot) {
		return nil, errors.New("unverified system boot identity")
	}
	if err := s.verifyExecutable(root, g); err != nil {
		return nil, err
	}
	release, err := lockDirectory(filepath.Join(s.Root(), id))
	if err != nil {
		return nil, fmt.Errorf("generation already running or process lock unavailable: %w", err)
	}
	if g.Role == "" {
		dir, err := root.OpenRoot(id)
		if err != nil {
			release()
			return nil, err
		}
		defer dir.Close()
		if err := durable.CreateFile(dir, "candidate-start.json", []byte("{}\n"), 0600); err != nil {
			release()
			if errors.Is(err, os.ErrExist) {
				return nil, ErrStartupSuppressed
			}
			return nil, err
		}
	}
	return release, nil
}

// Ready is a process-owned private startup point. A candidate waits before
// opening shared runtime state; a primary publishes after registration. It is not a claim that
// L1 promotion, renewal, or full shared-evidence support has completed.
type Ready struct {
	Phase        string `json:"phase"`
	GenerationID string `json:"agent_generation_id"`
	NodeID       string `json:"node_id"`
	Version      string `json:"binary_version"`
	Digest       string `json:"binary_sha256"`
	Binding
}

func (s Store) PublishReady(id, nodeID, version, executable, bootSession, systemBoot string) error {
	root, close, err := s.open(false)
	if err != nil {
		return err
	}
	defer close()
	c, err := s.read(root)
	if err != nil {
		return err
	}
	g, ok := c.Generations[id]
	if !ok || g.Role == "retiring" || c.NodeID != nodeID || version != g.Version || executable != g.Executable || bootSession == "" || !hostboot.ValidIdentity(systemBoot) {
		return errors.New("generation readiness identity mismatch")
	}
	if err := s.verifyExecutable(root, g); err != nil {
		return err
	}
	phase := "registered"
	if g.Role == "" {
		phase = "candidate_waiting"
		if bootSession != g.CandidateBootSessionID {
			return errors.New("candidate ready boot session differs from prepared identity")
		}
	}
	startID, err := s.processStart(os.Getpid())
	if err != nil {
		return err
	}
	return writePrivate(root, filepath.Join(id, "ready.json"), Ready{Phase: phase, GenerationID: id, NodeID: nodeID, Version: version, Digest: g.Digest, Binding: Binding{ProcessStartID: startID, PID: os.Getpid(), BootSessionID: bootSession, SystemBootID: systemBoot, ReadyAt: time.Now().UTC()}})
}

// BindReady requires an exact externally expected process binding and a fresh
// manager observation. Neither matching version nor a process launch is ready.
func (s Store) BindReady(ctx context.Context, id string, expected Binding, startedAfter time.Time) (Ready, error) {
	root, close, err := s.open(false)
	if err != nil {
		return Ready{}, err
	}
	defer close()
	c, err := s.read(root)
	if err != nil {
		return Ready{}, err
	}
	g, ok := c.Generations[id]
	if !ok || g.Role == "retiring" {
		return Ready{}, errors.New("generation cannot become ready")
	}
	r, err := s.ready(ctx, root, c, g, expected, startedAfter)
	if err != nil {
		return r, err
	}
	g.Process = &r.Binding
	c.Generations[id] = g
	c.Revision++
	return r, writePrivate(root, catalogName, c)
}
func (s Store) ready(ctx context.Context, root *os.Root, c Catalog, g Generation, expected Binding, startedAfter time.Time) (Ready, error) {
	var r Ready
	if err := s.verifyExecutable(root, g); err != nil {
		return r, err
	}
	if err := readPrivate(root, filepath.Join(g.ID, "ready.json"), &r); err != nil {
		return r, err
	}
	state, err := s.Manager.State(ctx, g.Label)
	if err != nil {
		return r, err
	}
	phase := "registered"
	if g.Role == "" {
		phase = "candidate_waiting"
		if expected.BootSessionID != g.CandidateBootSessionID {
			return r, errors.New("candidate expected process differs from prepared identity")
		}
	}
	if r.Phase != phase || r.GenerationID != g.ID || r.NodeID != c.NodeID || r.Digest != g.Digest || r.Version != g.Version || !state.Running || state.PID <= 0 || r.PID != state.PID || r.PID != expected.PID || expected.BootSessionID == "" || r.BootSessionID != expected.BootSessionID || !hostboot.SameBoot(r.SystemBootID, expected.SystemBootID) || startedAfter.IsZero() || r.ReadyAt.Before(startedAfter) || r.ReadyAt.After(time.Now().UTC()) || (!expected.ReadyAt.IsZero() && !r.ReadyAt.Equal(expected.ReadyAt)) {
		return r, errors.New("stale or mismatched generation readiness")
	}
	if r.ProcessStartID == "" || (expected.ProcessStartID != "" && r.ProcessStartID != expected.ProcessStartID) {
		return r, errors.New("missing or mismatched process start identity")
	}
	if err := requireLifetimeLock(filepath.Join(s.Root(), g.ID)); err != nil {
		return r, err
	}
	observed, err := s.processStart(state.PID)
	if err != nil {
		return r, err
	}
	if observed != r.ProcessStartID {
		return r, errors.New("generation readiness belongs to a different process incarnation")
	}
	return r, nil
}

func (s Store) processStart(pid int) (string, error) {
	reader := s.ProcessStartIdentity
	if reader == nil {
		reader = processStartIdentity
	}
	identity, err := reader(pid)
	if err != nil {
		return "", err
	}
	if identity == "" {
		return "", errors.New("unverified process start identity")
	}
	return identity, nil
}

// AwaitActivation keeps a staged candidate alive without opening participants,
// stores or running payloads. Only a committed local primary transition opens
// startup. The caller holds its generation process lock throughout this wait.
func (s Store) AwaitActivation(ctx context.Context, id string) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		c, err := s.Read()
		if err != nil {
			return err
		}
		g, ok := c.Generations[id]
		if !ok {
			return errors.New("candidate removed during activation")
		}
		if g.Role == "retiring" {
			return ErrStartupSuppressed
		}
		if c.PendingReceipt == "" && c.Primary == id && g.Role == "primary" {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
