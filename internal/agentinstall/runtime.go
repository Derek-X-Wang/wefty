package agentinstall

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/durable"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
)

// VerificationRefusal reports an observed identity, ownership, inventory or process-state
// mismatch. Reads, service-manager calls and OS failures keep their own errors.
type VerificationRefusal struct{ Message string }

func (e *VerificationRefusal) Error() string { return e.Message }

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
		return "", &VerificationRefusal{Message: "generation is not eligible to start"}
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
		if state.Running {
			if s.beforePlistOpen != nil {
				if err := s.beforePlistOpen(); err != nil {
					return "", err
				}
			}
			old, err := readRetainedPlist(folder, filepath.Base(g.Plist))
			if err != nil {
				return "", err
			}
			if bytes.Equal(old, payload) && g.PlistDigest == digestBytes(payload) {
				return launchagent.Unchanged, nil
			}
		}
		return "", &VerificationRefusal{Message: "retained generation definition cannot be replaced or restarted"}
	}
	g.PlistDigest = digestBytes(payload)
	c.Generations[id] = g
	c.Revision++
	if err := writePrivate(root, catalogName, c); err != nil {
		return "", err
	}
	if s.afterCandidateWrite != nil {
		if err := s.afterCandidateWrite("plist_intent_recorded"); err != nil {
			return "", err
		}
	}
	return s.Manager.LoadGeneration(ctx, folder, g.Plist, g.Label, payload)
}

// Startup runs before profiles, participants, managed roots or recovery open.
// It holds a generation-specific process lock for the entire agent lifetime.
// Candidates get a single start on the preparation boot; retirees never start.
// Primary restarts are allowed, including after reboot, unless a promotion is
// unresolved. A clean suppressed exit defeats stale loaded KeepAlive policy.
func (s Store) Startup(id, nodeID, executable, boot string) (func(), error) {
	lifetime, err := s.StartupWithLifetime(id, nodeID, executable, boot)
	if err != nil {
		return nil, err
	}
	return lifetime.Close, nil
}

// GenerationLifetime owns the one installed generation-directory lock. The
// installed process keeps it until runtime shutdown; the runtime borrows it
// once instead of acquiring a second per-generation lock.
type GenerationLifetime struct {
	mu                   sync.Mutex
	nodeID, generationID string
	release              func()
	runtimeClaimed       bool
}

// ClaimRuntime validates and consumes the process-local ownership proof without
// opening or flocking the directory again. It grants no primary authority.
func (l *GenerationLifetime) ClaimRuntime(nodeID, generationID string) error {
	if l == nil {
		return &VerificationRefusal{Message: "generation lifetime ownership is missing"}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.release == nil || l.runtimeClaimed || l.nodeID != nodeID || l.generationID != generationID {
		return &VerificationRefusal{Message: "generation lifetime ownership is released, already claimed, or mismatched"}
	}
	l.runtimeClaimed = true
	return nil
}

// Close releases the lifetime lock after the borrowing runtime has stopped.
func (l *GenerationLifetime) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.release != nil {
		l.release()
		l.release = nil
	}
}

// StartupWithLifetime is Startup with a borrowable process-local ownership
// proof for the runtime. Readiness still independently verifies the same held
// directory lock and the kernel process-start identity.
func (s Store) StartupWithLifetime(id, nodeID, executable, boot string) (*GenerationLifetime, error) {
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
		return nil, &VerificationRefusal{Message: "generation startup identity mismatch"}
	}
	if c.PendingReceipt != "" {
		return nil, &VerificationRefusal{Message: "generation promotion unresolved; startup requires reconciliation"}
	}
	if g.Role == "retiring" || g.AbortOperation != "" || (g.Role == "" && !hostboot.SameBoot(g.StartBootID, boot)) {
		return nil, ErrStartupSuppressed
	}
	if !hostboot.ValidIdentity(boot) {
		return nil, &VerificationRefusal{Message: "unverified system boot identity"}
	}
	if err := s.verifyExecutable(root, g); err != nil {
		return nil, err
	}
	held, err := holdGenerationDirectory(root, id)
	if err != nil {
		return nil, fmt.Errorf("generation already running or process lock unavailable: %w", err)
	}
	release := held.Close
	if g.Role == "" {
		if err := held.verify(root, id); err != nil {
			release()
			return nil, err
		}
		dir := held.root
		if err := durable.CreateFile(dir, "candidate-start.json", []byte("{}\n"), 0600); err != nil {
			release()
			if errors.Is(err, os.ErrExist) {
				return nil, ErrStartupSuppressed
			}
			return nil, err
		}
	}
	return &GenerationLifetime{nodeID: nodeID, generationID: id, release: release}, nil
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
		return &VerificationRefusal{Message: "generation readiness identity mismatch"}
	}
	if err := s.verifyExecutable(root, g); err != nil {
		return err
	}
	phase := "registered"
	if g.Role == "" {
		phase = "candidate_waiting"
		if bootSession != g.CandidateBootSessionID {
			return &VerificationRefusal{Message: "candidate ready boot session differs from prepared identity"}
		}
	}
	startID, err := s.processStart(os.Getpid())
	if err != nil {
		return err
	}
	ready := Ready{Phase: phase, GenerationID: id, NodeID: nodeID, Version: version, Digest: g.Digest, Binding: Binding{ProcessStartID: startID, PID: os.Getpid(), BootSessionID: bootSession, SystemBootID: systemBoot, ReadyAt: time.Now().UTC()}}
	if err := writePrivate(root, filepath.Join(id, "ready.json"), ready); err != nil {
		return err
	}
	// Primary restarts publish a fresh incarnation. Keep the catalog bound to
	// that exact registered process, rather than retaining candidate readiness.
	if g.Role == "primary" && c.PendingReceipt == "" {
		g.Process = &ready.Binding
		c.Generations[id] = g
		c.Revision++
		return writePrivate(root, catalogName, c)
	}
	return nil
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
		return Ready{}, &VerificationRefusal{Message: "generation cannot become ready"}
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
	if err := readPendingRecord(root, filepath.Join(g.ID, "ready.json"), &r); err != nil {
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
			return r, &VerificationRefusal{Message: "candidate expected process differs from prepared identity"}
		}
	}
	if r.Phase != phase || r.GenerationID != g.ID || r.NodeID != c.NodeID || r.Digest != g.Digest || r.Version != g.Version || !state.Running || state.PID <= 0 || r.PID != state.PID || r.PID != expected.PID || expected.BootSessionID == "" || r.BootSessionID != expected.BootSessionID || !hostboot.SameBoot(r.SystemBootID, expected.SystemBootID) || startedAfter.IsZero() || r.ReadyAt.Before(startedAfter) || r.ReadyAt.After(time.Now().UTC()) || (!expected.ReadyAt.IsZero() && !r.ReadyAt.Equal(expected.ReadyAt)) {
		return r, &VerificationRefusal{Message: "stale or mismatched generation readiness"}
	}
	if r.ProcessStartID == "" || (expected.ProcessStartID != "" && r.ProcessStartID != expected.ProcessStartID) {
		return r, &VerificationRefusal{Message: "missing or mismatched process start identity"}
	}
	if err := requireLifetimeLock(filepath.Join(s.Root(), g.ID)); err != nil {
		return r, err
	}
	observed, err := s.processStart(state.PID)
	if err != nil {
		return r, err
	}
	if observed != r.ProcessStartID {
		return r, &VerificationRefusal{Message: "generation readiness belongs to a different process incarnation"}
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
		return "", &VerificationRefusal{Message: "unverified process start identity"}
	}
	return identity, nil
}

// AwaitActivation keeps a staged candidate alive without opening participants,
// stores or running payloads. Only a committed local primary transition opens
// startup. The caller holds its generation process lock throughout this wait.
func (s Store) AwaitActivation(ctx context.Context, id string, reconcile ...func(context.Context) error) error {
	var nextReconcile time.Time
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !time.Now().Before(nextReconcile) {
			nextReconcile = time.Now().Add(time.Second)
			for _, resolve := range reconcile {
				if err := resolve(ctx); err != nil {
					return err
				}
			}
		}
		c, err := s.Read()
		if err != nil {
			return err
		}
		g, ok := c.Generations[id]
		if !ok {
			return &VerificationRefusal{Message: "candidate removed during activation"}
		}
		if g.Role == "retiring" || g.AbortOperation != "" {
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

// WithPrimary holds the same catalog lock as promotion while a short Node-wide
// local write runs. It grants no L1 authority and must never enclose an RPC.
func (s Store) WithPrimary(id string, write func() error) error {
	root, close, err := s.open(false)
	if err != nil {
		return err
	}
	defer close()
	c, err := s.read(root)
	if err != nil {
		return err
	}
	if c.PendingReceipt != "" || c.Primary != id || c.Generations[id].Role != "primary" {
		return ErrStartupSuppressed
	}
	return write()
}

// VerifyRunning verifies an existing live generation without rebinding it or
// changing its role. A PID alone is not process-incarnation or survival proof.
func (s Store) VerifyRunning(ctx context.Context, id, boot string) error {
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
	if !ok || g.Process == nil || !hostboot.SameBoot(g.Process.SystemBootID, boot) {
		return &VerificationRefusal{Message: "live generation binding unavailable"}
	}
	if err = s.verifyExecutable(root, g); err != nil {
		return err
	}
	state, err := s.Manager.State(ctx, g.Label)
	if err != nil {
		return err
	}
	if !state.Running || state.PID != g.Process.PID {
		return &VerificationRefusal{Message: "live generation process missing"}
	}
	if err = requireLifetimeLock(filepath.Join(s.Root(), id)); err != nil {
		return err
	}
	start, err := s.processStart(state.PID)
	if err != nil {
		return err
	}
	if start != g.Process.ProcessStartID {
		return &VerificationRefusal{Message: "live generation process incarnation changed"}
	}
	return nil
}
