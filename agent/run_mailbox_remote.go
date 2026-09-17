package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

// A remote mailbox is one the agent cannot open: an OCI attempt's handoff
// volume is helper-owned inside the node, so the publisher reaches it through
// the helper's bounded, attempt-scoped read path instead of an os.Root. Every
// rule above the mailboxFS seam is unchanged; what differs is stated here.
//
// Two things differ, and both make the OCI mailbox stricter than the process
// one rather than looser. The bookkeeping under `.published` lives on the agent
// side of the boundary instead of inside the workload-writable directory, so it
// is not merely validated but unreachable. And every read is authorized against
// the live attempt that declared the mailbox, so publication stops the moment
// that attempt is reaped -- which is why the final drain runs before the
// runtime is reaped and not after.
const (
	// runMailboxRemoteCallTimeout bounds one helper call. It is deliberately
	// short: the finalization budget in slice A covers a whole drain, and a
	// helper that has stopped answering must not consume it one call at a time.
	// A call that outlives the budget anyway leaves the mailbox incomplete and
	// the handoff volume retained, which is the designed degradation.
	runMailboxRemoteCallTimeout = 2 * time.Second
	// runMailboxStateDirectoryName is the agent-local root under which each
	// remote attempt's bookkeeping lives.
	runMailboxStateDirectoryName = "run-mailbox"
)

// helperMailboxFS is the publisher's view of a helper-owned mailbox. It holds
// no filesystem state: confinement, the regular-file proof and the size bounds
// are all the helper's, because the directory is on the other side of a trust
// boundary the agent does not cross.
type helperMailboxFS struct {
	runtime   workloadrunner.RunMailboxRuntime
	reference workloadrunner.RunMailboxReference
	// parent is the mailbox's publication context. Fencing it cancels an
	// in-flight helper call rather than waiting for the per-call bound.
	parent  context.Context
	timeout time.Duration
}

func (fs *helperMailboxFS) call() (context.Context, context.CancelFunc) {
	timeout := fs.timeout
	if timeout <= 0 {
		timeout = runMailboxRemoteCallTimeout
	}
	return context.WithTimeout(fs.parent, timeout)
}

func (fs *helperMailboxFS) list(limit int) ([]string, bool, error) {
	ctx, cancel := fs.call()
	defer cancel()
	return fs.runtime.ListRunMailbox(ctx, fs.reference, limit)
}

func (fs *helperMailboxFS) read(name string, limit int) ([]byte, bool, error) {
	ctx, cancel := fs.call()
	defer cancel()
	return fs.runtime.ReadRunMailbox(ctx, fs.reference, name, limit)
}

func (fs *helperMailboxFS) remove(name string) error {
	ctx, cancel := fs.call()
	defer cancel()
	return fs.runtime.RemoveRunMailboxEntry(ctx, fs.reference, name)
}

// close has nothing to release: the helper session is the adapter's and
// outlives every attempt that borrows it.
func (fs *helperMailboxFS) close() error { return nil }

// remoteRunMailboxAvailable reports whether this claim can have a mailbox the
// helper is able to serve. It is the OCI counterpart of runMailboxAvailable and
// asks the same questions, except that the handoff directory is the helper's
// and therefore not the agent's to inspect.
func remoteRunMailboxAvailable(spec contract.JobSpec) bool {
	return spec.Kind == contract.JobKindOCI && spec.Class == contract.JobClassOneShot &&
		!contract.IsComputerExecution(spec.Execution) &&
		strings.TrimSpace(spec.Execution.Env[contract.EnvL3Endpoint]) != "" &&
		validRunMailboxSegment(strings.TrimSpace(spec.Execution.Env[contract.EnvRunID])) &&
		strings.TrimSpace(spec.Execution.SensitiveEnv[contract.EnvRunToken]) != ""
}

// runMailboxSeed is the parameters document the runtime delivers into the
// mailbox it creates. The same validation the agent applies to its own
// params.json applies here: an oversize or non-object document is not
// delivered, because a workload reading garbage is worse than reading nothing.
func runMailboxSeed(spec contract.JobSpec) *workloadrunner.RunMailboxSeed {
	seed := &workloadrunner.RunMailboxSeed{RunID: strings.TrimSpace(spec.Execution.Env[contract.EnvRunID])}
	trimmed := strings.TrimSpace(spec.Labels[contract.LabelRunParams])
	if trimmed == "" || len(trimmed) > MaxRunMailboxParamsBytes || !strings.HasPrefix(trimmed, "{") {
		return seed
	}
	seed.Params = append([]byte(trimmed), '\n')
	return seed
}

// prepareRemoteRunMailbox creates the agent-local bookkeeping for one OCI
// attempt and binds the publisher to the helper's read path. It creates nothing
// inside the handoff volume: the runtime seeds that before the workload starts,
// because only the runtime knows which uid the container will run as.
func prepareRemoteRunMailbox(claim l1.Claim, stateRoot string, runtime workloadrunner.RunMailboxRuntime,
	authority workloadrunner.AttemptAuthority, appender runLedgerAppender, poll time.Duration,
	clock Clock, logf func(string, ...any)) (*runMailbox, error) {
	spec := claim.Job.Spec
	runID := strings.TrimSpace(spec.Execution.Env[contract.EnvRunID])
	attemptID := claim.Lease.AttemptID
	published, err := openRemoteRunMailboxState(stateRoot, attemptID)
	if err != nil {
		return nil, err
	}
	mailbox := newRunMailbox(runMailboxSettings{
		directory: contract.OCIContainerHandoffDirectory + "/" + runMailboxDirectoryName + "/" + runID,
		runID:     runID,
		attemptID: attemptID,
		runToken:  strings.TrimSpace(spec.Execution.SensitiveEnv[contract.EnvRunToken]),
		published: published,
		appender:  appender,
		poll:      poll,
		clock:     clock,
		logf:      logf,
		remote:    true,
	})
	mailbox.fs = &helperMailboxFS{
		runtime: runtime,
		reference: workloadrunner.RunMailboxReference{
			Authority: authority, OwnerKey: handoffOwnerRunID(spec), RunID: runID,
		},
		parent:  mailbox.publicationContext,
		timeout: runMailboxRemoteCallTimeout,
	}
	mailbox.loadState()
	return mailbox, nil
}

// openRemoteRunMailboxState opens this attempt's own bookkeeping directory
// under an agent-owned root. It is per attempt, not per run: a retried attempt
// never inherits another attempt's reservations, exactly as the in-volume
// bookkeeping refuses a state file written by a different attempt.
func openRemoteRunMailboxState(stateRoot, attemptID string) (*os.Root, error) {
	if strings.TrimSpace(stateRoot) == "" {
		return nil, errors.New("run mailbox state root is not configured")
	}
	component := runMailboxStateComponent(attemptID)
	directory := filepath.Join(filepath.Clean(stateRoot), runMailboxStateDirectoryName, component)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("create run mailbox state directory: %w", err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, fmt.Errorf("open run mailbox state directory: %w", err)
	}
	return root, nil
}

// runMailboxStateComponent maps an attempt ID to one safe path component. An
// attempt ID is L1's, not a workload's, but it still becomes a directory name
// here, so it is encoded rather than trusted.
func runMailboxStateComponent(attemptID string) string {
	var builder strings.Builder
	for _, value := range attemptID {
		switch {
		case value >= 'a' && value <= 'z', value >= 'A' && value <= 'Z', value >= '0' && value <= '9',
			value == '-', value == '_':
			builder.WriteRune(value)
		default:
			builder.WriteByte('_')
		}
		if builder.Len() >= 96 {
			break
		}
	}
	if builder.Len() == 0 {
		return "attempt"
	}
	return builder.String()
}

// discardRemoteRunMailboxState removes the agent-local bookkeeping for an
// attempt whose evidence the ledger has, so the directory does not accumulate.
// Bookkeeping for an attempt that ended incomplete is deliberately kept: it is
// what lets a later republication reuse its reservations instead of charging
// the same document twice.
func discardRemoteRunMailboxState(stateRoot, attemptID string) error {
	if strings.TrimSpace(stateRoot) == "" {
		return nil
	}
	directory := filepath.Join(filepath.Clean(stateRoot), runMailboxStateDirectoryName, runMailboxStateComponent(attemptID))
	if err := os.RemoveAll(directory); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
