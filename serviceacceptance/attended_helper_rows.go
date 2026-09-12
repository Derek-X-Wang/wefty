package serviceacceptance

// Attended exclusive-session helper rows (#410).
//
// docs/acceptance/m3-lima-transport.md opens two exclusive helper-session
// windows on owner hardware, with dev.wefty.agent booted out, and asks the
// owner to drive Run/mount-validate/DialAttemptPort/DialHostBridge and one
// fault-and-recovery sequence directly against the guest socket. Nothing
// shipped that, so seven rows stayed NOT-RUN across three attended runs.
//
// This file is the driver behind the two service_acceptance entrypoints. It
// adds no helper protocol and no product surface: every call is an existing
// ocihelper client call, and the only new thing is the assertion and evidence
// shape the receipt gate already requires. Everything here is unexported and
// compiled by the ordinary lane so `go vet ./...` and `go test ./...` cover
// it; the entrypoints that need a real socket live behind the build tag.
//
// The assertions are the rows. In particular every negative case must be
// refused with the exact typed helper error code, because a negative that is
// silently accepted would turn these rows into a fixture that proves nothing.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
	ocirunner "github.com/Derek-X-Wang/wefty/runner/oci"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// attendedRow is the receipt row shape from docs/acceptance/m3-lima-transport.md.
// The fields the gate reads for these seven rows are status, session_id,
// command, exit_code and -- for the three loss rows -- helper_generations,
// capability_revisions and inventories. The remaining non-omitempty fields are
// emitted empty so a merged receipt still decodes under DisallowUnknownFields.
type attendedRow struct {
	Status              string            `json:"status"`
	Reason              string            `json:"reason,omitempty"`
	BlockedBy           int               `json:"blocked_by,omitempty"`
	SessionID           string            `json:"session_id"`
	Command             []string          `json:"command"`
	ExitCode            int               `json:"exit_code"`
	HelperGenerations   []uint64          `json:"helper_generations"`
	CapabilityRevisions []int64           `json:"capability_revisions"`
	Inventories         []json.RawMessage `json:"inventories"`
	RoundTrip           bool              `json:"round_trip"`
	DynamicListeners    map[string]bool   `json:"dynamic_listeners"`
	AttemptIDs          []string          `json:"attempt_ids"`
	TopLevelDigests     []string          `json:"top_level_digests"`
	PlatformDigests     []string          `json:"platform_digests"`
	PayloadExecutions   int               `json:"payload_executions"`
	StdoutMarkers       []string          `json:"stdout_markers"`
	StderrMarkers       []string          `json:"stderr_markers"`
	HandoffMarkerBytes  []string          `json:"handoff_marker_bytes"`
	HandoffAbsent       bool              `json:"handoff_absent_after_completion"`
}

// attendedFragment is what an entrypoint writes to WEFTY_ATTENDED_ROWS_OUT.
// The owner merges the fragments into the receipt; the runbook carries a jq
// one-liner for that.
type attendedFragment struct {
	Rows map[string]attendedRow `json:"rows"`
}

// newAttendedRow returns a row with every non-omitempty slice and map present,
// so an emitted row never decodes as null in the merged receipt.
func newAttendedRow(sessionID string, command []string) attendedRow {
	return attendedRow{
		Status: "FAIL", SessionID: sessionID, Command: slices.Clone(command), ExitCode: 1,
		HelperGenerations: []uint64{}, CapabilityRevisions: []int64{}, Inventories: []json.RawMessage{},
		DynamicListeners: map[string]bool{}, AttemptIDs: []string{}, TopLevelDigests: []string{},
		PlatformDigests: []string{}, StdoutMarkers: []string{}, StderrMarkers: []string{},
		HandoffMarkerBytes: []string{},
	}
}

// pass marks a row successful. It is deliberately the only way to reach
// ExitCode 0, so a row that returns early on an error can never be PASS.
func (row *attendedRow) pass(reason string) {
	row.Status = "PASS"
	row.ExitCode = 0
	row.appendReason(reason)
}

func (row *attendedRow) fail(err error) {
	row.Status = "FAIL"
	row.ExitCode = 1
	row.appendReason(err.Error())
}

func (row *attendedRow) appendReason(text string) {
	text = strings.TrimSpace(text)
	if text == "" {
		return
	}
	if row.Reason == "" {
		row.Reason = text
		return
	}
	row.Reason += "; " + text
}

func writeAttendedFragment(path string, rows map[string]attendedRow) error {
	if path == "" {
		return errors.New("WEFTY_ATTENDED_ROWS_OUT is required")
	}
	payload, err := json.MarshalIndent(attendedFragment{Rows: rows}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(payload, '\n'), 0o600)
}

// attendedSession is the exact slice of *ocihelper.Session these rows use.
// Narrowing it is what lets the unit tests drive every assertion with a fake.
type attendedSession interface {
	Handshake() ocihelper.AcquireSessionResponse
	HealthError() error
	Run(context.Context, ocihelper.RunRequest) (ocihelper.RunResponse, error)
	Watch(context.Context, ocihelper.WatchRequest, func(ocihelper.WatchEvent) error) error
	Delete(context.Context, ocihelper.DeleteRequest) (ocihelper.DeleteResponse, error)
	Verify(context.Context, ocihelper.VerifyRequest) (ocihelper.VerifyResponse, error)
	DialAttemptPort(context.Context, ocihelper.DialAttemptPortRequest) (net.Conn, error)
	DialHostBridge(context.Context, ocihelper.DialHostBridgeRequest) (net.Conn, error)
}

// attendedBarrier is the boot barrier as the loss rows use it.
type attendedBarrier interface {
	Ensure(context.Context) error
	Invalidate()
	SweepReceipt() (ocihelper.VerifiedSweepReceipt, bool)
	CapabilityReasonCode() contract.CapabilityReasonCode
	Session() (attendedSession, error)
}

// liveBarrier adapts the real boot barrier to attendedBarrier. It exists only
// because BootBarrier.Session returns the concrete session type.
type liveBarrier struct{ barrier *ocihelper.BootBarrier }

func newLiveBarrier(barrier *ocihelper.BootBarrier) *liveBarrier {
	return &liveBarrier{barrier: barrier}
}

func (live *liveBarrier) Ensure(ctx context.Context) error { return live.barrier.Ensure(ctx) }
func (live *liveBarrier) Invalidate()                      { live.barrier.Invalidate() }

func (live *liveBarrier) SweepReceipt() (ocihelper.VerifiedSweepReceipt, bool) {
	return live.barrier.SweepReceipt()
}

func (live *liveBarrier) CapabilityReasonCode() contract.CapabilityReasonCode {
	return live.barrier.CapabilityReasonCode()
}

func (live *liveBarrier) Session() (attendedSession, error) {
	session, err := live.barrier.Session()
	if err != nil {
		return nil, err
	}
	return session, nil
}

// newLiveProbe returns the real functional probe: the same pinned image,
// runc-v2 task, Wait-before-Start, Watch and verified Delete path production
// attempts use.
func newLiveProbe(barrier *ocihelper.BootBarrier, config attendedConfig) func(context.Context) error {
	adapter := ocirunner.NewAdapter(barrier)
	return func(ctx context.Context) error {
		return adapter.Probe(ctx, config.NodeID, config.BootSessionID, config.Reference, config.Digest, config.ProbeDeadman)
	}
}

// capabilityLedger is the driver's own local OCI capability observation.
//
// Capability revisions are an L1 fact that the agent publishes, and both
// exclusive windows deliberately boot dev.wefty.agent out, so no L1 revision
// exists inside the window at all. The runbook's own wording for the first
// loss step is "local OCI capability becomes restrictive", so a local
// observation is the faithful thing to record -- but it must never be
// mistaken for an L1 revision, which is why every loss row's reason says so
// in one line and why the numbers are only ever produced by an observed
// transition, never fabricated to satisfy the gate's two-entry minimum.
type capabilityLedger struct {
	revision     int64
	observations []contract.CapabilityObservation
	now          func() time.Time
}

func newCapabilityLedger(now func() time.Time) *capabilityLedger {
	if now == nil {
		now = time.Now
	}
	return &capabilityLedger{now: now}
}

func (ledger *capabilityLedger) withdraw(reason contract.CapabilityReasonCode) contract.CapabilityObservation {
	ledger.revision++
	observation := contract.CapabilityObservation{
		Revision: ledger.revision, Capabilities: map[string]bool{"kind:oci": false},
		ObservedAt: ledger.now(), MissingCapabilities: []string{"kind:oci"}, ReasonCode: reason,
	}
	ledger.observations = append(ledger.observations, observation)
	return observation
}

func (ledger *capabilityLedger) reopen() contract.CapabilityObservation {
	ledger.revision++
	observation := contract.CapabilityObservation{
		Revision: ledger.revision, Capabilities: map[string]bool{"kind:oci": true},
		ObservedAt: ledger.now(), MissingCapabilities: []string{},
	}
	ledger.observations = append(ledger.observations, observation)
	return observation
}

const capabilityRevisionNote = "capability_revisions are this driver's own local OCI capability observations " +
	"during the exclusive window; no L1 revision exists here because dev.wefty.agent is booted out, and L1 " +
	"revision publication is proven separately by the Installed boot topology rows"

// checkRefusal is the single gate every negative case passes through. A nil
// error means the helper accepted something the runbook requires it to
// refuse, which fails the row; so does a refusal that is not the typed code
// the row expects, because an unrelated failure is not evidence of the
// refusal being tested.
func checkRefusal(label string, err error, want ocihelper.ErrorCode) (ocihelper.ErrorCode, error) {
	if err == nil {
		return "", fmt.Errorf("%s: helper accepted a case the row requires it to refuse", label)
	}
	var rpcErr *ocihelper.RPCError
	if !errors.As(err, &rpcErr) {
		return "", fmt.Errorf("%s: refusal is not a typed helper error (%v)", label, err)
	}
	if rpcErr.Code != want {
		return rpcErr.Code, fmt.Errorf("%s: refused with %q, want %q", label, rpcErr.Code, want)
	}
	return rpcErr.Code, nil
}

// logEvidence is the ordered, per-stream log observation task_logs_delete needs.
type logEvidence struct {
	stdout, stderr []string
	sequences      map[string][]uint64
	gaps           int
	seals          map[string]bool
	result         *ocihelper.WatchResponse
}

func collectLogEvidence(ctx context.Context, session attendedSession, authority ocihelper.AttemptAuthority) (logEvidence, error) {
	evidence := logEvidence{sequences: map[string][]uint64{}, seals: map[string]bool{}}
	err := session.Watch(ctx, ocihelper.WatchRequest{Authority: authority}, func(event ocihelper.WatchEvent) error {
		if frame := event.Log; frame != nil {
			if frame.Gap != nil {
				evidence.gaps++
			}
			evidence.sequences[frame.Stream] = append(evidence.sequences[frame.Stream], frame.Sequence)
			switch frame.Stream {
			case "stdout":
				evidence.stdout = append(evidence.stdout, string(frame.Bytes))
			case "stderr":
				evidence.stderr = append(evidence.stderr, string(frame.Bytes))
			}
		}
		if seal := event.Seal; seal != nil {
			evidence.seals[seal.Stream] = seal.Complete
		}
		if event.Result != nil {
			completion := *event.Result
			evidence.result = &completion
		}
		return nil
	})
	return evidence, err
}

// checkLogEvidence proves ordered, distinct stdout and stderr frames and a
// truthful terminal exit 0. A gap frame fails the row: the helper is telling
// us the log evidence is incomplete, and the row is about that evidence.
func checkLogEvidence(evidence logEvidence, stdoutMarker, stderrMarker string) error {
	if evidence.gaps != 0 {
		return fmt.Errorf("log evidence carried %d gap frames", evidence.gaps)
	}
	for _, stream := range []string{"stdout", "stderr"} {
		sequences := evidence.sequences[stream]
		if len(sequences) == 0 {
			return fmt.Errorf("%s produced no log frames", stream)
		}
		for index := 1; index < len(sequences); index++ {
			if sequences[index] <= sequences[index-1] {
				return fmt.Errorf("%s sequences are not strictly increasing: %v", stream, sequences)
			}
		}
	}
	if !strings.Contains(strings.Join(evidence.stdout, ""), stdoutMarker) {
		return fmt.Errorf("stdout did not carry %q", stdoutMarker)
	}
	if !strings.Contains(strings.Join(evidence.stderr, ""), stderrMarker) {
		return fmt.Errorf("stderr did not carry %q", stderrMarker)
	}
	if strings.Contains(strings.Join(evidence.stdout, ""), stderrMarker) ||
		strings.Contains(strings.Join(evidence.stderr, ""), stdoutMarker) {
		return errors.New("stdout and stderr frames were not distinct")
	}
	result := evidence.result
	if result == nil || result.ExitCode == nil {
		return errors.New("watch produced no terminal result")
	}
	if *result.ExitCode != 0 || result.Signal != "" || result.RuntimeFailure != "" {
		return fmt.Errorf("terminal result = %+v, want exit 0", *result)
	}
	if result.LogEvidenceIncomplete {
		return errors.New("helper reported incomplete log evidence")
	}
	// The helper seals each stream at its pipe-EOF boundary, Complete when the
	// tail drained and Complete=false with a reason when it did not. An
	// unsealed or incomplete stream means the frames above are not the whole
	// stream, which is exactly what this row claims to have observed.
	for _, stream := range []string{"stdout", "stderr"} {
		complete, sealed := evidence.seals[stream]
		if !sealed {
			return fmt.Errorf("%s was never sealed", stream)
		}
		if !complete {
			return fmt.Errorf("%s seal is incomplete", stream)
		}
	}
	return nil
}

// deleteAndVerify is the positive Delete plus the independent absent Verify
// every attempt-bearing row ends with.
func deleteAndVerify(ctx context.Context, session attendedSession, authority ocihelper.AttemptAuthority) error {
	deleted, err := session.Delete(ctx, ocihelper.DeleteRequest{Authority: authority})
	if err != nil {
		return fmt.Errorf("delete attempt: %w", err)
	}
	if !deleted.Deleted {
		return errors.New("delete did not report positive removal")
	}
	verification, err := session.Verify(ctx, ocihelper.VerifyRequest{Scope: ocihelper.VerifyAttempt, Authority: &authority})
	if err != nil {
		return fmt.Errorf("verify attempt absence: %w", err)
	}
	if !verification.Absent {
		return fmt.Errorf("attempt residue remains after delete: %+v", verification.Inventory)
	}
	return nil
}

// reapAttempt is best-effort cleanup for a row that failed mid-attempt. A
// leaked attempt would contaminate every later row in the same window.
func reapAttempt(session attendedSession, authority ocihelper.AttemptAuthority) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = session.Delete(ctx, ocihelper.DeleteRequest{Authority: authority})
}

// attendedConfig is everything the owner supplies through the environment.
type attendedConfig struct {
	SessionID     string
	Command       []string
	NodeID        string
	BootSessionID string
	Reference     string
	Digest        string
	MountRoot     string
	LimaInstance  string
	// Deadman is the attempt deadman requested for row workloads. It is
	// deliberately generous because nothing here renews it, and boundedDeadman
	// clamps it to whatever ceiling the helper advertises.
	Deadman time.Duration
	// BridgeWait bounds how long the fallback row waits for the payload's one
	// authenticated request to reach the host origin. Zero means the default.
	BridgeWait time.Duration
	// ProbeDeadman is separate because ocirunner.Adapter.Probe passes its
	// deadman straight through without clamping, and the helper refuses a
	// request above its ceiling outright.
	ProbeDeadman time.Duration
	// GuestPath reads a path inside the Lima guest, so the mount row can prove
	// the host-to-guest translation from the guest's own side rather than
	// inferring it. It is required: without it the mount row cannot make the
	// claim its reason states.
	GuestPath func(context.Context, string) (string, error)
	Now       func() time.Time
}

func (config attendedConfig) authority(class, id string) ocihelper.AttemptAuthority {
	return ocihelper.AttemptAuthority{
		NodeID: config.NodeID, BootSessionID: config.BootSessionID,
		JobID: "attended-" + id, AttemptID: "attended-" + id,
		FencingToken: "attended-" + id, Class: class, RemovalGeneration: "attempt",
	}
}

// boundedDeadman keeps the requested attempt deadman inside the helper's own
// advertised ceiling, which reserveAttempt refuses outright.
func boundedDeadman(session attendedSession, requested time.Duration) time.Duration {
	if requested <= 0 {
		requested = 60 * time.Second
	}
	if ceiling := session.Handshake().MaximumAttemptDeadman; ceiling > 0 && requested > ceiling {
		return ceiling
	}
	return requested
}

// ---------------------------------------------------------------------------
// Row 2: task_logs_delete
// ---------------------------------------------------------------------------

func driveTaskLogsDelete(ctx context.Context, session attendedSession, config attendedConfig) attendedRow {
	const stdoutMarker = "wefty-attended-task-stdout"
	const stderrMarker = "wefty-attended-task-stderr"
	row := newAttendedRow(config.SessionID, config.Command)
	authority := config.authority(contract.JobClassOneShot, "task-logs-delete")
	row.AttemptIDs = []string{authority.AttemptID}

	response, err := session.Run(ctx, ocihelper.RunRequest{
		Authority: authority, InitialDeadman: boundedDeadman(session, config.Deadman),
		Workload: ocihelper.WorkloadInput{
			ImageReference: config.Reference, ImageDigest: config.Digest,
			Argv: []string{"/bin/sh", "-c", fmt.Sprintf(
				"printf '%s\\n'; printf '%s\\n' >&2; exit 0", stdoutMarker, stderrMarker)},
		},
	})
	if err != nil {
		row.fail(fmt.Errorf("run task: %w", err))
		return row
	}
	defer reapAttempt(session, authority)
	if !response.Started || response.StartedAt.IsZero() || response.Image == nil {
		row.fail(fmt.Errorf("run returned no authoritative Started evidence: %+v", response))
		return row
	}
	row.TopLevelDigests = []string{response.Image.TopLevelDigest}
	row.PlatformDigests = []string{response.Image.PlatformManifestDigest}

	evidence, err := collectLogEvidence(ctx, session, authority)
	if err != nil {
		row.fail(fmt.Errorf("watch task: %w", err))
		return row
	}
	if err := checkLogEvidence(evidence, stdoutMarker, stderrMarker); err != nil {
		row.fail(err)
		return row
	}
	row.StdoutMarkers = []string{stdoutMarker}
	row.StderrMarkers = []string{stderrMarker}
	row.PayloadExecutions = 1
	if err := deleteAndVerify(ctx, session, authority); err != nil {
		row.fail(err)
		return row
	}
	row.pass(fmt.Sprintf(
		"Started with StartedAt %s; %d stdout and %d stderr frames, strictly ordered per stream and distinct; "+
			"terminal exit 0 with no signal or runtime failure; Delete positive; independent attempt Verify absent",
		response.StartedAt.UTC().Format(time.RFC3339Nano), len(evidence.sequences["stdout"]), len(evidence.sequences["stderr"])))
	return row
}

// ---------------------------------------------------------------------------
// Row 3: mount_validation
// ---------------------------------------------------------------------------

// mountNegative is one refusal the runbook requires, with the exact typed code
// the helper answers it with. The codes are grouped by where the refusal
// happens: wire validation (reserved-target overlap), host-to-guest
// translation (the root itself and anything outside it), and filesystem
// authority during spec construction (symlink component, socket, device, FIFO).
type mountNegative struct {
	label         string
	nodePath      string
	containerPath string
	want          ocihelper.ErrorCode
}

func reservedMountTargets() []string {
	return []string{
		contract.OCIContainerHandoffDirectory, contract.OCIContainerServiceDirectory,
		contract.OCIContainerControlDirectory, "/proc", "/dev", "/sys", "/run",
		"/etc/hosts", "/etc/resolv.conf",
	}
}

// mountFixtures are the host-side files the negatives need. Everything except
// the device node is created by the driver; a device node cannot be made
// without root on macOS, so the runbook names the one sudo command and the
// driver refuses rather than silently skipping the negative.
type mountFixtures struct {
	positiveDirectory string
	positiveFile      string
	outsidePath       string
	symlinkPath       string
	socketPath        string
	fifoPath          string
	devicePath        string
	closers           []io.Closer
}

func (fixtures *mountFixtures) Close() error {
	if fixtures == nil {
		return nil
	}
	var failures []error
	for _, closer := range fixtures.closers {
		if err := closer.Close(); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

type closerFunc func() error

func (closer closerFunc) Close() error { return closer() }

const attendedMountProbeBytes = "wefty-attended-mount-source\n"

// prepareMountFixtures builds every host-side fixture the mount row needs.
// The device node is the one exception: macOS refuses mknod to a non-root
// user, so it must be pre-created. Refusing loudly here is deliberate --
// silently skipping a negative is exactly the failure mode these rows exist
// to prevent.
func prepareMountFixtures(root, outsideRoot string) (_ *mountFixtures, resultErr error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == string(filepath.Separator) {
		return nil, fmt.Errorf("WEFTY_ATTENDED_MOUNT_ROOT %q must be a clean absolute non-root path", root)
	}
	negatives := filepath.Join(root, "negatives")
	fixtures := &mountFixtures{
		positiveDirectory: filepath.Join(root, "positive"),
		positiveFile:      filepath.Join(root, "positive", "source.txt"),
		outsidePath:       filepath.Join(outsideRoot, "wefty-attended-outside.txt"),
		symlinkPath:       filepath.Join(negatives, "symlink"),
		socketPath:        filepath.Join(negatives, "socket"),
		fifoPath:          filepath.Join(negatives, "fifo"),
		devicePath:        filepath.Join(negatives, "device"),
	}
	defer func() {
		if resultErr != nil {
			_ = fixtures.Close()
		}
	}()
	for _, directory := range []string{fixtures.positiveDirectory, negatives} {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return nil, err
		}
	}
	if err := os.WriteFile(fixtures.positiveFile, []byte(attendedMountProbeBytes), 0o644); err != nil {
		return nil, err
	}
	if err := os.WriteFile(fixtures.outsidePath, []byte(attendedMountProbeBytes), 0o644); err != nil {
		return nil, err
	}
	_ = os.Remove(fixtures.symlinkPath)
	if err := os.Symlink(fixtures.positiveFile, fixtures.symlinkPath); err != nil {
		return nil, err
	}
	// A unix socket is bound at a short path and moved into place: macOS caps
	// sun_path at 104 bytes and an operator mount root can easily exceed that,
	// which would otherwise silently cost the row its socket negative.
	staging := filepath.Join(string(filepath.Separator)+"tmp", fmt.Sprintf("wefty-attended-%d.sock", os.Getpid()))
	_ = os.Remove(staging)
	_ = os.Remove(fixtures.socketPath)
	listener, err := net.Listen("unix", staging)
	if err != nil {
		return nil, fmt.Errorf("create unix socket mount fixture: %w", err)
	}
	if unixListener, ok := listener.(*net.UnixListener); ok {
		unixListener.SetUnlinkOnClose(false)
	}
	fixtures.closers = append(fixtures.closers, closerFunc(func() error {
		return errors.Join(listener.Close(), os.Remove(fixtures.socketPath))
	}))
	if err := os.Rename(staging, fixtures.socketPath); err != nil {
		return nil, fmt.Errorf("place the unix socket mount fixture: %w", err)
	}
	_ = os.Remove(fixtures.fifoPath)
	if output, err := exec.Command("mkfifo", fixtures.fifoPath).CombinedOutput(); err != nil {
		return nil, fmt.Errorf("create fifo mount fixture: %w (%s)", err, strings.TrimSpace(string(output)))
	}
	info, err := os.Lstat(fixtures.devicePath)
	if err != nil || info.Mode()&os.ModeDevice == 0 {
		return nil, fmt.Errorf(
			"the mount row requires a device-node negative at %s; create it once before the window with: sudo mknod %s c 1 3",
			fixtures.devicePath, fixtures.devicePath)
	}
	return fixtures, nil
}

// ---------------------------------------------------------------------------

func driveMountValidation(ctx context.Context, session attendedSession, config attendedConfig, fixtures *mountFixtures) attendedRow {
	const readMarker = "wefty-attended-mount-read"
	row := newAttendedRow(config.SessionID, config.Command)
	authority := config.authority(contract.JobClassOneShot, "mount-validation")
	row.AttemptIDs = []string{authority.AttemptID}
	deadman := boundedDeadman(session, config.Deadman)

	// Positive: a strict descendant of the configured host root, readable and
	// writable through the helper's host-to-guest translation.
	writtenName := "wefty-attended-mount-write.txt"
	response, err := session.Run(ctx, ocihelper.RunRequest{
		Authority: authority, InitialDeadman: deadman,
		Workload: ocihelper.WorkloadInput{
			ImageReference: config.Reference, ImageDigest: config.Digest,
			OperatorMounts: []ocihelper.OperatorMount{{NodePath: fixtures.positiveDirectory, ContainerPath: "/data"}},
			Argv: []string{"/bin/sh", "-c", fmt.Sprintf(
				"set -e; cat /data/%s; printf '%s\\n'; printf '%s' >/data/%s; "+
					"grep ' /data ' /proc/self/mountinfo",
				filepath.Base(fixtures.positiveFile), readMarker, attendedMountProbeBytes, writtenName)},
		},
	})
	if err != nil {
		row.fail(fmt.Errorf("run positive mount: %w", err))
		return row
	}
	defer reapAttempt(session, authority)
	if !response.Started || response.Image == nil {
		row.fail(fmt.Errorf("positive mount run returned no Started evidence: %+v", response))
		return row
	}
	row.TopLevelDigests = []string{response.Image.TopLevelDigest}
	row.PlatformDigests = []string{response.Image.PlatformManifestDigest}
	evidence, err := collectLogEvidence(ctx, session, authority)
	if err != nil {
		row.fail(fmt.Errorf("watch positive mount: %w", err))
		return row
	}
	stdout := strings.Join(evidence.stdout, "")
	if !strings.Contains(stdout, attendedMountProbeBytes) || !strings.Contains(stdout, readMarker) {
		row.fail(fmt.Errorf("payload did not read the translated bind source: %q", stdout))
		return row
	}
	if evidence.result == nil || evidence.result.ExitCode == nil || *evidence.result.ExitCode != 0 {
		row.fail(fmt.Errorf("positive mount payload result = %+v, want exit 0", evidence.result))
		return row
	}
	mountLine, err := containerMountLine(stdout, "/data")
	if err != nil {
		row.fail(err)
		return row
	}
	row.PayloadExecutions = 1
	written, err := os.ReadFile(filepath.Join(fixtures.positiveDirectory, writtenName))
	if err != nil {
		row.fail(fmt.Errorf("payload write did not reach the host bind source: %w", err))
		return row
	}
	if string(written) != attendedMountProbeBytes {
		row.fail(fmt.Errorf("payload write = %q, want %q", written, attendedMountProbeBytes))
		return row
	}
	// The host sees the payload's write under the operator mount root; the
	// guest must see the same bytes under the Lima guest mount root. That pair
	// is the translation this row claims, observed from both sides rather than
	// inferred from the container's own mountinfo, whose source field for a
	// bind names the backing filesystem rather than the translated path.
	guestPath, err := translatedGuestPath(config.MountRoot, filepath.Join(fixtures.positiveDirectory, writtenName))
	if err != nil {
		row.fail(err)
		return row
	}
	if config.GuestPath == nil {
		row.fail(errors.New("the mount row requires a guest-side path reader to prove host-to-guest translation"))
		return row
	}
	guestBytes, err := config.GuestPath(ctx, guestPath)
	if err != nil {
		row.fail(fmt.Errorf("read %s inside the guest: %w", guestPath, err))
		return row
	}
	if guestBytes != attendedMountProbeBytes {
		row.fail(fmt.Errorf("guest %s = %q, want the bytes the payload wrote", guestPath, guestBytes))
		return row
	}
	if err := deleteAndVerify(ctx, session, authority); err != nil {
		row.fail(err)
		return row
	}
	source, err := os.ReadFile(fixtures.positiveFile)
	if err != nil {
		row.fail(fmt.Errorf("host bind source is gone after deletion: %w", err))
		return row
	}
	if string(source) != attendedMountProbeBytes {
		row.fail(fmt.Errorf("host bind source after deletion = %q, want %q", source, attendedMountProbeBytes))
		return row
	}

	observed, err := runMountNegatives(ctx, session, config, fixtures, deadman)
	if err != nil {
		row.appendReason(observed)
		row.fail(err)
		return row
	}
	row.pass(fmt.Sprintf(
		"positive: strict descendant read and written by the payload; the same bytes appear on the host under the "+
			"operator mount root and in the guest at %s, which is the host-to-guest translation; container mountinfo "+
			"for /data: %q; host bind source byte-identical and present after deletion; negatives: %s",
		guestPath, mountLine, observed))
	return row
}

// translatedGuestPath is where the helper's Lima translation puts a host path:
// the same relative path under the guest mount root.
func translatedGuestPath(hostRoot, hostPath string) (string, error) {
	relative, err := filepath.Rel(hostRoot, hostPath)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%q is not a strict descendant of the operator mount root %q", hostPath, hostRoot)
	}
	return path.Join(limarunner.GuestAllowedMountRoot, filepath.ToSlash(relative)), nil
}

// containerMountLine returns the payload's own mountinfo line for target. It
// is recorded as supporting evidence: it proves the bind is a real mount
// point in the payload's namespace, while the translated path itself is
// proven from the guest side.
func containerMountLine(stdout, target string) (string, error) {
	for _, line := range strings.Split(stdout, "\n") {
		if strings.Contains(line, " "+target+" ") {
			return strings.TrimSpace(line), nil
		}
	}
	return "", fmt.Errorf("payload mountinfo carried no mount at %s", target)
}

// mountNegatives is the complete refusal table the runbook names.
func mountNegatives(config attendedConfig, fixtures *mountFixtures) []mountNegative {
	negatives := []mountNegative{
		{"root itself", config.MountRoot, "/data", ocihelper.CodeEngineFailure},
		{"outside the root", fixtures.outsidePath, "/data", ocihelper.CodeEngineFailure},
		{"symlink component", fixtures.symlinkPath, "/data", ocihelper.CodeOCISpecRejected},
		{"unix socket", fixtures.socketPath, "/data", ocihelper.CodeOCISpecRejected},
		{"fifo", fixtures.fifoPath, "/data", ocihelper.CodeOCISpecRejected},
		{"device node", fixtures.devicePath, "/data", ocihelper.CodeOCISpecRejected},
	}
	for _, target := range reservedMountTargets() {
		negatives = append(negatives, mountNegative{
			label: "reserved target " + target, nodePath: fixtures.positiveFile,
			containerPath: target, want: ocihelper.CodeInvalidRequest,
		})
	}
	return negatives
}

func runMountNegatives(ctx context.Context, session attendedSession, config attendedConfig, fixtures *mountFixtures, deadman time.Duration) (string, error) {
	var observations []string
	for index, negative := range mountNegatives(config, fixtures) {
		authority := config.authority(contract.JobClassOneShot, "mount-negative-"+strconv.Itoa(index))
		_, runErr := session.Run(ctx, ocihelper.RunRequest{
			Authority: authority, InitialDeadman: deadman,
			Workload: ocihelper.WorkloadInput{
				ImageReference: config.Reference, ImageDigest: config.Digest,
				OperatorMounts: []ocihelper.OperatorMount{{NodePath: negative.nodePath, ContainerPath: negative.containerPath}},
				Argv:           []string{"/bin/true"},
			},
		})
		if runErr == nil {
			// An accepted negative is a live attempt; reap it before failing
			// so the window is not left contaminated.
			reapAttempt(session, authority)
		}
		code, err := checkRefusal(negative.label, runErr, negative.want)
		observations = append(observations, fmt.Sprintf("%s=%s", negative.label, refusalCode(code, negative.want, err)))
		if err != nil {
			return strings.Join(observations, " "), err
		}
	}
	return strings.Join(observations, " "), nil
}

func refusalCode(observed, want ocihelper.ErrorCode, err error) string {
	if err != nil {
		if observed == "" {
			return "ACCEPTED-OR-UNTYPED(want " + string(want) + ")"
		}
		return string(observed) + "(want " + string(want) + ")"
	}
	return string(observed)
}

// ---------------------------------------------------------------------------
// Row 4: host_to_guest
// ---------------------------------------------------------------------------

func driveHostToGuest(ctx context.Context, session attendedSession, config attendedConfig) attendedRow {
	const requestMarker = "wefty-attended-host-to-guest-request"
	const responseMarker = "wefty-attended-host-to-guest-response"
	row := newAttendedRow(config.SessionID, config.Command)
	authority := config.authority(contract.JobClassService, "host-to-guest")
	row.AttemptIDs = []string{authority.AttemptID}

	response, err := session.Run(ctx, ocihelper.RunRequest{
		Authority: authority, InitialDeadman: boundedDeadman(session, config.Deadman),
		AllocateEndpoints: []string{"service"},
		Workload: ocihelper.WorkloadInput{
			ImageReference: config.Reference, ImageDigest: config.Digest,
			ManagedVolumes: []ocihelper.ManagedVolumeDescriptor{{Kind: ocihelper.ManagedVolumeServiceData}},
			// The payload binds only guest loopback on the helper-allocated
			// port and answers one distinct request marker with one distinct
			// response marker.
			Argv: []string{"/bin/sh", "-c", fmt.Sprintf(
				"exec /usr/local/bin/wefty-echo-service >/dev/null 2>&1 & "+
					"printf 'host-to-guest payload started with request marker %s\\n'; wait", requestMarker)},
		},
	})
	if err != nil {
		row.fail(fmt.Errorf("run service payload: %w", err))
		return row
	}
	defer reapAttempt(session, authority)
	if !response.Started || response.Image == nil {
		row.fail(fmt.Errorf("service run returned no Started evidence: %+v", response))
		return row
	}
	row.TopLevelDigests = []string{response.Image.TopLevelDigest}
	row.PlatformDigests = []string{response.Image.PlatformManifestDigest}
	port := response.Endpoints["service"]
	if len(response.Endpoints) != 1 || port == 0 {
		row.fail(fmt.Errorf("helper allocated endpoints %v, want exactly one non-zero service endpoint", response.Endpoints))
		return row
	}
	row.PayloadExecutions = 1

	exchange, err := exchangeAttemptMarkers(ctx, session, authority, requestMarker)
	if err != nil {
		row.fail(err)
		return row
	}
	row.RoundTrip = true
	row.StdoutMarkers = []string{exchange.echoed}
	row.StderrMarkers = []string{}

	observations, err := runAttemptPortNegatives(ctx, session, config, authority)
	if err != nil {
		row.appendReason(observations)
		row.fail(err)
		return row
	}
	if err := deleteAndVerify(ctx, session, authority); err != nil {
		row.fail(err)
		return row
	}
	row.pass(fmt.Sprintf(
		"one helper-allocated service endpoint on guest loopback port %d; request marker %q echoed and distinct "+
			"payload-produced response marker %q returned through DialAttemptPort; negatives: %s; Delete positive "+
			"and attempt Verify absent", port, exchange.echoed, exchange.responseMarker, observations))
	return row
}

// attemptEndpointExchange is the guest-loopback round trip through the
// helper-authorized stream: a request marker this host sends, and a distinct
// response marker the guest payload itself produces.
type attemptEndpointExchange struct {
	echoed         string
	responseMarker string
}

func exchangeAttemptMarkers(ctx context.Context, session attendedSession, authority ocihelper.AttemptAuthority, requestMarker string) (attemptEndpointExchange, error) {
	var exchange attemptEndpointExchange
	transport := &http.Transport{
		DialContext: func(dialContext context.Context, _, _ string) (net.Conn, error) {
			return session.DialAttemptPort(dialContext, ocihelper.DialAttemptPortRequest{Authority: authority, Name: "service"})
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}

	echoed, err := attemptEndpointCall(ctx, client, http.MethodPost, "/echo", requestMarker)
	if err != nil {
		return exchange, err
	}
	if !strings.Contains(echoed, requestMarker) {
		return exchange, fmt.Errorf("attempt endpoint echoed %q, want the request marker %q", echoed, requestMarker)
	}
	exchange.echoed = requestMarker

	// The response marker must be produced by the payload rather than mirrored
	// from the request, or the round trip proves only that bytes came back.
	health, err := attemptEndpointCall(ctx, client, http.MethodGet, "/healthz", "")
	if err != nil {
		return exchange, err
	}
	var facts struct {
		PID              int    `json:"pid"`
		ServiceDirectory string `json:"service_directory"`
		ListeningPort    int    `json:"listening_port"`
	}
	if err := json.Unmarshal([]byte(health), &facts); err != nil {
		return exchange, fmt.Errorf("attempt endpoint health response %q is not payload-produced JSON: %w", health, err)
	}
	if facts.PID <= 0 || facts.ServiceDirectory != contract.OCIContainerServiceDirectory || facts.ListeningPort <= 0 {
		return exchange, fmt.Errorf("attempt endpoint health response = %+v, want a payload-produced marker", facts)
	}
	if strings.Contains(health, requestMarker) {
		return exchange, errors.New("request and response markers were not distinct")
	}
	exchange.responseMarker = fmt.Sprintf("pid=%d service_directory=%s listening_port=%d",
		facts.PID, facts.ServiceDirectory, facts.ListeningPort)
	return exchange, nil
}

func attemptEndpointCall(ctx context.Context, client *http.Client, method, path, body string) (string, error) {
	request, err := http.NewRequestWithContext(ctx, method, "http://attempt.invalid"+path, strings.NewReader(body))
	if err != nil {
		return "", err
	}
	reply, err := client.Do(request)
	if err != nil {
		return "", fmt.Errorf("dial the attempt endpoint %s: %w", path, err)
	}
	defer reply.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(reply.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read the attempt endpoint %s response: %w", path, err)
	}
	if reply.StatusCode != http.StatusOK {
		return "", fmt.Errorf("attempt endpoint %s returned HTTP %d body %q", path, reply.StatusCode, payload)
	}
	return string(payload), nil
}

func runAttemptPortNegatives(ctx context.Context, session attendedSession, config attendedConfig, live ocihelper.AttemptAuthority) (string, error) {
	var observations []string
	wrongName := live
	_, nameErr := session.DialAttemptPort(ctx, ocihelper.DialAttemptPortRequest{Authority: wrongName, Name: "not-the-service-endpoint"})
	code, err := checkRefusal("unallocated endpoint name", nameErr, ocihelper.CodeUnauthorizedPort)
	observations = append(observations, "unallocated_endpoint_name="+refusalCode(code, ocihelper.CodeUnauthorizedPort, err))
	if err != nil {
		return strings.Join(observations, " "), err
	}

	wrongAttempt := config.authority(contract.JobClassService, "host-to-guest-wrong-attempt")
	_, attemptErr := session.DialAttemptPort(ctx, ocihelper.DialAttemptPortRequest{Authority: wrongAttempt, Name: "service"})
	code, err = checkRefusal("unknown attempt tuple", attemptErr, ocihelper.CodeAttemptOutsideSession)
	observations = append(observations, "unknown_attempt_tuple="+refusalCode(code, ocihelper.CodeAttemptOutsideSession, err))
	if err != nil {
		return strings.Join(observations, " "), err
	}

	mutated := live
	mutated.FencingToken += "-mutated"
	_, fenceErr := session.DialAttemptPort(ctx, ocihelper.DialAttemptPortRequest{Authority: mutated, Name: "service"})
	code, err = checkRefusal("mutated fencing token", fenceErr, ocihelper.CodeAttemptOutsideSession)
	observations = append(observations, "mutated_fencing_token="+refusalCode(code, ocihelper.CodeAttemptOutsideSession, err))
	return strings.Join(observations, " "), err
}
