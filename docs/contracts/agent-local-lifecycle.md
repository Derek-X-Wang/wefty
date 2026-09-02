# Agent-local lifecycle

`agent.Agent.Status` is the process-local health surface for the daemon. It is
independent of `contract.NodeState`: L1 node state describes control-plane
reachability, while this surface describes what the local process is doing.

## Session state

| State | Meaning | Leaves the state when |
| --- | --- | --- |
| `registering` | The initial boot registration is in progress. | Registration succeeds, fails transiently, or is rejected semantically. |
| `ready` | The registered session can claim work. An empty attempt map means healthy idle. | The session loses reachability or authority, or drain begins. |
| `rejoining` | Registration or another session operation is retrying with capped exponential backoff and jitter. | Registration and a session operation succeed, or a semantic rejection quarantines or drains the agent. |
| `quarantined` | A non-retryable node-session rejection stopped autonomous work. | Outer cancellation or an operator-requested drain; the daemon does not exit merely because it is quarantined. |
| `draining` | New claims are stopped while resident attempts are joined. | The drain completes and `Run` returns cleanly, or a second shutdown signal forces cancellation. |

`session_backoff` is the current retry delay and is zero while ready. A
quarantined session reports the maximum backoff so it cannot look like a
healthy idle process. `last_semantic_error` retains the most recent L1 error
code, message, and local observation time; transport errors do not invent a
semantic code.

The first SIGINT or SIGTERM starts a graceful drain with a 30-second bound. A
second signal is an explicit forced-shutdown transition: it cancels resident
attempts immediately and emits `forced_shutdown ... reason=second_signal`
whether it arrives while Drain is joining residents or after Drain returns,
distinguishing operator abort from clean drain completion.

## Capability observation and local admission

One synchronized agent-owned snapshot contains the complete advertised
capability set, its boot-scoped Capability revision, local observation time,
missing capabilities, and one stable sanitized reason code. Registration,
heartbeat, removal-authority recovery, event-triggered probes, local start
admission, and future doctor output all read this module; none retains a
startup-captured capability set.

The OCI functional-probe interface supplies OCI-related observations while the
configured base set retains independent capabilities such as `kind:process`.
Configured OCI-related keys are always removed: only a successful functional
probe can earn them. Each probe has a ten-second default deadline and adapter
cancellation; timeout records `probe_failed` without blocking the heartbeat
loop. A failed probe commits a restrictive local snapshot before returning its
diagnostic error: every OCI-related badge is removed and local OCI start
admission fails immediately. New claim RPCs pause until registration or a
heartbeat acknowledges that restrictive revision, while resident attempts are
untouched. Completing a restrictive observation is serialized after every
claim begun under the preceding snapshot, so an older in-flight claim cannot
consume work created after the observation returns. The next successful
heartbeat publishes that same full snapshot. Recovery requires another
successful probe and a higher revision.

The Capability revision advances only when the canonical capability set,
missing set, or stable reason changes. Repeated identical probes retain the
revision while advancing observation time; the local snapshot separately
records the latest completed-probe time for doctor age. Probe detail remains
local; only the stable reason code and bounded missing set cross into L1.
Capability observation and its publication barrier are independent of durable
`claims_enabled` intent.

An OCI functional probe cannot be configured without an OCI boot barrier and
cannot run directly. Registration carries a restrictive OCI observation
and asks L1 to atomically assign stored same-boot revision `N+1`; the response
therefore both establishes node authority and removes a stale badge without a
second registration or authority-generation bump. Pending process-service
removal and Computer Storage reset resumption run unconditionally after that registration, even if helper
takeover, sweep, or verification failed. Only OCI probing and positive
publication depend on a successful barrier.

The successful path pins the opaque helper process/session generation across
sweep, verification, removal resumption, and probe, rechecks it while holding
the claim-publication lock immediately before and after the `N+2` heartbeat,
and opens claims only after L1 acknowledges that revision. If the generation
changes during that heartbeat, the same locked transaction publishes a newer
restrictive revision before claim admission can reopen. Helper heartbeat-pump
loss synchronously records a newer restrictive local revision behind the same lock;
new claim RPCs then remain paused until that restriction is published. A helper
bounce publishes the restriction before reacquisition and runs removal recovery
on that event path; ordinary healthy heartbeats probe without rescanning the
filesystem. `boot_sweep_failed` is the bounded L1 reason for an incomplete
sweep/verify/removal-resume barrier, while detailed errors remain local.

Computer Storage reset is standing heartbeat work, not attempt authority, and
L1 issues it only for an already-stopped Computer. The agent never quiesces or
restarts the service. It passes the exact reset directive to the OCI helper;
under the attachment flock the helper records a predecessor retirement fence,
then allocates, formats, and verifies the successor. Only the helper-derived,
managed-root-bound preparation receipt is sent to L1. Once L1 publishes the
successor, the standing directive advances to predecessor retirement: the
agent calls the shared authority-bound managed-volume deletion seam, requests
the shared assertion-derived removal attestation over every deterministic disk
row, validates that every row actually ran and passed, then sends the shared
cleanup acknowledgement to L1. Crashes retry these idempotent phases; no phase
starts the Computer.

Cold Backup create and prune are also standing heartbeat work, separate from
attempt leases. L1 releases a create directive only after disruptive intent
has stopped the current Job. The agent passes the exact Computer revision and
`storage_id@generation` to the helper; it never fabricates a resume. The helper
holds the Storage attachment fence while proving mount and loop absence,
copying, and digesting. Only helper-derived success or positive-absence failure
receipts reach L1. Prune removes only the deterministic Wefty-owned copy root
and likewise requires a positive absence receipt. Composite Computer removal
executes these copy directives before managed-root and disk cleanup, so a
superseded staged copy cannot be stranded outside L1 tracking.

Computer restore and clone are likewise standing heartbeat work. The agent
passes only the exact L1 directive to the helper and forwards only the
helper-derived, Node- and root-instance-bound receipt. Restore optionally
creates the precommitted predecessor Backup before switchover, then retires the
old generation through the same managed-volume deletion and assertion-derived
attestation used by reset. Clone never inherits grants or starts itself;
machine identity rekey and filesystem expansion are helper mechanics whose
positive facts are required in the receipt. Crashes replay the durable helper
phase manifest, and a changed revision, boot, root instance, source digest, or
size fails closed before L1 publication.
Before a Mac reaches that helper barrier, the supervised wrapper may instead
publish any closed OCI restriction, including `oci_intent_disabled`,
`lima_stopped`, `lima_broken`, or `lima_start_timeout`; L1 validates the actual
restrictive shape (`kind:oci` absent and missing) plus that reason before an
atomic registration supersede, and the same claim barrier applies.

## Mac boot supervision

The macOS agent process is the only Lima supervisor. Its system LaunchDaemon
label is `dev.wefty.agent`; launchd runs it as the operator user with absolute
program, working, and log paths plus explicit `HOME`, `LIMA_HOME`, `USER`,
`LOGNAME`, and `PATH`. The plist contains no Fabric credential, and a competing
`io.lima-vm.daemon.*` system unit or `io.lima-vm.autostart.*` system/user/gui
login unit makes installation fail before mutation.
The system unit uses `RunAtLoad`, throttled `KeepAlive`, and the system launchd
domain, so OCI failure cannot turn into a process-killing launchd loop.

Lima supervision reads an injected, read-only revisioned OCI-intent source.
Missing, unreadable, or malformed state is disabled. Explicit bootstrap creates
the initial enabled marker only when no marker exists and preserves an existing
disabled marker; Ticket #153 owns all later writes and CAS. The supervisor
rechecks intent after every inspection and lifecycle command. Disable wins an
intervening start or repair, leaves Lima stopped and OCI restrictive, and a
revision change fails closed for the next cycle. While enabled, `Stopped`
permits one bounded `limactl start`; `Broken` permits one bounded `stop --force`
followed by capped-backoff starts within an overall deadline. Failure leaves the same
agent process and `kind:process` capability available while OCI stays
restrictive; a successful start still must pass helper handshake, complete
boot sweep, removal resumption, and the functional probe before publication.

One shared cycle lock covers supervisor inspection/mutation and the helper boot
barrier, so the watchdog cannot force-stop Lima during sweep. Recovery retires
the old helper generation, publishes the restrictive revision, then performs
Lima recovery, handshake/sweep, probe, and pinned positive publication. Every
`limactl` call has a command deadline and the complete inspection/start/helper
readiness cycle has one recovery deadline. Expiry force-stops once and leaves a
capped-backoff retry for the next cycle, including a Running VM whose helper
never becomes ready.

The interim installer has an idempotent inverse: unload/remove the host unit,
stop/disable and remove the guest helper socket/service/binary, reload guest
systemd, remove the minimal facts and intent marker, and emit structured
absence evidence. Guest version replacement stops the helper socket and service
before overwriting the binary.

The #128 bootstrap facts file is an atomic operator-readable JSON snapshot,
not a control socket or general doctor UI. It contains only schema version,
observation time, launch unit, Lima/helper/probe state, Capability revision,
and one stable reason code. Unit/helper/probe/instance states are closed types;
`unit.state=launched_by_unit` is derived from the installed launch environment.
The writer checks for content changes and polls no faster than 20 seconds. It
never contains a raw command error, path detail,
environment dump, credential, or opaque helper session capability.

The versioned general node doctor is a separate facts-only read over the
operator-authenticated local control socket. JSON and human views contain the
same host/runtime platform, agent user/unit, Lima, helper handshake, recorded
functional probe and Capability revision, intent, cache, mount-root, version,
and convergence facts. Each check is `OK`, `FAILED`, or `NOT-RUN`; `OK` is
permitted only when the corresponding assertion ran and passed. The doctor
reads the existing helper session and capability snapshot and never acquires a
session, runs a functional probe, sweeps, changes intent, applies convergence,
starts a service, or enforces cache policy. Failed checks use the closed M3
reason vocabulary and stable `docs/runbooks/oci-node.md#doctor-code-*` anchors.
It also surfaces #220's process-payload UID-isolation limitation without
claiming that operator peer credentials distinguish the shared UID.

`NOT-RUN` is a first-class receipt: it carries no failure reason and instead
names one closed `not_run_cause`. A helper diagnostic failure preserves the
already-authenticated handshake and degrades only the reads that did not
complete; diagnostic transport is never evidence for session loss, attempt
reap, or capability withdrawal. Runtime versions are facts only. Difference
from the pinned real-time CI versions is `WARN`/`outside_tested_range`, not a
failed capability verdict. Doctor compares the applied setup-state receipt
with the configure-only desired-state receipt; if either side is unavailable,
it does not manufacture a convergence class.

## Durable OCI intent and node-local control

The versioned OCI intent marker is the node-local operator decision, separate
from L1 `claims_enabled` and Capability revision. Missing, unreadable, malformed,
or zero-revision state is disabled. `setup-oci` creates revision 1 enabled only
when the marker has never existed; every rerun preserves an existing disabled
marker exactly. `oci start` and `oci stop` compare-and-swap the revision and
fsync the replacement and its parent directory before any runtime effect.
`setup-oci` is configure-only: it checks every prerequisite before mutation,
writes the requested configuration/template or Linux units, and reports the
resulting convergence class without recovering or starting OCI. The sole
runtime exception is an explicit restart/recreate flag with zero live OCI
attempts; `wefty node oci start` remains the ordinary start operation.
Configure-only setup writes a separate desired-state receipt before returning
a restart/recreate refusal. The applied receipt advances only after authorized
convergence, so doctor derives unchanged, live-safe, restart-required, or
recreate-required by comparing current with desired rather than echoing the
last probe reason.

Singular `wefty node` commands resolve the installed node configuration before
Fabric initialization and use only the live agent's Unix control socket. Its
private runtime directory is `0700`, its socket is `0600`, an active socket is never replaced,
and a symlink or non-socket path fails closed. The server authenticates every
accepted peer with `SO_PEERCRED` on Linux or `LOCAL_PEERCRED` on macOS and
allows only the installed operator UID. Process-kind payloads currently share
that UID with the agent; #220 owns the required payload UID isolation and this
known pre-existing limitation grants no additional control authority. The agent is the sole writer of
intent and runtime state. `load-image` streams an OCI archive through this
surface to the existing helper-owned import/cache seam; it accepts no mutable
reference override and returns only verified top-level and admitted-platform
digests plus bounded evidence.
Node-local JSON failures always carry a closed, sanitized `details.reason`:
typed helper failures retain their helper protocol code, adapter-side platform
diagnostics use `diagnostic_failure`, other recognized adapter failures use
their bounded helper code, and an otherwise plain internal failure uses
`internal`. Raw paths, error strings, credentials, and other helper-local
mechanics never cross this boundary.

Stop ordering is durable disable → restrictive local Capability observation →
published-service withdrawal → attempt reap and positive runtime quiescence →
Mac Lima stop. Durable disable and restrictive local admission prevent recovery
during quiescence; only after positive reap receipts does the Mac path acquire
the shared recovery-cycle lock for the VM stop. This avoids a lock inversion
with attempt finalization while preventing the background watchdog from racing
the final VM transition. Linux leaves containerd running. A failed
post-persistence action never rolls the intent bit back. Start persists enabled,
reacquires the helper, completes the boot barrier and probe, and only then
publishes the positive Capability revision.

Linux privileged `setup-oci` renders and writes one unprivileged `wefty-agent.service` with
`SupplementaryGroups=wefty-oci` and one root socket-activated helper pair. The
socket is exactly `0660 root:wefty-oci`; the helper UID allowlist names only the
agent user. Setup creates the group, adds the user, writes installed node
configuration, and prints the exact `systemctl` commands the operator runs; it
never executes those commands itself. A newly added membership prints one
agent restart, while an already-member rerun prints an idempotent start.

## Workload runtime selection

The agent selects exactly one `WorkloadRuntime` by the job's open `kind` after
the shared local-capability admission check. Workload `class` never selects or
enters that adapter. Instead, the agent compiles class-specific policy into
runtime-neutral mechanics such as idle monitoring, the required lifetime
boundary, managed resources, and started/readiness hooks.

Capability eligibility and local implementation are separate fail-closed
checks. L1 leaves an unknown kind unschedulable until a node advertises its
`kind:<name>` capability. If a claimed job names a capability for which this
agent process has no matching adapter, only that attempt is refused as an
unsupported kind; process and other adapter siblings continue normally.

Every adapter preflights its request before the agent acquires managed service
resources, published ports, workflow bridges, handoffs, or log sinks. The
adapter then returns a structured attempt outcome and implements
`ReapAndVerify`. The attempt lifecycle requests a positive `runtimeQuiesced`
receipt after `Run` returns and treats its absence as an `output_error`
finalization failure before durable completion is stored.

The generic agent handoff manager remains the owner of process one-shot host
directories. An OCI one-shot does not reinterpret the forbidden flat
`execution.handoff_directory`: after `kind=oci` selects the adapter, the
agent puts exactly one `handoff` requirement on the runtime request, keyed by
the stable job or `handoff_owner_run_id`. The adapter passes that key opaquely
to the helper and unconditionally makes `/wefty/handoff` the reserved guest
value. Attempt reap preserves this helper-owned volume on failed or interrupted
runs and refreshes its default 24-hour retry window when another attempt or
rerun reuses it. Only after L1 accepts a successful completion does the agent
ask the runtime to delete the volume and require a positive absence receipt.
The payload sees only the reserved container path, never the helper source
path. The named `usesAgentHandoffLifecycle` predicate positively selects only
`kind=process`, `class=one-shot`; no negative kind gate can accidentally add a
future runtime to the host-directory manager.

Immediately before helper `Run`, the adapter atomically captures the session,
verified sweep epoch, and helper instance/session generation, replacing any
Preflight-era observation. If helper or engine loss invalidates that session,
only `attempt_outside_session` can enter replacement-sweep recovery;
`unauthorized_attempt` still means that the current session has no matching
live attempt and is not cleanup proof. Recovery requires both a different
sweep epoch and a different helper generation, plus an explicitly empty
verified namespace inventory. That receipt is consumed once for the exact
typed attempt-evidence key. A same-generation sweep, pre-run sweep, non-empty
verification, and untracked authority can never substitute for attempt
cleanup. The tracking entry remains present while the helper is unavailable,
so finalization can consume recovery evidence before its deadline without
replacing the original `runtime_unavailable` result with `output_error`.

`ReapEvidenceOCIRuntimeSweep` is cross-cutting helper-loss evidence: the same
typed receipt is valid for one-shot and service attempts when their lifecycle
reaches this adapter seam. Service publication, data, and removal policy remain
owned by the service tickets.

For an agent-boot-lifetime OCI service, execution cancellation first withdraws
publication and closes the Fabric front door. The OCI adapter then keeps the
helper `Watch` stream alive, sends `TERM`, waits the agent-compiled five-second
grace, sends `KILL` if the task has not exited, and waits for the structured
terminal result. A task that exits as the `KILL` races its terminal edge is
already terminated, not helper loss: the helper does not record that undelivered
`KILL`, and the retained `Watch` supplies the actual exit or signal evidence.
The same normalization applies when a task self-exits before `TERM` reaches
containerd. Because no signal was delivered in that race, the helper records no
signal and `Watch` reports the task's ordinary `ExitCode` arm. If containerd has
reaped the task but its Wait stream never supplies that terminal evidence, the
agent treats the missing confirmation as typed runtime loss and performs the
replacement-generation sweep.
`Delete` must subsequently verify task, container, snapshot,
lease, cgroup, shim, and log absence before L1 may observe `stopped`.

Helper/session or engine loss takes a different positive-proof path. The OCI
adapter invokes the agent recovery hook only for helper/engine evidence, never
for an L1 image-observation transport failure. Recovery immediately suppresses
OCI admission, publishes the restrictive capability observation, invalidates
the helper generation, and completes the boot sweep before the attempt can
finish. `ReapAndVerify` may consume that same-boot sweep once only when its
complete attempt authority matches and the independently verified namespace
inventory is empty. Replacement claims remain embargoed until the ordinary
capability publication handshake acknowledges the recovered generation.
Concurrent attempts that observed the same lost generation serialize recovery;
after one establishes a newer sweep, siblings consume that proof instead of
invalidating the recovered generation again.

The recovery hook first closes local OCI admission synchronously under the
claim/publication fence; heartbeat, invalidation, sweep, probe, and positive
republish remain serialized finalization work. The hook substitutes the helper
generation captured when it was armed if session acquisition failed before an
adapter could report a generation. A typed loss returned by `ReapAndVerify`
uses the same embargo and a separately bounded recovery before one retry or an
exact sweep receipt; absent positive quiescence remains a failed stop latch.

Prior-boot removal dispatches to the runtime recorded by the removal intent;
an OCI job can never consume process Guardian evidence, or vice versa. A
verified-absent new-boot OCI namespace sweep can prove prior-boot quiescence
even when its swept-attempt list omits the job because the old helper already
reaped that attempt before the agent lost its in-memory receipt, but only when
`PriorBootSessionsSeen` names the removal intent's exact Node and prior boot pair. A sweep that
names neither the exact attempt nor that prior boot is unbound and fails. The
helper may carry the boot ID across its own session generations only after a
successful session reap; a failed reap or an unseen boot never manufactures
that fact. The
frozen removal manifest still binds every subsequent durable-data deletion and
post-delete assertion. Legacy inventory reconstruction continues to require
matching attempt authority and cannot upgrade an empty sweep into a manifest.
The one no-attempt case is a Computer generation whose exact helper manifest is
prepared by a durable reset/copy receipt, unattached, has no loop or mount, and
has no prior attachment or retirement evidence. A current authenticated helper
inventory may freeze that generation as Storage-only removal evidence only for
every Storage generation claimed by the directive. The session still passes
that evidence through the resident/admitted service barrier; only after both
are clear may it record `no_runtime_resources` without signalling a guardian.
That complete-generation requirement is specific to the no-runtime shortcut.
When the session instead returns a positive runtime reap receipt, the guardian
receipt covers the Job while the helper independently finalizes every Storage
generation claimed by L1; historical generations do not need to be relabelled
as members of the current runtime attempt.
The frozen manifest contains empty runtime identifiers and does not manufacture
lease, task, container, or other attempt rows. After a reboot, prepared
Storage-only evidence is re-inventoried and refreshed under the current helper
session before it can produce a no-runtime receipt.

For a Mac OCI one-shot that needs the run bridge, the agent asks Lima itself to
resolve `host.lima.internal`, binds only that discovered guest-visible host
address, and injects the hostname plus the allocated port. It never binds a
wildcard or uses a fixed gateway address. Only failure to bind the successfully
discovered address selects the helper fallback: the host bridge stays on
loopback, `Run` explicitly requests fallback authority, and the OCI adapter
pumps attempt-capability streams from the helper into that one existing bridge.
Discovery failure is a start failure, not permission to broaden the listener.

A receipt names its evidence kind. Same-boot process attempts use consumed,
full-authority `attempt` evidence. When a removal directive first reaches a
returning node after an offline agent boot, the process adapter may instead
issue `prior_boot_guardian`: the prior boot differs from its configured current
boot and Guardian's disconnect contract reaped that boot's guarded payloads.
Same-boot removal never falls back to this proof. The OCI boot barrier supplies
the equivalent namespace sweep evidence to the OCI adapter. Same-boot helper
recovery names evidence `oci_sweep`; prior-agent-boot removal names
`prior_boot_oci_sweep`. Both receipts are single-use, while #150 persists
removal receipts across mid-removal crashes.

Before an OCI service attempt can enter helper `Run`, the agent persists its
immutable fenced resource manifest in the FULL-synchronous local SQLite ledger.
The manifest names the attempt lease, task, container, writable snapshot,
shim, cgroup, and framed-log directory. An ordinary service manifest then names
the stable service-data directory plus its owner record, while a Computer
manifest instead names its exact Computer, Storage, generation, and allocation
identity; the two durable-data branches are mutually exclusive. Operator bind source paths are excluded; their existing
descriptor-backed guards remain held through runtime teardown and removal never
traverses them.

The first removal-intent write atomically snapshots every retained attempt row
into one immutable job/removal-generation manifest. OCI removal then advances
only `prepared` (manifest durable) → `quarantined` (positive
`runtimeQuiesced` receipt durable) → `complete` (the helper-generation
post-delete attestation is durable). Each transition is insert-or-compare and
restart-resumable. Between `quarantined` and `complete`, the agent purges its
spool, completes managed-root deletion, and asks the helper to delete the
stable service-data directory and owner record. The helper then inventories
every class and identity in the frozen manifest; the receipt contains only
assertions that actually ran and observed absence. The binding image pin is
released and L1 may observe `agent_cleaned` only after `complete`. A named
runtime resource is validated against its complete authority labels before a
`NotFound` observation may participate in the compound absence proof.

For a legacy OCI service without an agent-local manifest, the adapter may
reconstruct the exact attempt inventory only from a positive helper sweep
receipt naming the same job and removal generation. No matching sweep evidence
fails closed; it is never upgraded into an empty or verified manifest.

For `kind=process`, `Run` already waits for process or Guardian reaping, so the
receipt verifies that blocking return contract. Inline executable decoding,
digest validation, interpreter resolution, materialization, and cleanup are
owned by the process adapter; the agent lifecycle never materializes a
kind-specific executable.

`ServiceAddress` remains only for the process readiness guardian. A portful OCI
request instead asks its adapter for the named `service` `AttemptEndpoint`: the
helper allocates a bounded named endpoint map and the adapter returns an
exact-authority dial closure keyed by that name.
The agent starts the OCI readiness deadline only after the adapter reports
`Started`, probes that closure, and gives the same closure to the local service
front door. First readiness enables forwarding; later loss withdraws it without
killing the payload, and sustained recovery republishes through the existing
hysteresis. Bidirectional forwarding preserves write-half closure through both
the helper tunnel and Fabric front door so close-delimited payload responses
complete without waiting for the caller to close its request side. The front
door remains ignorant of guest addresses. Portless OCI
services request no endpoint or probe and become running at authoritative
`Started`.

A Computer request instead asks for exactly `view` and `control`. The helper
protocol version that admits this request also carries the exact-authority
`SetComputerControlState` verb and Computer-disk attachment semantics. Only a
successful functional probe through such a negotiated version may add the
boot-scoped `computer` capability; an unsupported helper major is not admitted
by this agent and cannot satisfy a Computer claim. Display readiness and
publication remain a separate consumer of the returned opaque endpoints.

Take-over discovery returns L1's durable policy revision. The CLI carries that
revision to the private front door; if the live cache is older and therefore
does not yet contain the identity, the refusal is typed retryable
`stale_policy_revision`, not an undifferentiated 403. The CLI retries only that
code every 100 ms for at most two seconds inside its existing command context.
All other admission failures return immediately, and the native acceptance
harness permits at most three typed stale-policy process attempts within a
six-second sub-window while retaining each tolerated stderr in its receipt.

The session-bound control bearer is authenticated by a Node-durable local key
and binds the exact Computer, Storage identity and generation, attempt, Fabric
person and device, admission authority, and policy revision that issued it.
The sideband returns 401 for malformed bearers, bearers not issued by that Node
lineage, and identity mismatches. An authentic lineage bearer with no matching
live session is terminal by default and returns HTTP 410
`takeover_session_ended` without consulting Controller tenure or admitting a
replacement session. Its synchronous `ComputerControlReceipt` reports
`session_end_reason=attempt_authority_lost`; an in-process close observation
sharpens that reason (for example to `revoked`) and, for policy revocation,
reports the installed revoking policy revision. View-only admission remains
ineligible to take and returns `control_not_authorized` even after its session
ends, so terminal recognition does not expand control authority.

Take-over termination force-closes the client, `view`, and any active `control`
WebSockets before invoking their handshake-capable `net.Conn` wrappers, so a
peer close frame and goroutine scheduling cannot extend the authority boundary.
The policy drain barrier is released when those sockets are closed. Controller
signal clearing and asserted `control_released` then `session_close` audit
finalization follow under independent bounds; neither delays the drain
acknowledgement.

OCI service environment contains only reserved container-visible values:
`WEFTY_SERVICE_DIR=/wefty/service` for every service and the helper-allocated
`WEFTY_SERVICE_PORT` only for a portful service. Agent and guest backing paths
never enter the payload environment. The agent compiles every OCI service into
exactly one `service_data` managed-volume requirement. The helper derives its
opaque backing identity from the stable job ID, initializes the fresh guest
directory once to the pinned image's resolved UID:GID, and mounts it at the
reserved path on every attempt. Attempt reap, restart, stop, and start retain
that data while each attempt receives a fresh writable root snapshot.
For a Computer projection, the authoritative claim additionally carries its
exact `computer_id` and `storage_id@generation`; the agent compiles
`computer_disk` instead of `service_data` and copies the trait's positive
`disk_bytes`. A Computer claim missing that joined Storage authority fails
closed before runtime entry.
The helper injects the two reserved Computer port values, omits
`WEFTY_SERVICE_PORT`, and mounts its fresh attempt-local read-only
`/wefty/control/driver.json` outside that Storage generation.

Immediately before privileged runtime creation, the helper atomically reserves
the newcomer Computer's declared memory and disk caps against the setup-
configured Node/VM capacity and infrastructure reserve. Reservations are keyed
by bound Job, so a replacement attempt reuses its Computer's charge. Ordinary
OCI services do not participate. The refused newcomer changes no resident attempt,
publication, Controller tenure, or write budget. Low `MemAvailable` is retained
only as a timestamped fact. The disk decision is serialized against the
filesystem holding the Computer disk root before allocation. Full Computer disk
allocation remains the durable charge: pre-`Started` `ENOSPC` leaves no partial
image/loop/mount/manifest, and stopped Computer images retain their bytes.

At terminal observation, kernel OOM evidence becomes the exact Computer memory
latch. `disk_exhausted` requires a positively observed attempt-local ENOSPC
event; a post-hoc free-byte sample is advisory only and cannot create the
latch. Neither resource fact is inferred from stderr or a numeric exit status.

Mac setup ships a 4 GiB Computer ceiling with an infrastructure reserve
proportional to the fixed Lima VM sizing. Linux setup defaults the configured
usable arithmetic to `MemTotal - min(1 GiB, 25%)`. The helper consumes those
facts as configuration and never refuses to start because of their values.

## Attempts and occupancy

The `attempts` map is keyed by attempt ID. Each entry carries job ID, workload
class, local state, and its last error:

- `starting`: admitted locally but the payload has not begun. For `kind=oci`,
  this covers both the interval before image delivery and the later spec
  construction plus `Wait` registration;
- `pulling`: `kind=oci` image resolution, pull/import, unpack, or shared-
  operation wait is in progress. The payload has not begun and L1 remains
  `Claimed`;
- `running`: the payload and its authority watchdog are resident;
- `reaping`: authority or outer cancellation was issued and the agent is
  waiting for the runner to prove the payload is gone;
- `finalizing`: the payload returned and logs/completion/handoff are being
  finalized.

A runner that does not return after cancellation stays visible as `reaping`;
the daemon remains alive rather than claiming a process exit can make an
unreaped payload safe. Completed attempt entries are removed.

`one_shot` and `services` report independent occupied/limit pairs. They are
local admission counts, not slot identities and not L1 state.

Image delivery is one agent-owned policy window, defaulting to ten minutes and
tunable with `--oci-image-budget`. The deadline includes public resolution,
pull or import, unpack, and waiting on an existing singleflight operation.
For a one-shot, the claim's persisted absolute pre-start deadline clamps this
window, so a requeued attempt never receives a fresh budget.
The actual derived context deadline is re-read after applying the clamp so a
shorter parent deadline also bounds every backoff and helper operation.
Transient network, DNS, registry 5xx, and 429 failures retry with capped
exponential backoff and a longer in-budget `Retry-After`; permanent not-found,
invalid-manifest/archive, and unsupported-platform results fail immediately.
The helper reports only sanitized mechanics facts (HTTP status, network/DNS,
platform mismatch, engine loss, resource exhaustion, manifest rejection, and
`Retry-After`); this agent policy is the sole classification table.
Budget exhaustion is terminal `image_unavailable`, the three permanent
classes retain their matching spawn codes, and engine/session loss is
infrastructure `runtime_unavailable`. L1 remains `Claimed` throughout local
`pulling` and can become `Started` only through the existing image-observation
then fenced-start sequence below. Service restart policy explicitly treats all
four image spawn classifications as terminal.

Before image delivery, the adapter loads the canonical runtime platform saved
by the successful functional probe for the current helper generation. It sends
that platform through `EnsureImage`, keys shared image work by it, and rejects
first-binding evidence for any other platform. An L1 4xx refusal of the pre-Run
observation is terminal `process_request`; transport failure or L1 5xx is
`runtime_unavailable` and remains eligible for the one-shot pre-start budget.

When delivery fails before the helper `Run` RPC is entered, the OCI adapter
returns positive `no_runtime_resources` reap evidence without calling helper
`Delete`. Finalization therefore preserves the image/runtime spawn code instead
of replacing it with `output_error` merely because no attempt was ever created.

## Authority clock

Each claim and renewal establishes a local authority deadline from the
request's monotonic start plus the returned `lease_ttl`. The agent never
subtracts `lease_expires_at` from its own wall clock. An independent watchdog
cancels the attempt at that deadline even when the renewal RPC is silent.

The watchdog also compares wall-clock progress with the remaining monotonic
lease. A suspend gap that consumes the remainder cancels the attempt before
the renewal loop can issue another request. Every L1 operation is bounded, and
each renewal timeout is strictly shorter than the remaining local authority.

For `kind=oci`, the privileged helper provides the Guardian-equivalent second
boundary described in [OCI helper protocol](oci-helper-protocol.md). The agent
refreshes an attempt's helper deadman only after the matching L1 lease renewal
succeeds. Agent-helper control EOF, a helper-clock heartbeat blackhole, or an
expired per-attempt deadman therefore reaps runtime-owned state independently
of the agent's own authority watchdog.

An OCI claim remains L1 `Claimed` while the helper resolves and ensures the
image. Successful `EnsureImage` returns the complete immutable image evidence;
the agent persists it before invoking helper `Run`. The helper then registers
`Wait`, starts runc-v2, and returns `Started` plus the same image evidence. The
agent replays the observation, performs fenced L1 `StartAttempt`, and only after
both succeed marks its local observer running. Lease renewal, log append, and
completion remain incapable of implicitly promoting the attempt. If the
pre-Run observation is refused, no runtime resource is created; if either
post-start mutation is refused, the adapter kills and verifies deletion of the
real task before returning a spawn failure.

Image lifetime is represented by four different holds rather than one
overloaded reference count. The helper owns the short operation lease around
pull/import/unpack, a boot-scoped attempt pin, a service-binding pin, and the
evictable cache record. Every successful singleflight waiter attaches its own
attempt pin (and service-binding pin for service-class work) before the shared
operation lease can be released. Attempt reaping releases only the attempt pin;
service stop/restart retains the binding pin.

Before a service-class image operation starts, the agent commits the binding's
reference, top-level digest, canonical runtime platform, and snapshotter to its
FULL-synchronous local SQLite ledger using insert-or-compare: the first binding
identity is immutable, and a later probe-platform mismatch fails the service
rather than rewriting it. A newly inserted row is removed if its delivery
terminally fails.

Boot keeps OCI publication restrictive and reuses its restrictive heartbeat's
removal directives. For every ledger row the agent obtains positive current L1
service-binding proof; cold-empty L1 state is not proof, so unbound rows are
deleted. The remaining ledger is sent as an absolute helper reconciliation.
Every missing digest after an external cache wipe is automatically redelivered
under ordinary agent image policy, followed by a second helper reconciliation
that attaches recovered leases. Exhaustion latches the affected service and
leaves eviction disabled. Verified
removal releases the helper binding hold and then the local ledger row after
runtime/data absence proof but before L1 acknowledgement; it never requests an
image deletion.

If a waiter attached its attempt pin but execution is abandoned before helper
`Run` (platform mismatch, observation refusal, mount revalidation, or another
pre-Run failure), `ReapAndVerify` invokes the helper's exact-authority
idempotent pre-Run pin release. A successful helper `Run` response disarms that
path; a helper `Run` error leaves it armed. Ordinary verified attempt deletion
then owns both runtime cleanup and attempt-pin release after successful entry.
