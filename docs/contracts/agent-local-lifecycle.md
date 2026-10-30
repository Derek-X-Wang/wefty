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
| `retiring` | New claims are closed for this Node agent generation. Existing ordinary and Restartable process one-shots keep execution, renewal, output and publication without a retirement deadline. | Exact local reap and evidence proof plus L1 closure succeed, or an ordinary shutdown signal acts. |
| `draining` | New claims are stopped while resident attempts are joined. | The drain completes and `Run` returns cleanly, or a second shutdown signal forces cancellation. |

`session_backoff` is the current retry delay and is zero while ready. A
quarantined session reports the maximum backoff so it cannot look like a
healthy idle process. `last_semantic_error` retains the most recent L1 error
code, message, and local observation time; transport errors do not invent a
semantic code.

The first SIGINT or SIGTERM starts a graceful drain. The 30-second bound
applies to the drain request to L1, which moves the node to `draining`; the
agent stops claiming, interrupts Restartable process one-shots, and waits for
the other resident attempts to finish. Waiting for those other attempts is
bounded only by each attempt's own max runtime. Until a legacy process exits its
exclusive node lock is held, so another legacy agent for that stable Node is
refused. Enrolled generations use the cohort ownership rules below. A second signal is an explicit forced-shutdown transition: it cancels
resident attempts immediately. While Drain is joining residents it emits
`forced_shutdown transition=draining_to_forced reason=second_signal`; after
Drain returns it emits
`forced_shutdown transition=drained_to_forced reason=second_signal`. These
exact tokens distinguish operator abort from clean drain completion.
`--shutdown-budget D` (default `0`, which keeps that behavior for ordinary residents) makes the
agent take that same forced transition itself once `D` has passed since the
first signal, emitting `reason=shutdown_budget` in place of
`reason=second_signal`. Resident attempts are then cancelled in order: each is
terminated with `termination_cause=agent`, reaped and finalized, and its result
reaches L1. Each attempt's cancellation phases run one after another: the
process runner's termination grace and reap; the final log flush, which runs
to the finalization timeout and can overrun it by the one request in flight
(one L1 operation timeout); then the completion or its evidence recovery and
the result upload (one operation timeout each). `agent.ForcedShutdownBound` is
their sum. A service manager that
kills after a fixed timeout must allow `D` plus that bound; `wefty setup`
passes a budget to the installed node agent and derives launchd's `ExitTimeOut`
from both, with slack.

## Node agent generation retirement (#1175)

`--agent-generation-id ID` supplies an installation-allocated identity to
`agent.Config.AgentGenerationID`. An empty identity keeps the legacy session
path. The flag alone does not advertise `upgrade:retirement:v1`, allocate an
identity, change installation locks, or enable installed rolling upgrades.
Shared storage ownership is built in #1176 below. Installed activation sequencing
remains #1179 integration.

An enrolled primary can recover a `node_dead` heartbeat or claim RPC refusal
when an authenticated generation observation still confirms it as primary.
An authoritative inventory refusal with a re-registration reaction, such as
`node_not_registered`, also permits this recovery.
Re-registration keeps the same generation and boot, and runs inside the serving
session. Resident contexts, Guardians, output and Attempt renewal keep running;
actual Attempt Lease loss and explicit cancellation retain their existing rules.
If promotion races registration, another authenticated observation latches the
normal retirement path instead of quarantining or retrying registration as a
retiree. Legacy agents keep their existing session recovery behavior.

Generation-observation protocol refusals with a node-session reaction use the
same classifier as heartbeat refusals: re-register in place, drain, or quarantine
(including `identity_bound` and `principal_forbidden`). This applies before a
heartbeat, after a claim error, and during re-registration. Transient inventory
failures and local verification failures (boot mismatch, missing retiring
authority, or resident ownership) still retry with backoff. Re-registration
re-observes the generation, and a latched retiree never registers. The loops
share the first node-session failure log until a heartbeat succeeds; successful
re-registration logs the rejoin.

The dedicated trigger is L1's authenticated generation inventory after its
fenced promotion transaction. The existing serialized heartbeat loop reads
`GET /v1/agent/nodes/{node_id}/generations` for an enrolled session. An enrolled
claim loop also reads it after a claim refusal, so a demotion racing the normal
heartbeat cannot cancel sibling Attempts. Only an exact generation and original
boot in role `retiring`, selected protocol 1, with a promotion operation and
matching resident identities can latch local retirement. A committed claim with
a lost response has no local runtime or credentials to adopt; it remains pending
until L1 authoritatively ends its Lease. `Agent.Retire(ctx)`
requests and acknowledges that same observation immediately; identical calls
are safe. A local call while L1 still records `primary` refuses. No signal,
Drain, context cancellation, or shutdown-budget timer triggers retirement.
The upgrader drives L1's existing admission closure and promotion endpoints;
it needs no second local authority source. This uses L1's private authenticated
control response in place of a separate local control socket.

The latch joins the mutex held through an in-flight claim RPC and its local
admission record. A returned claim runs and completes normally. Claim workers
exit after their resident Attempt finishes. They never start another claim
once the latch closes. Ordinary and Restartable one-shots retain their parents,
Guardians, Lease renewal, output readers, run mailbox publication, and real exit
status. Each Job's own limits, explicit cancellation and Lease loss still apply.
The retiree reports `retiring` through connectivity retries and uses only
`POST /generations/{id}/heartbeat`; it never re-registers or publishes primary
Node observations. Node-wide Computer policy watches stop at retirement.

Generation-aware live finalization keeps normal renewal active while ordered
pending logs and first completion delivery retry. Before inspecting or draining
logs, delivery joins asynchronous durable redaction-tail appends, using the same
ordering barrier as shutdown recovery with the live Attempt lifetime. An empty
spool does not prove an outstanding append has committed. An ambiguous completion reply
retries the same durable request. It does not release that first delivery to
an outbox and deliberately age out the Lease. Ordinary shutdown still uses its
existing bounded evidence recovery path.

After every claim worker has returned, the retiree retains positive runtime
reap records and evidence publication records; an empty running map is not
proof. Every terminal path, including restart directives and Lease loss, retains
a query over late appends, pending logs, durable completion disposition, and
mailbox/result publication. Closure reconciles that query again after recovery;
a missing Run mailbox does not prove delivery. Positive delivery proof survives
bounded outbox receipt retention. Sealed incomplete or undelivered evidence
keeps retirement pending unless permitted durable custody is acknowledged.
Pending evidence keeps it alive. `Config.RetirementEvidenceSettled` is
the #1176 integration seam, with exact pending Attempt identities, for an exact
durable custody proof when log/completion delivery is incomplete. Mailbox and
result publication must already have settled under their own rules. When configured, it
always checks closure and may also veto it for other owned evidence. It cannot
replace runtime reap or the L1 closure
prerequisite. `POST /generations/{id}/close` carries the original boot, promotion
operation, exact frozen residents and positive local attestations. L1 itself
checks that every resident completion was accepted or its Lease authoritatively
ended. Refused, missing or ambiguous close responses keep the retiree alive and
retry identically. Successful closure lets `Run` return cleanly, independently
of any other retiring generation.

TERM during retirement starts today's drain and shutdown-budget path.
Drain is local for a retiree and returns no L1 Node projection; it must not
call the primary-only Node drain route or mutate the successor. If TERM races
local observation after L1 demotion, a `node_session_replaced` Drain refusal
triggers an authenticated generation read. A verified exact demotion makes it a
local drain; missing, rejected or different authority keeps ordinary error
handling. This race cannot start forced cancellation on a verified retiree.
Restartable residents receive `agent_shutdown`; ordinary residents wait until
they finish or existing forced cancellation acts. Retirement adds no alternate
signal semantics. It does not close a generation after forced shutdown without
its normal proof; later installation recovery owns that record.

## Immediate fleet heartbeat signal

A fleet-profile node agent handles `SIGUSR1` by coalescing a wake-up for its
existing serialized heartbeat loop. The signal does not drain, stop, register a
new boot, change claims intent or introduce an L1 API. Attempt renewal keeps its
independent cadence. The handler is installed before readiness can advertise
`immediate_heartbeat: true`. The private node-agent ready record also includes
`pid`, `ready_at`, `node_id` and `boot_session_id`.

A local upgrader may request this heartbeat only after successfully observing
L1 after restart. It must verify that the ready record is a private regular file,
that its PID matches the running LaunchAgent, that its Node/boot match L1, and
that `ready_at` is after this system boot. It sends `SIGUSR1` through launchctl
to that verified local node agent. Missing capability sends no signal. Invalid
receiver evidence sends no signal and logs one fallback notice; a signal error
uses the same fallback. The normal renewal proof and time budget remain in
force. Requests retry at most once per second until renewal is observed.
Legacy plain upgrade does not use this signal.

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
receipts reach L1. A create that ends in an error instead of a receipt is
retried on its own per-copy backoff -- the first retry one default heartbeat
later, doubling to a five-minute ceiling -- so neither the heartbeat nor an OCI
recovery pass redispatches it faster; a receipt of either kind clears it
(#558). Prune removes only the deterministic Wefty-owned copy root
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

The macOS node-agent process is the only Lima supervisor. The plain daily
install can now add the host VM to its interactive `dev.wefty.node-agent`
LaunchAgent in the owner's GUI domain, without Mac privilege. The isolated
attended proof uses `dev.wefty-proof.node-agent`. Both use the same supervisor
and preserve the recreate guard. Their paths, guest-root bootstrap, intent and
removal rules are in [per-user host VM installation](per-user-host-vm.md).
The older interim system bootstrap remains available; its system LaunchDaemon
label is `dev.wefty.agent`; launchd runs it as the operator user with absolute
program, working, and log paths plus explicit `HOME`, `LIMA_HOME`, `USER`,
`LOGNAME`, and `PATH`. The plist contains no Fabric credential, and a competing
`io.lima-vm.daemon.*` system unit or `io.lima-vm.autostart.*` system/user/gui
login unit makes installation fail before mutation.
The system unit uses `RunAtLoad`, throttled `KeepAlive`, and the system launchd
domain, so OCI failure cannot turn into a process-killing launchd loop.

Lima supervision reads an injected, read-only revisioned OCI-intent source.
Missing, unreadable, or malformed state is disabled. Explicit bootstrap creates
the initial on-demand marker only when no marker exists and preserves an
existing off marker. The operator controller owns later writes and CAS. The supervisor
rechecks intent after every inspection and lifecycle command. Disable wins an
intervening start or repair, leaves Lima stopped and OCI restrictive, and a
revision change fails closed for the next cycle. When the switch is always-on,
or on-demand has known demand, `Stopped`
permits one bounded `limactl start`; `Broken` permits one bounded `stop --force`
followed by capped-backoff starts within an overall deadline. Failure leaves the same
agent process and `kind:process` capability available while OCI stays
restrictive; a successful start still must pass helper handshake, complete
boot sweep, removal resumption, and the functional probe before publication.

One shared cycle lock covers supervisor inspection/mutation and the helper boot
barrier, so the watchdog cannot force-stop Lima during sweep. Recovery retires
the old helper generation, publishes the restrictive revision, then performs
Lima recovery, handshake/sweep, probe, and pinned positive publication. The
pinned positive publication is part of that one serialized recovery
transaction, not a step after it: a recovery already queued behind it must not
be able to retire the generation mid-publication and turn a recovery that
succeeded into a reported failure. Every
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
the supervisor's monotonic repair counter and its bounded trail of observed
Lima lifecycle transitions, and one stable reason code. The state field is a
current value and the writer polls no faster than 20 seconds, so the trail is
what makes a repair shorter than one observation window provable afterwards;
it is capped, carries only closed state values and timestamps, and adds no
polling. The repair counter counts every recovery that mutated a non-Running
instance back to Running, whichever non-Running state Lima reported. The trail
carries the states the supervisor itself read and performed and nothing else:
Lima's status for a host-level fault is not a stable readable state — two
`limactl list` invocations against the same instance in the same second have
returned Broken and Stopped — so a bounded repair is proven by the counter delta
plus the trail pair, and a Broken reading taken by an operator is recorded as the
host observation it is, never as a state the supervisor is required to have seen.
Unit/helper/probe/instance states are closed types;
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
It also surfaces the accepted process-payload UID limitation (#220) without
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

The versioned OCI intent marker records the owner's host VM switch, separate
from L1 `claims_enabled`, Capability revision and node-computed demand:

| Position | Runtime authority |
| --- | --- |
| `off` | No OCI work runs. The command immediately ends resident OCI work and lists its Job IDs. Process work continues. |
| `on-demand` | Start when node-owned demand exists; stop after the idle wait, default `10m`. |
| `always-on` | Keep the host VM running and repair it through the watchdog. |

`wefty node oci off|on-demand|always-on` replaces `start|stop`. Each command
compares and swaps the marker revision and fsyncs the replacement and parent
before runtime effects. Repeating a position does not create a revision.
Version 1 `enabled: true` maps to `always-on`; `enabled: false` and a missing
marker map to `off`. Missing or malformed state cannot grant recovery authority.
A new install writes version 2 with `position: "on-demand"`. Version 2 also
retains `enabled` as the projection `position != "off"`; a version 1 reader
refuses the new marker rather than interpreting on-demand as always-on. Reads
normalize valid legacy state in memory without changing its durable revision.
Demand is never written into the marker.

The node computes demand from running Computers and other resident OCI work,
queued OCI work waiting for this Node, unfinished helper directives (removal,
Backup, prune, restore, clone, reset and grow), and an owner `load-image`.
Reimage and Custody export helper work also retain runtime authority. Cleanup
counts until it finishes, including asynchronous calls that are still unwinding
and durable removals recovered after a node-agent restart. An unreadable demand
source prevents idle stop. `QueuedOCIDemand` reads node-owned cached claim-poll
facts. The new reader explicitly sends `accept_queued_oci_hint: true`; old
readers receive only their existing claim/204 reply shapes and no hint fields.
The Mac node agent replaces that cache on each successful poll, including
an empty reply, and immediately republishes its demand snapshot. A Linux node
agent ignores the hint. The demand read performs no network request under the
claim lock. No control
socket request accepts a demand value. The node control socket checks peer
ancestry through native parent-PID reads in each connection handler, outside
the accept loop. A descendant of the node agent or an unreadable ancestry
receives HTTP 403 `forbidden`, with an instruction to use the owner's own
shell; the CLI reports that refusal with exit 3.

The idle wait starts at the first empty demand observation, using the node's
clock. Each demand observation and the end of an owner image import stamp the
last-demand time, so demand between watchdog polls resets the wait. The watchdog
polls every five seconds. `--host-vm-idle-wait` changes the
node setting; per-user `wefty setup` persists it and preserves it when omitted.
After the idle wait, the node serializes with OCI recovery, publishes reduced
capability with `host_vm_idle`, and waits for L1's acknowledgement. It then
checks demand under the claim lock, takes the Lima cycle lock, and checks demand
and the switch again before shutdown. New demand before shutdown cancels the
stop and restores the pinned ready generation without replacing its helper
session. Full recovery is needed only if that generation is gone. Under the
claim lock, the node writes a durable stopping mark, then releases the claim
and recovery locks before the stop command. Only the Lima cycle lock spans
the command. Heartbeats skip OCI recovery while stopping; process claims,
completion bookkeeping and doctor remain available. A node-agent restart
reconciles the stopping mark with Lima state before watchdog recovery. The cycle
runs graceful `limactl stop` with a
separate 90-second timeout (`DefaultHostVMIdleStopTimeout`). Only a timeout
permits `--force`, with its independent short command bound (default 30 s). Other
failures remain failures and re-arm the idle wait before another attempt. This does not
raise the ordinary thirty-second command budget. A stalled shutdown consumes
at most the graceful budget before force-stop; the fixed three-minute Seat
approval deadline also covers force-stop and the subsequent cold start.
Demand observed before shutdown cancels the idle stop at the claim or final
cycle recheck and retains the pinned helper generation. A request approved
just before withdrawal cannot produce a hint while L1 still sees `kind:oci`.
There is no extra claim poll between withdrawal acknowledgement and the final
demand recheck. If no ordinary poll delivers its hint before that recheck, the
request follows finish-then-restart, still within its three-minute approval
limit. Once shutdown starts,
it finishes under the cycle lock. The claim-poll hint remains demand through
that shutdown, so the watchdog then starts the host VM and earns a fresh ready
generation. The Seat caller sees only the cold-start reply and its fixed
approval deadline. The limit applies only when the request encounters
`host_vm_idle`; other Seat starts retain their prestart budget plus one minute.
During the real cold start (about 25 seconds), capability probes can report
`lima_stopped` or `helper_unreachable`. Queued status may briefly read
unschedulable until ready capability is published; the cold Seat deadline
remains armed, and a CLI wait that saw the cold-start reply keeps waiting.
An owner `oci always-on` or `load-image` during shutdown
gets "the host VM is stopping; retry in a moment", rather than doctor advice.
On node-agent restart, the stale stopping mark is cleared even if the Lima
state probe fails; normal recovery reports `probe_failed`. Recovery reads the
owner's switch before trusting a stopping mark; *off* always wins.

A stopped host VM with no demand under `on-demand` has the closed capability
reason `host_vm_idle`. Every idle suppression reports the configured helper's
restorable set in `missing_capabilities`: `kind:oci`, `cgroup_v2`, configured
runtime handlers, and the helper's Linux runtime platform for this node's
configured architecture. It includes `computer` only when Computer room is
configured. The set also retains the last positive OCI probe facts in memory,
subject to that Computer-room rule. This works before any successful probe,
after node-agent restart, and after off → on-demand with the host VM stopped.
These missing capabilities are demand hints only; they do not advertise earned
execution capability or bypass the normal functional probe. Doctor treats that state and its reduced capability as
healthy, reads no guest diagnostics for that state, never starts the host VM,
and displays the demand items when it runs. The watchdog repairs stopped or
broken state only for `always-on`, or `on-demand` with known demand; it never
restarts an idle-stopped host VM. Broken state with no demand is force-stopped
and recorded as idle, without a repair start. Quiet idle heartbeats skip
`Ensure`, publish once, and log only the idle transition. Before a host VM
starts, the node checks free
space on its Mac disk volume. The default minimum is **5 GiB**; the node setting
`--host-vm-minimum-free-bytes` and per-user setup can change it. An unverified or
insufficient measurement refuses startup with a reason in GiB and a fix.
Removal preparation is exempt from this check, so cleanup works on a full disk.

`setup-oci` remains configure-only: it checks prerequisites, writes configuration
and reports convergence without starting OCI. An explicit restart/recreate with
zero live OCI attempts retains its existing guards. A per-user node startup
prepares helper and probe assets once; an unchanged prepared on-demand install
stays stopped across daemon restarts while its Lima instance still exists.
A receipt alone cannot skip preparation for a deleted instance. The proof
install keeps its attended
bootstrap and dialog behavior.

Configure-only setup writes a separate desired-state receipt before returning
a restart/recreate refusal. The applied receipt advances only after authorized
convergence, so doctor derives unchanged, live-safe, restart-required, or
recreate-required by comparing current with desired rather than echoing the
last probe reason.

Every host VM recreate, including a per-user installation's recreate, passes
the same guard immediately before Lima deletion, under the Lima lifecycle
lock and after OCI admission and resident work have quiesced. Zero live
attempts alone never authorize deletion. Any Computer Storage generation or
Backup copy on that disk refuses the recreate, including stopped Computers,
staging and retired generations, quarantined Storage, and unresolved reset
records. There is no acknowledged-loss override.

The guard reads the helper's durable guest files. The node agent's local spool
records removal acknowledgements, not a complete inventory of physical copies;
the helper's namespace Verify inventory also omits Backup copies. Neither an
empty spool nor an empty runtime inventory proves an empty host VM disk. The
shipped Lima helper uses `/var/lib/wefty/oci`; a different helper command or
runtime-root override is unknown and refuses. With the host VM known running,
the guard stops both the helper socket and service, joins their stop, and
inspects `computer-disks`, `computer-backups`, `computer-disk-quarantine` and
`computer-storage-resets`. Entries block except for
`computer-backups/supersessions`, which contains removal and supersession
records rather than Backup copies. That folder must be a real directory of
regular files named `wefty-backup-copy-<32 lowercase hex>.json` or
`.supersession.tmp-*` with a nonempty suffix, as written by the helper. Any other
filename blocks as an unexpected Backup supersession entry; a symlink,
subdirectory or other non-regular entry refuses.
Losing these records during recreation is acceptable once the guard has proved
that no source Storage or Backup copy remains on the disk. Copy folders still
block when those records are present. A directory in `computer-disks` holding
only a regular `attachment.lock` and `copy-refused.json` also blocks, but is
reported as a refused Computer copy record rather than a Storage generation.

A missing namespace directory is absence only after successful enumeration of
its parent; an empty namespace needs a successful directory read. The runtime
root is created lazily. Its absence counts as empty only after its real parent
`/var/lib/wefty` enumerates successfully; an unavailable parent remains unknown.
Symlinks, non-directories, failed reads, malformed or incomplete answers and
unavailable inventory all refuse. The script restores socket activation on
refusal and handles EXIT, HUP, INT and TERM while ignoring PIPE. Go also makes
a bounded best-effort socket-start attempt after a non-nil guard result when
the inventory command was attempted (including rejected output, cancellation
and timeout), and after failed Lima deletion. Before inventory is attempted,
refusal does not access the guest or attempt socket restoration. A restoration
attempt uses a context independent of the canceled request and the ordinary
command timeout. A failed restoration is logged without replacing
the original refusal. A successful empty inspection leaves the helper stopped
until host VM deletion, so no helper operation can create bytes between the
check and deletion.

A stopped, broken or unknown host VM never starts just to answer this guard.
Without a complete durable host-side absence record its inventory is unknown,
so recreation refuses and explains that the owner must start OCI before a new
inspection. A proven empty disk still permits recreation through the ordinary
template/start/recovery path. No Storage or Backup is moved or deleted to make
recreation pass.

The node-local refusal is HTTP 409 `host_vm_recreate_blocked`, CLI exit 5,
`retryable=false`. Its message and `details.blockers` name the blocking
Computer and Storage IDs and generation, or Backup and copy IDs, read from
regular helper identity manifests. A missing or invalid manifest still blocks
and is explicitly labeled identity unavailable; it never proves absence.
The message tells the owner to remove or export the named Computer or Backup
before manually rerunning setup. `details.reason` explains an unknown inventory
and the error says no automatic retry occurs. Empty `blockers` and `reason`
fields are omitted. Raw guest command output is never part of the refusal.
The real CLI binary checks this typed exit. A source-guard test confines host
VM delete calls to `runner/lima/supervisor.go`.

Singular `wefty node` commands resolve the installed node configuration before
Fabric initialization and use only the live agent's Unix control socket. The
configuration has two valid shapes: an OCI installation names its absolute
`control_socket` (an older one may omit `node_id`), and a plain-process node
installed by `wefty setup` records `node_id` with no `control_socket`. A file
with neither is invalid. The plain shape still names this machine's node for
`wefty status` and `wefty exec`; singular `wefty node` commands refuse it with
an error naming the file, because they need an OCI installation. Its
private runtime directory is `0700`, its socket is `0600`, an active socket is never replaced,
and a symlink or non-socket path fails closed. The server authenticates every
accepted peer with `SO_PEERCRED` on Linux or `LOCAL_PEERCRED` on macOS and
allows only the installed operator UID. It also reads the kernel peer PID and
refuses callers descended from the node agent, including process jobs, before
a request reaches the controller. Unavailable process identity fails closed.
This check applies to the whole socket, including switch and image-import
operations. The refusal is a control-path guard, not same-user
isolation: detached or proxied code that escapes its process ancestry remains
within the trusted-process limitation of #220. Process-kind payloads still share that UID with the node agent. That is accepted behavior (#220): choosing the non-sandboxed
process kind is choosing that trust level, container kinds are the isolation
story, and the limitation grants no additional control authority. The agent is the sole writer of
intent and runtime state. `load-image` streams an OCI archive through this
surface to the existing helper-owned import/cache seam and returns only
verified top-level and admitted-platform digests plus bounded evidence. The
import is keyed by the name the archive itself carries and never by an
invented one: a tagged export keeps its tag, a digest-only export keeps its
digest, and only a bare repository takes `:latest`, so two archives exported
from one repository cannot claim one name. `--reference` names only an export
that named no artifact. It may name an archive carrying no reference, or one
carrying a repository with no tag — bare, or qualified only by a digest —
inside that archive's own repository. A bare repository request whose
repository matches a tagged archive resolves to that archive's own tag rather
than renaming it; every other `--reference` cannot rename a tagged export,
cannot
carry a digest, and cannot rebind a name that already identifies different
bytes, which stays a typed `manifest_rejected` refusal. The reference travels
as the one `load-image` query parameter, and a repeated or unknown parameter is
refused rather than ignored. An agent older than this flag ignores it, so a new
CLI against an old agent imports under the archive's own name with no error;
compare the printed digests with the artifact receipt when that pairing is
possible.
Node-local JSON failures always carry a closed, sanitized `details.reason`:
typed helper failures retain their helper protocol code, adapter-side platform
diagnostics use `diagnostic_failure`, other recognized adapter failures use
their bounded helper code, and an otherwise plain internal failure uses
`internal`. Raw paths, error strings, credentials, and other helper-local
mechanics never cross this boundary.

Stop ordering is durable disable → restrictive local Capability observation →
published-service withdrawal → attempt reap and positive runtime quiescence →
Mac Lima stop. Durable disable and restrictive local admission prevent recovery
during quiescence; routine heartbeat probes cannot supersede the disabled
intent, and a serialized recovery transaction reopens positive OCI observation
only after it re-reads a durable enabled intent. Only after positive reap
receipts does the Mac path acquire
the shared recovery-cycle lock for the VM stop. This avoids a lock inversion
with attempt finalization while preventing the background watchdog from racing
the final VM transition. Linux leaves containerd running. A failed
post-persistence action never rolls the intent bit back. Start persists enabled,
reacquires the helper, completes the boot barrier and probe, and only then
publishes the positive Capability revision.

The durable marker also fences terminal evidence for a resident OCI service.
The live completion path and process-lifetime outbox take the shared read side
from their final marker observation through one L1 response or one suppression
transaction; after persisting disable, the controller publishes that revision
to the gate, takes the write side, and only then stops the runtime. An enabled
completion that linearized before the stop may land; every later completion
observes the disabled revision and is suppressed. A suppressing reader holds
the read side until its local disposition is durable. If that persistence
budget expires, the reader registers a typed
`OCIIntentSuppressionPersistenceError` before release. Within one agent process
lifetime, either the waiting writer or the resident-teardown join returns that
failure instead of allowing `Controller.Stop` to report runtime quiescence. A
successful controller stop is therefore also a same-process observation barrier
for the suppression receipt. Resident teardown retains every typed persistence
failure until the join consumes it, including a completion that exits between
the fence release and the first teardown scan, and joins all resident failures
before returning. A positive reopen supersedes every typed failure from an
older disabled-intent episode; resident teardown discards such a failure even
if its delayed defer becomes readable during a later stop. The receipt retains
the observed intent revision.

Across an agent restart, recovery re-evaluation is the durable classification
strategy; there is no separate `suppression pending` marker. Every reopened
`durable_completion` OCI service row governed by `requiresOCIIntentFence`
re-reads the current durable intent while holding the completion gate before
its first L1 publication call. A disabled revision therefore writes the
ordinary suppression disposition and excludes the row from replay before
anything can publish it; a re-enabled revision permits publication. The prior
process's typed error is not reconstructed, so an already-restarted controller
stop is not a startup-recovery completion barrier; the safety guarantee is that
recovery re-suppresses the row before publication.

The suppression lock order is completion-gate read side, then the spool's sole
synchronous=FULL database connection. Output-event appends use a second,
synchronous=NORMAL connection and take no lock the suppression path holds, so
they wait at most SQLite's 5 s busy timeout for the FULL connection's write
transaction and never form a cycle with it. The database transaction, including any retention
compaction, uses the lesser of the configured agent operation timeout (ten
seconds by default) and the OCI control server's 10m5s response-drain budget,
and retries only within that window. Database connection contention therefore
cannot outlive the control drain. The same committed disposition notification
is emitted by live completion and process-lifetime recovery so bounded
acceptance observation covers either path.

Only an authoritative `enabled=false` suppresses a payload. On a node with an
intent authority, an unavailable or malformed marker withholds publication,
records `intent_authority_unavailable`, keeps the serialized exit evidence
intact, and retries with outbox backoff. A nil marker reader means the fence is
not applicable and publication proceeds; construction requires an authority
for an OCI probe, runtime, normalized `kind:oci` capability, or bare `oci` key.
Suppression likewise retains the payload as typed late evidence and records
`service_intent_stop`, but excludes it from replay. The agent is
obligated not to publish the intent-stopped result and to let lease expiry
produce the ordinary `lost`/no-result outcome; L1 has other explicit lost paths
and is not claimed to enforce this node-local marker itself. This fence does not
suppress OCI one-shot completion or any process-kind workload. An
unclassifiable legacy service spool row is fenced by the same durable marker
rather than allowed to bypass it: enabled intent permits publication, disabled
intent suppresses it, and unavailable authority withholds it.

Suppressed terminal retention has two bounded tiers. Per service Job, the agent
keeps the newest 16 drained attempts as full serialized payloads. Once an older
suppressed attempt has no pending log events, it is compacted into the node's
completion-receipt ledger as a diagnostic-free terminal summary: typed
disposition and reason, intent revision, finish time, terminal arm and stable
failure code, additive resource flags, and runtime-quiescence evidence. The
receipt ledger keeps the newest 1,024 entries node-wide; service Job removal
purges both full and compact local evidence for that Job. An attempt with
unacknowledged logs is never compacted merely to satisfy the 16-payload window.

A suppressed attempt drains retained logs under the same policy as other
evidence: while L1 reports the attempt live, logs remain strictly before
completion and drain before recovery advances; after L1 reports the attempt
lost, each recovery pass is bounded by the configured lost-log batch limit.
Suppression uses the bounded persistence window above; it does not widen the
live log-drain or L1 request budgets.

Every syntactically OCI-capable `agent.Config` literal must declare its
`OCIIntent` authority. The source contract scans all Go files independently of
host platform and build tags; runtime construction refusal remains the
fail-closed backstop for dynamic configurations and explicitly nil authority.

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
finalization failure before durable completion is stored. A live lifecycle
owns its spool attempt, so process-lifetime recovery cannot race its log sink,
completion delivery, or node-local intent-suppression decision. If authority
loss or cancellation abandons live delivery after suppression is ruled out,
the lifecycle does not classify the cancellation cause as a `/complete`
response. If a one-shot cancel interrupts completion after execution already
ended, during either the request or transient retry backoff, the lifecycle
keeps cancellation provenance, joins abandoned delivery and immediately retries
its identical durable completion under a bounded uncanceled context, then
performs normal result upload and retention. Other authority loss releases ownership
and wakes the bounded outbox reconciler. So does a completion L1 permanently
rejects, on the codes on which recovery seals a completion: `invalid_request`,
`conflict`, `idempotency_conflict`, `not_found`, `unsupported_class`,
`unsupported_kind`, `unsupported_runtime_handler`, or `not_implemented`. A
post-`Started` OCI `spawn_error`, refused with 409 `conflict`, is one. The live
attempt sends it once and stops, which ends its renewal; the refusal is the
attempt's, never the node session's. Recovery drains the logs L1 still
accepts, replays the identical completion, and seals it when L1 permanently
rejects it again. With renewal stopped, L1 settles the attempt by lease
expiry.
The reconciler survives the attempt authority context, runs at most eight
attempt workers, and retries both spool scan failures and per-attempt failures
on the injected clock: the configured retry interval, doubled per consecutive
failure of the same work and capped at 30 seconds (or at the interval itself
when that is configured larger), reset by the first success. A retry that
falls due while it cannot start -- all eight workers busy, its attempt live
again, or the spool scan backing off -- waits for what unblocks it: a worker
finishing, the attempt's release, or the scan's own retry. A retry held by a
live attempt is also re-checked from the retry interval, doubling to the same
cap, for a release that does not wake the reconciler. The reconciler never
arms a timer for a retry it has just found due and blocked. A typed refusal
that repeating on a retry interval cannot clear -- `node_session_replaced`,
`identity_bound`, or `principal_forbidden` -- moves the attempt onto a
re-check schedule instead: 10 seconds, doubling per consecutive refusal, capped
at one hour, and never abandoned while the evidence is pending. Only the first
refusal of a run is logged (a different refusal code is logged once as well);
identical repeats are silent, and one line records the attempt's recovery when
it finally succeeds. A stuck attempt therefore costs at most one log line per
agent process and about one L1 request an hour. Its durable evidence is left
pending on disk throughout, neither delivered nor sealed, and a restarted agent
starts the schedule again. The re-checks exist because `node_session_replaced`
also answers a completion L1 has not accepted while the replaced registration's
lease is still running; once that lease has expired the replay is answered
`lease_expired` and the result is kept as late evidence, but only within L1's
late-evidence window (48 hours by default, configurable down to minutes; after
it L1 keeps a gap instead). The spool does not keep the lease a recovered
attempt was claimed with, so the schedule starts short and lets doubling find
it: every wait equals the time already waited plus 10 seconds, so the first
re-check after the lease runs out comes at most the lease's remainder plus 10
seconds later, and never more than an hour later. The result therefore lands
as itself on any L1 whose late-evidence window is longer than its lease plus 10
seconds; the cost is a few silent requests in the first minutes. A completion
L1 already accepted from an older registration is not refused: the agent always
replays the exact request it spooled, and L1 answers a byte-identical replay
from the owning node identity as an already-recorded success
(`Idempotent-Replay: true`, see `lease-fencing-dispatch.md`), which recovery
treats like any accepted completion -- the row is marked delivered and retired,
with no further requests or log lines. While L1 reports the attempt as
live, recovery drains every durable log batch before delivering the single
completion. After bounded log finalization leaves logs pending,
shutdown, one-shot cancellation, and renewal cancel/stop/restart directives
persist completion and stop renewal. One route decision selects live delivery
with `completeWithRetry` when no logs are pending, even if recovery has stopped.
This preserves transient retries and `run_ledger_unavailable` backoff within the
existing delivery budget. Pending logs require ordered delivery: hand off while
recovery is running, or deliver inline when process-lifetime recovery is stopping.
For inline delivery, the lifecycle keeps attempt ownership and delivers pending
logs first, then completion, under `context.WithoutCancel(ctx)` plus the existing
`operationTimeout`. It reads the actual recovery lifetime after finalization
joins, independently of the cancellation cause. If shutdown interrupts an
in-flight completion, an abandoned request is routed like pending evidence: hand
off while recovery is running, or deliver the unchanged persisted request inline
when recovery is stopping. Once the caller's context is canceled, this rule
applies whether the abandoned completion result or shutdown wins the lifecycle's
select. A delivery verdict or local persistence failure keeps its own meaning;
only an abandoned request is retried. This does not extend log
finalization. If inline delivery is incomplete, durable evidence stays on disk
for delivery on the next start, when L1 may already have settled the attempt by
lease expiry and can accept completion only as late evidence. Cancellation and
renewal cancel/stop/restart directives use the same route rule. A hand-off releases
attempt ownership and wakes ordered recovery without waiting. The cause still
determines the completion result and the lifecycle's return value.

Live completion and replay call one delivery step on the evidence outbox.
The step observes and releases the OCI intent fence around one completion
request, then applies the existing durable mutations and returns a typed
verdict. Success and `lease_expired` record delivery. Unavailable intent
withholds completion; disabled intent suppresses it, without suppressing logs.
Suppressed completion remains excluded from replay even after intent changes.
Attempt-authority refusal seals evidence. A permanent body refusal instead
leaves live evidence for recovery, which seals it on refusal. Node-session
refusal retains evidence; replay parks stop-and-escalate refusals on its slow
recheck schedule. Live delivery owns transient retries, run-ledger backoff,
and cancellation-abandonment provenance. Replay makes one pass and lets its
scheduler retry. A local persistence failure has no L1 verdict, even after L1
answered the request; it cannot acknowledge, seal, or abandon delivery.
A short source-pattern guard pins the durable effects to this step and replay's
log path, with counted, named persistence exceptions.

The logs-first drain can push cancel, stop or restart completion past L1's
cancel or settle deadline when log finalization is incomplete. Evidence order
takes precedence over settlement speed. A stop or restart directive received
during completion backoff also hands the abandoned request to recovery instead
of ending the node session, consistent with the abandoned-request rule above.
An accepted completion closes that attempt's log stream at L1 (`l1/store.go`,
`AppendLogs`). Once L1 reports the attempt as `lost`, the
completion is late evidence, so recovery may yield after eight log batches,
deliver the completion, and continue the remaining raw log stream on later
passes from its acknowledged high-water mark. Reaching that per-pass bound
never creates a gap. A permanently rejected raw batch is replaced by an
accepted `replay_rejected` gap; if the durable sequence is not contiguous enough
to form a truthful gap, the attempt is sealed with a typed incomplete-evidence
tombstone instead of retrying forever. The reconciler sends the
identical durable completion body and retains a matching post-lease result as
typed L1 observation without delaying fresh service admission.
For a healthy-lease hand-off, renewal is already stopped: logs still precede
completion only while L1 reports the attempt live, but if the remaining lease
expires mid-drain L1 seals by expiry without `completion_replay_attempt_id` and
the reconciler's lost-at-L1 path is thereafter bounded to eight log batches per
pass.

The reconciler also sweeps one-shot spool rows L1 has closed the door on, and
empty rows an interrupted attempt left behind, and nothing else. An empty
row is one a crashed agent leaves: created by an earlier agent process, with no spooled
event, no completion, no recorded delivery outcome or completion receipt, no
incomplete marker and no runtime attempt manifest. It holds nothing any
delivery could send, so no delivery predicate ever retires it; acknowledgement
high-water marks record evidence L1 already holds and do not keep it. A row
with any evidence is never swept this way. Otherwise a row goes only once L1 has answered a delivery of its evidence
with a refusal that holds for good -- `attempt_not_found`, `not_found`,
`stale_fence`, `attempt_mismatch`, `invalid_request`, `unsupported_class`, `unsupported_kind`,
`unsupported_runtime_handler`, or `not_implemented` -- which seals the row and
records that answer and its time on it. `conflict` and `idempotency_conflict`
are not refusals for good: a replay gap's conflict clears once the missing
earlier sequences arrive, and an idempotency conflict clears once retention
evicts the conflicting rows, so a row sealed on either is kept. No row is
swept by age. L1 never
refuses evidence as too late: past its late-evidence window it keeps a gap in
place of a result and answers a completion `lease_expired`, which retires the
row as delivered. That window starts when L1 records the loss, not when the
process finished, and L1 keeps late results for as long as it keeps the
attempt, so an L1 unreachable for any length of time still takes the evidence
when it returns, and the spool keeps it until then. A row L1 has not
answered, a row parked on a node-session refusal (#549), and a row sealed on
`attempt_not_owned` (which a node reconfigured with the wrong identity also
gets) are therefore kept. Disk use is bounded where it is large: pending
one-shot log payload counts against the one-shot spool budget
(`--log-spool-max-bytes`, 64 MiB by default), and at that limit the spool
refuses new output, which ends the producing attempt with an output-sink
failure, rather than evicting evidence already spooled. A completion or
tombstone row is not counted against that budget; each is one small document.
A service row and an attempt this process still owns are never swept. The
attempt's spool events and acknowledgements go with its row. The sweep rides
on the reconciler's own passes, at most once an hour, deleting at most 256
rows in one transaction; a full batch leaves it due at once, so a backlog
drains across passes.

The generic agent handoff manager remains the owner of process one-shot host
directories. An OCI one-shot does not reinterpret the forbidden flat
`execution.handoff_directory`: after `kind=oci` selects the adapter, the
agent puts exactly one `handoff` requirement on the runtime request, keyed by
the resolved execution owner: non-blank `handoff_owner_run_id`, else `run_id`,
else the server-assigned Job ID. The same owner keys admission, locking,
result capture before reap, upload, retention and eviction. Direct ordinary
L1 submissions require no L3 or run mailbox, and do not gain run-identity
entitlement. Malformed explicit owners still end with
`handoff_preparation_failed` before the runtime is asked; L1 refuses them
at submission after replay and entitlement checks. The adapter passes that key opaquely
to the helper and unconditionally makes `/wefty/handoff` the reserved guest
value. Attempt reap preserves this helper-owned volume on every outcome and refreshes
its retention window when another attempt or rerun reuses it. The agent no
longer asks the runtime to delete it after a successful completion: the volume
holds the run's results, and the helper's boot sweep expires it on the
contract's retention window (`run-execution-context.md`, "Results and their
retention").
The payload sees only the reserved container path, never the helper source
path. The named `usesAgentHandoffLifecycle` predicate positively selects only
`kind=process`, `class=one-shot`; no negative kind gate can accidentally add a
future runtime to the host-directory manager.

For process and OCI one-shots, the node marks a retained handoff published
only when it observed both halves of publication (`run-execution-context.md`,
"What the node gives up when it is over budget"): the attempt's result reached
L1, as its uploaded document or as an accepted `absent` when it wrote none,
and its run mailbox drained completely, which an attempt with no mailbox does
trivially. A successful upload does not hide a failed drain, and a drain does
not hide a failed upload. Any other skip reason or a refused upload leaves
the handoff unpublished, without changing the workload's verdict. An OCI
one-shot reads its admitted handoff root with exact live-attempt authority
before runtime reap, independently of a run mailbox, run token or L3
configuration. Event operations retain their declared-mailbox authorization.
Completion drains the
mailbox, then uploads while still holding the handoff lease and before
recording terminal retention, and writes the drain verdict on the upload
record beside the upload outcome. A handoff belongs to its execution owner,
and every attempt that writes into it must publish before it is published: a
rerun, a retry, or a child naming its parent's run as `handoff_owner_run_id`
cannot publish files an earlier attempt left there. Each admission clears the
previous attempt's verdict, records the producing attempt, and carries the
owner's answer for every earlier attempt forward on the admission record
(`earlier_attempts_published`). That answer is true only when the replaced
record was finished and published, or when nothing was written there before:
an empty process directory with no record, or an OCI owner with neither an
admission nor an upload record on this node, where another owner's upload
record at a name this owner's could be filed under counts as one. Restart recovery applies the same
rule to that same attempt's upload record only, and the admission's carried
answer still applies. A legacy record that cannot show both halves, or that
predates the carried answer, never grants early-eviction priority. Unpublished
results remain subject to the existing bounded retention and last-resort
budget eviction. L1's stored result has its own job retention, independent of
node cleanup.

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
An ordinary attempt `Delete` must subsequently verify task, container,
snapshot, lease, cgroup, shim, and log runtime absence before L1 may observe
`stopped`. Both attempt and namespace verification may project a still-finishing
LOST-Attempt log or cgroup from runtime residue only when an unexpired durable
retention receipt supplies the exact authority; every consumer validates the
observed/residue/retained partition before accepting that absence.

Helper/session or engine loss takes a different positive-proof path. The OCI
adapter invokes the agent recovery hook only for helper/engine evidence, never
for an L1 image-observation transport failure, and never for a `Watch` runtime
failure the helper marked `runtime_failure_attempt_scoped` (one attempt's lost
shim): that fails the one attempt and leaves the session and its neighbours
running (#560). Recovery immediately suppresses
OCI admission, publishes the restrictive capability observation, invalidates
the helper generation, and completes the boot sweep before the attempt can
finish. `ReapAndVerify` may consume that same-boot sweep once only when its
complete attempt authority matches and the independently verified namespace
inventory is empty. Replacement claims remain embargoed until the ordinary
capability publication handshake acknowledges the recovered generation.
A LOST-Attempt log spool still finishing its bounded seal is durable retained
only from the sweep's fsynced, unexpired helper-owner/`log_spool_sealing`
receipt; Verify never fabricates that authority from its name or contents. A
proven LOST-Attempt populated cgroup is SIGKILL-reaped by sweep (without adding
the agent's live-service TERM grace) or receives the same bounded typed
retention treatment. Exact-shaped unbound or live-Attempt collisions remain
operator-visible runtime residue and are never killed.
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
The no-attempt cases are closed per Computer generation. A generation whose
exact helper manifest is prepared by a durable reset/copy receipt, unattached,
has no loop or mount, and has no prior attachment or retirement evidence is
frozen as prepared Storage-only evidence. A generation whose exact managed
disk root is already absent is frozen as distinct typed absent-Storage
evidence; the helper never creates the missing root to prove that fact. Legacy
reconstruction scans the Job's runtime authorities once and each L1-claimed
Storage generation once, then deduplicates only byte-identical authorities.
The session still passes the complete generation inventory through the
resident/admitted service barrier; only after both are clear may it durably
record `no_runtime_resources` without signalling a guardian.
That complete-generation requirement is specific to the no-runtime shortcut.
When the session instead returns a positive runtime reap receipt, the guardian
receipt covers the Job while the helper independently finalizes every Storage
generation claimed by L1; historical generations do not need to be relabelled
as members of the current runtime attempt.
The frozen manifest contains empty runtime identifiers and does not manufacture
lease, task, container, or other attempt rows. After a reboot, prepared
Storage-only evidence is re-inventoried and refreshed under the current helper
session before it can produce a no-runtime receipt.

Only that durable `no_runtime_resources` receipt permits the local managed
service-resource deletion step to be skipped. Purging the Job spool, finalizing
every claimed Computer disk generation, helper removal attestation, L1
acknowledgement, and durable completion remain mandatory; a merely empty scan
or absent disk path grants none of those shortcuts.

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
issue `prior_boot_guardian` only with durable, job-scoped process inventory
from the managed root. Each attempt records its agent boot, host boot identity,
and Guardian-reported payload process group before `Started` is called. An
unknown group is stored before spawn, so a crash in the acknowledgement window
cannot manufacture absence. The adapter verifies every retained attempt,
including attempts from older agent boots: either it records a proven
`not_spawned` outcome, its host boot differs from
the current kernel boot, or `kill(-pgid, 0)` returns `ESRCH`. Agent boot IDs alone
are not host reboot evidence. macOS reads `kern.bootsessionuuid`;
old timestamp-plus-UUID identities remain readable. Only a changed kernel boot
session UUID proves reboot:
XNU adjusts its boot timestamp when the wall clock changes
([kernel clock implementation](https://github.com/apple-oss-distributions/xnu/blob/main/osfmk/kern/clock.c));
the UUID is generated at boot
([boot session initialization](https://github.com/apple-oss-distributions/xnu/blob/main/iokit/Kernel/IOPMrootDomain.cpp)).
A timestamp change with the same UUID remains a same-host-boot query. Linux reads
`/proc/sys/kernel/random/boot_id`, which is independent of wall-clock changes.
Missing or malformed identity, a live or reused group, and query errors fail
closed with no receipt and no signal. A same-host-boot unknown group also fails
closed. A known pre-Guardian failure, or a clean Guardian `Wait` following
`exited` before `started`, resolves its pending record as `not_spawned` after a
positive runtime receipt. Guardian loss preserves the unknown record.
Legacy ownership records have no complete inventory. Managed-root `Open` and
`Initialize` make a best-effort scan under the node lock to stamp each readable
version 1 service with the current host boot and agent boot. Later opens retain
that first observation. A service-open, manifest, or stamp-write failure skips
that service and records the reason; an unreadable or invalid host boot identity
skips the whole scan. These failures do not stop agent startup. Existing root
and layout checks remain fatal. The agent logs skipped stamping through its
configured logger. Inventory detection or promotion retries stamping lazily
and still refuses invalid records or unavailable host identity fail-closed. Legacy
uncertainty refuses removal until a later kernel boot identity differs; an agent
restart alone cannot resolve it. The adapter independently checks that reboot
proof, together with every newer attempt. Same-boot removal never falls back
to this proof. Ownership manifests and process-attempt
records use version 2. Each attempt has a separate encoded file under the
service's `process-attempts` directory, outside attempt scratch cleanup. The
managed-root reader still accepts version 1 ownership for ordinary guarded
removal, but prior-boot inventory reads first promote it while retaining
uncertainty. A version 1 record upgraded to version 2 retains `legacy_process_inventory`; adding a new attempt cannot
erase unknown historical processes. Unacknowledged `.tmp-` files are ignored
by inventory reads. Root, active-boot, and removal-record versions remain
unchanged. The OCI boot barrier supplies
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
receipt verifies that blocking return contract. If the payload is not reaped
within the bound after SIGKILL, or the Guardian ends without a decoded `exited`
report and a clean Guardian exit, the adapter issues no receipt. In
these cases, `ReapAndVerify` returns `ErrProcessReapTimeout` or `ErrGuardianLost`,
respectively. The Guardian has its own process group; the payload has another
group and retains the agent's session. No Guardian signal disposition is
ignored. Stop messages, agent disconnect, startup-readiness timeout, and
spontaneous exit all require both the payload leader's `Wait` and group
`ESRCH` before an `exited` report. After final SIGKILL, group proof is bounded by
`ProcessReapTimeout` (five seconds by default). Failed proof ends the control
stream and makes the agent report `ErrGuardianLost`.

Captured exec adds a storage-retirement sweep inside its wrapper cleanup,
before storage rename or deletion. It stops same-user processes with physical
cwd inside the capture, including descendants that left the command group.
It excludes the retiring shell, its parent, and processes named `wefty-agent`.
Its inventory is bounded to two seconds, TERM grace is 200 ms, and release wait is 100 ms;
this fits the runner's default five-second TERM grace, with scheduling overhead
outside that sweep budget. Failure warns without failing cleanup. See
[exec storage retirement](exec.md) for discovery and folder scope. The wrapper
logs each stopped process and writes optional `stopped_processes` (`pid`,
`name`, `cwd`) in its result document. The agent copies that list to the fenced
completion's `ProcessResult` without changing the runtime's verdict. A missing
list is omitted on the wire. This does not expand Guardian process-group
quiescence proof after an agent crash or an uncatchable wrapper kill.

Run output is uploaded to L1 and read with `wefty logs`; `wefty-agent` mirrors
it to stdout/stderr only with `-mirror-run-output` for foreground development
(default false), while lifecycle and error lines remain in the agent service log.

The agent's Guardian `WaitDelay` is termination grace plus that reap bound
plus a one-second scheduling margin (eleven seconds by default). Its independent
output context is cancelled on Guardian loss or one second before that output
wait bound, releasing production SQLite append waits and console-sink callers.
When mirroring is enabled, one bounded console queue and one process-wide worker
keep a stuck console fd out of attempt pipe copying;
closing an owned test pipe also releases the worker. A permanently blocked
console fd can hold that single mirror worker until the daemon exits.
After a proven `exited`, `exec.ErrWaitDelay` means output was cut off after a
clean Guardian exit. The positive receipt remains valid and
`log_evidence_incomplete` is true. Cancellation of the runner's own output
context is not an output-sink failure. Guardian loss and other suspect Guardian
`Wait` outcomes keep #869's `process_wait` `spawn_error` result shape. They also mark `log_evidence_incomplete` because
Guardian output was cut off or its completeness is unknown; captured events
remain available. Structured pre-start spawn failures keep their sole spawn
arm.

Rollback to an older agent deliberately fails closed: it refuses version 2
ownership manifests and cannot restart or remove services touched by the new
agent. The node still boots, but pending removals can suppress OCI admission on
that node. This protective refusal prevents older code from discarding process
inventory it cannot understand.

Inline executable decoding, digest validation, interpreter resolution,
materialization, and cleanup are owned by the process adapter; the agent
lifecycle never materializes a kind-specific executable.

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

A published port carries the application's own protocol, so it never expects a
wefty connection preface. On the localhost-only plain Fabric it is plain
loopback TCP and accepts any local client, like a development server on
127.0.0.1. On a Fabric that authenticates at the network layer, it admits the
peers that Fabric admits. Computer front doors are unchanged: they identify
their callers through the Fabric.

A Computer request instead asks for exactly `view` and `control`. The helper
protocol version that admits this request also carries the exact-authority
`SetComputerControlState` verb and Computer-disk attachment semantics. Only a
successful functional probe through such a negotiated version may add the
boot-scoped `computer` capability; an unsupported helper major is not admitted
by this agent and cannot satisfy a Computer claim. Display readiness and
publication remain a separate consumer of the returned opaque endpoints.

Take-over discovery returns L1's durable policy revision. The CLI carries that
revision in `X-Wefty-Policy-Revision` to the private front door as a minimum
admission revision. If the authorization decision is older, admission returns
HTTP 403 with typed retryable `stale_policy_revision` and
`requested_policy_revision` / `installed_policy_revision` details before any
backend dial, session creation, or control bearer issuance. This also applies
when the older policy already grants view or control. The comparison uses the
revision captured by the authorization decision when a grant exists, or a
revision floor read before grant acquisition when no grant is returned. A
concurrent policy install cannot conceal an older decision in either path;
the no-grant floor can cause one extra retryable refusal. When the requester
holds a grant, this retryable refusal is only a wait for installation and is
not recorded as a Take-over denial. Without a grant it still records
`unauthorized_identity` denial evidence, whatever revision the request names,
as does a final, non-retryable `control_not_authorized` refusal. Equal or newer installed
policy still evaluates the current grant and always admits view first. A grant
upgrade does not expand an existing session's admission authority; reconnect
with the new discovery revision to obtain a control-capable session.
The CLI retries only `stale_policy_revision` every 100 ms for at most two
seconds inside its existing command context.
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

The new per-user Mac install uses one quarter of physical Mac memory and
`min(4, logical CPUs)` for the fixed host VM sizing. Existing `node setup-oci`
and Linux sizing defaults retain their 4 GiB and half-core caps. Mac Computer
memory capacity equals the configured host VM memory, with a fixed 1 GiB
infrastructure reserve (phase 3 spec section 3). Linux setup defaults the configured
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

The final redaction flush and log upload share the bounded finalization
context. If that deadline expires after the payload has returned, the agent
preserves the payload's primary result arm, records additive
`log_evidence_incomplete` except for a pre-start `spawn_error`, and leaves
events already accepted by the durable spool available for later recovery.
A redaction tail that did not reach the spool before expiry is covered by the
incomplete-evidence fact. A non-deadline redaction, spool, or uploader failure
remains a genuine `output_error`; if it coincides with bounded expiry, the
output error remains primary and the
incomplete-evidence fact remains additive. The deadline rule applies to both
service and one-shot payload results, including `runtime_failure` and
`output_error`, but never adds runtime evidence to a pre-`Started` OCI
`spawn_error`. The logs-first route follows the local fact that bounded
finalization left logs pending, independently of the result arm or wire flag.
The agent hands delivery to ordered recovery, or delivers inline when
process-lifetime recovery is stopping, so L1 receives pending logs before
completion closes admission. A synthesized agent-terminated signal preserves
any existing `log_evidence_incomplete` flag.

Once an output event is durably appended, the sink decides whether to wake its
uploader from its attempt-local pending count; it does not synchronously recount
SQLite rows under the caller's finalization context. Contention while deciding
when to upload cannot turn a healthy payload result into `output_error`. If the
append itself instead returns the caller's expired finalization cause, the
preceding incomplete-evidence rule applies rather than reclassifying the payload;
the process-lifetime outbox retries the retained redacted event and orders its
late-evidence upload before completion. Seat front-door and claim-session
cancellation use a child scope; they do not cancel this outbox or retained
result collection. Those keep the caller's process-lifetime context until it
is canceled or `Agent.Close` cancels and joins them. Returning from `Run` on
a session authority error therefore does not strand withheld completions.

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
That receipt is frozen against an ordinary attempt manifest whose runtime
identifiers were reserved and never created, not against a Storage-only
inventory, and the durable removal record accepts it on exactly that shape: a
`no_runtime_resources` receipt binds every frozen attempt by boot session, and
requires Storage-only attempts only when the session's complete-generation
shortcut produced it. Only that Storage-only shortcut skips the local managed
service-resource deletion step; a removal reaped by the never-entered-`Run`
receipt still deletes the managed service resource this attempt prepared.

A helper that refuses `Delete` with `unauthorized_attempt` is saying the
authority no longer names a live attempt, typically because its own guardian or
deadman already completed it. That refusal is not absence and no later
`Delete` can become positive, so the removal proves absence the one way the
helper offers a session holding no live attempt: a read-only namespace `Verify`
projected onto that attempt's deterministic resource names, with every
transient name required absent from both the inventory and the runtime residue,
and a surviving log-segment or cgroup name accepted only under a bounded,
unexpired helper retention that names the attempt. The classes the contract keeps past an
attempt -- service data and its owner record, a handoff volume, and every
Computer disk class -- are exempt from that inventory check and are deleted by
the removal's own attested step. The resulting receipt is ordinary `attempt`
evidence, because it speaks for exactly one attempt authority. A projection
that cannot name every resource, or that finds one present, fails the reap
rather than granting absence. A `Verify` that never completed is not evidence
either way: it stays a recoverable runtime loss the caller retries.

The agent keeps one reap *receipt* per job, because quiescence once proven
stays proven. It keeps it until the boot ends, the removal that consumed it
completes, or a new attempt of that job starts -- a receipt speaks for the
attempt it reaped, and a later attempt has its own runtime to prove absent, so
admitting the job again drops it.

A reap that failed proved nothing, so it is never kept as that job's answer: a
removal retrying on its own cadence asks the runtime again under the frozen
attempt authority it already holds, and carries back whatever that attempt
refused rather than an older moment's failure. The adapter keeps that call
answerable for exactly this reason -- a typed runtime loss leaves the attempt's
tracking entry in place -- and every proof behind the receipt stays the
adapter's. Without it a removal whose helper was lost while its cleanup was
refused could never finish on the node still running it, however long the
refusal had since been cleared. Where the frozen manifest carries no authority
this node can ask again -- Storage-only rows, or rows belonging to another job,
node or boot -- the recorded failure remains the only answer there is.

A receipt earned that way is retained before it is returned, under the lock
claims and residency take and only while the job is still unadmitted. Asking
spends the adapter's tracking entry and its single-use sweep evidence for that
authority, so a repeated Delete comes back `unauthorized_attempt` with no
tracked fallback left: a receipt earned and then dropped -- one transient
failure to write it durably -- would wedge the next retry for the rest of the
boot, having already proven the thing it needed.

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
succeeds and helper `Run` has admitted that exact attempt. Renewals during image
delivery remain pending in the agent and the latest is queued only after every
helper Started-evidence check and the fenced L1 `StartAttempt` succeed. An
admission after the helper's initial deadman has expired the attempt needs no
gate of its own: the helper refuses the forwarded renewal as `attempt_expired`,
which ends that attempt alone
([OCI attempt-scoped late renewal loss](#oci-attempt-scoped-late-renewal-loss)). The
queued value retains its absolute monotonic L1 expiry; the heartbeat client
derives its relative TTL only when the queued heartbeat is flushed, and drops
an expiry already reached at that edge. It therefore never extends helper
authority beyond L1. The renewal is bound to the helper session generation
that admitted the attempt; a replacement generation drops it with typed local
evidence and closes the gate. Stop, restart, authority loss, and every
terminal/reap path also close the gate and discard pending evidence, while
`Run`'s initial deadman covers helper-side starting. Agent-helper
control EOF, a helper-clock heartbeat blackhole, or an
expired per-attempt deadman therefore reaps runtime-owned state independently
of the agent's own authority watchdog.

An OCI claim remains L1 `Claimed` while the helper resolves and ensures the
image. Successful `EnsureImage` returns the complete immutable image evidence;
the agent persists it before invoking helper `Run`. The helper then registers
`Wait`, starts runc-v2, and returns `Started` plus the same image evidence. The
agent replays the observation, performs fenced L1 `StartAttempt`, and only after
both succeed marks its local observer running. `StartAttempt` is idempotent, so
an answer that never arrived (a transport failure or timeout) or a 5xx other
than 501 is retried at the completion retry interval within one lease window,
as for a process start: a lost answer to a committed start gets L1's replay and
the attempt proceeds as started. The retry also ends at the helper's initial
deadman. The adapter reports the `Run` request just before sending it, and
the helper arms that deadman no earlier, so the agent ends the retry the
initial deadman after the report; until admission nothing renews it, and an
acceptance after it could only lose the attempt. Any other status is L1's
verdict even with an unreadable body; a 2xx is the committed start. A
refusal, or no verdict before either bound, is a refused post-start mutation.
Lease renewal, log append, and
completion remain incapable of implicitly promoting the attempt. If the
pre-Run observation is refused, no runtime resource is created; if either
post-start mutation is refused for a service, the adapter kills and verifies
deletion of the real task before returning a spawn failure. For a one-shot, it
stops the real task with TERM, grace and KILL before returning a spawn failure;
the caller captures any available result and then calls `ReapAndVerify` to
delete the task and verify cleanup.

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
deleted. L1 validates the current node session and reads the service binding
in one read-only transaction snapshot, without taking or waiting for SQLite's
write lock. The proof is an observation, not a reservation for a later pin
action. The remaining ledger is sent as an absolute helper reconciliation.
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

### Direct process one-shot output

A process one-shot with no explicit handoff path resolves its execution owner
from its immutable spec and the server-assigned Job ID. Entitled run labels
keep precedence; otherwise the Job owns `<node-handoff-root>/<job_id>`.
The resolved owner and directory drive its path lease, private preparation,
`WEFTY_HANDOFF_DIR`, receipt-authorized result read, upload record, retention
record, accounting and expiry. Explicit paths without run labels remain
unowned. No synthetic label is introduced, so execution ownership never grants
run-identity entitlement. Publication retains the accepted-upload and drained-
mailbox rule, including accepted `absent` when no result was written.

### OCI one-shot cancellation

Renewal and heartbeat cancellation address the exact resident Job, Attempt and
fence, including image preparation and helper startup. L1 checks pending
cancellation in the image-observation transaction after identical-replay
handling: a new observation after committed cancellation is refused with
HTTP 409 `conflict`, `retryable=false`. The adapter does not invoke helper
`Run` after that refusal. The node settles with pre-start refusal evidence,
releases any attached image pin through `ReapAndVerify`, and uploads accepted
`absent` result evidence without reading a handoff or mailbox that this
never-started payload could not have written. Existing contents belonging to
the same handoff owner cannot become this attempt's result. Normal publication
and retention rules apply to that absence.

After the helper proves payload start, the image observation and `Started`
acknowledgement use uncanceled contexts, the observation bounded by one
operation and the acknowledgement, with its retries, by one lease window and
the helper's initial deadman: an execution cancel cannot erase a response for a transaction that already
committed, nor the retry that recovers a lost one. L1 still refuses a new start after cancellation and
returns the stored job for an earlier durable start. OCI one-shots keep Watch
alive across execution cancellation to observe the existing TERM, five-second
grace and KILL path. Failed signal delivery remains runtime-failure evidence,
not a manufactured agent signal or confirmed termination. Cancellation can
still commit after the pre-Run observation was accepted. If the helper then
admits the attempt before durable `Started`, the node stops it and records
pre-start refusal evidence; it reads any available result before the normal
`ReapAndVerify`. A canceled attempt uses the same result upload,
publication and handoff retention rules, including unpublished retention when
capture or upload fails. If heartbeat or renewal cancellation abandons an
in-flight completion or its transient retry backoff, the node retains the
cancellation cause and retries the identical durable completion under
the existing bounded delivery budget. A transport outage, unavailability or
deadline that exhausts that retry releases the completion to the outbox without
ending the agent session; recovery later delivers the same evidence. A distinct
L1 authority/session verdict or local persistence failure keeps its own error
scope. L1's 30-second settlement deadline remains independent of runtime
confirmation, so a silent node still settles without inventing a stop.

## OCI attempt-scoped late renewal loss

An OCI helper heartbeat may refuse one exact, recently expired or deleted
attempt with `attempt_expired` while retaining the helper session. The client
notifies that attempt owner, stops its pending renewals, and the adapter ends
its Watch. The lifecycle classifies the typed refusal as attempt authority
loss and uses normal reap and lost-attempt evidence reconciliation, rather
than publishing it as successful completion or recovering the helper session.
The renewal loop preserves this loss even if Watch returns without the typed
error, so neither live completion delivery nor durable completion replay turns
a plain lost attempt into a payload result. Shutdown and cancel/stop/restart
directives keep their existing agent-terminated completion semantics: that
completion is persisted before delivery even when Watch also reports attempt
loss. Incomplete log finalization hands that persisted completion to ordered
recovery while recovery is running, or delivers pending logs and completion
inline if recovery is stopping. With no logs pending, live completion delivery
keeps its transient retries and backoff within the existing bounded budget,
even after recovery has stopped. Failed delivery remains replayable. This
exception does not manufacture a completion for plain helper attempt loss. L1
still fences replay.
Other attempts, OCI capability, and helper generation remain live. The exact
identity, bounded recognition record, and session-fatal exceptions are defined
in [OCI helper protocol](oci-helper-protocol.md#late-attempt-renewals).

## One-shot claim admission

Before a one-shot claim RPC, the agent checks its class gate and local machine
load under the same claim lock. It reads the one-minute load from macOS
`vm.loadavg` or Linux `/proc/loadavg`. If a one-shot attempt is already admitted
and load is at or above the limit, the agent sends no claim request. It checks
again on the next claim tick. With no one-shot attempts, load never blocks the
first claim. An unreadable load leaves load admission open and logs once per
agent session. Existing attempts continue.

The node setting is `--oneshot-load-limit`, then `WEFTY_ONESHOT_LOAD_LIMIT`,
then `oneshot_load_limit` in the setup connection profile. The default is
1.5 times `runtime.NumCPU()`. Zero turns the rule off: no load check and no
spacing. Negative or
non-finite limits are invalid. Restart the node agent to apply a setting.

While another one-shot attempt is admitted and the limit is not zero,
successful one-shot claims must be at least 30 seconds apart. The first successful claim starts that interval.
Empty responses and errors do not move it. The clock is injectable. Claim
loops share the interval, so parallel loops cannot burst. An empty node can
start immediately even if its last run just finished. Services use their
existing admission and placement rules.

## Restartable process one-shots

A Restartable process one-shot uses Guardian with `AgentBootLifetime`. The
process runtime refuses admission without a usable Guardian and never silently
runs it unguarded. The Node advertises `oneshot:restartable` only when the
process runtime has a usable Guardian and version 2 exec capture support.
Ordinary one-shots retain their existing lifetime behavior.

Use `--restartable` only for commands that are safe to repeat and harmless
if two copies briefly overlap, for example read-only watchers. After a
controlled stop, wefty confirms that the command's process group ended before
restarting. After an agent crash, the existing Guardian ends the group on
control EOF within its teardown bound: 5 seconds of TERM grace plus 5 seconds
to reap after KILL. This normally finishes long before the Lease expires:
with healthy renewals until the crash, the default 30-second Lease and
10-second renewal interval leave at least 20 seconds from agent death to expiry. Shorter configured Leases may leave
less time, so lease-loss restart backoff has an 11-second minimum (teardown
plus one second of margin), even after jitter. Recovery also waits for Lease
expiry. Acknowledged controlled stops retain zero restart delay.

This timing fence is not positive proof of predecessor absence after a crash.
If the Guardian itself is killed, the old command may briefly overlap the new
Attempt. Quiescence covers the command's process group only. Descendants that leave the
group, for example via `setsid` or a daemonizing fork, are not tracked, exactly
as for ordinary one-shots. Restartable commands must not daemonize.
Ordinary preparation errors complete as failures.

Restartable mailboxes use `<handoff>/.wefty/<run_id>/<attempt_id>`. Exec work
and TMPDIR storage are beneath that mailbox, so a delayed cleanup helper can
touch only its own Attempt's directory. Restartable result writes and reads
use `<handoff>/.wefty-attempts/<attempt_id>` via `WEFTY_HANDOFF_DIR`;
the Run result API remains stable and serves only the latest Attempt's result.
Restartable execution is at-least-once. A restartable command that ends while
wefty is stopping its Attempt may run once more; the latest Attempt's result is
the Run's verdict.

An exact renewal `restart` directive carries `interruption_reason=upgrade`
and `interruption_request_key`. The agent terminates and reaps the payload,
finalizes its mailbox and logs, and echoes both fields in durable completion.
A node agent shutdown signal interrupts Restartable residents, including an
in-flight claim, with reason `agent_shutdown`. Only positive process-group absence
and an agent/Guardian initiated termination qualify. Other agent failures,
runtime limits and naturally completed Attempts keep their own verdict. The first stop cause
that wins the fence determines interruption metadata. A supervision or
preparation failure that starts termination first remains a failure if a
shutdown signal or restart directive arrives during reaping or finalization.
Cancellation wins.
The interruption fields survive completion spool replay unchanged. After Lease
loss L1 recovers through the persisted restart backoff rather than inventing
shutdown evidence.

## Shared local evidence across Node agent generations (#1176)

The ownership rule is exclusion before inspection or mutation. A live producer
owns its records before creating them, and collection cannot infer abandonment
from absence in its own process map. There is one primary and any number of
retiring generations; no age or generation-count limit permits collection.

Legacy agents retain the exclusive stable-Node process lock. Enrolled agents
hold that same cohort lock shared and one exclusive generation lifetime lock.
Installed startup takes the generation-directory lock through
`Store.StartupWithLifetime` before readiness or shared runtime initialization.
It supplies `GenerationLifetime.ClaimRuntime` as `Config.GenerationLifetimeLock`:
this validates the Node/generation and permits exactly one runtime to borrow the
already-held lock, without opening another descriptor or taking another
per-generation lock in the spool directory. Startup keeps the lock until the
runtime stops. Readiness independently checks that same held directory lock
and the kernel process-start identity. Standalone enrolled library
configurations with no installed ownership proof take their generation lock in
the spool directory.
The executable requires an installation catalog whenever a generation ID is
supplied, so installed processes cannot bypass the directory lifetime lock.
This permits overlap, excludes duplicate generation processes, and prevents a
legacy agent from opening an enrolled cohort's shared state. Ownership is taken
before managed-root activation, not just before opening the spool. Installed
startup additionally runs shared-root activation through
`Config.PrimaryGeneration`, supplied by `agentinstall.Store.WithPrimary`. Its
catalog lock excludes staged/retiring identities and unresolved promotions from
Node-wide local writes. L1 remains the authority for Node registration and
capability observations. Only the primary can publish them; the installed
primary fence also encloses each durable capability-floor write, so a retiree's
late probe or response cannot overwrite the successor's floor. These locks do
not grant claims or execution authority.

Each spool Attempt has an immutable `owner_generation`, established with its
Job/Attempt/fence before the first output. Legacy rows carry the empty owner.
Recovery and sweep select only the calling generation's rows or generations
positively observed `closed` in authenticated L1 inventory. Missing entries,
missing PIDs, local clocks and local ownership maps never establish closure.
Read-only `PendingEvidence` inspection counts the whole cohort and reads old
spool schemas without migration. It supplies no recovery ownership. Legacy
recovery continues to read legacy rows. Enrolled processes leave legacy
rows protected; conversion/reboot reconciliation must positively settle that
legacy boot before changing ownership. Startup schema/backfill work is
serialized, while SQLite transactions apply the existing one-shot byte budget
once over all generations. They refuse excess output instead of evicting it.
Recovery additionally holds a shared-store per-Attempt process lock, so two
collectors cannot concurrently deliver the same closed-generation backlog. An
enrolled recovery loop scans the local cohort every five seconds even when
idle; another process's custody writes cannot wake its process-local channel.
A separate inventory loop permits one request at a time, bounds it by the
normal operation timeout, and waits five seconds after each completed or failed
refresh before trying again. Recovery wakes and worker completions do not poll
inventory. A successful refresh wakes local recovery; a pending or failed
refresh never delays own-generation or durably known-closed evidence. Unknown
owners remain protected until positive closure is cached. Shutdown cancels and
joins both loops before closing the shared spool.

Handoff admission holds an exclusive node-local record lock from before
preparation through execution, mailbox publication, result upload, capture
cleanup and finish. The existing process-local lease now also holds a stable
cross-process lock under the agent-private state root. Startup adoption,
expiry and budget eviction take that same lock without waiting; an owned
record is left intact, including an admission with no terminal deadline.
Lock files are never unlinked while the store is in use. No workload-writable
marker grants this ownership. Result accounting still reads the complete shared
record root and counts every generation's in-flight and retained bytes once.
Node-wide expiry/budget passes share a collection lock, so two generations do
not independently evict from the same old budget measurement. Attempt-scoped
mailbox/capture cleanup stays under its handoff lease; it never scans another
generation's open mailbox. Crash residue keeps today's recovery rules; this
does not add process adoption, SIGKILL or reboot survival.

An enrolled successor durably declares support for evidence custody when it
opens this shared store. After every resident runtime is positively reaped,
the default `RetirementEvidenceSettled` implementation can acknowledge pending
logs/completion bytes to that declared primary custodian. Publication must
already be settled under its existing rules: the lifecycle records this only
after mailbox/result publication succeeds, independently of spool delivery.
Late appends and live producers block custody. A FULL-synchronous transaction
records the exact resident identity, source and custodian generations, a SHA-256
digest of the stored completion/incomplete evidence and ordered log events, and
the acknowledgment time. The bytes stay in the same durable SQLite store;
receipt publication does not delete them or create a receipt without bytes.
Custody rows are excluded from destructive spool sweep.
`Agent.EvidenceCustodyReceipts` returns the exact stored identities and digests
for installation proof; an empty receipt list does not prove reap or delivery.

Custody supplies storage only. The target cannot recover a live source's rows,
even after acknowledgment. L1 independently requires accepted completion or an
authoritatively ended Lease before closing the source generation. Until then
the retiree retains its normal E authority, renewal and first-delivery loop.
Only after positive L1 closure can a successor deliver the stored bytes through
existing historical O / accepted-identical-replay R predicates. No first
completion authority, Lease renewal, mailbox entitlement or verdict transfers.
No declared custodian, failed durable commit, pending publication or unknown
closure keeps retirement pending. An explicit `RetirementEvidenceSettled`
callback remains the integration seam for additional evidence and may veto
closure; it cannot weaken runtime reap or L1's closure checks.

The tests use temporary roots, real process one-shots and plain Fabric. They
cover two and three concurrent stores, three running agent generations and two
promotions, Node/floor publication fencing, aggregate budgets, historical custody
replay and the legacy lock/spool path. Installed launchd survival remains #1182
acceptance; services, activation sequencing and rollback remain #1178–#1180.

## Selective service quiescence for rolling upgrade (#1178)

Before L1 promotion, the installation coordinator closes L1 admission, joins
its inventory read, and durably records the old primary's process service
Job/Attempt/boot identities in the existing upgrade drain receipt. It then
calls `Agent.QuiesceUpgradeServices` with that inventory, the exact Node,
generation, boot and activation operation ID. This is a pre-promotion barrier:
L1 already refuses promotion with active service Attempts. It does not stop
a retiring process or use Drain, shutdown, removal, or one-shot interruption.

The barrier joins the claim mutex before selecting work. Missing local
ownership, an incomplete service inventory, OCI, one-shots and changed replay
inputs refuse before cancellation. Local service claims close for the operation;
a service already admitted but not yet installed as a resident receives the
same selective cancellation when its worker enters. All selected services stop
before the barrier waits. Ordinary and Restartable process one-shots keep their
original contexts, Guardians, runtime limits, Lease renewal and finalization.

Success requires each selected service worker to join, a positive runtime reap,
and settled publication plus accepted completion and drained log evidence.
The barrier also joins earlier terminal process-service evidence, including
services no longer in active L1 inventory. The retained #1176 evidence query
supplies this proof. Neither absence from the
resident map nor an ended Lease certifies quiescence. This path requires actual
evidence delivery before promotion; it does not use custody to let a replacement
start while its predecessor still needs first completion. A bounded caller
context may return pending/error, with the service gate still closed for replay.

Upgrade finalization is local to each service. A stalled or refused completion
never exits the session or cancels sibling one-shots. The barrier retains the
immutable completion and ordered spool after the worker joins; generic recovery
cannot discard or seal that evidence. A service admitted before the gate closed
whose lifecycle has not begun runtime preparation records positive `no_runtime`
evidence and delivers its completion on a separate bounded context. Cancellation
of its execution context cannot cancel that finalization.

Before canceling selected payloads, the session takes over renewal of their exact
completion Leases. These service-only renewal owners survive worker exit and
bounded barrier returns, including the idle time before replay. Each stops and
joins on its own positive settlement, independently of other blocked services,
or when the session/agent closes. A terminal renewal rejection also ends that
Attempt's renewal owner. Payload stop, reap failure and completion delivery delay
therefore do not create a lease-length recovery deadline. Successful L1 renewal
remains necessary; the agent cannot undo L1 fencing or an operator's revocation.
An authoritative stop directive ends renewal immediately. If evidence remains
unsettled, the barrier reports phase `lease` with that directive and the evidence
failure, while retaining the immutable spool and keeping service admission
closed. Renewal ending does not preempt exact-authority completion replay,
including a completion that L1 accepted but whose response was lost. Replay
alone decides settlement; an expired, unaccepted completion stays an explicit
failure rather than fabricated proof.

Upgrade evidence ownership transfers share a local disposition lock with replay
refusal mutations. The lock never spans an HTTP request or holds the ownership
mutex across spool SQL. Each request records its Attempt's ownership revision;
a reply from an older revision cannot replace bytes with gaps or seal evidence,
even after upgrade ownership has been released. Transfer joins any local
disposition already in progress and fences the remaining in-flight replies.

`UpgradeServiceQuiescenceError` identifies the blocked Job, Attempt, phase and
cause. `Pending` means the service worker has not joined when the caller's bound
expires. A joined worker with failed reap or undelivered evidence returns a
failure immediately. There is no retirement deadline and no timeout that stops
kept one-shots. A sealed, missing or undecodable evidence record is an explicit
failure, never positive proof or a silent wait.

Replay requires the same operation, Node, generation, boot and original inventory,
while service admission stays closed. It retries the original runtime authority.
If the runtime has consumed its failed receipt, read-only verification checks the
exact Attempt's durable process-group record in the same agent boot. Only a
positive runtime receipt, a durable not-spawned record, or absence of that recorded
group supplies proof; live, unknown or inaccessible groups refuse. Replay then
retries the same retained completion and ordered logs. A refused completion,
including expired authority, cannot certify accepted upgrade evidence. Lost or
sealed evidence cannot be reconstructed into success. These failures retain
service data and leave resident one-shots untouched; the coordinator must resolve
the reported cause and replay before promotion or service recovery.

The returned `UpgradeServiceQuiescence` is local evidence for the existing
upgrade receipt's `service_quiescence` field. Persist it before promotion or
service recovery. Its full original inventory remains in the receipt while the
existing `services` retry queue shrinks durably. No managed service root, data,
binding or port reservation is removed. Cleanup stays in each Attempt's normal
runtime/finalization path; it never traverses one-shot resources.

After this barrier, L1 freezes only active process one-shots in the retiring
generation. Already-finished service records have positive reap and settled
evidence, so they cannot hold retirement open. A service removed by the ordinary removal controller instead retains positive
settlement only after job-wide runtime reap, spool purge, managed-root cleanup,
accepted removal acknowledgement and durable removal completion. Its obsolete
delivery query cannot wait forever on intentionally deleted evidence; failed
cleanup or acknowledgement does not grant this proof. This settles only process
service records, never one-shot records. The remaining one-shots and their
evidence are the closure barrier, with no automatic retirement deadline and no
limit on older retirees.

The activation coordinator (#1179) and interrupted-cutover recovery (#1180)
reuse `resumeUpgradeServices` and the ordinary durable restart endpoint described
in [plain upgrade](plain-node-upgrade.md). They retain one stable recovery key
per activation operation, predecessor boot and Attempt. Each still-desired
service, including `restart=never`, receives one accepted restart request; only
ordinary L1 claim creates a fresh Attempt and fence under the new primary.
There is no process adoption or uninterrupted-service guarantee.

If activation fails before promotion, the same positive proof permits recovery
under the original primary. After service recovery, call
`Agent.ResumeUpgradeServiceClaims` with the same operation ID. It requires a
completed local barrier and authenticated inventory naming that exact primary
and boot. A retiree cannot reopen service claims. The caller separately restores
only its owned L1 admission revision. The installation lock serializes these
operations with promotion. Agents without generation identity cannot enter this
path and keep their existing claim, shutdown and plain-upgrade behavior.

These are library integration seams. Installed activation transport, the ordered
upgrade sequence and interrupted installation reconciliation remain #1179 and
#1180; the existing installed upgrade does not select this path yet.
