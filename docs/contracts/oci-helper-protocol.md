# OCI helper protocol

The node agent's private `__wefty_oci_helper` mode is the privileged mechanics
boundary for `kind=oci`. The helper is stateless with respect to scheduling and
L1 policy: no containerd type, registry retry rule, sweep-selection policy, or
workload-class lifecycle policy crosses this protocol. The Linux and Lima
transports both present a Unix stream socket; the raw containerd socket remains
root-only and is never forwarded.

Computer Backup `copy.json` manifests durably record `encryption=none` before
copy bytes are allocated. ENOSPC and digest-mismatch receipts set `CopyAbsent`
only after a post-delete `lstat` observes the copy root absent. Composite
Computer removal first syncs an operation-keyed supersession tombstone while
holding the shared Backup mutex; a delayed create checks the tombstone before
writing and fails closed.
Computer restore and clone use a separate durable copy manifest under that
mutex. Its phases are `reserved`, `allocated`, `copied`, `source_verified`,
optional `identity_rekeyed`, optional `expanded`, `manifest_written`, and
`published`; restart resumes from the first incomplete boundary without
accepting a changed authority tuple.

On Mac, the Lima 2.2 `vz` template enables only rootful containerd with the
`overlayfs` snapshotter, maps one explicit operator-owned host root to
`/mnt/wefty-host`, and forwards only `/run/wefty/oci-helper.sock` into the
operator-owned instance `sock/` directory. A first-match `proto:any` ignore
rule disables Lima's dynamic TCP and UDP forwarding. The helper translates a
host operator-mount source lexically beneath the configured host root and then
performs the ordinary descriptor-based guest validation beneath the mapped
guest root; the root itself and any escape remain invalid.

The host client dials the current forwarded-socket inode on every connection.
Transient absence or refusal while Lima replaces the forward is retried only
within the caller's deadline. A replaced inode is a new VM transport epoch,
never continuing helper-session authority: the old control stream fails,
capability publication becomes restrictive, and recovery must reacquire a new
helper generation and repeat the complete boot sweep barrier.

Mac bootstrap copies the matching Linux build to the stable guest path
`/usr/local/libexec/wefty-agent` and installs a root systemd service plus socket
unit. It stops both units before a version replacement; no unused checksum
sidecar is installed because the authenticated handshake is the checksum
authority. The socket unit creates
`/run/wefty/oci-helper.sock` as exactly `0660 root:wefty-oci`; the Lima guest
user is added to that group, while the service runs the private helper mode as
root with a narrow UID allowlist. When setup adds that supplementary group for
the first time, it performs one ordinary stop/start so Lima's guest agent picks
up membership; an already-member rerun does not restart the VM. The host
verifies socket ownership, protocol major, helper version, and checksum
through the forwarded helper before installing the host LaunchDaemon. It then
imports the immutable probe archive
through the helper API, verifies the top-level digest, and runs the ordinary
functional create/start/wait/delete probe. No bootstrap command forwards or
uses the raw containerd socket from the host.

## Handshake and peer authority

Every connection captures kernel Unix peer credentials before reading protocol
data. It then reads exactly one bounded, deadline-protected initial frame before
returning a typed authorization refusal; this avoids OS-dependent EOF/EPIPE
results without dispatching or minting authority for the peer. Socket ownership
and `0660 root:wefty-oci` group mode
are the group-membership gate; the server then requires the peer's UID to be in
its configured UID allowlist and binds the session to the kernel credential.
Linux `SO_PEERCRED.gid` is only the process primary GID, so it is deliberately
not treated as proof of supplementary `wefty-oci` membership. A connection
that lacks a Unix credential or an allowed UID receives
`peer_unauthenticated` and cannot mint a session capability. A successful
handshake also returns a non-secret helper-instance ID, a monotonically
increasing process-local session generation, and the configured reap timeout;
the client uses that advertised timeout for sweep and verification.

Wire major `2` is carried on every request and response. It is the first major
that carries the complete Computer endpoint, control-state, and attachment
semantics. A different major is
rejected as `version_mismatch` before dispatch. `AcquireSession` also carries
the agent's required expected helper checksum; an empty expectation fails
closed and a mismatch is
`checksum_mismatch`. The response returns the helper version/checksum and the
negotiated deadlines, but the opaque session capability remains process-local
and must never enter logs, argv, evidence, or operator output.

Exactly one `(node_id, boot_session_id, authenticated peer)` owns the helper at
a time. The helper issues 256 bits of random session capability on a successful
acquisition and rejects another acquisition as `session_busy` until the prior
session has been invalidated and its reap has succeeded. Every non-handshake RPC
uses a new peer-authenticated connection and must carry that capability.
Capabilities from an ended session fail as `session_stale`; they are never
adopted by a later boot, even when the textual boot ID is reused.

Because ownership is first-peer-wins, an incorrectly permissive socket lets an
allowed local UID acquire the slot and deny the intended agent. Correct socket
mode/ownership and the narrow UID allowlist are therefore security-critical;
the helper does not attempt to adopt or preempt that session.

Every JSON message is a four-byte big-endian length followed by at most 1 MiB
of JSON. Handshake and initial request reads have deadlines, the helper caps
concurrent connections, and a decoder never reads beyond one frame.

The acquisition connection remains the control connection. Strictly increasing
heartbeats refresh a deadline measured only from the helper's monotonic clock.
Control EOF invalidates the session immediately, while an open but blackholed
connection is invalidated when that deadline expires. Invalid version,
capability, sequence, or deadman content on the control stream also invalidates
the session. Invalidation revokes RPC authority first, cancels in-flight
operations and connections, joins them, takes the exclusive create/sweep gate,
then calls the engine's boot-session reap. A new session is not issued until
that reap succeeds. If reap fails, the listener closes and `Serve` fails so
socket activation can start a fresh helper and boot sweep; the failed process
never restores authority or remains indefinitely `session_busy`.

The client boundary exposes runtime loss as a typed error only for an active
session's transport disappearance, `session_stale`, an `engine_failure` that
is neither a bounded `Delete` cancellation/deadline nor any
`DeleteManagedVolume` failure, or an explicit image `engine_loss` fact. A typed
`Delete` `deadline_exceeded` or `canceled` fact is attempt-scoped cleanup
failure. Every `DeleteManagedVolume` failure is scoped to its independently
authorized durable resource and removal operation. An `operation_failed`
Computer-disk deletion is retried at most three times with 100 ms between
attempts. If the third attempt still fails, the helper durably writes an
authority-bound `managed_volume_cleanup_quarantined` receipt, fences future
attachment of that Storage generation, and returns the receipt to the agent.
The agent records it on the standing reset/restore operation so Computer status
surfaces the failure and L1 stops redispatching it; recovery requires a later
helper sweep/operator workflow rather than an unbounded silent loop. Neither a
retry nor quarantine invalidates the live helper session.
Caller cancellation, deadlines owned by the
caller, `sweep_required`, validation/policy refusals, digest disagreement, and
unknown agent errors never manufacture runtime-loss evidence. A stop-specific
Signal deadline is runtime loss because the independently bounded helper RPC
could not be reached; by contrast, KILL sent without an observed exit is a
failed quiescence proof and does not authorize a namespace sweep.

Unary `engine_failure` responses include only a closed mechanics fact naming
the helper method and one sanitized reason (`deadline_exceeded`, `canceled`,
`permission_denied`, or `operation_failed`). Raw privileged error text,
containerd types, and host paths remain local, while CI and callers can still
distinguish the operation and bounded failure class before applying the
runtime-loss rule above.

## Boot sweep barrier

Helper process startup takes the exclusive create/sweep gate, sweeps every
resource in the `wefty` namespace, and verifies namespace absence before the
listener accepts a session. Startup failure terminates `Serve`; it never leaves
a helper accepting authority against unverified runtime state.

Every acquired agent session repeats that proof. Its admission state begins
unswept, a successful `Sweep` records only a pending verification, and only a
subsequent namespace `Verify` returning `absent=true` opens OCI operations for
that session. Sweep readiness is never inherited from an earlier session, even
when node and boot-session IDs are textually identical. A failed or negative
verification keeps every engine operation other than `Sweep` and namespace
`Verify` behind `sweep_required`.

The client-side boot barrier waits for an incumbent session's monotonic
heartbeat deadline and reap rather than preempting it, then acquires exclusive
authority and performs sweep plus verification. It reuses that proof only while
the acquired session remains healthy; replacement authority always repeats the
whole barrier and never adopts a survivor. Helper startup and client takeover
are bounded by the configured reap deadline and the caller's earlier deadline.
The takeover retry timer uses the injected helper clock. The heartbeat pump
notifies the barrier synchronously when control authority is lost.

Successful verification produces an immutable receipt retained by the client
barrier. It names the sweep epoch and helper process/session generation and
copies the prior boot sessions, class-separated swept inventory, independent
post-sweep observed inventory, runtime-residue projection, durable-retained
projection, and recovered `(removal_generation, attempt_id,
fencing_token, prior_boot_session_id)` tuples. A helper-startup sweep is folded
into the first session receipt so evidence is not discarded before session
acquisition. This is evidence for the later runtime/removal adapter; this
protocol ticket does not itself persist a deletion manifest or removal receipt.

## Attempt authority and deadmen

Every attempt RPC carries the exact tuple:

```text
node_id, job_id, attempt_id, fencing_token, boot_session_id,
class, removal_generation
```

`class` is only an immutable resource label; it does not select helper
mechanics. `Run` is admitted only when the node and boot session match the
exclusive session and the tuple has never appeared in that session. The helper
keeps the explicit states `starting -> live -> reaping -> tombstoned`; it arms
the initial deadman before entering the engine and retains every tombstone for
the session lifetime. Expiry cancels a blocked start, and replay of an
identical fenced tuple is refused. Authoritative `Started` evidence must be
delivered to the agent; a write failure reaps and tombstones the attempt.
Later attempt RPCs require an exact live tuple; a stale fence or changed label
is `unauthorized_attempt`.

`Run` establishes an initial attempt deadman within the helper's configured
maximum. The control heartbeat may carry exact attempt renewals, each with a
bounded TTL; the agent emits one only after the matching L1 lease renewal has
succeeded. Receipt sets an absolute deadline from the helper's monotonic clock.
Timer wakeups re-read that deadline before expiring authority, so a superseded
timer cannot reap a renewed attempt. A missing renewal reaps that attempt even
while session heartbeats continue.
Session loss reaps every attempt owned by the boot session. Logs and workload
streams never share the control connection, so their backpressure cannot delay
heartbeats.

## Narrow RPC surface

| RPC | Scope and result |
| --- | --- |
| `EnsureImage` | Session-authorized, typed progress/result stream on a dedicated connection. The agent supplies the canonical platform retained from the successful probe for this helper generation; manifest selection and image singleflight are keyed by it. The sole offline-bootstrap exception is the clean-cache `node load-image` archive import that must precede that probe: it may use the current helper diagnostic OS/architecture after OCI default-variant normalization, but it does not retain or promote that diagnostic fact as probe evidence. Every registry delivery, binding pin, and other caller remains gated on the probe-retained canonical platform, and later probe evidence must select the same archived platform digest. Registry mode resolves only a public reference, pins the returned top-level digest, pulls into the fixed namespace, and unpacks that platform. Archive mode receives an OCI-layout tar stream, recomputes every blob digest, validates descriptor sizes and reachability, admits exactly that platform, and imports/unpacks it. Both modes return the same complete image evidence used by `Run`, including top-level/platform digests, platform, runtime handler, and snapshotter; no containerd type, private registry credential, or retry policy crosses the boundary. |
| `ImageCacheStatus` | Session-authorized read of namespace content bytes, applied cap, and the last completed eviction. It never enforces the cap or changes a pin. |
| `DoctorStatus` | Session-authorized read of runtime platform, containerd/runc versions, allowed mount roots, and bounded `ImageCacheStatus`. Each sub-read carries its own assertion-derived receipt, so a partial failure does not erase the authenticated handshake or successful siblings. Runc comes only from containerd runtime info or a setup-resolved absolute executable path; the privileged helper never performs an operator-triggered PATH lookup. A whole-RPC failure uses `diagnostic_failure`, which is explicitly not runtime-loss evidence: the client does not invalidate the session, reap attempts, or withdraw capability. It never acquires a session, probes, sweeps, starts a task, mutates policy, or evicts content. |
| `Run` | Exact attempt authority, initial deadman, a bounded requested endpoint-name list, and closed workload inputs enter. The helper validates the immutable digest, argv, working directory, explicit environment list, enumerated managed volumes, and operator mounts against configured roots, then constructs the runtime spec itself. Only a successful runc-v2 `Start` after `Wait` registration returns authoritative `Started`, the helper-captured `started_at` timestamp from that exact edge, assertion-derived profile evidence, helper-observed image evidence, and a map from every requested endpoint name to its allocated loopback port. Ordinary attempts request either no endpoint or exactly `service`; a Computer requests exactly the distinct `{view, control}` set, receives authoritative `WEFTY_COMPUTER_VIEW_PORT` and `WEFTY_COMPUTER_CONTROL_PORT`, and cannot retain `WEFTY_SERVICE_PORT`. An explicit Mac bridge-fallback preparation creates a separate guest loopback listener and capability; activation rewrites the start-time endpoint, while a dormant preparation exposes nothing to a default-off Computer and remains available for a later policy enable. |
| `Signal` | Exact live attempt and only enumerated `TERM` or `KILL`. A containerd `NotFound` after authorization is the closed `task already terminated` mechanics fact: the helper returns `already_terminated=true` without recording delivery of that signal, and `Watch` remains authoritative for the terminal arm. This race alone is not `engine_failure` or runtime-loss evidence; if `Watch` then cannot publish terminal evidence inside the fixed post-KILL release bound, the positive reaped-task fact makes the missing Wait confirmation typed runtime loss. |
| `Watch` | Exact live attempt; live-tails checksum-protected stdout/stderr frames, requires an agent acknowledgement after each event, emits per-stream EOF/incomplete seals, and then exactly one structured exit, signal, OOM-additive, or runtime-failure result on a dedicated connection. Log incompleteness is additive and never replaces the real terminal arm. |
| `Delete` | Exact live attempt only, except that one tombstoned attempt whose helper deadman completed a successful guardian reap may authorize exactly one later `Delete` with full seven-field attempt-authority equality and the current node/boot-session gate. That exception still calls engine `Delete`, repeats independent absence verification, and releases image pins, capacity, ports, and retained runtime state before returning positive deletion; it never treats the earlier reap alone as the response. The helper consumes the guardian evidence when that call completes, so a second exact call, stale fence, foreign attempt, different removal generation or boot session, and every failed guardian reap remain refused. In every path, a positive deletion means the engine has removed and independently verified absence of the attempt's task, container, overlayfs snapshot, lease, and log segments while retaining any stable handoff volume; only then does the server tombstone authorization. |
| `DeleteManagedVolume` | Session-authorized and closed to a derived `handoff` or `service_data` owner key, or exact Computer-removal Storage and cleanup authority. The helper derives the source, deletes only that resource (plus any paired owner record), independently verifies absence, and returns no general path authority. |
| `AttestRemoval` | Session-authorized exact Job/removal generation plus reconstructed attempt authorities and deterministic resource rows. After separate stable service-data deletion, the helper inventories every row and returns only assertion-derived positive absence evidence. |
| `ResetComputerStorage` | Session-authorized exact reset revision and old/new Storage generations. Under the predecessor attachment flock it records a durable retirement fence, then fully allocates, formats, and verifies the successor from a manifest published before its image. It does not delete, publish, attach, or start; predecessor deletion and attestation reuse `DeleteManagedVolume` and `AttestRemoval` after L1 publication. |
| `CopyComputerStorage` | Session-authorized exact restore, clone, or import operation; binds its managed Backup source or immutable external manifest, destination Computer/Storage generation, Node/root instance, Job, revision, and cleanup fence. It verifies source bytes before destination creation. Restore preserves OS identity; clone/import narrowly rekey it and may expand a larger filesystem. |
| `ExportComputerCustody` | Session-authorized transfer of one published Backup copy to an absolute operator-owned path outside the managed root. L1 has already committed the permanent custody event. The helper retains partial bytes on interruption and returns only observed size, content-digest, and manifest-digest evidence. |
| `GrowComputerStorage` | Session-authorized exact current Storage generation, managed-root instance, Job, operation revision/fence, and old/new byte counts. Under attachment/detachment serialization it makes one newcomer-pays admission decision, fully allocates the final image size, refreshes an attached loop device when present, expands ext4, and only then publishes the new manifest size and assertion-derived receipt. |
| `PreflightComputerReimage` | Session-authorized exact current Storage generation, managed-root instance, old/staging Jobs, operation revision/fence, and target digest. It requires positive detachment evidence, verifies the locally selected manifest platform, reads image and ext4-root UID:GID, and returns assertion-derived evidence before L1 may publish the staging projection. |
| `Verify` | Exact live attempt, or the authenticated session's whole `wefty` namespace for boot-barrier absence proof. |
| `Sweep` | Authenticated session only. The boot barrier always sweeps the complete `wefty` namespace; there is no survivor selector. |
| `DialAttemptPort` | Bidirectional host-to-guest stream for exactly one endpoint name returned by that live attempt's `Run`; the server resolves the authorized name to its private allocated port. Success is withheld until the helper has connected that backend, and only a successful attempt-endpoint stream detaches from its setup context. It is never a general guest dialer. |
| `DialHostBridge` | Bidirectional guest-to-host reverse-tunnel stream only when `Run` explicitly requested the Mac bind-failure fallback and the helper issued that attempt's separate bridge capability. It never accepts an arbitrary host address or port. |
| `SetComputerControlState` | Exact live Computer-attempt authority and one boolean enter. The helper atomically replaces the attempt-local `/wefty/control/driver.json` body with the exact version-1 false or true document; ordinary, stale, old-boot, and reaped attempts are refused. |
| `SetComputerToken` | Exact live Computer-attempt authority plus the opaque bearer and matching attempt bridge endpoint enter. A non-empty pair is atomically installed as attempt-local `/wefty/control/computer-token` and `/wefty/control/l3-endpoint`, both mode 0400 and tenant-owned; an empty pair removes both. A partial pair, ordinary, stale, old-boot, or reaped attempt is refused. |

`DialAttemptPort` terminates inside the guest at `127.0.0.1:<allocated-port>`.
The helper emits an internal backend-ready marker only after that connection is
established, and the client consumes it before returning the opaque stream.
This makes an agent-side connect probe cover the payload, helper session, and
tunnel rather than merely the helper's authorization check.
The helper holds a kernel listener through runtime-spec construction, transfers
it directly into payload start, and retains the logical allocation until
independent absence verification; failed verification cannot recycle the port.
`DialHostBridge` pairs one authorized host
stream with one accepted connection on that attempt's helper-owned guest
listener; the host agent dials only its already-created loopback run bridge.
Neither direction accepts a caller-supplied network destination.

For service stop, the agent keeps `Watch` independent from execution-context
cancellation, sends `TERM`, waits the configured grace, and escalates to
`KILL`; the helper never chooses that policy. A helper/session or engine-loss
fact causes the agent to embargo OCI claims and establish a new verified sweep
before replacement. The OCI adapter accepts that sweep as same-boot quiescence
only when the recovered attempt's complete node/job/removal-generation/
attempt/fence/boot/class tuple matches, the verified namespace inventory is
empty, and that receipt has not already been consumed.
If the task crosses its terminal edge between grace expiry and the `KILL`, the
helper normalizes containerd's typed `NotFound` to `task already terminated`
and the adapter continues waiting for the existing `Watch`. It does not mint a
runtime-loss receipt or reset the verified helper session when `Watch` supplies
the terminal fact. If that reaped task's Wait stream never resolves inside the
fixed post-KILL release bound, the combination is positive runtime-loss evidence
and uses the ordinary replacement-sweep receipt above.

Stream RPCs use a JSON authorization response followed by a one-byte client
acknowledgement before raw bytes begin. This keeps JSON decoder read-ahead from
consuming stream payload. Authorization is complete before success is sent.
Non-EOF stream errors and client cancellation cancel the engine context. A
normal read EOF on a raw tunnel is a request-side half-close: it propagates
`CloseWrite` and leaves the response direction alive. `EnsureImage` is
content-addressed and does not take the attempt-create
side of the sweep gate.

Archive-mode `EnsureImage` sends its ordinary authorization frame before the
client half-closes a deadline-bound raw OCI tar stream. The helper spools that
stream under its private runtime root under the operation deadline and a hard
16 GiB total-byte bound; cancellation closes the transport and the client-side
upload source. It validates path safety, `oci-layout`,
`index.json`, every descriptor reachable for the admitted platform, every
declared size, and each recomputed `sha256` before containerd sees it. Bounded,
canonical regular extension files permitted by the OCI image-layout spec
(including containerd's Docker-compatible `manifest.json`) are ignored; paths
under `blobs/` must remain canonical `sha256` content paths. Containerd's
canonical image-name annotation takes precedence over a short OCI ref-name
when archive provenance is compared with the submitted public reference.
Same-digest/name replay is idempotent; a name already bound to different bytes fails as
`image_manifest_invalid`.

Resolve, pull/import, and unpack are helper mechanics. Failures expose only a
closed sanitized mechanics fact: registry HTTP status, network/DNS, platform
mismatch, engine loss, resource exhaustion, manifest rejection, optional
`Retry-After`, and any resolved digest. Total timing, retryability, and durable
spawn classification remain agent policy. A delivery operation is singleflight
by fixed namespace, top-level digest, admitted platform, and snapshotter, and
holds a short-lived content lease while containerd works. Canceling the first
waiter does not cancel the shared operation; helper-session loss cancels and
joins all operations. Every successful waiter attaches the complete reachable
content graph to its own deterministic attempt lease, and service waiters also
attach a deterministic binding lease, before the operation lease is deleted.
Sweep removes all stale image leases and archive spools while skipping only
resources registered to a live operation.

`ReconcileImagePins` is the absolute, session-authorized boot handoff of the
agent's durable binding-pin ledger, configured probe digest, and positive cache
ceiling. Until it succeeds, eviction is disabled. Missing content is returned
as a digest-only fact for agent-budgeted redelivery; reconciliation does not
contact a registry. The agent calls reconciliation again after redelivery so
the helper attaches the recovered binding leases before enabling eviction.
Failure to delete any stale helper lease fails reconciliation. Before enabling
eviction, the helper also reconciles the durable cache ledger with positive
containerd image/content inventory, pruning absent entries and seeding unknown
image roots as oldest-unused.

`ReleaseImagePin(job_id)` removes only a known binding lease and is idempotent.
`ReleaseAttemptImagePin(authority)` is the exact-authority, idempotent pre-Run
release used when successful image attachment is abandoned before `Run`; it
does not require a live helper attempt. Both deletion paths are bounded and
retain retryable pin state unless deletion succeeds or reports `NotFound`.
`ImageCacheStatus` reports namespace content bytes, the applied cap, the last
recorded eviction, and the last post-delivery bookkeeping/enforcement error for
the later doctor surface.

The fixed `wefty` namespace's content-store bytes are accounted directly.
Successful image delivery refreshes a durable cache record. After delivery and
once per minute, entries are considered oldest-unused-first; operation,
attempt, binding, probe, and durable operator-import holds are ineligible.
Candidate protection is revalidated immediately before deletion. Each bounded
enforcement pass deletes every image name for at most one top-level digest with
containerd synchronous garbage collection and then persists exactly one record
containing its digest, reason, actual reclaimed bytes, and time. Reconciliation
uses its own `reconcile` reason. Being over cap with only protected content is
truthful saturation, not authority to evict it. Post-delivery cache-ledger or
enforcement errors never turn successfully delivered image bytes into a spawn
failure; status records the error and the periodic pass retries.
Error detail remains local and a resolved digest is retained across agent
retries.

## Guest-side runtime-spec construction

`Run.workload` carries only the closed program inputs the agent owns: immutable
image digest, optional full argv and working-directory replacements, separate
public and sensitive operator environment, typed helper-minting inputs,
enumerated managed volumes, operator mounts, and optional memory/CPU limits.
The managed-volume list is closed to `handoff`, `service_data`,
`computer_disk`, and `log_segments`; `kind=oci`, `class=one-shot` selects exactly one `handoff`
descriptor carrying an opaque stable handoff-owner key, whose helper-owned
source is mounted at `/wefty/handoff`. Presence of that descriptor makes the
helper-reserved `WEFTY_HANDOFF_DIR=/wefty/handoff` value authoritative even if
an operator or image layer supplied another value. The
caller-supplied `reserved_environment` is always rejected at the privileged
trust boundary, including `WEFTY_COMPUTER_TOKEN`. `WEFTY_L3_ENDPOINT`,
`WEFTY_RUN_TOKEN`, and the Computer-only `WEFTY_COMPUTER_TOKEN` cross only
through their closed public or sensitive minting fields; a Computer token on
an ordinary workload is rejected, and a reserved name in either generic
operator list is rejected. The helper derives mount paths and endpoint ports
from its own descriptors and allocation. Image
configuration, image-rootfs user/group databases, guest architecture/kernel
facts, resolver and hosts files, translated Lima mount paths, namespace/device
policy, and OCI JSON never cross from the agent.

The privileged adapter constructs `wefty-v1` from containerd v2.3.4's generated
Linux baseline, then replaces every security-sensitive field explicitly. It
resolves the image `USER` and supplemental groups from the pinned guest rootfs;
sets the fixed capability sets, `noNewPrivileges`, containerd default seccomp,
private PID/IPC/UTS/mount/cgroup namespaces, shared networking, deny-all device
policy plus the six permitted pseudo-devices, masked/read-only proc paths, a
read-only `/sys/fs/cgroup` cgroup mount, and a writable rootfs for ordinary
workloads or read-only rootfs for Computers; and serializes
cgroup-v2 memory/CPU limits when present. A memory limit also sets OCI swap to
the same value, producing `memory.swap.max=0` on cgroup v2 rather than leaving a
swap escape. `Resources.Pids` remains absent in M3; the missing PID limit is a
known profile gap, not an implicit default. The
runtime handler, snapshotter, and containerd namespace are fixed at
`io.containerd.runc.v2`, `overlayfs`, and `wefty`.

Before any task reaches `Started`, the helper serializes one node-local
newcomer decision over declared memory and disk caps for Computers only.
Setup supplies the configured Node/VM memory capacity and infrastructure
reserve; helper startup never derives or rejects those values. A
request whose cap exceeds the remaining declared-cap budget fails as exact
`insufficient_memory`. `MemAvailable` is sampled with the receipt timestamp but
never participates in the verdict. The receipt records configured
capacity/reserve, committed memory and disk before and after, requested memory/disk,
`MemTotal`, `MemAvailable`, filesystem free bytes, and the declared Computer
tmpfs ceiling without forecasting a fit count. Disk admission samples the
filesystem that holds the Computer disk root under the same lock and reserves
the declared bytes before allocation. Reservations are bound-Job scoped so a
replacement attempt reuses, rather than doubles, its charge.

The shipped Mac setup records a 4 GiB Computer ceiling and an infrastructure
reserve proportional to the setup-time Lima VM memory. The Linux setup records
the node `MemTotal` as capacity and defaults its reserve to the smaller of
1 GiB or 25 percent. A zero capacity is explicitly unknown: the helper still
records and sums declared Computer caps but has no ceiling against which to
refuse them, and it never treats that configuration as a startup failure.

For a Computer the canonical profile also requests `memory.oom.group=1`.
After task creation and before `Start`, the helper reads back `memory.max`,
`memory.oom.group`, and `memory.swap.max`; only exact `<cap>`, `1`, and `0` may
produce `Started`. Missing, malformed, or mismatched files fail closed as an
OCI profile rejection rather than recording a successful receipt.

`Watch` retains OOM as kernel-observed additive evidence. It adds
`disk_exhausted` only from a positively observed attempt-local ENOSPC event;
when no helper-visible write-budget or filesystem error event exists the field
stays absent. A post-hoc filesystem-free sample, exit code, or error text cannot
create either fact. The agent maps supported post-`Started` observations to the exact terminal
`insufficient_memory|insufficient_disk` latch with declared-cap facts.

Capability parity is exact: bounding, permitted, and effective contain the 12
allowed capabilities; inheritable and ambient are explicit empty arrays. The
latter two, plus the explicit workload-trait-selected `root.readonly`, are emitted by the canonical
`RuntimeSpecDocument` rather than disappearing through runtime-spec's ordinary
`omitempty` serialization. Its JSON number round-trip retains 64-bit limits,
and its containerd `Any` carries those canonical bytes unchanged. The raw Go
spec builder is private so the engine cannot accidentally choose lossy plain
JSON serialization.

Environment construction is image environment, then public operator
environment, sensitive operator environment, and authoritative reserved
environment. Every reserved name is removed from all first three layers before
the final layer is applied; the host helper environment is never inherited.
Golden review redacts sensitive values deterministically without changing the
runtime document. `/etc/resolv.conf` and `/etc/hosts` are
explicit read-only private bind mounts from helper-managed files. Their
targets, the fixed `/proc`, `/dev`, `/sys`, and `/run` hierarchies, and
`/wefty/handoff`, `/wefty/service`, and `/wefty/control` are helper-reserved
mount targets. A Computer receives a private 1 GiB `/dev/shm` tmpfs with
`mode=1777,nosuid,nodev,noexec`; the private mount/IPC namespaces make it
attempt-local and the kernel charges its pages to the attempt cgroup. Ordinary
workloads retain the shared profile's 64 MiB `/dev/shm` ceiling.

Wire validation is lexical only. The engine translates every operator source
inside the helper (identity on native Linux; preconfigured shared-root mapping
for Lima), then performs guest filesystem validation. Validation resolves each
component through retained `os.OpenRoot`/open-at descriptors, rejects symlinks,
and retains the allowed-root and leaf identities with the canonical document.
The engine obtains `ContainerdSpec` immediately before mounting; that mandatory
handoff revalidates every identity, and the explicit `RevalidateMounts` hook is
also available to #141. A path swap after construction is rejected. The selected source must be a strict
descendant of a configured allowed root and the leaf must be a regular file or
directory; sockets, devices, FIFOs, roots, and nonexistent paths fail closed.
Bind propagation is private, and a read-only mount uses a recursive read-only
bind. Nested operator targets are sorted parent-before-child so input order
cannot shadow a child silently. Managed sources undergo the same checks beneath
the helper-managed root but are not delegated operator paths.

The baseline `RLIMIT_NOFILE` soft/hard value of 1024 remains containerd's pinned
default and is an explicit M3 decision; raising it needs workload evidence and
a profile amendment. Opportunistic AppArmor names use the closed
`[A-Za-z0-9][A-Za-z0-9_.-]{0,127}` shape. The `ociVersion` remains the linked
runtime-spec v1.3.0 used by containerd v2.3.4 and the runc v2 shim targeted by
#141; it is not versioned independently.

The serialized fixtures under
`runner/ocihelper/testdata/containerd-v2.3.4/` are the review boundary for
native Linux amd64, Lima Linux arm64, service mounts/environment, default-root,
numeric-user, unlimited-resource, and the complete Computer profile. The
Computer fixture proves image USER/ENTRYPOINT/CMD semantics, the unchanged
capability/seccomp/namespace/pseudo-device walls, no new privilege or GPU, and
the private 1 GiB shm mount. The Linux-only oracle also compares
each architecture's seccomp fixture with the real containerd generator after
final capabilities are applied. Regenerate complete fixtures only in their
matching Linux architecture with:

```sh
UPDATE_OCI_PROFILE_GOLDENS=1 go test ./runner/ocihelper \
  -run 'TestRuntimeSpecGoldens/(amd64|arm64)' -count=1
```

The builder also checks the linked containerd module version against the
fixture version. A containerd patch change therefore fails until the baseline,
architecture-specific seccomp profile, and complete serialized specs are
regenerated and reviewed together.

Profile-construction rejection and a service-data directory/owner-record
conflict cross helper RPC as `oci_spec_rejected` and map to the existing agent
`SpawnFailureOCISpecRejected`; neither is collapsed into the ambiguous
`engine_failure` bucket. The latter diagnostic includes the observed and
wanted primary owners so an always-restart service latches with an actionable
reason instead of retrying an unsafe ownership transition forever.

## Deterministic identities and serialization

The helper hashes the complete attempt authority tuple with SHA-256 and uses
the first 128 bits to derive deterministic names for the lease, snapshot,
container, task, shim, cgroup, and log-segment directory. It separately hashes
the stable service job ID to derive its service-data volume and its paired
owner-record name, and the opaque stable handoff-owner key to derive the
handoff volume directory, so no durable identity can be replaced by an attempt
identity. The service-data directory lives under
`<runtime-root>/service-data/<service-volume-directory>` and its record lives
under `<runtime-root>/service-data-state/<service-volume-owner-record>`; both
names are explicit `ResourceIdentity` fields so removal manifests and
inventory verification can name them independently. Every label-capable
attempt resource carries the
unabridged labels:

```text
io.wefty/node_id
io.wefty/job_id
io.wefty/attempt_id
io.wefty/fencing_token
io.wefty/boot_session_id
io.wefty/class
io.wefty/removal_generation
```

Handoff volumes live under a distinct helper-owned durable root, not the
attempt namespace. `Delete` reaps and verifies the attempt while retaining its
handoff volume. Session reap and boot sweep likewise leave unexpired handoffs
intact; reuse refreshes the default 24-hour retry age, and sweep removes only
expired direct children with the deterministic handoff prefix. Attempt and
namespace quiescence therefore project only unexpired handoff volumes (the
retained bindings) out of their absence decision. `Verify` returns the observed
inventory unchanged alongside the exact runtime-residue and durable-retained
projections, and the boot receipt records all three with `verified_absent`.
Thus stable service data and unexpired handoffs remain auditable without being
mislabeled as runtime residue, while an expired handoff left behind by a failed
sweep remains visible residue rather than passing by name prefix. The
narrow
`DeleteManagedVolume(kind, owner_key)` operation is closed to `handoff` and
`service_data`. It derives exactly one helper-owned identity, removes only that
volume (and, for service data, its paired owner record), and returns success
only after separate absence checks. The agent calls the handoff arm after
accepted successful completion and the service-data arm during Job removal;
neither arm grants general path deletion authority.

Service-data volumes live under their own helper-owned guest-native durable
root, outside the attempt inventory swept during boot takeover. They and their
owner records remain visible as separate inventory classes so explicit job
removal can positively verify both absent; attempt verification reports them
even though attempt deletion treats them as allowed job-lifetime residue.
Namespace boot-sweep verification projects them out. Before the first mount, the helper
resolves the image `USER` once while building the runtime spec, sets the new
directory's primary UID:GID through its no-follow directory descriptor, and
atomically publishes and fsyncs an owner record containing the directory
device/inode and owner. The directory's descriptor-observed UID:GID is
authority; the record is evidence, not truth. A fresh directory is initialized
even when an orphan record exists, a matching existing directory with a
missing record is recorded without re-ownership, and a record/directory
identity disagreement fails closed. Later attempts require the same owner and
never re-chown existing data. Attempt `Delete`, session reap, and boot sweep
preserve the directory; the separate removal-manifest contract owns its
eventual deletion.

A Computer replaces `service_data` with exactly one `computer_disk`
descriptor carrying the L1-claimed `computer_id`, non-transferable
`storage_id`, positive Storage generation, Computer intent revision, and current `disk_bytes` intent; no host
path crosses RPC. The helper hashes Computer, Storage, and generation identity
(not resizable size intent), publishes the authority manifest before fully
allocating a staging image, formats ext4 with zero reserved blocks, and
publishes the image only after allocation and formatting succeed. It attaches
the image through one loop device and mounts that filesystem as the source of
`/wefty/service`. `ENOSPC` before image publication leaves no image, loop,
mount, or attachment; a reset successor remains a tracked L1 staging generation
whose helper manifest can be retried. The first attach records pending
attempt authority before mounting; a crash before the attached manifest is
published is recovered only by a prior-boot sweep receipt after mount and loop
absence are proved.
The read-only root and fixed-size `/dev`, `/dev/shm`, `/run`, `/tmp`, and
`/var/tmp` tmpfs mounts leave the Computer disk as the only unbounded writable
filesystem; Computer operator mounts are therefore required to be read-only.
The helper also mounts a fresh attempt-local tmpfs outside the Computer disk,
writes `driver.json` false before `Started`, and bind-mounts that directory
read-only at the non-shadowable `/wefty/control` target. `driver.json` is mode
0444. The optional paired `computer-token` and `l3-endpoint` files are mode
0400 and owned by the tenant process identity; enabling submission installs or
rotates both, while disabling or revoking submission removes both. Each replacement ends with a same-directory
atomic rename, so the image sees only complete documents and the helper cannot
report a post-publication failure. The attempt log/resource cleanup path owns
this tmpfs mount; attempt reap, session
loss, and boot sweep unmount and remove it without adding it to Computer
Storage evidence.

For a Computer, the profile receipt reports the 1600 MiB combined ceiling for
`/dev/shm`, `/tmp`, and `/var/tmp`, the largest single ceiling, the cgroup
memory limit, and typed ceiling-over-limit warnings. These tmpfs values are
caps rather than reservations, so the warnings do not reject admission; the
memory cgroup remains the enforcement boundary. The node doctor repeats the
last assertion-derived comparison as `WARN` when applicable and exposes the
last atomic admission facts. A missing receipt remains `NOT-RUN`.

The helper holds an exclusive per-generation file lock for the attachment
lifetime and durably records exact attempt/fence/boot authority beside the
image, never inside tenant bytes. The manifest's attached state is authority:
lock disappearance and helper death do not authorize another attach. Exact
same-boot `Delete`/`ReapAndVerify` writes a single-use detachment receipt; boot
sweep positively unmounts and detaches every helper-owned Computer disk,
retains image bytes, and writes a receipt bound to its sweep epoch. The next
attach consumes exactly one such receipt. Inventory separately enumerates disk
images, verified full allocations, fixed filesystem quotas, manifests, mounts,
loop devices, and live attachments; namespace quiescence projects out only
durable images/allocations/quotas/manifests, never live attachment mechanics.
The sweep receipt retains every observed Computer disk class while its
post-sweep namespace verification projects only those durable classes out.
The L1 claim's current `(computer_id, storage_id, generation, intent_revision)`
is the attach authority. Storage identity itself is only
`(computer_id, storage_id, generation)`; the deterministic disk name excludes
revision and resizable byte intent. Reset and attach serialize on the same
per-generation flock and attach revalidates the manifest after acquisition. A
durable retirement fence therefore blocks a delayed attach without deleting
the current generation before successor publication.

`ResetComputerStorage` accepts only the authenticated current helper session
plus exact Node, managed-root instance, Job, reset-intent revision, cleanup
fence, old generation, and successor generation. It requires the predecessor's
same-boot detach receipt or prior-boot sweep receipt, writes its retirement
fence under the attachment flock, then resumes successor
`allocation_manifest_written → allocated_and_formatted → image_published →
verified`. The verified receipt is stable across retries and binds every input
above plus both generations and the helper generation that performed positive
verification. Helper mechanics never delete the predecessor, publish or attach
the successor, or start the Computer. After L1 durably publishes the successor,
the agent uses shared deterministic removal resource classes for the old disk,
legacy reset manifest, and quarantine root; `AttestRemoval` returns only
assertions that were actually inventoried absent. A later helper session may
replay that receipt but cannot restamp it with a newer generation.

`GrowComputerStorage` binds its receipt to Computer, Storage generation, Node,
managed-root instance, Job, operation revision and fence, helper generation,
and both byte counts. Grow serializes with reset, Backup, attach, detach, and
removal. Its crash boundaries are capacity reservation, filesystem expansion,
and manifest publication; retry inspects durable image and manifest facts and
never reports applied before full allocation plus filesystem expansion. The
capacity decision includes unmaterialized admitted reservations under the same
lock, so existing workloads retain their reservations and the newcomer pays.
An insufficient-capacity receipt is valid only before bytes change.

`PreflightComputerReimage` runs only after the old Job is stopped and the disk
manifest contains exact same-boot reap or prior-boot sweep evidence. Image
delivery remains agent policy; the helper consumes only the locally selected
digest and verifies its manifest platform before reading image-user metadata.
It reads the detached ext4 root UID:GID without following tenant paths, refuses
an ownership mismatch unless the durable operation explicitly carries `chown`,
and binds the receipt to both Jobs, the operation revision/fence, Node,
managed-root instance, helper generation, and consumed detachment receipt.
If the exact digest is unavailable or has no manifest for the bound Node's
platform, the helper returns a typed `failed_unchanged` receipt only after the
same detachment and disk-allocation checks. L1 then retires the refused staging
projection while preserving the stopped current Job and disk generation.

Computer removal carries the same exact Storage identity plus current node,
boot, Job, removal-generation, and cleanup-fence authority to the helper. The
helper requires a matching detached receipt, verifies mount and loop absence,
deletes the image, manifest, and generation quota directory, and positively
checks their absence before the agent may acknowledge `removed_verified`.

`CreateComputerBackup` is narrow privileged mechanics for one source-node cold
copy. Its authority binds Backup, copy, Computer, `storage_id@generation`,
allocated size, Node, boot, root instance, Job, operation revision, cleanup
fence, and helper generation. Before reading, the helper requires the exact
source lock, an accepted detach receipt, and positive mount and loop absence.
It writes a durable copy manifest before bytes, uses full allocation, copies
under the attachment fence, compares source and destination SHA-256 digests,
publishes by rename, and records `encryption=none`. Durable phases cover
reserve, allocate, copy, digest, manifest, and publish so every injected crash
resumes from a tracked manifest. ENOSPC or digest mismatch removes the copy
root, positively checks absence, and returns only the corresponding failure
receipt; it never modifies the source.

`DeleteComputerBackupCopy` accepts new prune or composite-removal authority but
requires any present manifest to match the exact copy, source Storage identity,
Node, and root instance. It deletes only the deterministic Wefty-owned copy
root and returns `computer_backup_copy_removed` only after positive absence.
The helper does not choose retention, auto-delete, restore, clone, export,
encryption, or replica policy. `ExportComputerCustody` accepts only an
already-recorded event bound to one published Backup copy and rejects paths
inside the managed root. It writes the external manifest before the disk,
retains partial bytes after interruption, and returns a receipt only after
size, content digest, and manifest digest are observed. `CopyComputerStorage`
also accepts `import`: it verifies the recorded manifest digest and full disk
digest before creating a managed destination, then applies the same narrow OS
identity rekey and optional filesystem expansion as clone. Both receipts bind
the Node, managed-root instance, operation revision, cleanup/custody fence,
and helper generation; neither helper call decides removal truth.

`AttestRemoval` accepts only an exact service Job/generation plus reconstructed
attempt authorities and their deterministic resource rows, and is called after
the separate idempotent `DeleteManagedVolume(service_data, job_id)` succeeds.
It inventories every named lease, snapshot, container, task, shim, cgroup,
framed-log directory, and durable-data class. Ordinary services assert both
their service-data directory and owner record; Computers instead assert disk
image, allocation, quota, manifest, mount, loop, and attachment identities.
It returns one positive row only after that class was actually inspected and
the identity was absent. Unknown future classes fail closed until the helper's
central inventory registry is extended; they cannot be silently omitted or
reported as passing.

The native Linux engine uses containerd namespace `wefty`, runtime handler
`io.containerd.runc.v2`, and snapshotter `overlayfs`. It creates the labelled
attempt lease first, prepares the writable snapshot under that lease, stores
the canonical `wefty-v1` spec, creates the task with a `binary-v2` logger, and
registers `Task.Wait` before calling `Task.Start`. The logger writes `WLF1`
frames containing per-stream sequence, length, SHA-256, and bytes, followed by
a per-stream pipe-EOF seal. `Watch` tails both append-only segments while the
task runs and retains each segment until the corresponding events have been
acknowledged into the agent's `OutputSink`. A corrupt, missing, or truncated
record emits an exact gap when its discarded byte extent is known and always
emits an incomplete seal; finalization is anchored on logger pipe EOF or that
explicit incomplete seal, never file-size stability. The adapter
persists helper-observed image identity and performs the fenced L1 `Started`
mutation before it exposes local running state; a rejected acknowledgement
kills and deletes the already-started task.

`Run` takes the create side of one helper-wide gate; `Sweep` and namespace
verification take its exclusive side. `Run` rechecks the verified bit only
after acquiring its read side and reserves its attempt under that lock. Every
engine operation except `Sweep` and namespace `Verify` fails with
`sweep_required` until sweep plus absence verification have succeeded for the
current session. No attempt creation can cross a sweep boundary. Before calling
the engine, the helper derives the complete deterministic resource identity and
labels and places them in the engine-only request field; wire callers cannot
supply or override them. The containerd adapter must create and label the
per-attempt lease first; its deterministic `wefty-lease-` name makes the
create-before-label crash window independently discoverable. It then creates
and labels every dependent resource under that lease (§5.2). Sweep and
verification enumerate every resource class rather than trusting labels or a
single total. All seven authority labels must reconstruct the deterministic
identity exactly; unexpected containerd resources in namespace `wefty`, shim
bundle entries under containerd's runtime-v2 state root, and cgroups found by a
recursive scan of the configured cgroup hierarchy make absence verification
fail. Filesystem and deletion errors are fatal except verified `NotFound`.
Whole-namespace session reaping relies on the exclusive one-agent-per-node
helper-session assumption, while volume deletion is scoped to identities
recovered from swept attempts and never removes the whole `volumes/` tree.
Ticket #140 supplies the fixed isolation profile used for
helper-side runtime-spec construction; the unprivileged agent never supplies
OCI runtime-spec material. This native adapter owns containerd mechanics, logs,
and the real one-shot task lifecycle; packaging still owns installed units and
socket activation.

## Agent-side client responsibilities

The client owns the control-stream heartbeat pump and its strictly monotonic
sequence counter. Ordinary callers cannot submit arbitrary heartbeat renewal
lists. The agent queues one attempt renewal only on the successful L1 renewal
path and only when the returned directive is empty; failed, timed-out, stale,
`stop`, and `restart` responses never refresh the helper deadman. The pump uses
separate operation connections for image/watch streams so backpressure cannot
starve session authority, and it locally verifies the returned helper checksum
against a non-empty installed expectation before exposing the session.

Prior-boot removal consumes positive sweep evidence once. A swept attempt still
requires node, job, class, prior boot, attempt, fence, and removal generation.
When an older helper already reaped that attempt, a verified-empty replacement
sweep may omit the attempt only if `PriorBootSessionsSeen` contains the removal
intent's prior boot. A successful session reap records its boot-session ID in
the running helper process, and every later session-generation sweep includes
that process-local fact; a failed reap records nothing, and a restarted helper
must recover evidence from its own startup sweep. Both forms remain bound to
the sweep epoch and helper
generation; a bare verified-empty sweep is insufficient. Legacy manifest
reconstruction and replacement-sweep attempt recovery continue to require the
complete authority match.

When native OCI is configured, the production agent opens a boot barrier and
installs the OCI adapter as one unit. It advertises `kind:oci`, `cgroup_v2`, and
the runc-v2 handler only after a pinned local `/bin/true` probe creates, starts,
waits for, and verifies deletion of a real task inside the ten-second probe
deadline. The same probe runs through the existing startup and heartbeat
capability refresh loop. Successful L1 renewals are mapped to the exact helper
attempt tuple and queued on that session's heartbeat pump.
