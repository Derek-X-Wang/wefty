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
root with a narrow UID allowlist. Every shipped systemd helper service, native
Linux and Lima, sets `StartLimitIntervalSec=0` under `[Unit]` and
`Restart=on-failure`, `RestartSec=250ms`, `RestartSteps=6`, and
`RestartMaxDelaySec=1s` under `[Service]`; systemd versions before 254 use a fixed
`RestartSec=1s` because the geometric directives are unavailable. The workflow-written realtiming
units use the same policy. `RestartSteps` and `RestartMaxDelaySec` require
systemd 254; `ubuntu-latest` and the Lima `template:_images/ubuntu-24.04`
baseline both provide systemd 255, and the Linux receipt records the executing
version and selected rendering. The six steps are derived from the incident's six service starts in
610 ms. At a saturated production restart counter, the kill plus six injected
exits incur seven delays capped at one second: seven seconds total. Startup
recovery is bounded by the enforced ten-second `ReapTimeout`; with the stated
two-second margin, `7 s + 10 s + 2 s = 19 s`, within the unchanged
`TakeoverTimeoutForReap(10 s)` window. The helper-exec-to-ready observation is
reported as a measurement, not treated as a separate unenforced bound.
This bounds saturated deterministic-failure churn at no more than 0.5 Hz
instead of sustaining a four-Hz journal loop. Disabling the
start-limit interval prevents service exhaustion from failing the triggering
socket with `service-start-limit-hit`. The socket retains systemd's default
trigger-limit policy; the current lane proves service recovery and active
socket topology, but does not claim a separate trigger-limit proof. The lane
records the exact helper-kill action-name set
`{native-computer-helper-death,native-lost-attempt-sweep,service-reconfiguration-reset,service-restart-survival}`
from the root fault-action journal. When
setup adds that supplementary group for
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
`peer_unauthenticated` and cannot mint a session capability. An authenticated
acquisition first returns an authority-free handshake with the non-secret
helper-instance ID and configured reap timeout. It contains no session
capability or session generation. A second response returns the process-local
session generation and opaque capability only after the startup barrier is
verified. The client uses the first response's advertised timeout to derive
takeover, sweep, and verification bounds, rejects an advertised timeout above
its configured contract maximum, and rejects changed facts between the two
responses.

Protocol-major-2 rollout permits one narrow compatibility case: a new client
may accept a legacy helper's first response when it carries both capability and
generation. The client sends and locally verifies the expected helper checksum
before interpreting that response; a legacy helper also completed startup
Sweep+Verify before it entered the accept loop, so its early authority is not
pre-verification authority. Partial authority is always invalid. A new helper
always emits the authority-free preface followed by admission. Rollout is
client-first: the dual-form client must be installed before replacing the
helper. An old client rejects a new helper's authority-free first frame, while a
new client can safely operate either helper form and therefore permits helper
rollback without weakening authority ordering.

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
The agent records it with an explicit `reset`, `restore`, or `removal`
operation on the standing operation so Computer status surfaces every failure
without cross-operation shadowing and L1 stops redispatching it; recovery requires a later
helper sweep/operator workflow rather than an unbounded silent loop. Neither a
retry nor quarantine invalidates the live helper session.
`computer_storage_busy` and `computer_storage_retired` are definitive
attempt-scoped `Run` refusals only after the helper positively reaps the losing
attempt and verifies no runtime remains. The retired form means a durable reset
fence makes that Storage generation permanently ineligible for attachment;
`computer_storage_grow_uncertain` keeps the same grow
authority pending for inspection and retry after the filesystem may have
expanded. None of these codes invalidates the helper session.
If the losing attempt cannot be positively reaped, the helper invalidates its
session and returns `session_stale` instead of manufacturing the busy proof.
Caller cancellation, deadlines owned by the
caller, `sweep_required`, validation/policy refusals, digest disagreement, and
unknown agent errors never manufacture runtime-loss evidence. A stop-specific
Signal deadline is runtime loss because the independently bounded helper RPC
could not be reached; by contrast, KILL sent without an observed exit is a
failed quiescence proof and does not authorize a namespace sweep.

Unary `engine_failure` responses include only a closed mechanics fact naming
the helper method and one sanitized reason (`deadline_exceeded`, `canceled`,
`permission_denied`, `retention_bound_exceeded`, `egress_dns_unavailable`, or
`operation_failed`). The DNS reason means neither the advertised non-loopback
resolver nor the Node-loopback stub answered the helper's bounded preflight.
Raw privileged error text,
containerd types, and host paths remain local. The Computer reimage preflight
may additionally report the fixed positive-detachment refusal so native
evidence distinguishes that durable authority mismatch; the closed fact,
never that detail, remains policy authority.

## Boot sweep barrier

Helper process startup takes the exclusive create/sweep gate, sweeps every
resource in the `wefty` namespace, and verifies namespace absence. The listener
accepts authenticated connections while that work is in flight so it can
return the authority-free handshake and configured reap bound, but session
admission waits behind startup verification. Other methods remain
`session_stale` because no capability exists. Startup failure closes every
prefaced acquisition, terminates `Serve`, and never mints usable authority
against unverified runtime state.

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
whole barrier and never adopts a survivor. Startup sweep retains the unchanged
ten-second reap bound. The authority-free first response makes that configured
bound observable while startup cleanup is still running. From the first
handshake in a takeover, the client fixes the exclusive-session deadline at the
smaller of the configured takeover window and `reap + reap`. Production
configures a ten-second maximum advertised reap and a 20-second takeover, so
the arithmetic remains 20 seconds: one interval for startup cleanup and one for
an incumbent authority to expire. A helper advertising above the configured
maximum is rejected with `ReapTimeoutConfigurationError`; any advertised drift
in either direction across retries is rejected. A smaller valid advertisement
shortens but never extends the configured window. The initial no-handshake
discovery window remains bounded so wholly unavailable units and
connected-but-silent socket backlogs retain their typed outcomes. This
implements #301 and addresses
the measured #307 path where helper EOF at `08:19:14.844` was followed by new
startup-sweep runtime activity only at `08:19:24.458`, leaving no barrier
authority at `08:19:29.851`; round-4 #304 measured ordinary lost-attempt cleanup
at 595.949 ms (XFCE) and 588.031 ms (Wayland), so widening the lease is neither
needed nor authorized.
While taking over, the client retries dial-time `ENOENT`, `ECONNREFUSED`, and
`ECONNRESET` within that same fixed window. `session_busy` is the sole typed
admission response retried inside that window; every other protocol,
authentication, or typed RPC error is hard and immediate. Transport loss while
waiting for the second frame may retry, but a helper that completes the preface
and never supplies admission before the window expires is
`helper_handshake_stalled`, preserving Lima's
`helper_handshake_stalled_persistent` escalation. Window expiry
returns `helper_unit_unavailable` only when every dial positively observed
`ENOENT`, `ECONNREFUSED`, or `ECONNRESET`. A connected socket backlog that never
completes a handshake returns retryable `helper_handshake_stalled`; Lima
rechecks and backs off but never force-stops the VM for that reason.
`BootBarrier`
publishes that distinct closed-vocabulary reason through the native agent
capability receipt, and Lima preserves it while still running the same bounded
automated repair loop as `helper_unreachable`. A fully missing socket therefore now costs the complete
20-second takeover window before the typed failure is published; sweep and
verification may then use one fresh ten-second reap window, so the composed
kill-to-verified-ready success bound is 30 seconds. An earlier caller deadline
or cancellation remains distinct.
The takeover retry timer uses the injected helper clock. The heartbeat pump
notifies the barrier synchronously when control authority is lost.

Successful verification produces an immutable receipt retained by the client
barrier, including across loss of the session that produced it. An unavailable
execution snapshot returns that last verified receipt alongside its error so
the adapter can bind a preparation outcome to production evidence; it never
returns the retained session as runnable. The receipt names the sweep epoch and
helper process/session generation and
copies the prior boot sessions, class-separated swept inventory, independent
post-sweep observed inventory, runtime-residue projection, durable-retained
projection, exact durable-retention bindings, and recovered `(removal_generation, attempt_id,
fencing_token, prior_boot_session_id)` tuples. A helper-startup sweep is folded
into the first session receipt so evidence is not discarded before session
acquisition. This is evidence for the later runtime/removal adapter; this
protocol ticket does not itself persist a deletion manifest or removal receipt.
The receipt also records the advertised reap timeout, derived takeover and
verified-ready bounds; absolute helper-loss, barrier-start, preface, admission,
and verified-ready observations; whether the admitted connection was prefaced
while startup was still in progress; and measured handshake,
session-admission, session-sweep, verification, and verified-ready durations.
The Linux L1 receipt adds the fresh-attempt admission observation. Its
production lease remains 30 seconds, while both kill-to-fresh-admission and
kill-to-healthy recovery must complete within 28 seconds. The two-second margin
is derived from round-4 #304 cleanup below 600 ms, rounded to a one-second
cleanup ceiling, plus a one-second preface/admission ceiling. The literal
10-second reap, 20-second takeover, and 30-second verified-ready relationships
remain separate production configuration checks.
Every swept and verified inventory class has identity-set semantics: merges are
sorted and compacted per class, never counted as a multiset. Recovered Attempt
authority tuples use the same set semantics across startup, session-reap, and
current-session sweep sources.

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
is `unauthorized_attempt`. A replayed `Run` tuple remains
`unauthorized_attempt` because the matching live attempt may still own runtime;
a different attempt refused by that live Computer Storage owner is instead
`computer_storage_busy` only after the helper positively verifies that the
losing attempt has no remaining runtime.

`Run` establishes an initial attempt deadman within the helper's configured
maximum. The control heartbeat may carry exact attempt renewals, each with a
bounded TTL; the agent emits one only after the matching L1 lease renewal has
succeeded and helper `Run` has admitted that exact attempt. Successful L1
renewals that arrive during image delivery remain agent-local; the first such
renewal is queued only after `Run` returns authoritative `Started` evidence.
Receipt sets an absolute deadline from the helper's monotonic clock.
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
| `DoctorStatus` | Session-authorized read of runtime platform, containerd/runc versions, allowed mount roots, bounded `ImageCacheStatus`, and the actually observed IPv4/IPv6 Computer firewall attachment state. Each sub-read carries its own assertion-derived receipt, so a partial failure does not erase the authenticated handshake or successful siblings. A failed firewall read fails closed, and a missing chain or jump while a Computer attempt is live is a typed `FAILED` screen-isolation finding. Runc comes only from containerd runtime info or a setup-resolved absolute executable path; the privileged helper never performs an operator-triggered PATH lookup. A whole-RPC failure uses `diagnostic_failure`, which is explicitly not runtime-loss evidence: the client does not invalidate the session, reap attempts, or withdraw capability. It never acquires a session, probes, sweeps, starts a task, mutates policy, or evicts content. |
| `Run` | Exact attempt authority, initial deadman, a bounded requested endpoint-name list, and closed workload inputs enter. The helper validates the immutable digest, argv, working directory, explicit environment list, enumerated managed volumes, and operator mounts against configured roots, then constructs the runtime spec itself. Only a successful runc-v2 `Start` after `Wait` registration returns authoritative `Started`, the helper-captured `started_at` timestamp from that exact edge, assertion-derived profile evidence, helper-observed image evidence, and a map from every requested endpoint name to its allocated loopback port. Ordinary attempts request either no endpoint or exactly `service`; a Computer requests exactly the distinct `{view, control}` set, receives authoritative `WEFTY_COMPUTER_VIEW_PORT` and `WEFTY_COMPUTER_CONTROL_PORT`, and cannot retain `WEFTY_SERVICE_PORT`. Before a Computer starts, the helper brings up its private network namespace's loopback interface and transfers the held view, control, and submission listeners into it. A live Computer Storage attachment refuses a different attempt with `computer_storage_busy`; this is definitive no-runtime evidence for only the losing attempt and never changes the live owner's authority. An ordinary OCI Mac bridge-fallback preparation creates a separate guest loopback listener and capability; every Computer instead uses the same constrained bridge shape as its only agent submission path. Default-off exposes no endpoint file while retaining the private listener for a later policy enable. |
| `Signal` | Exact live attempt and only enumerated `TERM` or `KILL`. A containerd `NotFound` after authorization is the closed `task already terminated` mechanics fact: the helper returns `already_terminated=true` without recording delivery of that signal, and `Watch` remains authoritative for the terminal arm. This race alone is not `engine_failure` or runtime-loss evidence; if `Watch` then cannot publish terminal evidence inside the fixed post-KILL release bound, the positive reaped-task fact makes the missing Wait confirmation typed runtime loss. |
| `Watch` | Exact live attempt; live-tails checksum-protected stdout/stderr frames, requires an agent acknowledgement after each event, emits per-stream EOF/incomplete seals, and then exactly one structured exit, signal, OOM-additive, or runtime-failure result on a dedicated connection. Log incompleteness is additive and never replaces the real terminal arm. |
| `Delete` | Exact live attempt only, except that one tombstoned attempt whose helper deadman completed a successful guardian reap may authorize exactly one later `Delete` with full seven-field attempt-authority equality and the current node/boot-session gate. That exception still calls engine `Delete`, repeats independent absence verification, and releases image pins, capacity, ports, and retained runtime state before returning positive deletion; it never treats the earlier reap alone as the response. The helper consumes the guardian evidence when that call completes, so a second exact call, stale fence, foreign attempt, different removal generation or boot session, and every failed guardian reap remain refused. In every path, a positive deletion means the engine has removed and independently verified absence of the attempt's task, container, overlayfs snapshot, lease, and log segments while retaining any stable handoff volume; only then does the server tombstone authorization. |
| `DeleteManagedVolume` | Session-authorized and closed to a derived `handoff` or `service_data` owner key, or exact Computer-removal Storage and cleanup authority. The helper derives the source, deletes only that resource (plus any paired owner record), independently verifies absence, and returns no general path authority. |
| `InventoryRemoval` | Session-authorized current inventory for legacy removal reconstruction. The server snapshots the live-attempt registry, releases its mutex for the engine scan, then rechecks the registry before returning; heartbeat and Run dispatch stay live throughout the scan, and a new matching attempt fails the inventory closed. Computer reimage serialization is context-bounded. A Job-scoped scan returns runtime authorities once. Each per-generation scan returns only Storage proof and never repeats runtime authorities: either a prepared disk backed by a durable copy/reset receipt and no attachment lineage, loop, mount, pending attachment, or retirement, or a distinct typed already-absent disk-root authority. The helper never creates a missing root while proving absence. Every result is bound to the current Node, boot, Job, removal generation, cleanup fence, and exact disk identity. |
| `AttestRemoval` | Session-authorized exact Job/removal generation plus reconstructed attempt authorities and deterministic resource rows. A prepared-removal Storage-only authority requires its helper-originated never-attached witness; reset/restore predecessor and failed-import cleanup use separate typed operation authorities and cannot claim that witness. After separate durable-data deletion, the helper inventories every row and returns only assertion-derived positive absence evidence. |
| `ResetComputerStorage` | Session-authorized exact reset revision and old/new Storage generations. Under the predecessor attachment flock it records a durable retirement fence, then fully allocates, formats, and verifies the successor from a manifest published before its image. It does not delete, publish, attach, or start; predecessor deletion and attestation reuse `DeleteManagedVolume` and `AttestRemoval` after L1 publication. |
| `CopyComputerStorage` | Session-authorized exact restore, clone, or import operation; binds its managed Backup source or immutable external manifest, destination Computer/Storage generation, Node/root instance, Job, revision, and cleanup fence. It verifies source bytes before destination creation. Restore preserves machine identity; clone/import narrowly rekey it and may expand a larger filesystem. |
| `ExportComputerCustody` | Session-authorized transfer of one published Backup copy to an absolute operator-owned path outside the managed root. L1 has already committed the permanent custody event. The helper retains partial bytes on interruption and returns only observed size, content-digest, manifest-digest, path-derived owner UID/GID, ownership-applied, and private-mode-applied evidence. |
| `GrowComputerStorage` | Session-authorized exact current Storage generation, managed-root instance, Job, operation revision/fence, and old/new byte counts. Under attachment/detachment serialization it makes one newcomer-pays admission decision, fully allocates the final image size, refreshes an attached loop device when present, expands ext4, and only then publishes the new manifest size and assertion-derived receipt. A missing manifest cannot be reconstructed as empty lineage when an immutable copy receipt or durable reset-preparation record proves prior storage preparation; that contradiction returns typed `computer_storage_grow_uncertain` before reserving capacity or mutating bytes. A failure after ext4 may have expanded returns the same typed uncertainty, preserves the expanded image, and leaves the exact authority resumable; it never claims `failed_unchanged`. |
| `PreflightComputerReimage` | Session-authorized exact current Storage generation and byte budget, managed-root instance, old/staging Jobs, operation revision/fence, and target digest. Under the generation flock it requires real detachment or explicit verified never-attached reset-preparation evidence, verifies the locally selected manifest platform, reads image and ext4-root UID:GID, and returns assertion-derived success or closed stage/reason failure evidence before L1 may publish or refuse the staging projection. |
| `Verify` | Exact live attempt, or the authenticated session's whole `wefty` namespace. `namespace` is the mutating boot-barrier proof that may update pins, cache state, and sweep completion. `namespace_read_only` is an observation-only inventory route for acceptance baselines; it cannot satisfy the boot barrier or update helper policy state. |
| `Sweep` | Authenticated session only. The boot barrier always sweeps the complete `wefty` namespace; there is no survivor selector. It inventories and removes every unowned `wftch*` link and `WEFTY-COMPUTER-*` per-attempt firewall rule with typed evidence; verified live attempts are the only exclusions. |
| `DialAttemptPort` | Bidirectional host-to-guest stream for exactly one endpoint name returned by that live attempt's `Run`; the server resolves the authorized name to its private allocated port. For a Computer, the engine enters that exact task's network namespace only while creating the backend socket, restores the helper namespace, then relays the authority-bound stream. Success is withheld until the helper has connected that backend. A refused payload listener is a typed, attempt-scoped `engine_failure` and does not invalidate the healthy helper session, so readiness can observe withdrawal and retry republication. Only a successful attempt-endpoint stream detaches from its setup context. It is never a general guest dialer. |
| `DialHostBridge` | Bidirectional guest-to-host reverse-tunnel stream only when `Run` explicitly requested the bridge and the helper issued that attempt's separate capability. It is mandatory for Computers because their private network namespace cannot address the agent's Node-loopback listener directly; ordinary OCI uses it only for the Mac bind-failure fallback. It never accepts an arbitrary host address or port. |
| `SetComputerControlState` | Exact live Computer-attempt authority and one boolean enter. The helper atomically replaces the attempt-local `/wefty/control/driver.json` body with the exact version-1 false or true document; ordinary, stale, old-boot, and reaped attempts are refused. |
| `SetComputerToken` | Exact live Computer-attempt authority plus the opaque bearer and matching attempt bridge endpoint enter. A non-empty pair is atomically installed as attempt-local `/wefty/control/computer-token` and `/wefty/control/l3-endpoint`, both mode 0400 and tenant-owned; an empty pair removes both. A partial pair, ordinary, stale, old-boot, or reaped attempt is refused. |

`DialAttemptPort` terminates inside the guest at `127.0.0.1:<allocated-port>`.
The helper emits an internal backend-ready marker only after that connection is
established, and the client consumes it before returning the opaque stream.
This makes an agent-side connect probe cover the payload, helper session, and
tunnel rather than merely the helper's authorization check.
Like the attempt-scoped `DialAttemptPort` backend refusal,
`computer_storage_busy` and `computer_storage_retired` do not invalidate the
helper session; unlike that retryable readiness refusal, each definitively
proves the losing `Run` has no remaining runtime and therefore needs no second
`Delete`.
The helper holds a kernel listener through runtime-spec construction, transfers
it directly into payload start, and retains the logical allocation until
independent absence verification; failed verification cannot recycle the port.

An interrupted Computer Storage reset successor remains disposable until its
preparation receipt is durably published. A replacement helper removes that
exact unverified successor during its sweep, allowing the standing L1 reset
authority to recreate it; verified successors and tenant-bearing generations
remain retained inventory.
A verified reset successor that has never been attached may use its exact
preparation receipt as Computer reimage quiescence evidence. The receipt binds
the current Storage generation, prior Job, Node, managed root, and reset fence;
once any attempt attaches, the ordinary detach or prior-boot sweep receipt is
required instead.
`DialHostBridge` pairs one authorized host
stream with one accepted connection on that attempt's helper-owned guest
listener; the host agent dials only its already-created loopback run bridge.
Neither direction accepts a caller-supplied network destination.
The OCI adapter runs exactly four `DialHostBridge` pumps per Computer attempt;
that fixed bound is the only Computer submission path on every platform, and
additional guest connections wait for one of those pumps.

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
private PID/IPC/UTS/mount/cgroup namespaces, plus a private network namespace
for Computers only, deny-all device policy plus the six permitted pseudo-devices,
masked/read-only proc paths, a
read-only `/sys/fs/cgroup` cgroup mount, and a writable rootfs for ordinary
workloads or read-only rootfs for Computers; and serializes
cgroup-v2 memory/CPU limits when present. A memory limit also sets OCI swap to
the same value, producing `memory.swap.max=0` on cgroup v2 rather than leaving a
swap escape. `Resources.Pids` remains absent in M3; the missing PID limit is a
known profile gap, not an implicit default. The helper attaches a point-to-point
veth, brings the Computer namespace's loopback and veth interfaces up, installs
a default route, disables IPv6 through the namespace `all` and `default`
sysctls, and masquerades the exact Computer IPv4 address through the Node/VM.
Computer `/30`s use RFC 2544 benchmarking space `198.18.0.0/15`; endpoint port
`p` selects byte offset `(p - AttemptPortMin) * 4`. The helper admits at most
32,768 endpoint ports and refuses a conflicting non-Wefty Node
route. A
Node-loopback resolver in the exact mounted snapshot is mirrored by an
attempt-private UDP/TCP helper proxy at the same address inside the Computer
namespace. The proxy forwards from the Node namespace to systemd-resolved's
advertised non-loopback uplink after a bounded lookup proves it reachable,
then falls back to the Node stub only after that stub answers the same probe; a
routable resolver traverses the veth directly. The proxy validates DNS framing,
rate limits each attempt, and bounds concurrent TCP clients. Mirrored
`ip6tables` chains retain the same boundary if an image re-enables IPv6. The
absence of the kernel `ip6table_nat` table is recorded as
`unavailable_ipv6_disabled`, rather than refusing a Computer whose namespace
has IPv6 disabled; IPv6 filter-chain failures still fail closed. The
firewall rejects Computer-to-Computer, Node-local, and unsolicited inbound
traffic except the attempt/interface-bound helper egress proof endpoint, whose
ACCEPT additionally requires the owning transparent helper socket. The helper
reconciles both chain sets at every Computer start and on its periodic sweep
cadence. The INPUT, FORWARD, and POSTROUTING jumps must each be the first base
chain rule; later presence is reported absent and repaired. The helper also
compares every helper-owned chain body with its canonical ordered rules and
rebuilds a body whose rule order, membership, or duplication differs. A new
first-position jump is inserted before stale copies are removed, so repair
does not create a no-jump window. Startup removes
unowned host/guest link ends and IPv4/IPv6 rule residue, and tears down live
network state plus helper-owned Node-wide state on close. It creates the `view`,
`control`, and Computer submission listeners inside that namespace, then enters
it only to create an exact-authority named-endpoint dial. The resulting socket
returns to the helper namespace for relay; no helper thread or general guest
dialer remains inside the Computer namespace. Ordinary OCI workloads retain
shared networking. The runtime handler, snapshotter, and containerd namespace are fixed at
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

Before task creation, the helper fsyncs a versioned Attempt-ownership record
containing the complete seven-field authority and its derived resource
identities. Publication, snapshot loading, stale-temp cleanup, record removal,
and unknown-version GC share one engine lock, so no sweep or read-only Verify
can unlink an in-flight publication. Removing the final record preserves the
`attempt-ownership` parent directory. Labelled containerd metadata may reconstruct the same record before
that metadata is deleted during sweep. A deterministic-looking name is never
ownership: sweep mutates a log directory or cgroup only when the exact resource
identity is bound by that durable record and the helper's locked registry says
the Attempt is no longer live. It rechecks that registry immediately before
each remove or KILL. An exact 32-hex collision without the binding, a binding
for a live Attempt, a symlink, or a foreign-owned directory remains typed
runtime residue; the helper fails closed instead of guessing ownership.
Unexpected, unreadable, stale-version, or invalid ownership entries emit a
typed `invalid_entry`, `unreadable`, or `invalid_record` operator outcome with
`disposition=resources_unbound`; they cannot authorize removal and therefore do
not turn any observed resource into retained state. Each Verify reads this
directory once and uses that snapshot for its complete retention projection. A
structurally valid unknown-version record is `unknown_version` and remains
unbound until its named resources are absent, when Verify garbage-collects it;
invalid records that cannot safely name resources remain operator-owned.

After containerd teardown, sweep gives log sealing at most the configured
five-second cap, further reduced to half of the usable time remaining before
the caller's barrier deadline. Twenty percent of the then-remaining barrier
time, capped at one second, is reserved for later cgroup/final-inventory and
Verify work. Scanning resumes from the last complete framed-record offset on
each poll, resets the offset after shrink or a changed first frame header, and
checks cancellation inside the scan. A directory with neither frames file has nothing to seal and is
removed immediately. A corrupt frame likewise has no trustworthy seal work
left: sweep removes the spool with `method=corrupt_frames` evidence instead of
failing the boot barrier. If the phase budget
expires, sweep fsyncs and returns an exact retention receipt containing class,
resource and Attempt IDs, observed `unsealed` state, owner, reason, recorded
time, five-minute bound, and absolute deadline. Verify may classify that visible
directory as durable retained only by reading the unexpired receipt; it never
reconstructs retention from a name, UID, or bytes. A later sweep removes a
sealed directory, and expiry of the five-minute bound forces removal with typed
evidence or returns the typed `retention bound exceeded` operator outcome.

Handoff volumes live under a distinct helper-owned durable root, not the
attempt namespace. `Delete` reaps and verifies the attempt while retaining its
handoff volume. Session reap and boot sweep likewise leave unexpired handoffs
intact; reuse refreshes the default 24-hour retry age, and sweep removes only
expired direct children with the deterministic handoff prefix. Attempt and
namespace quiescence therefore project only unexpired handoff volumes (the
retained bindings) out of their absence decision. `Verify` returns the observed
inventory unchanged alongside the exact, disjoint runtime-residue and
durable-retained projections. Every consumer of `Absent` validates that their
union is exactly the observation, and the boot receipt records all three with
`verified_absent`.
Thus service data is durable-retained only while its paired owner record still
matches the no-follow directory identity, and an unexpired handoff is retained
only while its retention authority is live. An orphan directory, orphan owner
record, mismatched binding, or expired handoff remains runtime residue rather
than passing by name prefix. The
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
Namespace boot-sweep verification projects a service-data pair out only while
that ownership binding is valid or an authorized removal still owns it. Before the first mount, the helper
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
attempt authority before mounting. A helper sweep recovers a pending or
attached manifest only after mount and loop absence are proved. It also
refreshes an already-detached `same_boot_reap` manifest into a sweep receipt
after the same absence proof, so a clean reap followed by an agent reboot does
not strand the Storage generation.
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
last atomic admission facts. The post-start runtime receipt records the helper
and task network namespace inodes, their observed inequality, the veth
address/gateway, mounted resolver address, UDP/TCP proxy listeners, selected
upstream address/source/reachability, the typed IPv6 NAT state, and whether the exact abstract X11 socket token is visible from
the helper namespace. The node doctor reports screen isolation as `OK` only for
distinct non-empty inodes, `present=true`, and `visible=false`; a missing
Computer receipt remains `NOT-RUN`, and any other combination is `FAILED`.
Doctor also performs a fresh `iptables -S` and `ip6tables -S` observation:
`computer_firewall_present=true` requires every helper chain in canonical
order and every Node jump exactly once at position one, and
a missing attachment while any Computer attempt is live is `FAILED` rather than
inferred from startup configuration.

The helper holds an exclusive per-generation file lock for the attachment
lifetime and durably records exact attempt/fence/boot authority beside the
image, never inside tenant bytes. The manifest's attached state is authority:
lock disappearance and helper death do not authorize another attach. Exact
same-boot `Delete`/`ReapAndVerify` writes a single-use detachment receipt; boot
sweep positively unmounts and detaches every helper-owned Computer disk,
retains image bytes, and writes a receipt bound to its sweep epoch. The next
attach consumes exactly one such receipt. A boot-sweep receipt names the prior
Job/attempt/fence rather than the successor: helper replacement may happen
within the same agent boot, and Computer reconfiguration may replace the Job,
so successor admission instead requires the same durable Computer/Storage
generation and Node plus the independently authenticated fresh attempt. The
persisted `prior_boot_sweep` kind distinguishes a helper sweep from a
same-session reap; it does not assert that the agent boot-session ID changed.
All consumers require the exact durable Computer/Storage generation, Node,
complete historical Job/attempt/fence/boot fields, and kind-specific proof: a
`same_boot_reap` receipt must match the consumer's boot and carry no sweep
epoch, while a `prior_boot_sweep` receipt must carry a sweep epoch and is not
compared to the consumer's boot. Attach does not compare the historical Job to
the authenticated successor Job. Reset, Backup creation, and removal instead
carry a separately named prior Job and require the historical receipt Job to
equal it; reimage applies the same rule through `OldJobID`. Thus a replacement
Job can consume its predecessor's proof without allowing an unrelated Job to
copy, retire, or delete the Storage bytes.
Inventory separately enumerates disk
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

Fresh ownership initialization is closed to two classes: an image newly
formatted by the first attach, and an empty Reset successor whose prepared
manifest carries its verified `PreparationReceipt`. Copy, restore, clone,
import, and grow destinations are already tenant-owned even when their
manifests say `Prepared`; without explicit `chown` they must refuse a root
ownership mismatch, and with `chown` they recursively migrate every tenant
entry. Reset freshness survives the pending-attach and mount boundaries and is
cleared only by the same durable manifest write that publishes `Attached`, so
a failed or crashed first attach cannot silently consume the one-time fact.
After attachment and before runtime-profile source retention, the helper
creates a storage-local machine ID when absent or repairs it when malformed,
logs that repair, and then verifies root ownership before task start. The
canonical `/etc/machine-id` mount is read-only, while the same persistent file
remains writable through `/wefty/service/etc/machine-id`. Backup and restore
preserve those bytes; clone and import initialize legacy storage when needed
and then replace the machine ID while the copied filesystem is detached. The identity path remains part of the tenant disk image, so the
helper's positive rekey receipt describes bytes the destination attempt
actually consumes rather than immutable image-layer lookalikes.

`ResetComputerStorage` accepts only the authenticated current helper session
plus exact Node, managed-root instance, consumer Job, named prior Job,
reset-intent revision, cleanup fence, old generation, and successor generation.
It requires either a same-boot reap receipt for the current boot or a
sweep-epoch receipt whose historical Job equals that named prior Job, writes
its retirement fence under the attachment flock, then resumes successor
`allocation_manifest_written → allocated_and_formatted → image_published →
verified`. The verified receipt is stable across retries and binds every input
above plus both generations and the helper generation that performed positive
verification. Helper mechanics never delete the predecessor, publish or attach
the successor, or start the Computer. After L1 durably publishes the successor,
the agent uses shared deterministic removal resource classes for the old disk,
legacy reset manifest, and quarantine root; `AttestRemoval` returns only
assertions that were actually inventoried absent. A later helper session may
replay that receipt but cannot restamp it with a newer generation.

Startup recovery scopes structural and I/O failures to the affected disk.
Identity mismatches and invalid clean-reap evidence produce typed quarantine;
unreadable per-disk files and quarantine-root inventory produce typed deferral.
Neither condition turns one disk's state into a whole-node sweep failure.

`GrowComputerStorage` binds its receipt to Computer, Storage generation, Node,
managed-root instance, Job, operation revision and fence, helper generation,
and both byte counts. Grow serializes with reset, Backup, attach, detach, and
removal. Before filesystem expansion it durably writes `storage-grow.json`
with the exact old and target sizes and operation authority. Startup reconciles
that record before namespace verification: an untouched old-size image rolls
back by removing the intent, while a target-size image is idempotently resized,
allocation-verified, and published in `attachment.json`. Its crash boundaries
are therefore capacity reservation, durable intent, filesystem expansion, and
manifest publication; retry inspects durable image and manifest facts and
never reports applied before full allocation plus filesystem expansion. The
capacity decision includes unmaterialized admitted reservations under the same
lock, so existing workloads retain their reservations and the newcomer pays.
An insufficient-capacity receipt is valid only before bytes change.
After ext4 expansion begins, failure is instead the typed resumable
`computer_storage_grow_uncertain` outcome: the helper never truncates the image
or reports `failed_unchanged`, and retry inspects the expanded image before it
publishes a receipt.
L1 persists the validated receipt with the grow outcome. Computer operator
projections expose it as `capacity.last_grow`; requested,
observed-available, and failure-code facts come only from that persisted
receipt. `capacity.active_failure` separately projects current launch/runtime
resource latches from `last_failure`. A pending, superseded, or absent grow
receipt remains `NOT-RUN` with `grow_pending`, `grow_superseded`, or
`grow_receipt_absent`, respectively.

`CopyComputerStorage` records its staged identity and phases in
`storage-copy.json`. Startup treats the `manifest_written` phase plus the exact
staged or renamed image digest as resumable authority, completes the atomic
rename when needed, verifies allocation, and publishes `attachment.json`
before inventory admission; an earlier staged phase is rolled back because no
destination generation was published. Grow and copy recovery emit typed
`resumed` or `rolled_back` sweep evidence. Operational recovery failures with
valid durable authority increment `attempts`, preserve `first_deferred_at` and
a closed reason, emit `resume_deferred`, and keep that Computer generation
unattachable. Generation-local operational deferrals, including unreadable
manifests and images, are persisted separately from the unreadable artifact so
their attempt count and first-deferred time survive helper replacement. A later
`CopyComputerStorage` call for the same durable request
returns typed `computer_storage_resume_deferred` while that deferred manifest
remains; a quarantined generation returns the existing typed quarantine result.
Only boot-barrier startup sweeps increment the durable attempt
count; in-session `ReapSession` sweeps retain state without consuming it.
Recovery becomes terminal only after both 24 failed helper-start sweeps and 24
elapsed hours: `resume_abandoned` quarantines the
generation while preserving its last deferral reason and original
`first_deferred_at` timestamp in the receipt and typed inventory. The 24-attempt
cap is a minimum repeated-observation bound, not an assumed hourly cadence; the
independent wall-clock floor prevents rapid helper flaps or barrier retries from
consuming that budget in minutes. Grow recovery preens
ext4 with `e2fsck -f -p` before resizing, and exit 1 is recorded as corrected
filesystem sweep evidence. A
size/allocation mismatch without matching durable operation
authority is never reinterpreted as a successful operation: startup moves the
whole generation into `computer-disk-quarantine`, writes a typed
`computer_disk_anomaly_quarantined` record, and emits `quarantined` sweep
evidence with the closed reason. Quarantined generations remain visible in
`ComputerQuarantines` and operator/removal surfaces but are durable retained
state, not runnable namespace residue. Quarantine receipts retain the full
payload for 24 hours. GC revalidates the complete receipt under the generation
flock, records `payload_dropped_at` and typed evidence, and keeps the receipt
and lock tombstone until authorized removal. Invalid authority never permits
byte deletion, and GC failure does not fail helper startup, so generation N is
never admissible again. The
authorized recovery path is a reset that prepares and admits generation N+1,
followed by normal removal authority for N. The affected Computer therefore stays
fail-closed while the helper continues serving the rest of the Node. Startup's
namespace-absence promise remains exact for every non-quarantined generation.

For a Custody import, typed helper runtime loss during `CopyComputerStorage`
becomes an exact-generation `computer_storage_preparation_interrupted`
observation; startup recovery also carries exact-generation
`computer_storage_resume_deferred` or `computer_storage_quarantined` results.
Receipt-backed outcomes retain the sweep epoch, disk name, closed reason,
attempt count, and deferral timestamps. All outcomes bind Computer ID, Storage
ID, generation, intent revision, disk bytes, helper generation, and observation
time; an identity mismatch never attaches to an import. L1 records the
preparation idempotency key and body hash, accepts identical replay, and rejects
an older helper generation or timestamp. This evidence grants no publication
or cleanup authority, leaves the reserved directive retryable, and is cleared
by later success, terminal failure, or supersession. L1's import ledger remains
the immutable idempotency and provenance authority even after a terminal result
has released the reserved Computer identity, so its row is not age-pruned.
`services custody import --wait` ends on the first durable preparation outcome
and distinguishes deferred, quarantined, failed/interrupted, and superseded
exit statuses instead of timing out on an unchanging Computer projection.

Required-file recovery classification is exact:

| Observation | Classification | Startup action |
| --- | --- | --- |
| `ENOENT` or `ENOTDIR` for a required file | structural absence | quarantine with a typed missing/authority reason |
| non-absence read or stat error on a regular required file, including `EIO` or `EACCES` | operational | retain and emit `resume_deferred` |
| required recovery record is a directory, symlink, or other non-regular file | structural invalidity | quarantine with `record_not_regular` |
| bytes read completely but invalid JSON, version, or fields | structural invalidity | quarantine with typed authority-invalid evidence |
| verified size, allocation, or digest mismatch | structural mismatch | quarantine the generation |

`PreflightComputerReimage` runs only after the old Job is stopped and the disk
manifest contains exact same-boot reap or prior-boot sweep evidence. Image
delivery remains agent policy; the helper consumes only the locally selected
digest and verifies its manifest platform before reading image-user metadata.
It reads the detached ext4 root UID:GID without following tenant paths, refuses
an ownership mismatch unless the durable operation explicitly carries `chown`,
and binds the receipt to both Jobs, the operation revision/fence, Node,
managed-root instance, helper generation, and one explicit Storage proof kind:
`computer_reimage_detachment` carries the real reap/sweep receipt plus its
historical attempt and fence, while `computer_reimage_reset_preparation`
carries only the verified never-attached reset-preparation receipt. Reset
preparation is never recast as an invented detachment attempt.

Attach and delete hold the node-global reimage mutex only through manifest/flock
admission. After they acquire the exact generation flock, that flock owns the
remaining disk work so a slow attach or delete for one Computer cannot block
preflight admission for every other Computer on the Node. Preflight retains its
admission locks and the exact generation flock while it reads the manifest,
verifies its durable byte budget, and reads the ext4 root owner, then releases
the flock before publishing the receipt. The whole helper operation, including
context-free filesystem reads executed behind cancellation-aware joins, has a
10-second deadline; cancellation joins each worker and closes any late-opened
lock descriptor before returning and releasing the flock. The tagged native lane
logs the measured helper duration
for each reimage. On the 2026-08-31 PR lane, the complete adapter/helper
preflight against the 160 MiB acceptance disk measured 27.263 milliseconds for
XFCE and 22.245 milliseconds for Wayland; the 10-second bound leaves more than
366 times the slower measured duration without widening the four-minute
end-to-end Computer transition budget.

Every refusal is a `computer_reimage_preflight_failed_unchanged` receipt with
one closed stage and a bounded reason. The stage vocabulary is exactly
`generation_lock`, `manifest_read`, `allocation_verify`, `receipt_create`,
`image_identity`, `image_config`, `image_owner`, `disk_owner`, and
`ownership_match`. Reasons are `operation_failed`, `deadline_exceeded`,
`detachment_required`, `image_unavailable`, or
`image_platform_unsupported`. L1 accepts these fail-closed receipts, releases
the refused staging projection, and surfaces the stage and reason on the
stopped current Job instead of redispatching the operation indefinitely. The
agent alone treats `detachment_required` and `generation_lock` plus
`deadline_exceeded` as transient: it retries them on the next two polls and
makes the third receipt definitive. Every other stage/reason pair remains
immediately definitive.
If the exact digest is unavailable or has no manifest for the bound Node's
platform, the helper returns a typed `failed_unchanged` receipt only after the
same detachment and disk-allocation checks. L1 then retires the refused staging
projection while preserving the stopped current Job and disk generation.
If the durable generation row is missing, L1 still dispatches the operation
with a zero byte budget and the helper returns a typed `allocation_verify`
refusal; the directive is never silently omitted. Image-stage failure
acknowledgements must carry the same valid detachment or reset-preparation
evidence as success.

L1 records a successful preflight receipt and activates the staged Computer
projection in the same acknowledgement transaction. The agent does not wait
for an operator replay or a later heartbeat to move the Computer out of
`reimaging`; an identical acknowledgement replay returns the already-completed
projection. When the retiring projection is live, L1 preserves the Computer's
running intent but writes that service projection desired-stopped, so its next
fenced lease renewal carries the stop directive and reaches the stopped
preflight precondition.
An acknowledgement-time capacity refusal is persisted in that transaction as
a typed reimage-preflight failure and releases the staging projection instead
of leaving the agent to retry an unobservable transaction error.

Computer removal carries the same exact Storage identity plus current Node,
boot, consumer Job, named prior Job, removal-generation, and cleanup-fence
authority to the helper. The helper requires a matching detached receipt whose
historical Job equals the named prior Job, verifies mount and loop absence,
deletes the image, manifest, and generation quota directory, and positively
checks their absence before the agent may acknowledge `removed_verified`.

`CreateComputerBackup` is narrow privileged mechanics for one source-node cold
copy. Its authority binds Backup, copy, Computer, `storage_id@generation`,
allocated size, Node, boot, root instance, consumer Job, named prior Job,
operation revision, cleanup fence, and helper generation. Before reading, the
helper requires the exact source lock, an accepted detach receipt whose
historical Job equals the named prior Job, and positive mount and loop absence.
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
size, content digest, and manifest digest are observed. Files remain mode
`0600` but inherit the owner and group of the nearest existing ancestor of the
operator-selected path; every missing directory is created `0700` with that
owner. The owner is path-derived, not identity-bound. The helper refuses
symlink, non-regular, or unexpectedly owned replacement inodes, and truncates,
chowns, writes, syncs, and hashes only the `O_NOFOLLOW`-opened file descriptor,
so a privileged helper does not strand the portable result under helper
identity or follow a substituted target. Receipt ownership and private-mode
facts make those mechanics acceptance-visible. L1 maps the verified receipt to
durable status `available`; here `available` means verified portable external
bytes, whereas Backup `available` means a verified wefty-managed copy.
`CopyComputerStorage` also accepts `import`: it verifies the recorded manifest
digest and reopens the external disk without following links to verify the full
disk digest before creating a managed destination. Those post-copy and import
digest reads are load-bearing because path-derived ownership is not identity
authority. Import then applies
the same narrow OS machine-ID rekey and optional filesystem expansion as clone.
Successful receipts record distinct well-formed pre/post identity digests,
unchanged source bytes, and the prepared/no-freshness/no-chown destination
facts. Both receipts bind the Node, managed-root instance, operation revision,
cleanup/custody fence, and helper generation; neither helper call decides
removal truth.

`InventoryRemoval` normally reconstructs exact observed attempt authority from
one Job-scoped scan. Each subsequent Computer-generation request returns only
that generation's deterministic Storage rows, so the adapter cannot duplicate
runtime authority across generations. Its no-runtime results are either an
exact prepared disk with no attached, pending, previously detached, or retired
authority, or an exact typed absent-disk-root authority. The latter is absence
evidence, not preparation evidence, and does not create filesystem state.
Every result remains bound to the authenticated current helper session and
durable removal fence; malformed, anomalous, historically attached, or
identity-mismatched disks still fail closed.

`AttestRemoval` accepts only an exact service Job/generation plus reconstructed
attempt authorities and their deterministic resource rows. Ordinary services
call it after the separate idempotent
`DeleteManagedVolume(service_data, job_id)` succeeds; Computer operation
cleanup calls it after the corresponding Computer disk deletion succeeds.
Storage-only attestation has two closed authority shapes: the prepared-removal
shape carries the exact helper-originated never-attached witness returned by
`InventoryRemoval`, while reset/restore predecessor and failed-import cleanup
carry a typed operation/revision attempt identity after their separately
authorized `DeleteManagedVolume` call. The operation shapes cannot claim the
never-attached witness, and an arbitrary synthetic attempt identity is refused.
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
fail. After containerd task deletion, sweep acts only on a cgroup bound to a
durable LOST Attempt record. Observation remains broad for compatible CRI,
systemd-composed, and pre-upgrade names containing `wefty-cgroup-`; inventory
and sweep both descend through broad matches so an exact bound child cannot be
hidden. Only the direct deterministic name and its single `.scope` form are
sweepable. After a bound child is reaped, its broad wrapper is removed only when
empty; a nonempty wrapper remains residue. The barrier names its relative path
with the typed operator outcome `unbound wefty-shaped cgroup; not helper-owned;
remove manually or bind`. With the live registry
locked and rechecked, the helper sends `SIGKILL` through cgroup v2's
`cgroup.kill` (falling back on open, write, or close `EOPNOTSUPP` to `SIGKILL`
for every PID in the subtree), then removes directories bottom-up. This is
helper cleanup of proven LOST authority, not service-stop policy: as stated
above in the Attempt authority section, only the agent chooses TERM grace and
KILL escalation for a live service.

The cgroup wait uses the configured five-second task-release cap reduced to
the usable barrier time remaining after the same twenty-percent/max-one-second
Verify reserve. Budget expiry is not a generic sweep failure: it fsyncs a
five-minute `cgroup_reaping` retention receipt with observed `populated` state,
and Verify reads that receipt to classify the cgroup as durable retained. At
retention expiry, sweep retries KILL/removal and either emits typed bound-reap
evidence or returns `retention bound exceeded`. Every KILL receipt records the
observed PID set, `cgroup.kill` versus `recursive_signal`, and duration. An
unbound exact collision remains operator-visible runtime residue. Filesystem
errors other than verified `NotFound` still fail closed.
Attempt Delete gives this cgroup poll at most half of its remaining cleanup
budget, leaving time for final verification. If the inventory needed to decide
whether the ownership record is quiescent is temporarily unavailable, release
succeeds with typed `inventory_retryable` evidence and keeps the record for a
later Verify or sweep rather than converting observation failure into an
attempt-release failure.

`SweepResponse.removed` is the number of identities in the initial observed
inventory that are absent from the final observed inventory. Retained resources
are therefore not counted as removed. The response also carries durable
retention receipts and typed per-resource sweep evidence; startup/session-reap
folding preserves those fields in the client's verified receipt.
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
path, only when the returned directive is empty, and only after helper `Run`
has admitted that exact tuple. A pre-admission renewal is retained locally and
the latest one is queued after authoritative `Started`; failed, timed-out,
stale, `stop`, and `restart` responses never refresh the helper deadman. The pump uses
separate operation connections for image/watch streams so backpressure cannot
starve session authority, and it locally verifies the returned helper checksum
against a non-empty installed expectation before exposing the session.

Prior-boot removal consumes positive sweep evidence once. A swept attempt still
requires node, job, class, prior boot, attempt, fence, and removal generation.
When an older helper already reaped that attempt, a verified-empty replacement
sweep may omit the attempt only if `PriorBootSessionsSeen` contains the exact
`(NodeID, BootSessionID)` from the removal intent. A successful session reap records that session identity in
the running helper process, and every later session-generation sweep includes
that process-local fact; a failed reap records nothing, and a restarted helper
must recover evidence from its own startup sweep. Both forms remain bound to
the sweep epoch and helper
generation. The helper retains a bounded process-local history of these
identities across session generations; a bare verified-empty sweep is insufficient. Legacy manifest
reconstruction and replacement-sweep attempt recovery continue to require the
complete authority match, except for the current-session prepared Computer
Storage-only inventory described above.

When native OCI is configured, the production agent opens a boot barrier and
installs the OCI adapter as one unit. It advertises `kind:oci`, `cgroup_v2`, and
the runc-v2 handler only after a pinned local `/bin/true` probe creates, starts,
waits for, and verifies deletion of a real task inside the ten-second probe
deadline. The same probe runs through the existing startup and heartbeat
capability refresh loop. Successful L1 renewals are mapped to the exact helper
attempt tuple and queued on that session's heartbeat pump.
