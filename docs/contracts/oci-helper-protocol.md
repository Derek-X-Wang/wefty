# OCI helper protocol

The node agent's private `__wefty_oci_helper` mode is the privileged mechanics
boundary for `kind=oci`. The helper is stateless with respect to scheduling and
L1 policy: no containerd type, registry retry rule, sweep-selection policy, or
workload-class lifecycle policy crosses this protocol. The Linux and Lima
transports both present a Unix stream socket; the raw containerd socket remains
root-only and is never forwarded.

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

Wire major `1` is carried on every request and response. A different major is
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
post-sweep inventory, and recovered `(removal_generation, attempt_id,
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
| `EnsureImage` | Session-authorized, typed progress/result stream on a dedicated connection. The agent supplies the canonical platform retained from the successful probe for this helper generation; manifest selection and image singleflight are keyed by it. Registry mode resolves only a public reference, pins the returned top-level digest, pulls into the fixed namespace, and unpacks that platform. Archive mode receives an OCI-layout tar stream, recomputes every blob digest, validates descriptor sizes and reachability, admits exactly that platform, and imports/unpacks it. Both modes return the same complete image evidence used by `Run`, including top-level/platform digests, platform, runtime handler, and snapshotter; no containerd type, private registry credential, or retry policy crosses the boundary. |
| `Run` | Exact attempt authority, initial deadman, and closed workload inputs enter. The helper validates the immutable digest, argv, working directory, explicit environment list, enumerated managed volumes, and operator mounts against configured roots, then constructs the runtime spec itself. Only a successful runc-v2 `Start` after `Wait` registration returns authoritative `Started` with helper-observed image evidence. An explicit attempt-port request allocates from the helper-owned reserved range and injects the authoritative loopback port; an explicit Mac bridge-fallback request creates a separate guest loopback listener and capability. |
| `Signal` | Exact live attempt and only enumerated `TERM` or `KILL`. |
| `Watch` | Exact live attempt; live-tails checksum-protected stdout/stderr frames, requires an agent acknowledgement after each event, emits per-stream EOF/incomplete seals, and then exactly one structured exit, signal, OOM-additive, or runtime-failure result on a dedicated connection. Log incompleteness is additive and never replaces the real terminal arm. |
| `Delete` | Exact live attempt only. A positive deletion means the engine has removed and independently verified absence of the attempt's task, container, overlayfs snapshot, lease, and log segments; only then does the server tombstone authorization. |
| `Verify` | Exact live attempt, or the authenticated session's whole `wefty` namespace for boot-barrier absence proof. |
| `Sweep` | Authenticated session only. The boot barrier always sweeps the complete `wefty` namespace; there is no survivor selector. |
| `DialAttemptPort` | Bidirectional host-to-guest stream for exactly the port returned by that live attempt's `Run`; never a general guest dialer. |
| `DialHostBridge` | Bidirectional guest-to-host reverse-tunnel stream only when `Run` explicitly requested the Mac bind-failure fallback and the helper issued that attempt's separate bridge capability. It never accepts an arbitrary host address or port. |

`DialAttemptPort` terminates inside the guest at `127.0.0.1:<allocated-port>`.
The helper holds a kernel listener through runtime-spec construction, transfers
it directly into payload start, and retains the logical allocation until
independent absence verification; failed verification cannot recycle the port.
`DialHostBridge` pairs one authorized host
stream with one accepted connection on that attempt's helper-owned guest
listener; the host agent dials only its already-created loopback run bridge.
Neither direction accepts a caller-supplied network destination.

Stream RPCs use a JSON authorization response followed by a one-byte client
acknowledgement before raw bytes begin. This keeps JSON decoder read-ahead from
consuming stream payload. Authorization is complete before success is sent.
EOF or client cancellation on any operation connection cancels its engine
context. `EnsureImage` is content-addressed and does not take the attempt-create
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
spawn classification remain agent policy. A delivery operation is singleflight by fixed
namespace, top-level digest, admitted platform, and snapshotter, and holds a
short-lived content lease while containerd works. Canceling the first waiter
does not cancel the shared operation; helper-session loss cancels and joins all
operations. Sweep removes stale operation leases and archive spools but skips
resources registered to live image operations. Per-waiter attempt and binding
pin attachment before shared-lease release lands with bounded cache policy in
#144; M3 has no evictor before that ticket.
Error detail remains local and a resolved digest is retained across agent
retries.

## Guest-side runtime-spec construction

`Run.workload` carries only the closed program inputs the agent owns: immutable
image digest, optional full argv and working-directory replacements, separate
public and sensitive operator environment, helper-managed reserved environment,
enumerated managed volumes, operator mounts, and optional memory/CPU limits.
The helper-managed list may contain only the exact five reserved names. A
reserved name arriving defensively in either operator list is stripped rather
than winning authority. Image
configuration, image-rootfs user/group databases, guest architecture/kernel
facts, resolver and hosts files, translated Lima mount paths, namespace/device
policy, and OCI JSON never cross from the agent.

The privileged adapter constructs `wefty-v1` from containerd v2.3.4's generated
Linux baseline, then replaces every security-sensitive field explicitly. It
resolves the image `USER` and supplemental groups from the pinned guest rootfs;
sets the fixed capability sets, `noNewPrivileges`, containerd default seccomp,
private PID/IPC/UTS/mount/cgroup namespaces, shared networking, deny-all device
policy plus the six permitted pseudo-devices, masked/read-only proc paths, a
read-only `/sys/fs/cgroup` cgroup mount, and writable rootfs; and serializes
cgroup-v2 memory/CPU limits when present. A memory limit also sets OCI swap to
the same value, producing `memory.swap.max=0` on cgroup v2 rather than leaving a
swap escape. `Resources.Pids` remains absent in M3; the missing PID limit is a
known profile gap, not an implicit default. The
runtime handler, snapshotter, and containerd namespace are fixed at
`io.containerd.runc.v2`, `overlayfs`, and `wefty`.

Capability parity is exact: bounding, permitted, and effective contain the 12
allowed capabilities; inheritable and ambient are explicit empty arrays. The
latter two, plus `root.readonly=false`, are emitted by the canonical
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
`/wefty/handoff` and `/wefty/service` are helper-reserved mount targets.

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
numeric-user, and unlimited-resource inputs. The Linux-only oracle also compares
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

Profile-construction rejection crosses helper RPC as `oci_spec_rejected` and
maps to the existing agent `SpawnFailureOCISpecRejected`; it is never collapsed
into the ambiguous `engine_failure` bucket.

## Deterministic identities and serialization

The helper hashes the complete attempt authority tuple with SHA-256 and uses
the first 128 bits to derive deterministic names for the lease, snapshot,
container, task, shim, cgroup, log-segment directory, handoff volume directory,
and service-data volume directory. Every label-capable resource carries the
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

Prior-boot removal consumes a matching sweep attempt once. The match includes
node, job, class, prior boot, attempt, fence, and removal generation, and the
positive receipt is bound to both the sweep epoch and helper generation.

When native OCI is configured, the production agent opens a boot barrier and
installs the OCI adapter as one unit. It advertises `kind:oci`, `cgroup_v2`, and
the runc-v2 handler only after a pinned local `/bin/true` probe creates, starts,
waits for, and verifies deletion of a real task inside the ten-second probe
deadline. The same probe runs through the existing startup and heartbeat
capability refresh loop. Successful L1 renewals are mapped to the exact helper
attempt tuple and queued on that session's heartbeat pump.
