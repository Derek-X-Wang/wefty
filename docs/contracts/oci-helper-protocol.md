# OCI helper protocol

The node agent's private `__wefty_oci_helper` mode is the privileged mechanics
boundary for `kind=oci`. The helper is stateless with respect to scheduling and
L1 policy: no containerd type, registry retry rule, sweep-selection policy, or
workload-class lifecycle policy crosses this protocol. The Linux and Lima
transports both present a Unix stream socket; the raw containerd socket remains
root-only and is never forwarded.

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
`peer_unauthenticated` and cannot mint a session capability.

Wire major `1` is carried on every request and response. A different major is
rejected as `version_mismatch` before dispatch. `AcquireSession` also carries
the agent's expected helper checksum when one is installed; a mismatch is
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
| `EnsureImage` | Session-authorized, typed progress/result stream on a dedicated connection. Reference and digest enter; no containerd client or retry policy crosses the boundary. |
| `Run` | Exact attempt authority, initial deadman, and closed workload inputs enter. The helper validates the immutable digest, argv, working directory, explicit environment list, enumerated managed volumes, and operator mounts against configured roots, then constructs the runtime spec itself. Returns authoritative `Started`, the optional allocated attempt port, and an optional helper-issued Mac fallback bridge capability. |
| `Signal` | Exact live attempt and only enumerated `TERM` or `KILL`. |
| `Watch` | Exact live attempt; emits typed progress and structured result events on a dedicated connection. |
| `Delete` | Exact live attempt only. A positive deletion tombstones its authorization for the remainder of the session. |
| `Verify` | Exact live attempt, or the authenticated session's whole `wefty` namespace for boot-barrier absence proof. |
| `Sweep` | Authenticated session only. The RPC supplies mechanics; ticket #139 owns what and when to sweep. |
| `DialAttemptPort` | Bidirectional host-to-guest stream for exactly the port returned by that live attempt's `Run`; never a general guest dialer. |
| `DialHostBridge` | Bidirectional guest-to-host reverse-tunnel stream only when `Run` explicitly requested the Mac bind-failure fallback and the helper issued that attempt's separate bridge capability. It never accepts an arbitrary host address or port. |

Stream RPCs use a JSON authorization response followed by a one-byte client
acknowledgement before raw bytes begin. This keeps JSON decoder read-ahead from
consuming stream payload. Authorization is complete before success is sent.
EOF or client cancellation on any operation connection cancels its engine
context. `EnsureImage` is content-addressed and does not take the attempt-create
side of the sweep gate.

## Deterministic identities and serialization

The helper hashes the complete attempt authority tuple with SHA-256 and uses
the first 128 bits to derive deterministic `wefty-{lease,snapshot,container,task}`
names. Every enumerable resource carries the unabridged labels:

```text
io.wefty/node_id
io.wefty/job_id
io.wefty/attempt_id
io.wefty/fencing_token
io.wefty/boot_session_id
io.wefty/class
io.wefty/removal_generation
```

`Run` takes the create side of one helper-wide gate; `Sweep` takes its exclusive
side. Every engine operation except `Sweep` and namespace `Verify` fails with
`sweep_required` until one sweep has succeeded in the current helper process.
No attempt creation can cross a sweep boundary. Within `Run`, the helper-side
adapter must create and label the per-attempt containerd lease before any other
resource (§5.2). The later boot-barrier ticket owns sweep selection, survivor
policy, and capability publication. Ticket #140 fills in the fixed isolation
profile used for helper-side runtime-spec construction; the unprivileged agent
never supplies OCI runtime-spec material. The later runtime ticket also owns
the containerd adapter, logs, and real task lifecycle; packaging owns installed
units and socket activation.

## Agent-side client responsibilities

The client owns the control-stream heartbeat pump and its strictly monotonic
sequence counter. Ordinary callers cannot submit arbitrary heartbeat renewal
lists. The agent queues one attempt renewal only on the successful L1 renewal
path and only when the returned directive is empty; failed, timed-out, stale,
`stop`, and `restart` responses never refresh the helper deadman. The pump uses
separate operation connections for image/watch streams so backpressure cannot
starve session authority, and it locally verifies the returned helper checksum
against the installed expectation before exposing the session.
