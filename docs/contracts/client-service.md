# Per-user Fabric client service

A participant profile uses one long-lived `wefty client-service` process to own
its `client` and optional `person` participants. The CLI and other local clients
use `fabric/local` to dial through that process. L1 and L3 public APIs do not
change. Their Fabric `WhoIs` sees the selected service participant's real
identity. The socket peer's user ID is never substituted for that identity.

`client-service` is an internal lifecycle subcommand. Start it with
`wefty --profile PROFILE client-service`. It reads the existing participant
records and never enrols, creates, or repairs a participant. Its client backend
must match its saved identity before the service publishes its socket. Missing
state and conflicting bootstrap overrides are typed startup refusals; see
[connection profile](connection-profile.md#saved-identity-service-startup). The person remains
optional and laptop-only. Missing or expired person sign-in does not prevent
client operations. This includes saved person state disappearing between local
validation and backend construction: a typed person identity refusal keeps the
client service running, and person requests retain their sign-in refusal.
Each check and dial verifies the saved participant before
an application connection is used. The service holds backend ownership even
when a check fails, so a CLI process cannot replace or reopen the backend.
A changed participant node ID requires restarting the service with the new
profile; a stale service refuses the selection.

The existing per-command selector chooses the client or person. A participant
CLI first checks that selected identity through the service. A missing service
produces `fabric_unreachable` (exit 13, `retryable=false`), with advice to ask
the owner to start or repair `wefty client-service` with that profile before
retrying. Waiting and backing off cannot start a missing service. This includes
`status`. The CLI never
falls back to opening saved identity state. A running service's failed identity
check retains `machine_identity_required` or `person_identity_required`
(exit 3); an unconfirmed backend or failed outbound dial retains
`fabric_unreachable` (exit 13). `status` keeps its existing exit-12
`participant_check_failed` verdict for a running service's failed backend
check, within its command and probe budgets. Local missing-state and
missing-person refusals remain exit 3. Caller cancellation remains exit 1.

Legacy profiles have no participant records and never contact the client
service. Plain legacy profiles (every current installation) keep their previous
transport, identity shape, and behavior. A legacy `tsnet` process with an explicit
`--state-dir` now takes the same participant-folder lock; concurrent processes
using that folder are refused with `participant_in_use` (exit 3). The production
adapter's outbound destination restriction below also applies to legacy calls.
Local authoring, setup, upgrade, and node commands keep their existing dispatch.
Rendering and managing the service as a LaunchAgent or systemd user unit belongs
to #900. That work must stop the client service before removal or purge, add the
client socket folder and its publication lock to the removal plan, and decide
the Linux runtime-folder policy (including `$XDG_RUNTIME_DIR`). This slice keeps
the current default location and starts the subcommand directly in tests.

## Socket and caller boundary

The default socket is `~/.config/wefty/client/service.sock`.
`WEFTY_CLIENT_SOCKET` selects an explicit socket for local clients and the
service; `client-service --socket PATH` overrides the service endpoint. Use the
same endpoint for both. Paths must be absolute, clean, and at most 100 bytes,
which fits macOS's short Unix-socket path limit. Tests use short temporary
folders, never the live installation.

The service creates its socket folder with mode `0700` and requires an existing
folder to belong to the current user, be unlinked, and already have that mode.
It refuses an unsuitable folder without changing its permissions. The socket
has mode `0600`. A separate advisory lock at `service.sock.lock` prevents two
services from
replacing each other's endpoint. It is not a participant lock; a competing
publisher gets `conflict` (exit 5), saying the client service is already running.
A confirmed stale socket is replaced after a
crash; a non-socket, linked endpoint, or uncertain endpoint is refused.
Every accepted connection must have the same effective user ID as the service,
checked with the operating system's Unix peer credentials on macOS and Linux.
The client also verifies the server's peer UID before sending even its request
frame, including for explicitly configured endpoints in shared folders. A wrong
UID or unavailable credential check fails closed with `forbidden` (exit 3).

**The socket does not distinguish programs running as the owner.** Any program
that can reach it as that user can select a configured participant, including
the person. This is an accepted boundary, not proof that a caller is human or
that a host command has the owner's approval. A fenced caller must retain its
sandbox's network and socket restrictions. The service does not relax those
restrictions or implement the later fenced-caller execution policy from #973.
A socket dial refused with EPERM produces a non-retryable `forbidden` response
(exit 3): the sandbox intentionally blocks the service; ask the owner and do not
look for another way in. It does not advise starting the service or changing the
sandbox.
Manual checks recorded on #899 show that both the Codex sandbox with network
off and the Claude Code sandbox with default settings refuse a Unix socket
connect. Attended Fabric acceptance is not proved by these tests.

## Bounded outbound protocol

One socket connection carries one request and one response, then either closes
or becomes a bidirectional byte stream. Requests and responses are JSON framed
by a four-byte unsigned big-endian length, with a maximum payload of 4096
bytes. Requests reject unknown fields, trailing values, unsupported versions
and operations, and oversized frames. Outside overflow, every such refusal is
answered with `invalid_request`; only a caller that closed or sent nothing
before the read deadline gets no reply. An overflow worker closes an
undecodable request silently, so that caller sees a busy service once. The CLI reports `invalid_request` as non-retryable
`fabric_unreachable` with owner-action advice, because its own requests are
valid: the running service speaks another version and must be restarted or
repaired. Clients ignore unknown response fields,
while retaining the framing bound and rejecting trailing values. Version 1 requests contain:

- `version: 1`, `operation: "check" | "dial"`;
- `participant: "client" | "person"` and the profile's opaque `node_id`;
- `timeout_ms`, positive and at most 30000;
- for `dial` only, `network: "tcp"` and `address`.

The address is a nonempty Fabric address of at most 512 bytes with no spaces or
control characters. For every production participant dial, the adapter resolves
hostnames only from authenticated remote peer state. It accepts a peer's full
network name or its short name within the authenticated network's name suffix.
Unknown names and names matching more than one node are refused before outbound
planning or any DNS resolver call. Planning receives only a literal IP:port.
Both that target IP and the plan's resolved IP must be addresses assigned to a
real remote peer in authenticated state, and the plan must select the Fabric
transport. The adapter refuses its own node by name, opaque ID, or IP address,
and refuses every node's built-in peer service endpoints listed in authenticated
state. Built-in network DNS and service addresses are explicitly refused, even
if listed in peer state. A route alone is not sufficient; public
internet, loopback, link-local, subnet-route and unassigned network addresses
cannot use the service as a host TCP proxy.
The dial uses only the embedded peer transport after those checks: a later route
change cannot reclassify the target into an ordinary host dial. Logical names,
opaque node IDs, and peer endpoints for Computer take-over remain supported;
there is no L1/L3-only allowlist. A refused destination retains its typed policy
failure through the service and becomes non-retryable `forbidden` (exit 3).
The plain adapter remains a localhost-only development transport used in tests.
The private network's access policy is the boundary for any other service a
remote peer exposes, including another device's resolver or an exit node. The
client service does not filter application requests to those peer services.
It exposes no listen operation, shell execution, credentials, or writable
backend API of its own.

At most 128 streams or unfinished requests are served concurrently. Initial
request reads have a five-second deadline, so a caller that sends no request
cannot hold a slot forever. When capacity is full, up to four separate workers
read overflow requests and return `client_service_busy` (exit 13,
`retryable=true`), with advice to wait for capacity. These exchanges have the
same bounded read and write deadlines and never block the accept loop. If all
four overflow workers are occupied too, the service closes an extra connection
immediately without a reply. After the client has authenticated the service's
peer user ID, a close with no reply bytes during request write or response read
is treated as retryable `client_service_busy` (exit 13), with wait-and-retry
advice. Failure to connect to a missing service retains non-retryable
`fabric_unreachable` and its owner-action advice. A partial or malformed reply
retains its protocol failure. Freeing a normal slot permits new admissions even
while overflow workers are waiting. A busy reply does not tell the operator to
restart a healthy service.
Temporary accept failures, including process/system descriptor exhaustion, use
cancellable backoff from 5 milliseconds up to one second and do not stop service.

Each response write gets a fresh five-second budget immediately before writing;
the preceding identity check or dial does not spend that budget. Identity checks
and outbound
establishment share the request's budget, capped at 30 seconds. Caller
closure cancels a pending check or dial. Established streams have no total
lifetime limit, support incremental reads and writes, and end when either side
closes or the service stops. The tunnel exposes `net.Conn` full-close semantics,
not a half-close API. Request cancellation closes the tunnel and its outbound
connection. Cancelling a dial's establishment context after it returns does
not close the returned stream; the consumer owns its lifetime.

An empty response object means success. A failure response has `code` and an
optional sanitized `message`. Identity refusals use
`machine_identity_required` or `person_identity_required`; unconfirmed identity
or destination reachability uses `fabric_unreachable`. A full service uses
`client_service_busy`; a non-Fabric destination uses `fabric_destination_refused`
on the socket and maps to `forbidden` in CLI output. No backend diagnostic,
credential, login URL, or participant folder path is returned.

## Participant-folder ownership

Every embedded backend with an explicit participant state folder takes an
exclusive, nonblocking advisory OS lock before backend construction/start,
and retains it through backend close. This covers direct CLI bootstrap callers,
L1, L3, node agents, the client service, and setup enrolment/logout. The plain
Fabric's participant mode takes the same lock for secretless acceptance tests.
Ownership locks cover both the folder's directory inode and `.participant.lock`
inside it. The file is regular and is opened without following a symbolic link.
It stays on disk, and its presence is never proof of ownership or valid saved
state. The directory lock prevents replacement of an unlinked lock file while
folder deletion is still in progress. Before deletion, the file lock also detects
an existing owner that uses the earlier file-lock implementation.

A second owner fails immediately with typed `participant_in_use` (exit 3),
naming the participant's logical name, not its state path. Closing the backend
releases ownership. The operating system releases it after a crash, including
SIGKILL; there is no PID file or stale-lock deletion. A folder containing only
the lock file fails saved-state checks, including setup reruns and local person
sign-in checks. Purge and rollback acquire ownership before deleting a
participant folder and retain it until deletion finishes. Rollback reacquires
ownership after logout closes its backend. If another owner started between
logout and deletion, deletion is refused. A held folder returns
`participant_in_use` and is preserved.
If that refusal joins an enrolment failure, the JSON message keeps the original
enrolment cause as well as the ownership refusal.

For a joining Mac, the installation plan supplies `--ready-file PATH`.
The client service durably publishes PID, startup time, saved client node ID
and private socket address after saved-client verification and socket creation,
before accepting requests. A publication failure closes and removes the socket.
This is a local startup point, not continuing health evidence; setup also checks
the live saved-client protocol. Without the flag, existing startup is unchanged.
