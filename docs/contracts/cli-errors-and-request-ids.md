# CLI errors and HTTP request correlation

Every `wefty` command uses the same process exit map, including `exec`, `submit`,
`rerun`, `drain`, `nodes`, `runs`, `logs`, `inspect`, `results`, and the local
and service command families. No command-specific whitelist can discard a
typed result.

| Exit | Meaning |
| --- | --- |
| 0 | Success |
| 1 | Other failure, including non-retryable internal or an unknown protocol code |
| 2 | CLI usage or `invalid_request` |
| 3 | Authentication or authorization refusal |
| 4 | Not found or an ended take-over session |
| 5 | State, revision, idempotency, capacity, or controller conflict |
| 6–9 | Custody import deferred, quarantined, failed, or superseded |
| 10 | Run failed (`exec`, `wait`) |
| 11 | Wait timeout (`wait`) |
| 12 | Cluster not ready (`status`) or doctor findings |
| 13 | Transport or service unavailable (`fabric_unreachable`, `unavailable`, or retryable server-unavailable envelope); for `exec`, a lost watch, an unconfirmed submission, a run infrastructure failure, or a missing command result |
| 14 | Accepted-mutation observation timeout (`wait_timeout` or `revocation_wait_timeout`) |

The protocol-code membership of the exit map remains authoritative. L1/L3 HTTP
5xx error envelopes with `code=internal` or `code=run_ledger_unavailable` and
`retryable=true` exit 13; their server code, details, retryability, and request ID
remain unchanged in JSON. Non-retryable `internal` and
`run_ledger_unavailable` remain exit 1. Retryability alone never changes an
authority refusal or an unknown protocol code's exit. Structured take-over
refusals map by server code for every HTTP status, including on take and release
as on view open.

`person_device_mismatch` is a non-retryable L1 person-route authorization
refusal (`403`, existing exit 3). It means a current administrator is bound to
another device, or has a legacy unbound roster row. Its message contains no
device IDs and directs the caller to the enrolled person device or an explicit
local control-plane rebind. `GET /v1/whoami` remains available from any device.

For a participant profile, a missing or expired local person sign-in is a typed
`person_identity_required` refusal (exit 3), with recovery advice for the saved
participant. Missing machine participant records or saved state use
`machine_identity_required` (exit 3). A saved backend that needs login or machine
authorization, or whose node ID or role identity differs from the profile, uses
the code for the selected participant: `person_identity_required` for a person,
`machine_identity_required` for a machine. Commands do not expose enrollment
URLs or retry another identity. Ordinary backend diagnostics remain available
through the caller's logger, under the existing redaction policy.
Recovery can restore the original state or retire the old device and enrol a
new identity through fleet setup; public fleet setup is deferred to #900.
These refusals use the same command report as `main` and add no exit number.

[Fleet doctor](fleet-doctor.md) prints its findings before returning the same
identity-refusal exit 3 or transport-uncertainty exit 13. Local checklist findings
use exit 12; JSON stderr never adds a second error document after this report.
Legacy plain doctor and status text remain unchanged. Fleet peer observations
that report unreachable are doctor findings (exit 12). Reading an observer
retains the same identity/transport exit precedence. Missing observer or ping
routes during a staged upgrade are NOT-RUN, never FAILED; see
[peer health](peer-health.md).

A saved participant that cannot be reached or confirmed within its start
budget returns typed `fabric_unreachable`, `retryable=true`, exit 13. This
includes failed status reads and running-state waits. Public messages retain
only the context error or a generic backend reason, while the original cause
remains available for error classification. Backend URLs and implementation
names stay behind the Fabric seam. This error does not claim that
the saved identity is invalid. The start budget is at most 30 seconds, bounded
by the command's context. Caller cancellation, including Ctrl-C, ends the check
at once and remains a local failure (exit 1, `internal`, `retryable=false`).
A bare deadline elsewhere keeps its existing classification.

For participant profiles, `wefty status` includes startup in its command
budget (`--timeout`, default 5 seconds) and bounds the participant check to the
existing 3-second probe budget, or the remaining command budget if shorter.
If that check fails, text and JSON status report `participant_check_failed`,
identity detail and local person sign-in state, with exit 12. L1 and L3 are
marked as not probed; no request uses the unverified participant. Cancellation
still exits 1. The verdict is written before backend cleanup; process shutdown
can take longer than the probe budget. Local missing-profile/state refusals before opening the Fabric
retain their typed exit 3. Legacy status behavior is unchanged.

An L1 identity lookup that fails operationally returns `503 unavailable`,
`retryable:true`, `details.reason="identity_unverifiable"`; the CLI preserves
that server envelope and exits 13. One exception: inside a `--wait` window a
retryable `unavailable` answer — including `identity_unverifiable` and the
`read_snapshot_*` reasons — is transient (#763); every wait or observation loop
keeps polling at its normal interval until the wait deadline, and exhausting
that deadline on retryable answers alone is the wait-timeout outcome (exit 14,
`wait_timeout`) with the last retryable answer recorded in the observation
detail. The log follows (`wefty logs --follow`, `services logs --follow`) have
no deadline: they retry such answers at their poll interval until the reader
stops them, which is an ordinary interrupted follow, never exit 13. One-shot
reads (no `--wait`) still exit 13, after quietly retrying (see below).
Non-retryable answers still
end a wait immediately and keep their own classification. Genuine Fabric
identity absence remains
`401 unauthorized` and exits 3. Node agents classify `unavailable` as
transient without treating it as lost attempt or node-session authority.
Ledger-only admission refusals remain `403 forbidden` and exit 3, adding
`details.reason="run_ledger_not_admitted"`; per-Computer and per-host
authority refusals carry no ledger-admission reason.

A one-shot read — a GET with no `--wait`, `--follow`, or wait window of its
own — quietly retries an answer of `code=unavailable`, `retryable=true`
(#773) before reporting anything: up to three retries at about 100, 200 and
400 ms with jitter, inside about a second. Each retry is a new request with
its own request ID, and if every try is refused the last answer's envelope
and exit 13 are reported exactly as a single try would be. Mutations are
never retried; transport failures without an envelope are not retried;
non-retryable answers and caller cancellation return at once, including
during the backoff. Wait and follow loops that already retry such answers at
their own interval (#763) skip this one-shot retry, so their cadence is
unchanged. Most `--all` page walks are one-shot reads and may make up to three
extra requests per page. Run log `--tail` walks and run and service log
`--all` walks instead retry at `--poll-interval`, with at most 30
consecutive retries before returning the last answer.

## Run log paging

`wefty logs RUN_ID` reads one page by default. `--limit` caps events per
page (default 200); the read cutoff may return fewer. `--cursor CURSOR`
continues after a previous page's `next_cursor`, including with `--follow`.
A nonempty plain page probes the next cursor with limit 1. If more events
exist, stderr says `wefty: more log events exist; resume with --cursor CURSOR or use --all`.
If the probe is empty, the CLI reads the run status. A non-terminal run says
`wefty: run is still STATUS; more log events may follow; use --follow --cursor CURSOR`.
A terminal run has no paging notice. If either read fails, stderr says
`wefty: log read may be partial; resume with --cursor CURSOR or use --all for every retained event`.
These extra reads cannot fail the command. Stdout stays as log bytes.
An empty initial page has no paging notice.
One-page `--json` keeps the existing page object, including `next_cursor`.

`--all` reads every retained event once, starting at `--cursor` if supplied,
until an empty page. Like `services logs --all`, it uses a 10,000-page cap;
reaching the cap fails (exit 1) and names `--all --cursor CURSOR` on stderr.
The cap does not add a paging notice that suggests `use --all`.
`--all --json` writes one event per line, matching `--follow --json` and
`services logs --all --json`. Truncation notices stay on stderr. `--all` with
`--follow` is a usage error (exit 2). `--poll-interval` controls follow and
retry polling.

`--tail N` requires N > 0 and cannot be combined with `--all` (usage errors,
exit 2). It walks from the start or `--cursor` using the same `--limit`,
retry bound, and page cap as `--all`, but buffers only the last N events and
prints them once after the walk. At the cap it fails with exit 1 and names
`--tail N --cursor CURSOR` to resume (with `--follow` when it was given); a
resumed walk sees only the events after that cursor, so it prints fewer than
N when fewer remain. `--tail N --follow` prints that suffix,
then continues with the existing follow loop from the walk's final cursor.
`--tail N --json` writes the same event lines as `--all --json`; retention
notices stay on stderr. No new exit code is introduced.

## JSON flags and errors

`--json` and `--json=true` are global wherever they appear in the argument list,
including after the command or its operands, except when consumed as another
flag's value or after the `--` argument terminator. `--json=false` disables JSON;
the last occurrence wins. An invalid boolean is a usage error and emits JSON.
The value-flag vocabulary is checked against CLI and authoring registrations
and the real Lima sizing FlagSet (`--vm-memory`, `--vm-cpus`, `--vm-disk`).
Boolean flags cannot consume the following `--json`; the overloaded `--wait`
is boolean on grant/revoke and takes a duration on mutation waits.
Errors go to stderr as the shared envelope, for example
`{"error":{"code":"invalid_request","message":"a command is required","retryable":false}}`,
with optional `details` and `request_id`. This includes global and command flag
errors and failures raised inside the CLI. Parser usage text is suppressed in JSON
mode. Existing take-over refusals may also include their `receipt`. Unknown
CLI-local failures use `code=internal`, `retryable=false`; they have no server
request ID. All CLI HTTP adapters — L1/L3, Computer take-over, registry, and the node-local
control socket — use one
failed-exchange classification, with the Unix-socket request exception below.
Connection failures on TCP adapters (including dial timeouts,
request deadlines,
and a deadline or timeout waiting for the display banner after a take-over
WebSocket upgrade) and non-envelope HTTP 5xx responses use `code=unavailable`,
`retryable=true`,
exit 13. Response-body I/O failures, including a stalled or truncated body,
follow the same rule on every adapter, including registry token reads and
take-over take, release, and refused view-open handshakes. A completely read
body that fails to decode is a local failure (exit 1). A take-over WebSocket
session that ends normally is also a local failure. Non-envelope 5xx include
responses from the registry and the Computer take-over front door
on `services takeover view`, `take`, and `release`, such as a plain-text 503
when the display is not ready or Fabric identity cannot be verified. The
unavailable message retains each adapter's plain-text reason after bounding
the response to 4096 bytes, dropping any split UTF-8 rune and non-printable
characters except tab. Structured take-over refusals, including HTTP 5xx on view
open, take, and release, retain the server's code, message, retryability, details,
request ID, and any receipt; view open also adds the HTTP status to the message.
The CLI exit map still applies: `tenure_unavailable` exits 1 even when retryable,
and is never rewritten to `internal` or transport `unavailable`. A plain-text or
structured 401 authentication refusal exits 3. An unavailable result advises the operator to check reachability and
retry with backoff within its authority. A bare `context.DeadlineExceeded` is
not evidence of unavailability: only a typed transport/service availability
failure or the named server-envelope cases map to exit 13. Caller cancellation
(including Ctrl-C during L1/L3 response-body reads, take-over or registry
requests) wins at dial, headers, and body-read time and remains a local failure
(exit 1), not `unavailable`. This includes cancellation while reading a refused
5xx take-over view-open handshake body. Structured server refusals still use
their adapter-specific decoding and exit rules.

At the request phase of a node-local Unix-socket exchange, only dial errors
`ENOENT` and `ECONNREFUSED` mean the node agent is not running:
`unavailable`, `retryable=true`, exit 13.
The message tells the operator to check that the node agent is running.
Every other Unix-socket request failure remains local (exit 1), including
`EINVAL`, `ENOTDIR`, `EACCES`, `EPERM`, dial or request timeouts, and EOF or
connection reset before a response.
An agent that refuses the caller's user ID closes the connection; this reports as a local failure (exit 1).
Invalid socket configuration remains a local or usage failure. Stalled or
truncated response bodies and bare non-envelope 5xx responses exit 13;
a completely read malformed response remains exit 1. Caller cancellation
remains exit 1 at every phase. Structured socket refusals retain the server
code, message, retryability, details, and request ID and use the existing exit
map; retryable `internal` on this adapter still exits 1. These failures do not
cause an automatic socket retry.

A Computer storage, resize, or removal `--wait` that expires after acceptance
exits 14 and emits a CLI-local `error.code=wait_timeout`, `retryable=false` on
stderr. `error.details.mutation_applied` matches the mutation result on stdout
(true for a newly applied change, false for a replay or no-op).
The field is always present; `null` means the applied evidence is unknown.
If no Computer projection was observed, no result document is emitted, but the
error still carries the known receipt value. Otherwise the mutation result and
failed observation remain on stdout. Service start, stop,
and removal observation timeouts also exit 14 with the same error code. For
services, `mutation_applied` is always `null`: L1 provides no applied receipt
for desired-state or removal mutations. A prior read cannot prove whether a
mutation applied under a concurrent change, even for a repeated request.
Read or wait for completion; do not assume the mutation failed or repeat it to
recover observation.
The wait deadline bounds every observation request, including response-body
reads and any follow-up Storage provenance read. An L1 that accepts a
connection but never answers cannot extend the wait. Once the wait times out
or the caller cancels, no follow-up provenance request is started.
A missing `storage_provenance` after a timeout means "not read", not "untainted";
`services backup list COMPUTER` reads it.
An already-observed typed failure keeps its own exit code if a follow-up
provenance read fails or times out; that miss is recorded in `observation.error`.
For every accepted-mutation wait, including follow-up Storage provenance,
only a deadline-caused failure or an exhausted typed retryable `unavailable`
answer becomes an observation timeout. The timeout records the last retryable
answer. A permanent HTTP refusal or typed operation error arriving at the
deadline keeps its classification, even in provenance with an earlier transient
503. An already-known typed operation verdict still survives a provenance miss.

Caller interruption (Ctrl-C or SIGTERM) wins when handling a failed
read or polling delay in every accepted-mutation wait, the service log follow,
and the take-over admission retry. This includes a fully decoded typed answer
handled after the caller stops. The interruption is a local failure (exit 1,
`retryable=false`), rather than the last unavailable or stale-policy answer,
subject to provenance's already-known typed verdict rule above.
The service log follow's own `--follow-for` window ending remains a clean stop
(exit 0). The take-over admission retry's own window ending retains the last
`stale_policy_revision` refusal (exit 5); caller interruption during its retry
delay exits 1.

Grant revocation
(`services revoke --wait`) also exits 14 on an accepted-mutation
observation timeout. It retains `revocation_wait_timeout`, its existing
retryability, and receipt-derived `details.mutation_applied`; it is not
transport unavailability. A retryable `unavailable` read during that
observation window is retried at the normal poll interval, and running out of
the window on retryable answers alone is the same `revocation_wait_timeout`
outcome (exit 14) with the last answer recorded in
`details.observation_failure`, including when the deadline interrupts the next
read. One elapsed-time deadline bounds polling delays and every revocation
read, including response-body reads; slow answers cannot extend
`--wait-timeout`. An answer that a deadline-bounded read returns is used even
if the deadline passes while it is processed: a completed revocation succeeds,
and a pending one becomes `details.last_observed_revocation` in the timeout.
Caller cancellation during a delay or read is a local failure (exit 1,
`retryable=false`): the mutation receipt and last observed revocation go to
stdout in both JSON and human output, and stderr carries `error.code=internal`
with a message saying the revocation was applied (or, for a replay, not
reapplied) and that observation failed with `context canceled`. A non-retryable read error
reports `revocation_observation_failed` at once. `services grant` does not accept `--wait`.

Typed outcomes (custody exits 6–9,
failed run 10, wait timeout 11, not-ready status 12) already write their result
document on stdout and emit no additional JSON error on stderr. A wait timeout
is not a failed Run: its document carries `timed_out=true` and the last observed
status; waiting again is allowed. A failed Run's JSON record includes its
recorded `failure_reason` or, when absent, the reason derived from a bounded
execution-evidence lookup. Unavailable evidence leaves that field absent and
does not change the Run's verdict. A not-ready verdict names readiness reasons,
not an internal error. Missing `--l1` or a required `--l3` is usage (exit 2).

L1 and L3 generate an opaque request ID for **each** HTTP request, including
requests refused by authentication. A caller-supplied correlation header is
never adopted as the server ID and grants no authority. Every response has
`X-Request-Id`; every error envelope has `error.request_id`. The request
completion log is written only for HTTP errors (status ≥ 400) and panics;
routine successes, including agent hot routes, produce no completion line.
It records method, path (without query), status, and request ID,
without request bodies, authorization values, query parameters, or panic values.
A panic is logged as `status=500 panic=true` even if a success header was already
sent, then rethrown for the HTTP server to handle; the logged failure does not
claim that an already-sent wire status was changed. L1 and L3 loggers are
injectable; L3 accepts `ServerConfig.Logf` (nil uses the default logger, a
no-op function disables logging). Mux plain-text 404/405 responses are normalized into error envelopes
with `details.reason="no_route"`, distinguishing version skew from a typed
resource refusal. L1's scrubbed-internal-cause log also includes that request ID.

For an L3 error relayed from L1, `error.request_id` and `X-Request-Id` preserve
L1's error ID. L3 adds its independently generated ID in `X-L3-Request-Id`
and `error.details.l3_request_id`. L3's completion log records its own
`request_id` plus a quoted `upstream_request_id`, allowing correlation to both logs.
Other upstream details and retryability are preserved under the existing
error-scrubbing rules. Internal errors still scrub private messages and details;
L3's correlation detail is added after scrubbing and never discloses a cause.
This metadata is additive and does not change authorization, request replay,
or the L1 client boundary (ADR-0006).

## Status readiness and busy nodes

`wefty status` stays `ready` (exit 0) when an alive, claiming node advertises an
executable one-shot kind but all such nodes have their one-shot slots in use.
It adds `busy: all one-shot slots in use; N runs waiting; a new run waits in line`.
Submit with `--detach` and wait. Busy does not require a repair.

`N` comes from the existing queued-run read within the status probe budget.
It is the visible queued count, not a separate queue census. If that read has
another page or cannot provide the count, omit `N runs waiting; `.
JSON adds `busy` and, when known, `waiting_runs`. Free slots and per-kind
limitations still report immediate capacity. `accepts_oneshot` keeps its meaning
of immediate eligibility, including a free slot.

An active `wefty upgrade` drain is also busy with exit 0 when its disabled
admission is the only obstacle on an otherwise eligible alive node. Text and
JSON `busy` say `busy, queued: upgrading; new runs wait and start when it ends`.
Node JSON adds `upgrading`; immediate `accepts_oneshot` stays false and capacity
stays zero for that node. Submit and wait as for occupied slots. An unresolved
local upgrade receipt after its installation lock is released adds
`upgrade_recovery`, naming `wefty upgrade`. A matching disabled intent says
`claims disabled by an interrupted upgrade` and keeps exit 12 when it leaves
no eligible node; node JSON adds `upgrade_interrupted`.

A local receipt plus the held installation lock also covers upgrade restarts:
unreachable L1/L3 returns exit 0 with `busy: upgrading; retry in a few seconds`,
and the old node boot in `draining` gets the busy/queued answer. Status observes
the lock without acquiring it. With no readable receipt, an upgrade reason
carrying `drain_deadline` stays busy until that deadline plus 30 minutes.
Legacy reasons without a deadline instead use 35 minutes from
`intent_updated_at`. An expired or invalid deadline, or an old or undated
legacy reason, names an interrupted upgrade and its recovery command.

Exit 12 remains for unreachable control services, no registered or alive node,
operator claims disabled on every alive node, no executable kind on an alive claiming
node, or no one-shot slots configured on any such node. Zero configured slots
cannot move the line and keep the `no_free_slot` reason. Other draining nodes keep
their existing not-ready advice.

For the installed local node, the `this machine` row adds `load L; limit M`.
With one-shot occupancy and load at or above a positive limit, an alive,
claiming, executable local node adds `holding new work: machine busy`.
An unreadable load says `load unavailable` and does not claim a hold. JSON
adds `local_load` with `average` when known, `limit`, and `holding`.
Status reads this machine directly with the agent's platform reader. It uses
the local profile and environment limit; keep installed settings there so both
commands agree. Other nodes' load is not reported in phase 1.

## Client-service and participant ownership errors

`participant_in_use` is an identity-ownership refusal (exit 3,
`retryable=false`). It names the participant and advises stopping its current
owner. It does not disclose the participant folder path. A second embedded
backend or client-service process receives this error before it starts that
participant. Advisory OS ownership releases after close or process death;
removing a lock file is never a recovery step.

A participant-profile CLI with no usable client service returns
`fabric_unreachable` (exit 13, `retryable=false`), including on `status`. Its
message asks the owner to start or repair `wefty client-service` with that
profile before retrying. This owner action is not an automatic backoff loop. It never falls back to direct saved-state ownership. A running
service preserves identity refusal and backend-unreachable types; `status`
keeps its existing failed-check verdict for those backend failures. Unix peer
refusal closes the socket without running a backend request. Caller cancellation
remains exit 1. Legacy profiles never contact this socket. See
[client service](client-service.md) for protocol limits and the accepted
same-user caller boundary.

The local service's capacity refusal is distinct: `client_service_busy`
(exit 13, `retryable=true`), asking callers to wait for capacity instead of
restarting the service. A socket publication conflict uses `conflict` (exit 5,
`retryable=false`) and says the service is already running. A non-Fabric
outbound destination, failed client-side server UID check, or intentional EPERM
socket fence uses non-retryable `forbidden` (exit 3). Fence advice asks the owner
and says not to look for another way in. The participant HTTP adapter preserves
that advice instead of converting it into legacy connection-allowance advice.

When participant ownership refusal joins an enrolment failure, classification
still selects `participant_in_use` and exit 3, but the JSON message retains both
the original enrolment cause and the ownership refusal. Response extensions on
the local protocol are tolerated by older clients; server requests stay strict.
